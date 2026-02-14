import os
import sys
import time
import json
import pandas as pd
import re
from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
import logging

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from bs4 import BeautifulSoup

# ================== تنظیم encoding ==================
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# ================== تنظیمات لاگینگ ==================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ifb_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

IFB_MAIN_URL = "https://www.ifb.ir/Finstars/AllCrowdFundingProject.aspx"


@dataclass
class IFBProject:
    row_number: str
    project_name: str
    company_name: str
    national_id: str
    platform_url: str
    status: str
    fund_collection_start_date: str
    project_end_date: str
    description: str
    documents_url: str
    scraped_date: str
    # فیلدهای اضافی (از سکو)
    target_amount: Optional[str] = None
    collected_amount: Optional[str] = None
    progress_percentage: Optional[str] = None
    expected_return: Optional[str] = None
    project_duration: Optional[str] = None
    capital_guarantee: Optional[str] = None
    project_type: Optional[str] = None
    project_symbol: Optional[str] = None
    investor_count: Optional[str] = None
    profit_payment_frequency: Optional[str] = None
    start_date_on_platform: Optional[str] = None
    platform_name: Optional[str] = None
    financial_institution: Optional[str] = None
    project_id_on_platform: Optional[str] = None
    thumbnail_url: Optional[str] = None
    applicant_name: Optional[str] = None

    def to_dict(self):
        return asdict(self)


class IFBScraper:
    def __init__(self, headless: bool = False):
        self.config = {
            'headless': headless,
            'timeout': 30,
            'implicit_wait': 10,
            'delay': 3,
        }
        self.driver = self._init_driver()
        self.wait = WebDriverWait(self.driver, self.config['timeout'])

    def _init_driver(self) -> webdriver.Chrome:
        script_dir = os.path.dirname(__file__)
        possible_paths = [
            os.path.join(script_dir, 'chromedriver.exe'),
            os.path.join(script_dir, 'chromedriver-win64', 'chromedriver.exe'),
            os.path.join(script_dir, 'chromedriver'),
            'chromedriver',
            'chromedriver.exe'
        ]
        chromedriver_path = None
        for path in possible_paths:
            if os.path.exists(path):
                chromedriver_path = path
                break
        if not chromedriver_path:
            raise FileNotFoundError("chromedriver.exe یافت نشد!")

        service = Service(chromedriver_path)
        options = Options()
        if self.config['headless']:
            options.add_argument("--headless=new")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--disable-blink-features=AutomationControlled')

        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(self.config['implicit_wait'])
        logger.info(f"ChromeDriver راه‌اندازی شد: {chromedriver_path}")
        return driver

    # ========== متدهای کمکی برای پیجینیشن و استخراج از فرابورس ==========

    def _navigate_to_page(self, page_number: int) -> bool:
        """رفتن به صفحه مشخص با استفاده از __doPostBack"""
        try:
            event_target = 'ctl00$ContentPlaceHolder1$grdCrowdFundingData'
            event_argument = f'Page${page_number}'
            script = f"__doPostBack('{event_target}', '{event_argument}');"
            self.driver.execute_script(script)
            time.sleep(self.config['delay'] * 2)
            WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_grdCrowdFundingData"))
            )
            return True
        except Exception as e:
            logger.error(f"خطا در ناوبری به صفحه {page_number}: {e}")
            return False

    def _extract_description_from_modal(self, desc_id: str) -> str:
        """استخراج متن توضیحات از modal (نسخه نهایی بدون jQuery)"""
        try:
            self.driver.execute_script(f"showDesc('{desc_id}');")
            time.sleep(2)

            # روش ۱: JavaScript
            desc_script = """
                var el = document.getElementById('Message');
                return el ? el.innerText || el.textContent : '';
            """
            description = self.driver.execute_script(desc_script)
            if description:
                # بستن modal با JS خالص
                self.driver.execute_script("""
                    var modal = document.getElementById('FileForm');
                    if (modal) {
                        modal.style.display = 'none';
                        modal.classList.remove('in');
                    }
                    var backdrops = document.getElementsByClassName('modal-backdrop');
                    for(var i=0; i<backdrops.length; i++) backdrops[i].remove();
                    document.body.classList.remove('modal-open');
                """)
                return description.strip()

            # روش ۲: BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            msg = soup.find('small', {'id': 'Message'}) or soup.find('div', {'id': 'Message'})
            if msg:
                description = msg.get_text(strip=True)
                # بستن modal
                self.driver.execute_script("""
                    var modal = document.getElementById('FileForm');
                    if (modal) {
                        modal.style.display = 'none';
                        modal.classList.remove('in');
                    }
                    var backdrops = document.getElementsByClassName('modal-backdrop');
                    for(var i=0; i<backdrops.length; i++) backdrops[i].remove();
                    document.body.classList.remove('modal-open');
                """)
                return description

            return "توضیحات در دسترس نیست"
        except Exception as e:
            logger.error(f"خطا در استخراج توضیحات ID {desc_id}: {e}")
            return "خطا در دریافت توضیحات"

    def _extract_current_page_projects(self) -> List[IFBProject]:
        """استخراج پروژه‌های صفحه جاری از جدول فرابورس"""
        projects = []
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup.find('table', {'id': 'ContentPlaceHolder1_grdCrowdFundingData'})
        if not table:
            logger.error("جدول در صفحه جاری یافت نشد!")
            return projects

        rows = table.find_all('tr')
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) >= 10:
                try:
                    row_number = cells[0].text.strip()
                    project_name = cells[1].text.strip()
                    company_name = cells[2].text.strip()
                    national_id = cells[3].text.strip()

                    platform_link = cells[4].find('a')
                    platform_url = platform_link['href'] if platform_link else ""

                    status = cells[5].text.strip()
                    start_date = cells[6].text.strip()
                    end_date = cells[7].text.strip()

                    # استخراج توضیحات
                    description = "توضیحات در دسترس نیست"
                    details_link = cells[8].find('a')
                    if details_link and 'onclick' in details_link.attrs:
                        onclick = details_link['onclick']
                        match = re.search(r"showDesc\('(\d+)'\)", onclick)
                        if match:
                            desc_id = match.group(1)
                            description = self._extract_description_from_modal(desc_id)

                    # لینک مدارک
                    documents_url = ""
                    documents_cell = cells[9]
                    documents_link = documents_cell.find('i', {'class': 'icon-folder'})
                    if documents_link and 'onclick' in documents_link.attrs:
                        onclick = documents_link['onclick']
                        match = re.search(r"GoToDocuments\('(\d+)'\)", onclick)
                        if match:
                            doc_id = match.group(1)
                            documents_url = f"{IFB_MAIN_URL}?doc_id={doc_id}"

                    project = IFBProject(
                        row_number=row_number,
                        project_name=project_name,
                        company_name=company_name,
                        national_id=national_id,
                        platform_url=platform_url,
                        status=status,
                        fund_collection_start_date=start_date,
                        project_end_date=end_date,
                        description=description,
                        documents_url=documents_url,
                        scraped_date=datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                    )
                    projects.append(project)
                    logger.info(f"ردیف {row_number}: {project_name} - تاریخ شروع {start_date}")
                except Exception as e:
                    logger.error(f"خطا در پردازش یک ردیف: {e}")
        return projects

    def scrape_all_pages(self) -> List[IFBProject]:
        """پیمایش صفحات فرابورس تا مواجهه با تاریخ غیر ۱۴۰۴"""
        all_projects = []
        page_num = 1
        stop_pagination = False

        logger.info("شروع استخراج چند صفحه‌ای از فرابورس")
        self.driver.get(IFB_MAIN_URL)
        time.sleep(self.config['delay'])

        while not stop_pagination:
            logger.info(f"📄 پردازش صفحه {page_num} ...")

            if page_num > 1:
                if not self._navigate_to_page(page_num):
                    logger.info("امکان رفتن به صفحه بعد وجود ندارد.")
                    break

            page_projects = self._extract_current_page_projects()
            if not page_projects:
                logger.info(f"صفحه {page_num} پروژه‌ای ندارد.")
                break

            page_has_non_1404 = False
            for proj in page_projects:
                start = proj.fund_collection_start_date
                year_match = re.search(r'(\d{4})', start)
                if year_match:
                    year = year_match.group(1)
                    if year != "1404":
                        logger.info(f"❗ پروژه {proj.project_name} دارای تاریخ {start} (غیر ۱۴۰۴)")
                        page_has_non_1404 = True
                    else:
                        all_projects.append(proj)
                else:
                    logger.warning(f"تاریخ نامعتبر: {start}")

            logger.info(f"✅ صفحه {page_num}: {len(page_projects)} پروژه، {len(all_projects)} پروژه ۱۴۰۴ (مجموع)")

            if page_has_non_1404:
                logger.info(f"🛑 توقف پیجینیشن در صفحه {page_num}")
                break

            page_num += 1

        logger.info(f"🎯 پایان: {len(all_projects)} پروژه ۱۴۰۴")
        return all_projects

    # ========== متدهای استخراج جزئیات از سکوها ==========

    def enrich_projects_with_platform_details(self, projects: List[IFBProject]) -> List[Dict]:
        enriched = []
        total = len(projects)
        for idx, project in enumerate(projects, 1):
            logger.info(f"\n[{idx}/{total}] پردازش پروژه: {project.project_name}")
            combined = project.to_dict()
            try:
                details = self._scrape_single_platform(project)
                combined.update(details)
                logger.info(f"   ✅ {len(details)} فیلد جدید استخراج شد.")
            except Exception as e:
                logger.error(f"   ❌ خطا: {e}")
            enriched.append(combined)
            time.sleep(self.config['delay'])
        return enriched

    def _scrape_single_platform(self, project: IFBProject) -> Dict:
        url = project.platform_url
        if not url:
            return {}
        domain = url.lower()
        if 'hamafarin.ir' in domain:
            return self._scrape_hamafarin(project)
        elif 'fundocrowd.ir' in domain:
            return self._scrape_fundocrowd(project)
        elif 'karencrowd.com' in domain:
            return self._scrape_karencrowd(project)
        else:
            return self._scrape_generic(project)

    # ---------- هم‌آفرین ----------
    def _scrape_hamafarin(self, project: IFBProject) -> Dict:
        details = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])
            if "businessplans" not in self.driver.current_url:
                try:
                    view_all = self.driver.find_element(By.CSS_SELECTOR, "a[href='/businessplans']")
                    view_all.click()
                    time.sleep(self.config['delay'])
                except:
                    self.driver.get("https://hamafarin.ir/businessplans")
                    time.sleep(self.config['delay'])

            self._scroll_page()
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_=lambda c: c and 'w-full flex flex-col gap-y-4 group' in c)

            target = project.project_name.strip()
            for card in cards:
                title_elem = card.find('a', class_=lambda c: c and 'text-[#2E2300]' in c)
                if not title_elem:
                    continue
                card_title = title_elem.text.strip()
                if target in card_title or card_title in target:
                    details = self._extract_hamafarin_card(card)
                    break
        except Exception as e:
            logger.error(f"خطا در هم‌آفرین: {e}")
        return details

    def _extract_hamafarin_card(self, card) -> Dict:
        d = {}
        title = card.find('a', class_=lambda c: c and 'text-[#2E2300]' in c)
        if title:
            d['title_on_platform'] = title.text.strip()
        link = card.find('a', href=re.compile(r'/businessplans/\d+'))
        if link and 'href' in link.attrs:
            match = re.search(r'/businessplans/(\d+)', link['href'])
            if match:
                d['project_id_on_platform'] = match.group(1)
        img = card.find('img')
        if img and img.get('src'):
            d['thumbnail_url'] = img['src']
        fin = card.find('p', string=re.compile('نهاد مالی:'))
        if fin:
            d['financial_institution'] = fin.text.strip()
        exec_p = card.find('p', class_='text-black17 font-YekanBakh text-md')
        if exec_p:
            d['applicant_name'] = exec_p.text.strip()
        bottom = card.find('div', class_=lambda c: c and 'bg-white' in c and '!pb-12' in c)
        if bottom:
            status_p = bottom.find('p', class_=lambda c: c and ('text-green67' in c or 'text-primary' in c))
            if status_p:
                d['status_on_platform'] = status_p.text.strip()
            perc = bottom.find('p', class_=lambda c: c and 'text-black17/70' in c)
            if perc and '%' in perc.text:
                d['progress_percentage'] = perc.text.strip()
            grid = bottom.find('div', class_=lambda c: c and 'grid-cols-3' in c)
            if grid:
                for item in grid.find_all('div', class_=lambda c: c and 'flex flex-col items-center gap-y-1' in c):
                    label = item.find('p', class_='text-gray-500')
                    value = item.find('p', class_=lambda c: c and 'text-gray-700' in c and 'font-bold' in c)
                    if label and value:
                        lbl = label.text.strip()
                        val = value.text.strip()
                        if 'مبلغ هدف' in lbl:
                            d['target_amount'] = val
                        elif 'پیشبینی سود' in lbl:
                            d['expected_return'] = val
                        elif 'مدت طرح' in lbl:
                            d['project_duration'] = val
                        elif 'تضمین اصل سرمایه' in lbl:
                            d['capital_guarantee'] = val
                        elif 'نوع طرح' in lbl:
                            d['project_type'] = val
                        elif 'نماد طرح' in lbl:
                            d['project_symbol'] = val
                        elif 'تاریخ شروع' in lbl:
                            d['start_date_on_platform'] = val
                        elif 'سرمایه گذاران' in lbl:
                            d['investor_count'] = re.sub(r'[^\d]', '', val)
                        elif 'تواتر پرداخت سود' in lbl:
                            d['profit_payment_frequency'] = val
        return d

    # ---------- فاندوکراد ----------
    def _scrape_fundocrowd(self, project: IFBProject) -> Dict:
        details = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'] * 2)

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_='home-box-design')

            target = project.project_name.strip()
            for card in cards:
                title_elem = card.find('h5', class_='main-h2')
                if not title_elem:
                    continue
                card_title = title_elem.text.strip()
                if target in card_title or card_title in target:
                    details = self._extract_fundocrowd_card(card)
                    if not details.get('expected_return') or not details.get('project_duration'):
                        details.update(self._scrape_fundocrowd_details(card))
                    break
        except Exception as e:
            logger.error(f"خطا در فاندوکراد: {e}")
        return details

    def _extract_fundocrowd_card(self, card) -> Dict:
        d = {}
        try:
            title = card.find('h5', class_='main-h2')
            if title:
                d['title_on_platform'] = title.text.strip()
            img = card.find('img', src=re.compile(r'common/DownloadFile'))
            if img and img.get('src'):
                d['thumbnail_url'] = img['src']
            company_span = card.find('span', string=re.compile(r'شرکت'))
            if company_span:
                d['applicant_name'] = company_span.parent.get_text(strip=True) if company_span.parent else company_span.text
            target_div = card.find('div', class_='d-flex mt-3')
            if target_div:
                spans = target_div.find_all('span')
                if len(spans) >= 2:
                    d['target_amount'] = spans[0].text.strip()
                    d['progress_percentage'] = spans[1].text.strip()
            progress_bar = card.find('div', class_='progress-bar')
            if progress_bar and progress_bar.has_attr('style'):
                match = re.search(r'width:\s*(\d+)%', progress_bar['style'])
                if match:
                    d['progress_width'] = match.group(1)
            duration_div = card.find('div', class_='row mt-3 ml-0')
            if duration_div:
                cols = duration_div.find_all('div', class_='col')
                if len(cols) >= 2:
                    duration_b = cols[0].find('b')
                    if duration_b:
                        d['project_duration'] = duration_b.text.strip()
                    profit_b = cols[1].find('b')
                    if profit_b:
                        d['expected_return'] = profit_b.text.strip()
            detail_link = card.find('a', href=re.compile(r'/companyDetail/\d+'))
            if detail_link and 'href' in detail_link.attrs:
                d['details_page_url'] = "https://fundocrowd.ir" + detail_link['href']
        except Exception as e:
            logger.warning(f"خطا در استخراج کارت فاندوکراد: {e}")
        return d

    def _scrape_fundocrowd_details(self, card) -> Dict:
        d = {}
        try:
            detail_link = card.find('a', href=re.compile(r'/companyDetail/\d+'))
            if not detail_link or 'href' not in detail_link.attrs:
                return d
            href = detail_link['href']
            full_url = "https://fundocrowd.ir" + href
            self.driver.get(full_url)
            time.sleep(self.config['delay'])

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            payment_div = soup.find('div', class_='detail-little-b')
            if payment_div:
                p = payment_div.find('p', class_='main-h2')
                if p:
                    d['profit_payment_frequency'] = p.text.strip()
        except Exception as e:
            logger.error(f"خطا در صفحه جزئیات فاندوکراد: {e}")
        return d

    # ---------- کارن‌کراد ----------
    def _scrape_karencrowd(self, project: IFBProject) -> Dict:
        details = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])
            if "plans" not in self.driver.current_url:
                try:
                    view_all = self.driver.find_element(By.XPATH, "//a[contains(text(), 'مشاهده همه طرح‌ها')]")
                    view_all.click()
                    time.sleep(self.config['delay'])
                except:
                    self.driver.get("https://www.karencrowd.com/plans")
                    time.sleep(self.config['delay'])

            self._scroll_page()
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_=lambda c: c and 'flex flex-col' in c and 'h-[775px]' in c)
            if not cards:
                cards = soup.find_all('div', class_=lambda c: c and 'bg-white' in c and 'shadow-md' in c)

            target = project.project_name.strip()
            for card in cards:
                title_elem = card.find('h2', class_='text-xl font-bold')
                if not title_elem:
                    continue
                card_title = title_elem.text.strip()
                if target in card_title or card_title in target:
                    details = self._extract_karencrowd_card(card)
                    break
        except Exception as e:
            logger.error(f"خطا در کارن‌کراد: {e}")
        return details

    def _extract_karencrowd_card(self, card) -> Dict:
        d = {}
        title = card.find('h2', class_='text-xl font-bold')
        if title:
            d['title_on_platform'] = title.text.strip()
        link = card.find('a', href=re.compile(r'/plans/\d+'))
        if link and 'href' in link.attrs:
            match = re.search(r'/plans/(\d+)', link['href'])
            if match:
                d['project_id_on_platform'] = match.group(1)
        img = card.find('img')
        if img and img.get('src'):
            d['thumbnail_url'] = img['src']
        target_label = card.find('span', string=re.compile('مبلغ هدف'))
        if target_label:
            parent = target_label.find_parent('div', class_='grid')
            if parent:
                cols = parent.find_all('div', class_='text-xs text-center')
                for col in cols:
                    label_span = col.find('span', class_='text-gray-card')
                    value_span = col.find('span', class_='text-dark font-bold')
                    if label_span and value_span:
                        lbl = label_span.text.strip()
                        val = value_span.text.strip()
                        if 'مبلغ هدف' in lbl:
                            d['target_amount'] = val
                        elif 'مدت طرح' in lbl:
                            d['project_duration'] = val
                        elif 'پیش بینی سود' in lbl:
                            d['expected_return'] = val
        return d

    # ---------- متد عمومی ----------
    def _scrape_generic(self, project: IFBProject) -> Dict:
        d = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            page_text = soup.get_text()
            patterns = {
                'target_amount': [r'مبلغ هدف.*?([\d,٬]+)', r'هدف.*?([\d,٬]+)\s*تومان', r'سرمایه مورد نیاز.*?([\d,٬]+)'],
                'expected_return': [r'(\d+\.?\d*)\s*٪', r'سود پیش‌بینی.*?(\d+\.?\d*)', r'بازده.*?(\d+\.?\d*)'],
                'project_duration': [r'(\d+)\s*ماه', r'مدت طرح.*?(\d+)\s*ماه'],
                'investor_count': [r'(\d+)\s*نفر', r'تعداد سرمایه‌گذار.*?(\d+)'],
            }
            for field, pat_list in patterns.items():
                for pat in pat_list:
                    match = re.search(pat, page_text, re.IGNORECASE)
                    if match:
                        d[field] = match.group(1)
                        break
            company_patterns = [r'شرکت\s*([\w\s]+)', r'متقاضی\s*:\s*([\w\s]+)']
            for pat in company_patterns:
                match = re.search(pat, page_text)
                if match:
                    d['applicant_name'] = match.group(1).strip()
                    break
        except Exception as e:
            logger.error(f"خطا در متد عمومی: {e}")
        return d

    def _scroll_page(self, times=2):
        for _ in range(times):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

    # ========== ذخیره‌سازی ==========
    def save_combined_data(self, data: List[Dict], base_name: str = "ifb_projects_1404_with_details"):
        json_file = f"{base_name}.json"
        csv_file = f"{base_name}.csv"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"فایل JSON ذخیره شد: {json_file}")

        df = pd.DataFrame(data)
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        logger.info(f"فایل CSV ذخیره شد: {csv_file}")

    def close(self):
        if self.driver:
            self.driver.quit()
            logger.info("مرورگر بسته شد")


def main():
    logger.info("=" * 60)
    logger.info("شروع استخراج طرح‌های تامین مالی جمعی از فرابورس ایران")
    logger.info(f"آدرس: {IFB_MAIN_URL}")
    logger.info("=" * 60)

    scraper = IFBScraper(headless=False)
    try:
        projects = scraper.scrape_all_pages()
        if not projects:
            logger.warning("هیچ پروژه‌ای با تاریخ شروع ۱۴۰۴ یافت نشد.")
            return

        logger.info(f"تعداد {len(projects)} پروژه با تاریخ شروع ۱۴۰۴ استخراج شد.")

        logger.info("\n" + "=" * 60)
        logger.info("مرحله 2: استخراج جزئیات از سکوها")
        logger.info("=" * 60)
        enriched = scraper.enrich_projects_with_platform_details(projects)

        scraper.save_combined_data(enriched, "ifb_projects_1404_complete")

        # نمایش نمونه
        logger.info("\n📊 نمونه داده‌های ترکیبی:")
        for i, item in enumerate(enriched[:3]):
            logger.info(f"\nپروژه {i+1}: {item.get('project_name')}")
            logger.info(f"   مبلغ هدف: {item.get('target_amount', '---')}")
            logger.info(f"   مدت طرح: {item.get('project_duration', '---')}")
            logger.info(f"   سود پیش‌بینی: {item.get('expected_return', '---')}")
            logger.info(f"   متقاضی: {item.get('applicant_name', '---')}")
            logger.info(f"   تاریخ شروع در سکو: {item.get('start_date_on_platform', '---')}")

    except Exception as e:
        logger.error(f"خطای کلی: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        scraper.close()


if __name__ == "__main__":
    main()