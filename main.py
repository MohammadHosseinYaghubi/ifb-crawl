#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import time
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
import logging
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup

# -------------------- تنظیمات اولیه --------------------
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

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
SHEET_NAME = "Crowdfunding_Projects_1404"
CREDS_ENV_VAR = "GOOGLE_CREDENTIALS"

# -------------------- مدل داده --------------------
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
    ifb_project_id: Optional[str] = None          # شناسه یکتای فرابورس (از showDesc)
    # فیلدهای استخراج‌شده از سکو
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


# -------------------- کلاس اصلی اسکرپر فرابورس --------------------
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

    # ---------- متدهای کمکی برای پیجینیشن و استخراج از فرابورس ----------
    def _navigate_to_page(self, page_number: int) -> bool:
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

            desc_script = """
                var el = document.getElementById('Message');
                return el ? el.innerText || el.textContent : '';
            """
            description = self.driver.execute_script(desc_script)
            if description:
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

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            msg = soup.find('small', {'id': 'Message'}) or soup.find('div', {'id': 'Message'})
            if msg:
                description = msg.get_text(strip=True)
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

                    # استخراج توضیحات و شناسه یکتا
                    ifb_id = None
                    description = "توضیحات در دسترس نیست"
                    details_link = cells[8].find('a')
                    if details_link and 'onclick' in details_link.attrs:
                        onclick = details_link['onclick']
                        match = re.search(r"showDesc\('(\d+)'\)", onclick)
                        if match:
                            ifb_id = match.group(1)
                            description = self._extract_description_from_modal(ifb_id)

                    # اگر از لینک توضیحات id گرفته نشد، از لینک مدارک بگیر
                    if not ifb_id:
                        documents_cell = cells[9]
                        documents_link = documents_cell.find('i', {'class': 'icon-folder'})
                        if documents_link and 'onclick' in documents_link.attrs:
                            onclick = documents_link['onclick']
                            match = re.search(r"GoToDocuments\('(\d+)'\)", onclick)
                            if match:
                                ifb_id = match.group(1)

                    documents_url = ""
                    if ifb_id:  # ساخت documents_url با همان id
                        documents_url = f"{IFB_MAIN_URL}?doc_id={ifb_id}"

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
                        scraped_date=datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
                        ifb_project_id=ifb_id
                    )
                    projects.append(project)
                    logger.info(f"ردیف {row_number}: {project_name} - شناسه {ifb_id}")
                except Exception as e:
                    logger.error(f"خطا در پردازش یک ردیف: {e}")
        return projects

    def scrape_all_pages(self) -> List[IFBProject]:
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

    def close(self):
        if self.driver:
            self.driver.quit()
            logger.info("مرورگر بسته شد")


# -------------------- کلاس استخراج اطلاعات از سکوها --------------------
class PlatformDetailScraper:
    """
    این کلاس وظیفه دارد برای یک پروژه IFB به آدرس سکو رفته و اطلاعات کارت را استخراج کند.
    تشخیص دامنه و انتخاب روش مناسب در این کلاس انجام می‌شود.
    """
    def __init__(self, driver: webdriver.Chrome, config: dict):
        self.driver = driver
        self.config = config
        self.wait = WebDriverWait(self.driver, config['timeout'])

    def scrape(self, project: IFBProject) -> Dict[str, Any]:
        if not project.platform_url:
            return {}

        domain = urlparse(project.platform_url).netloc.lower()
        logger.info(f"🔍 شروع استخراج از {domain} برای پروژه {project.project_name}")

        # انتخاب متد بر اساس دامنه
        if 'hamafarin.ir' in domain:
            return self._scrape_hamafarin(project)
        elif 'fundocrowd.ir' in domain:
            return self._scrape_fundocrowd(project)
        elif 'karencrowd.com' in domain:
            return self._scrape_karencrowd(project)
        elif 'ifund.ir' in domain:
            return self._scrape_ifund(project)
        elif 'zeema.fund' in domain:
            return self._scrape_zeema(project)
        else:
            # متد عمومی برای سایر سکوها
            return self._scrape_generic(project)

    # ---------- متدهای اختصاصی برای هر سکو ----------
    def _scrape_hamafarin(self, project: IFBProject) -> Dict:
        """هم‌آفرین – ساختار کارت‌های گروهی"""
        details = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])

            # اگر به صفحه اصلی رفت، به لیست طرح‌ها برو
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
        # عنوان
        title = card.find('a', class_=lambda c: c and 'text-[#2E2300]' in c)
        if title:
            d['title_on_platform'] = title.text.strip()
        # لینک و شناسه
        link = card.find('a', href=re.compile(r'/businessplans/\d+'))
        if link and 'href' in link.attrs:
            match = re.search(r'/businessplans/(\d+)', link['href'])
            if match:
                d['project_id_on_platform'] = match.group(1)
        # تصویر
        img = card.find('img')
        if img and img.get('src'):
            d['thumbnail_url'] = img['src']
        # نهاد مالی
        fin = card.find('p', string=re.compile('نهاد مالی:'))
        if fin:
            d['financial_institution'] = fin.text.strip()
        # مجری
        exec_p = card.find('p', class_='text-black17 font-YekanBakh text-md')
        if exec_p:
            d['applicant_name'] = exec_p.text.strip()
        # بخش پایینی
        bottom = card.find('div', class_=lambda c: c and 'bg-white' in c and '!pb-12' in c)
        if bottom:
            # وضعیت
            status_p = bottom.find('p', class_=lambda c: c and ('text-green67' in c or 'text-primary' in c))
            if status_p:
                d['status_on_platform'] = status_p.text.strip()
            # درصد
            perc = bottom.find('p', class_=lambda c: c and 'text-black17/70' in c)
            if perc and '%' in perc.text:
                d['progress_percentage'] = perc.text.strip()
            # گرید اطلاعات
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

    def _scrape_fundocrowd(self, project: IFBProject) -> Dict:
        """فاندوکراد – ساختار home-box-design"""
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
                    # اگر برخی فیلدها ناقص بود، روی دکمه جزئیات کلیک کن
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

    def _scrape_karencrowd(self, project: IFBProject) -> Dict:
        """کارن‌کراد – مشابه کد قبلی"""
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

    def _scrape_ifund(self, project: IFBProject) -> Dict:
        """آی‌فاند – ساختار آی‌فاند"""
        details = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            # جستجوی کارت‌ها با کلاس‌های متداول
            cards = soup.find_all('div', class_=lambda c: c and 'col-span-1' in c and 'bg-white' in c)
            target = project.project_name.strip()
            for card in cards:
                title_elem = card.find('p', class_='text-lg lg:text-xl font-medium')
                if not title_elem:
                    continue
                card_title = title_elem.text.strip()
                if target in card_title or card_title in target:
                    details = self._extract_ifund_card(card)
                    break
        except Exception as e:
            logger.error(f"خطا در آی‌فاند: {e}")
        return details

    def _extract_ifund_card(self, card) -> Dict:
        d = {}
        try:
            # عنوان
            title = card.find('p', class_='text-lg lg:text-xl font-medium')
            if title:
                d['title_on_platform'] = title.text.strip()
            # سود پیش‌بینی
            profit_span = card.find('span', class_='bg-custom-orange')
            if profit_span:
                d['expected_return'] = profit_span.text.strip()
            # نماد
            symbol_a = card.find('a', string=re.compile(r'فاندویرا'))
            if symbol_a:
                d['project_symbol'] = symbol_a.text.strip()
            # مبلغ هدف و جمع‌آوری شده
            divs = card.find_all('div', class_='flex justify-between text-base font-medium')
            if len(divs) >= 1:
                spans = divs[0].find_all('span')
                if len(spans) >= 2:
                    d['collected_amount'] = spans[0].text.strip()
                    d['target_amount'] = spans[1].text.strip()
            # نهاد مالی، متقاضی، مدت، نوع، تضمین از لیست
            items = card.find_all('div', class_='flex items-center justify-start text-black')
            for it in items:
                text = it.get_text(" ", strip=True)
                if 'سکوی تامین مالی جمعی آیفاند' in text:
                    d['platform_name'] = 'آی‌فاند'
                elif 'نام متقاضی :' in text:
                    d['applicant_name'] = text.replace('نام متقاضی :', '').strip()
                elif 'نهاد مالی :' in text:
                    d['financial_institution'] = text.replace('نهاد مالی :', '').strip()
                elif 'مدت طرح :' in text:
                    d['project_duration'] = text.replace('مدت طرح :', '').strip()
                elif 'نماد طرح :' in text:
                    d['project_symbol'] = text.replace('نماد طرح :', '').strip()
                elif 'نوع تامین مالی :' in text:
                    d['project_type'] = text.replace('نوع تامین مالی :', '').strip()
                elif 'سود پیش بینی شده سالانه:' in text:
                    # قبلاً از profit_span گرفتیم
                    pass
                elif 'مواعد پرداخت سود پیش بینی شده :' in text:
                    d['profit_payment_frequency'] = text.replace('مواعد پرداخت سود پیش بینی شده :', '').strip()
                elif 'بدون تضمین سود' in text:
                    d['capital_guarantee'] = text
        except Exception as e:
            logger.warning(f"خطا در استخراج آی‌فاند: {e}")
        return d

    def _scrape_zeema(self, project: IFBProject) -> Dict:
        """زیمه – ساختار Material-UI"""
        details = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_=lambda c: c and 'MuiGrid-root' in c)
            target = project.project_name.strip()
            for card in cards:
                # عنوان در <span class="MuiTypography-root MuiTypography-subtitleBold">
                title_span = card.find('span', class_='MuiTypography-subtitleBold')
                if title_span and target in title_span.text:
                    details = self._extract_zeema_card(card)
                    break
        except Exception as e:
            logger.error(f"خطا در زیمه: {e}")
        return details

    def _extract_zeema_card(self, card) -> Dict:
        d = {}
        try:
            # عنوان
            title = card.find('span', class_='MuiTypography-subtitleBold')
            if title:
                d['title_on_platform'] = title.text.strip()
            # تصویر
            img = card.find('img')
            if img and img.get('src'):
                d['thumbnail_url'] = img['src']
            # شرکت/متقاضی
            company = card.find('span', class_='MuiTypography-smallMedium')
            if company:
                d['applicant_name'] = company.text.strip()
            # سرمایه مورد نیاز و پیش‌بینی سود
            req_divs = card.find_all('div', class_='MuiStack-root muirtl-bu0fgp')
            for div in req_divs:
                spans = div.find_all('span')
                if len(spans) >= 2:
                    label = spans[0].text.strip()
                    value = spans[1].text.strip()
                    if 'سرمایه مورد نیاز' in label:
                        d['target_amount'] = value
                    elif 'پیش بینی سود پروژه' in label:
                        d['expected_return'] = value
            # مدت طرح
            duration = card.find('div', class_='MuiStack-root muirtl-bl0m4')
            if duration:
                spans = duration.find_all('span')
                if len(spans) >= 2:
                    d['project_duration'] = spans[1].text.strip()
            # نهاد مالی
            fin = card.find('div', class_='MuiStack-root muirtl-bl0m4', string=re.compile('نام نهاد مالی'))
            if fin:
                spans = fin.find_all('span')
                if len(spans) >= 2:
                    d['financial_institution'] = spans[1].text.strip()
            # تضمین
            guar = card.find('div', class_='MuiStack-root muirtl-14mq6mq')
            if guar:
                d['capital_guarantee'] = guar.text.strip()
            # سرمایه تامین شده
            collected = card.find('div', class_='MuiStack-root muirtl-1pbtxwi')
            if collected:
                spans = collected.find_all('span')
                if len(spans) >= 2:
                    d['collected_amount'] = spans[1].text.strip()
            # درصد پیشرفت
            progress = card.find('div', class_='MuiLinearProgress-root')
            if progress and progress.has_attr('aria-valuenow'):
                d['progress_percentage'] = progress['aria-valuenow']
            # تعداد سرمایه‌گذاران
            investors = card.find('div', class_='MuiStack-root muirtl-mk4amx')
            if investors:
                spans = investors.find_all('span')
                if len(spans) >= 2:
                    d['investor_count'] = spans[1].text.strip()
        except Exception as e:
            logger.warning(f"خطا در استخراج زیمه: {e}")
        return d

    # ---------- متد عمومی (fallback) ----------
    def _scrape_generic(self, project: IFBProject) -> Dict:
        """تلاش برای استخراج با الگوهای عمومی (عنوان، مبلغ، سود، ...)"""
        d = {}
        try:
            self.driver.get(project.platform_url)
            time.sleep(self.config['delay'])
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            page_text = soup.get_text()
            # الگوهای رایج
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
            # نام شرکت/متقاضی
            company_patterns = [r'شرکت\s*([\w\s]+)', r'متقاضی\s*:\s*([\w\s]+)']
            for pat in company_patterns:
                match = re.search(pat, page_text)
                if match:
                    d['applicant_name'] = match.group(1).strip()
                    break
            # نام سکو (از دامنه)
            d['platform_name'] = urlparse(project.platform_url).netloc
        except Exception as e:
            logger.error(f"خطا در متد عمومی: {e}")
        return d

    def _scroll_page(self, times=2):
        for _ in range(times):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)


# -------------------- کلاس مدیریت Google Sheets (افزایشی) --------------------
class GoogleSheetsHandler:
    def __init__(self, credentials_dict: dict = None, credentials_path: str = 'service_account.json'):
        self.credentials_dict = credentials_dict
        self.credentials_path = credentials_path
        self.client = self._authenticate()

    def _authenticate(self):
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets',
                      'https://www.googleapis.com/auth/drive']
            if self.credentials_dict:
                credentials = Credentials.from_service_account_info(self.credentials_dict, scopes=scopes)
            else:
                if not os.path.exists(self.credentials_path):
                    logger.warning(f"فایل {self.credentials_path} یافت نشد.")
                    return None
                credentials = Credentials.from_service_account_file(self.credentials_path, scopes=scopes)
            return gspread.authorize(credentials)
        except Exception as e:
            logger.error(f"خطا در احراز هویت Google Sheets: {str(e)}")
            return None

    def get_existing_ids(self, sheet_name: str, worksheet_index: int = 0) -> set:
        if not self.client:
            return set()
        try:
            spreadsheet = self.client.open(sheet_name)
            worksheet = spreadsheet.get_worksheet(worksheet_index)
            if not worksheet:
                return set()
            headers = worksheet.row_values(1)
            try:
                col_index = headers.index('ifb_project_id') + 1
            except ValueError:
                logger.warning("ستون 'ifb_project_id' در شیت یافت نشد.")
                return set()
            ids = worksheet.col_values(col_index)[1:]
            return set(ids)
        except gspread.SpreadsheetNotFound:
            logger.info(f"شیت {sheet_name} وجود ندارد.")
            return set()
        except Exception as e:
            logger.error(f"خطا در خواندن شناسه‌های موجود: {e}")
            return set()

    def append_new_rows(self, sheet_name: str, data: List[Dict], id_field: str = 'ifb_project_id'):
        if not self.client:
            logger.error("Google Sheets client not available.")
            return False

        try:
            spreadsheet = self.client.open(sheet_name)
            worksheet = spreadsheet.sheet1
        except gspread.SpreadsheetNotFound:
            logger.info(f"شیت {sheet_name} یافت نشد. در حال ایجاد...")
            spreadsheet = self.client.create(sheet_name)
            worksheet = spreadsheet.sheet1
            if data:
                headers = list(data[0].keys())
                worksheet.append_row(headers)
                logger.info("هدر ایجاد شد.")

        existing_ids = self.get_existing_ids(sheet_name)
        logger.info(f"تعداد شناسه‌های موجود در شیت: {len(existing_ids)}")

        new_rows = []
        for item in data:
            pid = str(item.get(id_field, ''))
            if pid and pid not in existing_ids:
                new_rows.append(list(item.values()))
            elif not pid:
                logger.warning(f"ردیف بدون شناسه: {item.get('project_name', '')} - اضافه نمی‌شود.")

        if new_rows:
            worksheet.append_rows(new_rows)
            logger.info(f"تعداد {len(new_rows)} ردیف جدید به شیت اضافه شد.")
        else:
            logger.info("هیچ ردیف جدیدی برای اضافه کردن وجود ندارد.")

        logger.info(f"لینک شیت: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
        return True


# -------------------- تابع اصلی --------------------
def main():
    logger.info("=" * 60)
    logger.info("شروع استخراج طرح‌های تامین مالی جمعی از فرابورس ایران")
    logger.info(f"آدرس: {IFB_MAIN_URL}")
    logger.info("=" * 60)

    # خواندن credentials از متغیر محیطی (برای GitHub Actions)
    creds_json = os.environ.get(CREDS_ENV_VAR)
    sheets_handler = None
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            sheets_handler = GoogleSheetsHandler(credentials_dict=creds_dict)
            logger.info("✅ اعتبارنامه Google Sheets از متغیر محیطی خوانده شد.")
        except Exception as e:
            logger.error(f"❌ خطا در پارس کردن GOOGLE_CREDENTIALS: {e}")
    else:
        # fallback به فایل محلی (برای تست محلی)
        sheets_handler = GoogleSheetsHandler(credentials_path='service_account.json')
        logger.info("📁 از فایل محلی service_account.json استفاده می‌شود.")

    scraper = IFBScraper(headless=False)  # در GitHub Actions headless=True می‌گذاریم
    try:
        # مرحله 1: استخراج پروژه‌های ۱۴۰۴ از فرابورس
        projects = scraper.scrape_all_pages()
        if not projects:
            logger.warning("⚠️ هیچ پروژه‌ای با تاریخ شروع ۱۴۰۴ یافت نشد.")
            return

        logger.info(f"📦 تعداد {len(projects)} پروژه با تاریخ شروع ۱۴۰۴ استخراج شد.")

        # مرحله 2: استخراج جزئیات از سکوها
        logger.info("\n" + "=" * 60)
        logger.info("مرحله 2: استخراج جزئیات از سکوها")
        logger.info("=" * 60)

        # برای هر پروژه یک شیء PlatformDetailScraper ایجاد می‌کنیم (با همان driver)
        detail_scraper = PlatformDetailScraper(scraper.driver, scraper.config)
        enriched_projects = []
        for idx, proj in enumerate(projects, 1):
            logger.info(f"\n[{idx}/{len(projects)}] پردازش پروژه: {proj.project_name}")
            details = detail_scraper.scrape(proj)
            # ترکیب اطلاعات
            combined = proj.to_dict()
            combined.update(details)
            enriched_projects.append(combined)
            logger.info(f"   ✅ {len(details)} فیلد جدید استخراج شد.")
            time.sleep(scraper.config['delay'])

        # مرحله 3: ذخیره محلی (JSON و CSV)
        base_filename = "ifb_projects_1404_complete"
        with open(f"{base_filename}.json", 'w', encoding='utf-8') as f:
            json.dump(enriched_projects, f, ensure_ascii=False, indent=4)
        logger.info(f"💾 فایل JSON ذخیره شد: {base_filename}.json")

        df = pd.DataFrame(enriched_projects)
        df.to_csv(f"{base_filename}.csv", index=False, encoding='utf-8-sig')
        logger.info(f"💾 فایل CSV ذخیره شد: {base_filename}.csv")

        # مرحله 4: اضافه کردن افزایشی به Google Sheets
        if sheets_handler and sheets_handler.client:
            sheets_handler.append_new_rows(SHEET_NAME, enriched_projects, id_field='ifb_project_id')
        else:
            logger.warning("⚠️ Google Sheets در دسترس نیست.")

        # نمایش نمونه
        logger.info("\n📊 نمونه داده‌های ترکیبی (۳ پروژه اول):")
        for i, item in enumerate(enriched_projects[:3]):
            logger.info(f"\nپروژه {i+1}: {item.get('project_name')}")
            logger.info(f"   شناسه فرابورس: {item.get('ifb_project_id', '---')}")
            logger.info(f"   مبلغ هدف: {item.get('target_amount', '---')}")
            logger.info(f"   مدت طرح: {item.get('project_duration', '---')}")
            logger.info(f"   سود پیش‌بینی: {item.get('expected_return', '---')}")
            logger.info(f"   متقاضی: {item.get('applicant_name', '---')}")

    except Exception as e:
        logger.error(f"❌ خطای کلی: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        scraper.close()


if __name__ == "__main__":
    main()