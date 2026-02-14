# services/sheets.py

import os
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from typing import List, Dict
import json

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsRepository:
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.client = self._build_client()
        self.sheet = self.client.open_by_key(spreadsheet_id).sheet1

    def _build_client(self):
        creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        creds_dict = json.loads(creds_json)

        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=SCOPES
        )
        return gspread.authorize(credentials)

    def fetch_existing_links(self) -> set:
        rows = self.sheet.get_all_records()
        return {row["detail_url"] for row in rows if row.get("detail_url")}

    def append_rows(self, rows: List[Dict]):
        if not rows:
            return

        df = pd.DataFrame(rows)
        self.sheet.append_rows(
            df.values.tolist(),
            value_input_option="USER_ENTERED"
        )

    def fetch_existing_keys(self) -> set:
        rows = self.sheet.get_all_records()
        return {row["unique_key"] for row in rows if row.get("unique_key")}