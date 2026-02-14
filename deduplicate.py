def filter_new_projects(projects, existing_links: set):
    return [
        p for p in projects
        if p.get("detail_url") not in existing_links
    ]

import hashlib


def build_unique_key(row: dict) -> str:
    raw = f"{row['project_name']}-{row['company_name']}-{row['fund_collection_start_date']}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def attach_unique_keys(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["unique_key"] = build_unique_key(row)
    return rows


def filter_new_rows(rows: list[dict], existing_keys: set) -> list[dict]:
    return [
        row for row in rows
        if row["unique_key"] not in existing_keys
    ]
