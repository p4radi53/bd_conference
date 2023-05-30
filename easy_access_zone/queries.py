from datetime import date


def format_dwh_date(d: date):
    return int(d.strftime("%Y%M%d"))


def dwh_query_easy_access_zone(ingest_start_date: date):
    return """some query here"""
