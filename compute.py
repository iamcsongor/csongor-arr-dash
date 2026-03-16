#!/usr/bin/env python3
"""Compute dashboard data for GitHub Actions.
   Downloads Revenue Recon Auto.xlsx from SharePoint.
   Extracts BCL (big customer list), CEO dashboard, and cumulative chart data.
   Writes to dashboard_data.json.
"""

import json
import datetime
import calendar
from collections import defaultdict
from io import BytesIO
from openpyxl import load_workbook
import requests


# SharePoint URLs (download enabled)
REVENUE_RECON_URL = (
    "https://wiseandsallycom-my.sharepoint.com/:x:/g/personal/"
    "csongor_doma_cambri_io/"
    "EUVYvPrRZb5Ouu7Tm-eG1aYB3x3wlJLhUQr8VNc5jEG6rQ"
    "?e=0nKpJe&download=1"
)


def download_workbook(url, name):
    """Download Excel workbook from SharePoint URL."""
    print(f"  Downloading {name}...")
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
    )
    try:
        resp = session.get(url, allow_redirects=True, timeout=60)
        resp.raise_for_status()
        print(f"    Downloaded {len(resp.content)} bytes")
        return load_workbook(BytesIO(resp.content), data_only=True, read_only=True)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to download {name}: {e}")


def safe_float(v):
    """Safely convert value to float."""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def safe_str(v):
    """Safely convert value to string, handling #VALUE! errors."""
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s == '#VALUE!' else s


def safe_date(v):
    """Safely convert value to ISO date string."""
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.strftime('%Y-%m-%d')
    return ''


def safe_cum(v):
    """Safely convert cumulative chart value."""
    if v is None or v == '#N/A' or v == '':
        return None
    try:
        return float(v)
    except:
        return None


def extract_cumulative_chart(ws_formulas):
    """Extract cumulative chart data from Formulas tab."""
    print("  Extracting cumulative chart...")

    # Row 5, Column U (21) = subtitle
    cum_subtitle = ''
    for _r in ws_formulas.iter_rows(min_row=5, max_row=5, min_col=21, max_col=21):
        cum_subtitle = str(_r[0].value or '')

    # Row 7 headers: U-AH = columns 21-34
    cum_headers = []
    for _r in ws_formulas.iter_rows(min_row=7, max_row=7, min_col=21, max_col=34):
        cum_headers = [str(c.value or '') for c in list(_r)]

    # Read 31 rows of data (days 1-31)
    # Columns: U-AH (21-34), AL (38), AO (41)
    cum_series = {h: [] for h in cum_headers[1:]}  # skip 'Days'
    cum_l8m_avg = []
    cum_forecast = []

    for _r in ws_formulas.iter_rows(min_row=8, max_row=38, min_col=21, max_col=41):
        cells = list(_r)
        for i, h in enumerate(cum_headers[1:], start=1):
            cum_series[h].append(safe_cum(cells[i].value))
        cum_l8m_avg.append(safe_cum(cells[17].value))
        cum_forecast.append(safe_cum(cells[20].value))

    # Find last actual data day
    cum_last_day = 0
    for i, v in enumerate(cum_series.get('This Month', [])):
        if v is not None:
            cum_last_day = i + 1
        else:
            break

    cum_this_mtd = (
        cum_series.get('This Month', [None])[cum_last_day - 1]
        if cum_last_day > 0
        else None
    )
    cum_avg_at_day = (
        cum_l8m_avg[cum_last_day - 1]
        if cum_last_day > 0 and cum_last_day <= len(cum_l8m_avg)
        else None
    )

    result = {
        'subtitle': cum_subtitle,
        'series': cum_series,
        'l8m_avg': cum_l8m_avg,
        'forecast': cum_forecast,
        'last_day': cum_last_day,
        'this_mtd': cum_this_mtd,
        'avg_at_day': cum_avg_at_day,
    }

    if cum_this_mtd:
        print(
            f"    Last day={cum_last_day}, MTD={cum_this_mtd:.0f}, {len(cum_series)} series"
        )
    else:
        print(f"    No data")

    return result


def extract_big_customer_list(wb_rr):
    """Extract big customer list from Revenue Recon workbook."""
    print("  Extracting big customer list...")

    ws_bcl = wb_rr['big customer list']

    # Last updated timestamp from E6
    bcl_last_updated = ''
    for _row in ws_bcl.iter_rows(min_row=6, max_row=6, min_col=5, max_col=5):
        e6 = _row[0].value
        if isinstance(e6, (datetime.datetime, datetime.date)):
            bcl_last_updated = e6.strftime('%Y-%m-%d %H:%M')
        elif e6:
            bcl_last_updated = str(e6)

    # Build Salesforce URL map from accounts sheet
    ws_acc_sf = wb_rr['accounts']
    sf_name_to_id = {}
    for _row in ws_acc_sf.iter_rows(min_row=2, max_row=600, min_col=1, max_col=50):
        cells = list(_row)
        if len(cells) > 41:
            acct_name = str(cells[7].value).strip() if cells[7].value else None
            up_id_sf = str(cells[40].value).strip() if cells[40].value else None
            if acct_name and up_id_sf and len(up_id_sf) > 10:
                sf_name_to_id[acct_name] = up_id_sf

    # Month headers from rows 7-8, columns H-AH (8-34)
    row7_vals = list(
        ws_bcl.iter_rows(min_row=7, max_row=7, min_col=8, max_col=34, values_only=True)
    )[0]
    row8_vals = list(
        ws_bcl.iter_rows(min_row=8, max_row=8, min_col=8, max_col=34, values_only=True)
    )[0]
    month_headers = []
    for i in range(27):
        yr = int(row7_vals[i]) if row7_vals[i] else 0
        mn = int(row8_vals[i]) if row8_vals[i] else 0
        month_headers.append(f"{yr}-{mn:02d}" if yr and mn else f"m{i}")

    # Extract rows
    bcl_rows = []
    for r_idx in range(9, 296):
        row = list(
            ws_bcl.iter_rows(
                min_row=r_idx, max_row=r_idx, min_col=1, max_col=83, values_only=True
            )
        )[0]
        up_name = safe_str(row[5])  # F = index 5
        if not up_name:
            continue

        monthly = [safe_float(row[i]) for i in range(7, 34)]

        rec = {
            'csm': safe_str(row[1]),  # B
            'owner': safe_str(row[2]),  # C
            'cls': safe_str(row[3]),  # D
            'industry': safe_str(row[4]),  # E
            'up': up_name,  # F
            'monthly': monthly,
            'status': safe_str(row[35]),  # AJ
            'l12m': safe_float(row[36]),  # AK
            'l6m': safe_float(row[37]),  # AL
            'lytd': safe_float(row[38]),  # AM
            'tytd': safe_float(row[39]),  # AN
            'nrr': safe_float(row[40]),  # AO
            'fy24': safe_float(row[41]),  # AP
            'fy25': safe_float(row[42]),  # AQ
            'fc25': safe_float(row[43]),  # AR
            'target26': safe_float(row[44]),  # AS
            'perf_quad': safe_str(row[45]),  # AT
            'rev_gap': safe_float(row[46]),  # AU
            'ly_vs_ty': safe_float(row[47]),  # AV
            'ytd_vs_tgt': safe_float(row[48]),  # AW
            'growth_cohort': safe_str(row[49]),  # AX
            'tenure': safe_str(row[50]),  # AY
            'trend_18m': safe_str(row[51]),  # AZ
            'trend_12m': safe_str(row[52]),  # BA
            'trend_6m': safe_str(row[53]),  # BB
            'activity': safe_str(row[54]),  # BC
            'active_months': safe_float(row[55]),  # BD
            'frequency': safe_float(row[56]),  # BE
            'first_test': safe_date(row[57]),  # BF
            'months_since_first': safe_float(row[58]),  # BG
            'last_test': safe_date(row[59]),  # BH
            'months_since_last': safe_float(row[60]),  # BI
            'velocity': safe_float(row[61]),  # BJ
            'h1_24': safe_float(row[62]),  # BK
            'l12m_avg': safe_float(row[63]),  # BL
            'l6m_avg': safe_float(row[64]),  # BM
            'ratio_12v6': safe_float(row[65]),  # BN
            'momentum': safe_float(row[66]),  # BO
            'score_rank': safe_float(row[67]),  # BP
            'lic_fy24': safe_float(row[68]),  # BQ
            'lic_ytd': safe_float(row[69]),  # BR
            'cred_fy24': safe_float(row[70]),  # BS
            'cred_ytd': safe_float(row[71]),  # BT
            'ms_fy24': safe_float(row[72]),  # BU
            'ms_ytd': safe_float(row[73]),  # BV
            'test_fy24': safe_float(row[74]),  # BW
            'test_ytd': safe_float(row[75]),  # BX
            'total_fy24': safe_float(row[76]),  # BY
            'total_fy25': safe_float(row[77]),  # BZ
            'arr_calc': safe_float(row[78]),  # CA
            'pending': safe_float(row[79]),  # CB
            'up_id': safe_str(row[80]),  # CC
            'tam_type': safe_str(row[81]),  # CD
            'credit_bal': safe_float(row[82]),  # CE
        }

        sf_id = sf_name_to_id.get(up_name)
        rec['sf_url'] = (
            f'https://cambri.lightning.force.com/lightning/r/Account/{sf_id}/view'
            if sf_id
            else ''
        )
        bcl_rows.append(rec)

    print(f"    Extracted {len(bcl_rows)} rows, {len(month_headers)} months")
    print(f"    Period: {month_headers[0]} to {month_headers[-1]}")

    return {
        'month_headers': month_headers,
        'rows': bcl_rows,
        'last_updated': bcl_last_updated,
    }


def extract_ceo_dashboard(wb_rr):
    """Extract CEO dashboard data from Revenue Recon workbook."""
    print("  Extracting CEO dashboard...")

    ws_formulas = wb_rr['formulas']
    today = datetime.date.today()
    current_year = today.year
    current_month = today.month

    # Read testing revenue from 'samples testing revenue' (via bcl rows we'll compute)
    ws_bcl = wb_rr['big customer list']

    # Aggregate monthly revenue from BCL
    company_monthly_rev = defaultdict(float)
    company_daily_rev = defaultdict(lambda: defaultdict(float))

    # Read month headers again to align
    row7_vals = list(
        ws_bcl.iter_rows(min_row=7, max_row=7, min_col=8, max_col=34, values_only=True)
    )[0]
    row8_vals = list(
        ws_bcl.iter_rows(min_row=8, max_row=8, min_col=8, max_col=34, values_only=True)
    )[0]
    month_headers = []
    for i in range(27):
        yr = int(row7_vals[i]) if row7_vals[i] else 0
        mn = int(row8_vals[i]) if row8_vals[i] else 0
        month_headers.append(f"{yr}-{mn:02d}" if yr and mn else f"m{i}")

    # Aggregate from BCL rows
    for r_idx in range(9, 296):
        row = list(
            ws_bcl.iter_rows(
                min_row=r_idx, max_row=r_idx, min_col=1, max_col=34, values_only=True
            )
        )[0]
        up_name = safe_str(row[5]) if len(row) > 5 else ''
        if not up_name:
            continue

        # Monthly data: H-AH (indices 7-33)
        for i in range(7, min(34, len(row))):
            if i < len(month_headers):
                company_monthly_rev[month_headers[i - 7]] += safe_float(row[i])

    # Build CEO months (last 25 months)
    ceo_months = []
    for i in range(24, -1, -1):
        m = current_month - i
        y = current_year
        while m <= 0:
            m += 12
            y -= 1
        ceo_months.append(f"{y}-{m:02d}")

    ceo_monthly = {m: round(company_monthly_rev.get(m, 0), 2) for m in ceo_months}

    # Daily cumulative data (last 12 months + current)
    ceo_daily_cumulative = {}
    for i in range(11, -1, -1):
        m = current_month - i
        y = current_year
        while m <= 0:
            m += 12
            y -= 1
        key = f"{y}-{m:02d}"
        days_in_month = calendar.monthrange(y, m)[1]
        daily = company_daily_rev.get(key, {})
        cumulative = []
        running = 0.0
        for d in range(1, days_in_month + 1):
            running += daily.get(d, 0)
            cumulative.append(round(running, 2))
        ceo_daily_cumulative[key] = cumulative

    # KPIs
    lytd_total = sum(
        company_monthly_rev.get(f"{current_year - 1}-{m:02d}", 0)
        for m in range(1, current_month + 1)
    )
    tytd_total = sum(
        company_monthly_rev.get(f"{current_year}-{m:02d}", 0)
        for m in range(1, current_month + 1)
    )
    ytd_growth = (
        round((tytd_total / lytd_total - 1) * 100, 1) if lytd_total > 0 else None
    )

    # Active customers
    active_customers_ytd = set()
    for r_idx in range(9, 296):
        row = list(
            ws_bcl.iter_rows(
                min_row=r_idx, max_row=r_idx, min_col=1, max_col=39, values_only=True
            )
        )[0]
        up_name = safe_str(row[5]) if len(row) > 5 else ''
        if not up_name:
            continue
        # Check TYTD (col AN = 39)
        tytd_val = safe_float(row[38]) if len(row) > 38 else 0
        if tytd_val > 0:
            active_customers_ytd.add(up_name)

    cum_chart_data = extract_cumulative_chart(ws_formulas)

    print(
        f"    LYTD: {round(lytd_total):,}, TYTD: {round(tytd_total):,}, "
        f"Growth: {ytd_growth}%"
    )
    print(f"    Active customers YTD: {len(active_customers_ytd)}")
    print(f"    Monthly data: {len(ceo_monthly)} months")

    return {
        'months': ceo_months,
        'monthly_rev': ceo_monthly,
        'daily_cumulative': ceo_daily_cumulative,
        'cum_chart': cum_chart_data,
        'lytd': round(lytd_total, 2),
        'tytd': round(tytd_total, 2),
        'ytd_growth': ytd_growth,
        'active_customers_ytd': len(active_customers_ytd),
    }


def main():
    """Main entry point."""
    print("Starting compute job...")
    today = datetime.date.today()

    try:
        # Download workbook
        print("\nDownloading Revenue Recon Auto.xlsx...")
        wb_rr = download_workbook(REVENUE_RECON_URL, 'Revenue Recon Auto.xlsx')

        # Extract data
        print("\nExtracting data...")
        bcl_data = extract_big_customer_list(wb_rr)
        ceo_data = extract_ceo_dashboard(wb_rr)

        wb_rr.close()

        # Build output
        print("\nBuilding output...")
        full_output = {
            'bcl': bcl_data,
            'ceo': ceo_data,
            'meta': {
                'extracted': today.isoformat(),
                'source': 'Revenue Recon Auto.xlsx',
            },
        }

        # Write JSON
        output_path = '/sessions/wizardly-exciting-pasteur/repo/dashboard_data.json'
        output_json = json.dumps(full_output, indent=2, ensure_ascii=False)
        with open(output_path, 'w') as f:
            f.write(output_json)

        print(f"\nSuccess!")
        print(f"  Wrote {len(output_json)} bytes to {output_path}")
        print(f"  BCL: {len(bcl_data['rows'])} customers, {len(bcl_data['month_headers'])} months")
        print(f"  CEO: {len(ceo_data['months'])} months, cumulative chart: {ceo_data['cum_chart']['last_day']} days")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
