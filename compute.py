#!/usr/bin/env python3
"""Compute dashboard data for GitHub Actions.
   Downloads Revenue Recon Auto.xlsx from SharePoint.
   Extracts BCL (big customer list), CEO dashboard, and cumulative chart data.
   Writes to dashboard_data.json.
"""

import json
import os
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
    "IQC-0I5NvykRQ7kVT77wbQiPATL6NTHnqzRyDhoCcVAoaVc"
    "?e=UC2Cc0&download=1"
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


def fmt_date(d):
    """Format date to ISO string."""
    if isinstance(d, datetime.datetime):
        return d.strftime('%Y-%m-%d')
    if isinstance(d, datetime.date):
        return d.strftime('%Y-%m-%d')
    return str(d) if d else ''


def fmt_date_display(d):
    """Format date to dd/mm/yyyy."""
    if isinstance(d, datetime.datetime):
        return d.strftime('%d/%m/%Y')
    if isinstance(d, datetime.date):
        return d.strftime('%d/%m/%Y')
    return str(d) if d else ''


def extract_up_explorer(wb_rr):
    """Extract UP Explorer data: accounts, contract items, testing revenue."""
    print("  Extracting UP Explorer data...")
    today = datetime.date.today()
    today_minus_365 = today - datetime.timedelta(days=365)
    current_year = today.year
    current_month = today.month

    CUSTOMER_TYPES = {
        'Customer', 'Customer - Dormant (90days)', 'Customer - Pilot',
        'Customer - Repeat', 'License Client'
    }
    EXCLUDED_STATUSES = {
        'Pending Details', 'Data Loaded Back Data', 'Not to be Invoiced'
    }

    # ── 1. Read accounts ──
    ws_acc = wb_rr['accounts']
    accounts = {}
    id15_to_id18 = {}
    acc_casesafe_to_up = {}

    for _row in ws_acc.iter_rows(min_row=5, values_only=True):
        row = list(_row)
        if len(row) < 42:
            continue
        acc_id = row[31]  # Col 32 (AF) = Acc Casesafe ID 18 (0-indexed: 31)
        if not acc_id:
            continue
        acc_id = str(acc_id).strip()
        name = row[7]     # Col 8 (H) = Account Name
        acc_type = str(row[13] or '')  # Col 14 (N) = Type
        up_id = str(row[40] or '').strip()  # Col 41 (AO) = Ultimate Parent ID casesafe 18

        accounts[acc_id] = {
            'name': name,
            'type': acc_type,
            'country': row[11] or '',  # Col 12 (L) = Billing Country
            'parent_id': row[28] or '',  # Col 29 (AC) = Parent Account ID
            'up_id': up_id,
            'owner': row[6] or '',  # Col 7 (G) = Account Owner
            'csm': row[42] or '',  # Col 43 (AQ) = Customer Success Manager
            'industry': row[33] or row[10] or '',  # Col 34 (AH) Primary Industry or Col 11 (K) Industry
            'employees': row[20],  # Col 21 (U)
            'last_activity': fmt_date(row[23]),  # Col 24 (X)
            'credit_balance': row[46] if len(row) > 46 else 0,  # Col 47 (AU)
            'rev_target_2026': row[43] if len(row) > 43 else 0,  # Col 44 (AR)
            'is_customer': acc_type in CUSTOMER_TYPES,
        }
        acc_casesafe_to_up[acc_id] = up_id
        if len(acc_id) >= 15:
            id15_to_id18[acc_id[:15]] = acc_id

    print(f"    {len(accounts)} accounts")

    # ── 2. Read contract items ──
    ws_ci = wb_rr['contract items']
    acc_b1_starts = defaultdict(list)
    all_ci_raw = []

    for _row in ws_ci.iter_rows(min_row=5, values_only=True):
        row = list(_row)
        if len(row) < 58:
            continue

        acc_id18 = str(row[57] or '').strip()  # Col 58 = Account Casesafe ID 18
        up_id = acc_casesafe_to_up.get(acc_id18, '')
        active = row[37]  # Col 38 = Active Contract
        account = str(row[14] or '')  # Col 15 = Account
        ci_name = str(row[16] or '')  # Col 17 = Contract Items Name
        pf = str(row[19] or '')  # Col 20 = Product Family
        bv = safe_float(row[21])  # Col 22 = Billed Value (converted)
        billing_status = str(row[23] or '')  # Col 24 = Billing Status
        start_raw = row[30]  # Col 31 = Start Date
        end_raw = row[31]   # Col 32 = End Date
        ci_casesafe18 = str(row[56] or '') if len(row) > 56 else ''  # Col 57 = Contract Line Casesafe 18

        start_d = start_raw.date() if isinstance(start_raw, datetime.datetime) else (
            start_raw if isinstance(start_raw, datetime.date) else None)
        end_d = end_raw.date() if isinstance(end_raw, datetime.datetime) else (
            end_raw if isinstance(end_raw, datetime.date) else None)

        all_ci_raw.append({
            'up_id': up_id, 'active': active, 'account': account,
            'pf': pf, 'bv': bv, 'billing_status': billing_status,
            'start_d': start_d, 'end_d': end_d,
            'start_raw': start_raw, 'end_raw': end_raw,
            'ci_name': ci_name, 'ci_id': ci_casesafe18,
            'acc_id': acc_id18,
        })

        # Track B1 start dates for cutoff
        if active == 1 and billing_status not in EXCLUDED_STATUSES:
            if start_d and end_d and start_d <= today and end_d >= today:
                acc_b1_starts[account].append(start_d)

    # Cutoff per account
    acc_cutoff = {}
    for acc, starts in acc_b1_starts.items():
        acc_cutoff[acc] = min(starts) if starts else today_minus_365

    # Classify B1/B2
    BUCKET_ORDER = {'B1': 0, 'B2': 1, 'Excluded': 2, '—': 3}
    all_ci = []
    account_b1_arr = defaultdict(float)
    account_b2_arr = defaultdict(float)

    for ci in all_ci_raw:
        bucket = '—'
        if ci['billing_status'] in EXCLUDED_STATUSES:
            bucket = 'Excluded'
        elif ci['active'] == 1 and ci['start_d'] and ci['end_d'] and ci['start_d'] <= today and ci['end_d'] >= today:
            bucket = 'B1'
        elif ci['billing_status'] not in EXCLUDED_STATUSES and ci['start_d']:
            cutoff = acc_cutoff.get(ci['account'], today_minus_365)
            if ci['start_d'] >= cutoff:
                bucket = 'B2'

        if bucket == 'B1':
            account_b1_arr[ci['account']] += ci['bv']
        if bucket == 'B2':
            account_b2_arr[ci['account']] += ci['bv']

        if ci['up_id']:
            all_ci.append({
                'up_id': ci['up_id'],
                'bucket': bucket,
                'account': ci['account'],
                'account_id': ci['acc_id'],
                'name': ci['ci_name'],
                'ci_id': ci['ci_id'],
                'product_family': ci['pf'],
                'billed_value': ci['bv'],
                'billing_status': ci['billing_status'],
                'start_date': fmt_date_display(ci['start_raw']),
                'end_date': fmt_date_display(ci['end_raw']),
                'start_iso': fmt_date(ci['start_raw']),
                'end_iso': fmt_date(ci['end_raw']),
                'active': 1 if ci['active'] else 0,
                'contributing': 1 if bucket in ('B1', 'B2') else 0,
            })

    print(f"    {len(all_ci)} CIs with UP IDs")

    # ── 3. Enrich accounts with ARR ──
    for acc_id, info in accounts.items():
        name = info['name']
        info['b1_arr'] = account_b1_arr.get(name, 0)
        info['b2_arr'] = account_b2_arr.get(name, 0)
        info['total_arr'] = info['b1_arr'] + info['b2_arr']

    # Group by UP
    up_groups = defaultdict(list)
    for acc_id, info in accounts.items():
        if info['up_id']:
            up_groups[info['up_id']].append(acc_id)

    # Build children map
    children_map = defaultdict(list)
    for acc_id, info in accounts.items():
        pid = info['parent_id']
        if pid:
            pid_str = str(pid)
            pid18 = id15_to_id18.get(pid_str[:15] if pid_str else None)
            if pid18 and pid18 in accounts:
                children_map[pid18].append(acc_id)

    # DFS hierarchy builder
    def build_tree(up_id, acc_ids):
        acc_set = set(acc_ids)
        roots = []
        for aid in acc_ids:
            pid = accounts[aid]['parent_id']
            if not pid:
                roots.append(aid)
                continue
            pid_str = str(pid)
            pid18 = id15_to_id18.get(pid_str[:15] if pid_str else None)
            if not pid18 or pid18 not in acc_set:
                roots.append(aid)
        roots.sort(key=lambda x: (accounts[x]['name'] or '').lower())

        def visit(node, level):
            info = accounts[node]
            result = [{
                'id': node, 'name': info['name'], 'type': info['type'],
                'country': info['country'], 'level': level,
                'owner': info['owner'], 'csm': info['csm'],
                'industry': info['industry'], 'is_customer': info['is_customer'],
                'last_activity': info['last_activity'],
                'credit_balance': info.get('credit_balance', 0) or 0,
                'rev_target_2026': info.get('rev_target_2026', 0) or 0,
                'b1_arr': info['b1_arr'], 'b2_arr': info['b2_arr'],
                'total_arr': info['total_arr'],
            }]
            kids = [c for c in children_map.get(node, []) if c in acc_set]
            kids.sort(key=lambda x: (accounts[x]['name'] or '').lower())
            for kid in kids:
                result.extend(visit(kid, level + 1))
            return result

        tree = []
        for root in roots:
            tree.extend(visit(root, 0))
        return tree

    # Group CIs by UP
    ci_by_up = defaultdict(list)
    for ci in all_ci:
        ci_by_up[ci['up_id']].append(ci)
    for up_id in ci_by_up:
        ci_by_up[up_id].sort(key=lambda x: BUCKET_ORDER.get(x['bucket'], 99))

    # UP-level ARR
    up_b1_arr = defaultdict(float)
    up_b2_arr = defaultdict(float)
    up_b1_items = defaultdict(int)
    up_b2_items = defaultdict(int)
    for ci in all_ci:
        if ci['bucket'] == 'B1':
            up_b1_arr[ci['up_id']] += ci['billed_value']
            up_b1_items[ci['up_id']] += 1
        elif ci['bucket'] == 'B2':
            up_b2_arr[ci['up_id']] += ci['billed_value']
            up_b2_items[ci['up_id']] += 1

    up_data = []
    seen_ups = set()
    for up_id, items in ci_by_up.items():
        if not up_id or up_id in seen_ups:
            continue
        seen_ups.add(up_id)
        b1 = up_b1_arr.get(up_id, 0)
        b2 = up_b2_arr.get(up_id, 0)
        total = b1 + b2

        up_name = accounts.get(up_id, {}).get('name', up_id)
        acc_ids = up_groups.get(up_id, [])
        hierarchy = build_tree(up_id, acc_ids)
        up_accounts = sorted(set(ci['account'] for ci in items if ci['account']))
        up_billing_statuses = sorted(set(ci['billing_status'] for ci in items if ci['billing_status']))

        up_data.append({
            'id': up_id,
            'name': up_name,
            'b1_arr': b1, 'b2_arr': b2, 'total_arr': total,
            'b1_items': up_b1_items.get(up_id, 0),
            'b2_items': up_b2_items.get(up_id, 0),
            'account_count': len(acc_ids),
            'ci_count': len(items),
            'hierarchy': hierarchy,
            'contract_items': items,
            'ci_accounts': up_accounts,
            'ci_billing_statuses': up_billing_statuses,
        })

    up_data.sort(key=lambda x: -x['total_arr'])
    print(f"    {len(up_data)} UPs with contract items")

    # ── 4. Testing revenue from All Samples ──
    print("    Reading testing revenue...")
    ws_samples = wb_rr['All Samples All Info New XAPPEX']

    up_monthly_rev = defaultdict(lambda: defaultdict(float))
    up_yearly_rev = defaultdict(lambda: defaultdict(float))
    up_total_rev = defaultdict(float)
    acc_monthly_rev = defaultdict(lambda: defaultdict(float))
    acc_yearly_rev = defaultdict(lambda: defaultdict(float))
    acc_total_rev = defaultdict(float)

    for _row in ws_samples.iter_rows(min_row=6, values_only=True):
        row = list(_row)
        if len(row) < 128:
            continue
        up_name = row[3]        # Col 4 = UP Name
        acc_name = row[7] if len(row) > 7 else None  # Col 8 = Account
        month = row[61] if len(row) > 61 else None    # Col 62 = Month Completed
        rev = row[90] if len(row) > 90 else None      # Col 91 = Revenue (converted)
        status = row[103] if len(row) > 103 else None  # Col 104 = Status
        year = row[126] if len(row) > 126 else None    # Col 127 = Year Completed
        day = row[129] if len(row) > 129 else None     # Col 130 = Day Completed

        if not up_name or not rev or not year or not month:
            continue
        if status in ('Not to be Invoiced', 'Not reconciled', 'Data Loaded Back Data'):
            continue
        try:
            rev_f = float(rev)
            yr = int(year)
            mo = int(month)
        except (ValueError, TypeError):
            continue
        if yr > 2100:
            continue

        key = f"{yr}-{mo:02d}"
        up_monthly_rev[str(up_name)][key] += rev_f
        up_yearly_rev[str(up_name)][yr] += rev_f
        up_total_rev[str(up_name)] += rev_f

        if acc_name:
            acc_monthly_rev[str(acc_name)][key] += rev_f
            acc_yearly_rev[str(acc_name)][yr] += rev_f
            acc_total_rev[str(acc_name)] += rev_f

    print(f"    {len(up_total_rev)} UPs with testing revenue")

    # Sparkline months (last 24)
    sparkline_months = []
    for i in range(23, -1, -1):
        m = current_month - i
        y = current_year
        while m <= 0:
            m += 12
            y -= 1
        sparkline_months.append(f"{y}-{m:02d}")

    def compute_ytd_metrics(monthly_dict):
        lytd = sum(monthly_dict.get(f"{current_year - 1}-{mo:02d}", 0) for mo in range(1, current_month + 1))
        tytd = sum(monthly_dict.get(f"{current_year}-{mo:02d}", 0) for mo in range(1, current_month + 1))
        nrr = round((tytd / lytd - 1) * 100) if lytd > 0 else None
        return round(lytd, 2), round(tytd, 2), nrr

    # Enrich UP data with testing revenue
    up_name_to_idx = {u['name']: i for i, u in enumerate(up_data)}
    matched = 0
    for up_name, total in up_total_rev.items():
        idx = up_name_to_idx.get(up_name)
        if idx is not None:
            up = up_data[idx]
            up['testing_rev_total'] = total
            up['testing_rev_yearly'] = dict(sorted(up_yearly_rev[up_name].items()))
            up['testing_rev_sparkline'] = [round(up_monthly_rev[up_name].get(m, 0), 2) for m in sparkline_months]
            up['testing_rev_months'] = sparkline_months
            lytd, tytd, nrr = compute_ytd_metrics(up_monthly_rev[up_name])
            up['testing_rev_lytd'] = lytd
            up['testing_rev_tytd'] = tytd
            up['testing_rev_nrr'] = nrr
            acc_ids = up_groups.get(up['id'], [])
            up['testing_rev_target_2026'] = sum(
                (accounts[aid].get('rev_target_2026', 0) or 0) for aid in acc_ids
            )
            matched += 1

    # Enrich hierarchy accounts
    for up in up_data:
        for acc in up.get('hierarchy', []):
            acc_name = acc['name']
            if acc_name in acc_total_rev:
                acc['testing_rev_total'] = round(acc_total_rev[acc_name], 2)
                acc['testing_rev_yearly'] = dict(sorted(acc_yearly_rev[acc_name].items()))
                acc['testing_rev_sparkline'] = [round(acc_monthly_rev[acc_name].get(m, 0), 2) for m in sparkline_months]
                lytd, tytd, nrr = compute_ytd_metrics(acc_monthly_rev[acc_name])
                acc['testing_rev_lytd'] = lytd
                acc['testing_rev_tytd'] = tytd
                acc['testing_rev_nrr'] = nrr

    # Testing-only UPs (have testing revenue but no ARR)
    testing_only_ups = []
    for up_name, total in sorted(up_total_rev.items(), key=lambda x: -x[1]):
        if up_name not in up_name_to_idx:
            lytd, tytd, nrr = compute_ytd_metrics(up_monthly_rev[up_name])
            testing_only_ups.append({
                'name': up_name,
                'testing_rev_total': total,
                'testing_rev_yearly': dict(sorted(up_yearly_rev[up_name].items())),
                'testing_rev_sparkline': [round(up_monthly_rev[up_name].get(m, 0), 2) for m in sparkline_months],
                'testing_rev_months': sparkline_months,
                'testing_rev_lytd': lytd,
                'testing_rev_tytd': tytd,
                'testing_rev_nrr': nrr,
            })

    print(f"    {matched} matched (ARR+testing), {len(testing_only_ups)} testing-only")

    return up_data, testing_only_ups, sparkline_months


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
        up_data, testing_only_ups, sparkline_months = extract_up_explorer(wb_rr)

        wb_rr.close()

        # Build output
        print("\nBuilding output...")
        full_output = {
            'up_data': up_data,
            'testing_only_ups': testing_only_ups,
            'sparkline_months': sparkline_months,
            'bcl': bcl_data,
            'ceo': ceo_data,
            'meta': {
                'extracted': today.isoformat(),
                'source': 'Revenue Recon Auto.xlsx',
                'total_ups_arr': len(up_data),
                'testing_only_count': len(testing_only_ups),
            },
        }

        # Write JSON
        output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard_data.json')
        output_json = json.dumps(full_output, indent=2, ensure_ascii=False)
        with open(output_path, 'w') as f:
            f.write(output_json)

        print(f"\nSuccess!")
        print(f"  Wrote {len(output_json)} bytes to {output_path}")
        print(f"  UP Explorer: {len(up_data)} UPs, {len(testing_only_ups)} testing-only")
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
