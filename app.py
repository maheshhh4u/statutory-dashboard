import os, io, csv, zipfile, requests, pandas as pd
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

app = Flask(__name__)

API_KEY  = os.environ.get("CC_API_KEY", "80c5abfc86864f8d9330288c3521bb78")
API_BASE = "https://api.charitycommission.gov.uk/register/api"
API_HDR  = {"Ocp-Apim-Subscription-Key": API_KEY}

BULK_BASE = "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/txt"
BULK_URLS = {
    "charity": f"{BULK_BASE}/publicextract.charity.zip",
    "parta":   f"{BULK_BASE}/publicextract.charity_annual_return_parta.zip",
    "partb":   f"{BULK_BASE}/publicextract.charity_annual_return_partb.zip",
}

# ── In-memory stores ───────────────────────────────────────────────────────────
_cache          = {}
_called_log     = {}   # "page|reg" → {reg_number, page, called_by, timestamp}
_comments       = {}   # "page|reg" → string
_statuses       = {}   # "page|reg" → status string
_saved_searches = {}   # name → criteria dict

# ── Cache helpers ──────────────────────────────────────────────────────────────
def cache_get(key, max_age=180):
    if key in _cache:
        ts, val = _cache[key]
        if (datetime.utcnow() - ts).total_seconds() < max_age * 60:
            return val
    return None

def cache_set(key, val):
    _cache[key] = (datetime.utcnow(), val)

# ── Bulk extract loader ────────────────────────────────────────────────────────
# Only load the columns we actually need — saves ~70% RAM on free Render tier
NEEDED_COLS = {
    "parta": [
        "registered_charity_number","organisation_number",
        "income_from_government_grants","income_from_government_contracts",
        "total_gross_income","total_gross_expenditure",
        "fin_period_end_date","ar_due_date","ar_received_date",
        "latest_fin_period_submitted_ind","fin_period_order_number",
    ],
    "charity": [
        "registered_charity_number","organisation_number",
        "charity_name","charity_registration_status",
        "date_of_registration","date_of_removal",
        "charity_contact_phone","charity_contact_email","charity_contact_web",
        "charity_contact_address1","charity_contact_address2",
        "charity_contact_address3","charity_contact_address4",
        "charity_contact_postcode","charity_activities",
        "latest_income","charity_type",
    ],
}

def load_extract(name):
    cached = cache_get(f"ex_{name}")
    if cached is not None:
        return cached
    url = BULK_URLS.get(name)
    if not url:
        return None
    try:
        print(f"Downloading {name} ...")
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            inner = z.namelist()[0]
            with z.open(inner) as f:
                raw = f.read()
        buf = io.BytesIO(raw)
        # First pass: read just the header to find which columns exist
        header_buf = io.BytesIO(raw)
        try:
            header_df = pd.read_csv(header_buf, sep="\t", encoding="utf-8",
                                    nrows=0, dtype=str, on_bad_lines="skip")
        except TypeError:
            header_buf.seek(0)
            header_df = pd.read_csv(header_buf, sep="\t", encoding="utf-8",
                                    nrows=0, dtype=str, error_bad_lines=False)
        all_cols = [c.strip().lower() for c in header_df.columns]
        # Only keep needed columns if defined for this extract
        usecols = None
        needed = NEEDED_COLS.get(name)
        if needed:
            usecols = [c for c in needed if c in all_cols]
            print(f"  {name}: loading {len(usecols)} of {len(all_cols)} columns")
        # Second pass: read full data with column filter
        buf.seek(0)
        try:
            df = pd.read_csv(buf, sep="\t", encoding="utf-8",
                             low_memory=False, dtype=str,
                             on_bad_lines="skip",
                             usecols=usecols if usecols else None)
        except TypeError:
            buf.seek(0)
            df = pd.read_csv(buf, sep="\t", encoding="utf-8",
                             low_memory=False, dtype=str,
                             error_bad_lines=False, warn_bad_lines=False,
                             usecols=usecols if usecols else None)
        df.columns = [c.strip().lower() for c in df.columns]
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        df.replace("nan", "", inplace=True)
        print(f"  {name}: {len(df)} rows, {len(df.columns)} cols loaded")
        cache_set(f"ex_{name}", df)
        return df
    except Exception as e:
        print(f"  ERROR {name}: {e}")
        import traceback; traceback.print_exc()
        return None

def find_col(df, *frags):
    for frag in frags:
        for c in df.columns:
            if frag.lower() in c.lower():
                return c
    return None

# ── CC API helpers ─────────────────────────────────────────────────────────────
def api_get_charity(reg):
    cached = cache_get(f"c_{reg}", max_age=480)
    if cached is not None:
        return cached
    try:
        r = requests.get(f"{API_BASE}/charityRegNumber/{reg}/0",
                         headers=API_HDR, timeout=10)
        if r.ok:
            d = r.json()
            cache_set(f"c_{reg}", d)
            return d
    except Exception as e:
        print(f"api_get_charity({reg}): {e}")
    return None

def api_get_financials(reg):
    cached = cache_get(f"f_{reg}", max_age=480)
    if cached is not None:
        return cached
    try:
        r = requests.get(f"{API_BASE}/charityFinancialHistory/{reg}/0",
                         headers=API_HDR, timeout=10)
        if r.ok:
            d = r.json()
            cache_set(f"f_{reg}", d)
            return d
    except Exception as e:
        print(f"api_get_financials({reg}): {e}")
    return []

def parse_financials(history):
    if not history or not isinstance(history, list):
        return {}
    recs = sorted(history,
                  key=lambda x: str(x.get("financial_period_end_date") or ""),
                  reverse=True)
    L = recs[0]; P = recs[1] if len(recs) > 1 else {}
    def m(rec, *keys):
        for k in keys:
            v = rec.get(k)
            if v not in (None, "", "None"):
                try: return float(v)
                except: pass
        return 0.0
    gg = m(L, "income_from_govt_grants"); gc = m(L, "income_from_govt_contracts")
    ti = m(L, "income", "inc_total");     te = m(L, "expenditure", "exp_total")
    pg = m(P, "income_from_govt_grants"); pc = m(P, "income_from_govt_contracts")
    stat = gg + gc; prev = pg + pc; pct = round(stat/ti*100, 1) if ti > 0 else 0.0
    return {"total_income": ti, "total_expenditure": te, "gov_grants": gg,
            "gov_contracts": gc, "statutory": stat, "stat_pct": pct,
            "prev_grants": pg, "prev_contracts": pc, "prev_statutory": prev,
            "fin_year": str(L.get("financial_period_end_date") or "")[:4]}

# ── Prospect rules ─────────────────────────────────────────────────────────────
def classify(f):
    inc = f.get("total_income", 0); stat = f.get("statutory", 0)
    pct = f.get("stat_pct", 0);     cont = f.get("gov_contracts", 0)
    lists = []
    if 250_000 <= inc <= 3_000_000 and 50_000 <= stat <= 300_000 and 10 <= pct <= 40:
        lists.append("Best Immediate")
    if 150_000 <= inc <= 1_500_000 and 10_000 <= stat <= 100_000:
        lists.append("Readiness Package")
    if (500_000 <= inc <= 5_000_000 and 100_000 <= cont <= 500_000
            and (cont/inc*100 < 35 if inc > 0 else False)):
        lists.append("Contract Growth")
    if 500_000 <= inc <= 5_000_000 and stat >= 200_000 and 40 <= pct <= 70:
        lists.append("Retention/Replacement")
    return lists

def build_flags(f):
    flags = []
    if f.get("total_income", 0) > 0 and f.get("total_expenditure", 0) >= f.get("total_income", 0) * 0.95:
        flags.append({"label": "Spend≥Income", "cls": "red"})
    if f.get("prev_statutory", 0) > 0 and f.get("statutory", 0) < f.get("prev_statutory", 0) * 0.9:
        flags.append({"label": "Stat↓10%+", "cls": "orange"})
    if f.get("prev_contracts", 0) > 0 and f.get("gov_contracts", 0) == 0:
        flags.append({"label": "Lost contracts", "cls": "red"})
    return flags

# ── Contact details from extract ───────────────────────────────────────────────
def get_charity_details(reg_number):
    df = load_extract("charity")
    if df is None or df.empty:
        return {}
    reg_col  = find_col(df, "registered_charity_number", "regno")
    if not reg_col:
        return {}
    match = df[df[reg_col].astype(str).str.strip() == str(reg_number).strip()]
    if match.empty:
        return {}
    r = match.iloc[0]
    def g(col): return str(r.get(col, "")) if col and col in r.index else ""
    return {
        "name":     g(find_col(df, "charity_name", "name")),
        "phone":    g(find_col(df, "charity_contact_phone", "phone")),
        "email":    g(find_col(df, "charity_contact_email", "email")),
        "website":  g(find_col(df, "charity_contact_web", "web")),
        "address1": g(find_col(df, "charity_contact_address1")),
        "address2": g(find_col(df, "charity_contact_address2")),
        "town":     g(find_col(df, "charity_contact_address3")),
        "county":   g(find_col(df, "charity_contact_address4")),
        "postcode": g(find_col(df, "charity_contact_postcode", "postcode")),
        "activities": g(find_col(df, "charity_activities", "activities")),
    }

# ── CSV response helper ────────────────────────────────────────────────────────
def csv_response(rows, headers, filename):
    def gen():
        buf = io.StringIO(); w = csv.writer(buf)
        w.writerow(headers); yield buf.getvalue(); buf.seek(0); buf.truncate()
        for row in rows:
            w.writerow(row); yield buf.getvalue(); buf.seek(0); buf.truncate()
    return Response(stream_with_context(gen()), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})

CONTACT_HDRS = ["Phone", "Email", "Address 1", "Address 2", "Town", "County",
                "Postcode", "Activities", "Call Status", "Comment", "Called By", "Call Timestamp"]

def contact_row(reg, page):
    d   = get_charity_details(reg)
    key = f"{page}|{reg}"
    cal = _called_log.get(key, {})
    return [d.get("phone",""), d.get("email",""), d.get("address1",""),
            d.get("address2",""), d.get("town",""), d.get("county",""),
            d.get("postcode",""), d.get("activities",""),
            _statuses.get(key,""), _comments.get(key,""),
            cal.get("called_by",""), cal.get("timestamp","")]

# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/debug/columns")
def debug_columns():
    out = {}
    for name in ["charity", "parta"]:
        df = load_extract(name)
        if df is not None and not df.empty:
            out[name] = {"rows": len(df), "columns": list(df.columns)}
        else:
            out[name] = {"error": "Could not load"}
    return jsonify(out)

# ── PROSPECTS ─────────────────────────────────────────────────────────────────
@app.route("/api/prospects")
def api_prospects():
    try:
        return _api_prospects_inner()
    except MemoryError:
        return jsonify({"error": "Server ran out of memory loading charity data. Please try again in 30 seconds."}), 500
    except Exception as e:
        import traceback
        print("PROSPECTS ERROR:", traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), 500

def _api_prospects_inner():
    cached = cache_get("prospects", max_age=60)
    if cached: return jsonify(cached)

    results = {"best": [], "readiness": [], "contract": [], "retention": []}
    df_parta = load_extract("parta")
    if df_parta is None or df_parta.empty:
        return jsonify({"error": "Could not load charity data from Charity Commission. Please try again."}), 500

    reg_col = "registered_charity_number"
    df_parta["_sort"] = pd.to_datetime(
        df_parta.get("fin_period_end_date", pd.Series(dtype=str)), errors="coerce")
    latest = (df_parta.sort_values("_sort", ascending=False)
                      .drop_duplicates(subset=[reg_col], keep="first").copy())

    for col in ["total_gross_income", "total_gross_expenditure",
                "income_from_government_grants", "income_from_government_contracts"]:
        if col in latest.columns:
            latest[col] = pd.to_numeric(latest[col], errors="coerce").fillna(0)

    latest = latest[(latest["total_gross_income"] >= 150_000) &
                    (latest["total_gross_income"] <= 5_000_000)]

    prev_df = (df_parta.sort_values("_sort", ascending=False)
                       .groupby(reg_col).nth(1).reset_index())
    for col in ["income_from_government_grants", "income_from_government_contracts"]:
        if col in prev_df.columns:
            prev_df[col] = pd.to_numeric(prev_df[col], errors="coerce").fillna(0)
    prev_map = prev_df.set_index(reg_col)[
        ["income_from_government_grants", "income_from_government_contracts"]
    ].to_dict(orient="index") if not prev_df.empty else {}

    name_lookup = {}; web_lookup = {}
    df_charity = load_extract("charity")
    if df_charity is not None and not df_charity.empty:
        creg  = find_col(df_charity, "registered_charity_number", "regno")
        cname = find_col(df_charity, "charity_name", "name")
        cweb  = find_col(df_charity, "charity_contact_web", "web")
        if creg and cname:
            ks = df_charity[creg].astype(str).str.strip()
            name_lookup  = dict(zip(ks, df_charity[cname].astype(str).str.strip()))
            name_lookup_nz = dict(zip(ks.str.lstrip("0"), df_charity[cname].astype(str).str.strip()))
        else:
            name_lookup_nz = {}
        if creg and cweb:
            ks2 = df_charity[creg].astype(str).str.strip()
            web_lookup  = dict(zip(ks2, df_charity[cweb].astype(str).str.strip()))
            web_lookup_nz = dict(zip(ks2.str.lstrip("0"), df_charity[cweb].astype(str).str.strip()))
        else:
            web_lookup_nz = {}
    else:
        name_lookup_nz = {}; web_lookup_nz = {}

    processed = 0
    for _, row in latest.iterrows():
        reg = str(row.get(reg_col, "")).strip()
        if not reg or reg == "": continue
        ti = float(row.get("total_gross_income", 0) or 0)
        te = float(row.get("total_gross_expenditure", 0) or 0)
        gg = float(row.get("income_from_government_grants", 0) or 0)
        gc = float(row.get("income_from_government_contracts", 0) or 0)
        stat = gg + gc; pct = round(stat/ti*100, 1) if ti > 0 else 0.0
        yr   = str(row.get("fin_period_end_date", ""))[:4]
        pr   = prev_map.get(reg, {})
        pg_  = float(pr.get("income_from_government_grants", 0) or 0)
        pc_  = float(pr.get("income_from_government_contracts", 0) or 0)
        fin  = {"total_income": ti, "total_expenditure": te, "gov_grants": gg,
                "gov_contracts": gc, "statutory": stat, "stat_pct": pct,
                "prev_grants": pg_, "prev_contracts": pc_, "prev_statutory": pg_+pc_,
                "fin_year": yr}
        fin["flags"] = build_flags(fin)
        lists = classify(fin)
        if not lists: continue

        reg_nz = reg.lstrip("0")
        name = (name_lookup.get(reg,"") or name_lookup.get(reg_nz,"") or
                name_lookup_nz.get(reg,"") or name_lookup_nz.get(reg_nz,"") or reg)
        web  = (web_lookup.get(reg,"") or web_lookup.get(reg_nz,"") or
                web_lookup_nz.get(reg,"") or web_lookup_nz.get(reg_nz,"") or "")
        if name in ("nan","None","NaN",""): name = reg
        if web in ("nan","None","NaN"):     web = ""

        obj = {"reg_number": reg, "name": name, "website": web, "financials": fin, "lists": lists}
        if "Best Immediate"        in lists: results["best"].append(obj)
        if "Readiness Package"     in lists: results["readiness"].append(obj)
        if "Contract Growth"       in lists: results["contract"].append(obj)
        if "Retention/Replacement" in lists: results["retention"].append(obj)
        processed += 1
        if processed >= 300: break

    out = {"lists": results, "counts": {k: len(v) for k, v in results.items()},
           "total": sum(len(v) for v in results.values()),
           "generated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
    cache_set("prospects", out)
    return jsonify(out)

# ── SEARCH ─────────────────────────────────────────────────────────────────────
@app.route("/api/search")
def api_search():
    term = request.args.get("q", "").strip()
    if not term: return jsonify([])
    results = []
    if term.isdigit():
        reg_numbers = [term]
    else:
        reg_numbers = []
        df = load_extract("charity")
        if df is not None and not df.empty:
            creg   = find_col(df, "registered_charity_number", "regno")
            cname  = find_col(df, "charity_name", "name")
            cstatus= find_col(df, "charity_registration_status", "reg_status")
            if cname and creg:
                sub = df.copy()
                if cstatus:
                    sub = sub[sub[cstatus].str.upper().str.strip().isin(["REGISTERED","R"])]
                matches = sub[sub[cname].str.upper().str.contains(term.upper(), na=False)]
                reg_numbers = matches[creg].dropna().astype(str).tolist()[:25]
        if not reg_numbers: return jsonify([])

    df_parta = load_extract("parta")
    df_charity = load_extract("charity")
    for reg in reg_numbers[:25]:
        c = api_get_charity(reg)
        name = c.get("charity_name", "") if c else ""
        removed = c.get("date_of_removal") if c else None
        details = {}
        if df_charity is not None and not df_charity.empty:
            creg  = find_col(df_charity, "registered_charity_number", "regno")
            cname = find_col(df_charity, "charity_name", "name")
            cweb  = find_col(df_charity, "charity_contact_web", "web")
            cphone= find_col(df_charity, "charity_contact_phone", "phone")
            cemail= find_col(df_charity, "charity_contact_email", "email")
            caddr3= find_col(df_charity, "charity_contact_address3")
            caddr4= find_col(df_charity, "charity_contact_address4")
            if creg:
                row = df_charity[df_charity[creg].astype(str).str.strip()==str(reg)]
                if not row.empty:
                    r = row.iloc[0]
                    def g(col): return str(r.get(col,"")) if col else ""
                    details = {"phone": g(cphone), "email": g(cemail), "town": g(caddr3),
                               "county": g(caddr4), "website": g(cweb)}
                    if not name and cname: name = g(cname)
        fin = {}
        if df_parta is not None and not df_parta.empty:
            rows = df_parta[df_parta["registered_charity_number"].astype(str).str.strip()==str(reg)]
            if not rows.empty:
                rows = rows.copy()
                rows["_s"] = pd.to_datetime(rows.get("fin_period_end_date",pd.Series(dtype=str)), errors="coerce")
                rows = rows.sort_values("_s", ascending=False)
                L = rows.iloc[0]; P = rows.iloc[1] if len(rows) > 1 else pd.Series(dtype=str)
                def fv(rec, *keys):
                    for k in keys:
                        v = rec.get(k)
                        if v not in (None,"nan","","None"):
                            try: return float(v)
                            except: pass
                    return 0.0
                gg=fv(L,"income_from_government_grants"); gc=fv(L,"income_from_government_contracts")
                ti=fv(L,"total_gross_income"); te=fv(L,"total_gross_expenditure")
                pg_=fv(P,"income_from_government_grants") if not P.empty else 0.0
                pc_=fv(P,"income_from_government_contracts") if not P.empty else 0.0
                stat=gg+gc; prev=pg_+pc_; pct=round(stat/ti*100,1) if ti>0 else 0.0
                fin = {"total_income":ti,"total_expenditure":te,"gov_grants":gg,
                       "gov_contracts":gc,"statutory":stat,"stat_pct":pct,
                       "prev_grants":pg_,"prev_contracts":pc_,"prev_statutory":prev,
                       "fin_year":str(L.get("fin_period_end_date",""))[:4]}
        if not fin:
            hist = api_get_financials(reg); fin = parse_financials(hist)
        fin["flags"] = build_flags(fin)
        results.append({"reg_number":str(reg),"name":name,
                        "website":details.get("website",""),
                        "phone":details.get("phone",""),"email":details.get("email",""),
                        "town":details.get("town",""),"county":details.get("county",""),
                        "status":"Removed" if removed else "Active",
                        "financials":fin,"lists":classify(fin)})
    return jsonify(results)

# ── ADVANCED SEARCH ────────────────────────────────────────────────────────────
@app.route("/api/advanced_search")
def advanced_search():
    name          = request.args.get("name","").strip()
    reg_number    = request.args.get("reg_number","").strip()
    postcode      = request.args.get("postcode","").strip()
    county        = request.args.get("county","").strip()
    what          = request.args.get("what","").strip()
    reg_status    = request.args.get("reg_status","registered").strip()
    inc_min       = request.args.get("inc_min","")
    inc_max       = request.args.get("inc_max","")
    reg_date_from = request.args.get("reg_date_from","").strip()
    reg_date_to   = request.args.get("reg_date_to","").strip()
    charity_type  = request.args.get("charity_type","").strip()

    df = load_extract("charity")
    if df is None or df.empty:
        return jsonify({"charities":[],"count":0,"error":"Could not load extract"})

    reg_col     = find_col(df,"registered_charity_number","regno")
    name_col    = find_col(df,"charity_name","name")
    status_col  = find_col(df,"charity_registration_status","reg_status")
    inc_col     = find_col(df,"latest_income","gross_income")
    date_col    = find_col(df,"date_of_registration","date_registered")
    web_col     = find_col(df,"charity_contact_web","web")
    phone_col   = find_col(df,"charity_contact_phone","phone")
    email_col   = find_col(df,"charity_contact_email","email")
    addr3_col   = find_col(df,"charity_contact_address3")
    addr4_col   = find_col(df,"charity_contact_address4")
    postcode_col= find_col(df,"charity_contact_postcode","postcode")
    what_col    = find_col(df,"charity_activities","activities")
    type_col    = find_col(df,"charity_type","type")

    sub = df.copy()
    if reg_status == "registered" and status_col:
        sub = sub[sub[status_col].str.upper().str.strip().isin(["REGISTERED","R"])]
    elif reg_status == "removed" and status_col:
        sub = sub[sub[status_col].str.upper().str.strip().isin(["REMOVED","D"])]
    if name     and name_col:    sub = sub[sub[name_col].str.upper().str.contains(name.upper(),na=False)]
    if reg_number and reg_col:   sub = sub[sub[reg_col].astype(str).str.strip()==reg_number]
    if postcode and postcode_col:sub = sub[sub[postcode_col].str.upper().str.startswith(postcode.upper(),na=False)]
    if county   and addr4_col:   sub = sub[sub[addr4_col].str.upper().str.contains(county.upper(),na=False)]
    if what     and what_col:    sub = sub[sub[what_col].str.upper().str.contains(what.upper(),na=False)]
    if charity_type and type_col:sub = sub[sub[type_col].str.upper().str.contains(charity_type.upper(),na=False)]
    if (inc_min or inc_max) and inc_col:
        sub[inc_col] = pd.to_numeric(sub[inc_col], errors="coerce").fillna(0)
        if inc_min:
            try: sub = sub[sub[inc_col] >= float(inc_min)]
            except: pass
        if inc_max:
            try: sub = sub[sub[inc_col] <= float(inc_max)]
            except: pass
    if (reg_date_from or reg_date_to) and date_col:
        sub["_dt"] = pd.to_datetime(sub[date_col].str[:10], errors="coerce")
        if reg_date_from:
            try: sub = sub[sub["_dt"] >= pd.Timestamp(reg_date_from)]
            except: pass
        if reg_date_to:
            try: sub = sub[sub["_dt"] <= pd.Timestamp(reg_date_to)]
            except: pass

    results = []
    for _, row in sub.head(200).iterrows():
        def g(col): return str(row.get(col,"")) if col else ""
        inc_val = 0
        if inc_col:
            try: inc_val = float(row.get(inc_col,0) or 0)
            except: pass
        web=g(web_col); web="" if web in ("nan","None","NaN") else web
        phone=g(phone_col); phone="" if phone in ("nan","None","NaN") else phone
        email=g(email_col); email="" if email in ("nan","None","NaN") else email
        results.append({
            "reg_number":     g(reg_col),
            "name":           g(name_col),
            "status":         g(status_col),
            "latest_income":  inc_val,
            "date_registered":g(date_col)[:10] if date_col else "",
            "website":        web,
            "phone":          phone,
            "email":          email,
            "town":           g(addr3_col),
            "county":         g(addr4_col),
            "postcode":       g(postcode_col),
            "activities":     g(what_col)[:200] if what_col else "",
            "charity_type":   g(type_col),
            "financials":     {},
            "lists":          [],
        })
    return jsonify({"charities": results, "count": len(results)})

# ── NEW CHARITIES ──────────────────────────────────────────────────────────────
@app.route("/api/new_charities")
def api_new_charities():
    days = int(request.args.get("days", 30))
    cached = cache_get(f"new_{days}", max_age=120)
    if cached: return jsonify(cached)
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    results = []
    df = load_extract("charity")
    if df is not None and not df.empty:
        reg_col    = "registered_charity_number"
        name_col   = "charity_name"
        date_col   = "date_of_registration"
        status_col = "charity_registration_status"
        web_col    = "charity_contact_web"
        if all(c in df.columns for c in [reg_col, name_col, date_col]):
            sub = df.copy()
            sub = sub[sub[status_col].str.upper().str.strip().isin(["REGISTERED","R"])]
            sub["_dt"] = pd.to_datetime(sub[date_col].str[:10], errors="coerce")
            filtered = sub[sub["_dt"].notna() & (sub["_dt"] >= pd.Timestamp(cutoff))]
            print(f"New charities (cutoff {cutoff}): {len(filtered)}")
            for _, row in filtered.iterrows():
                web = str(row.get(web_col,""))
                if web in ("nan","None","NaN",""): web=""
                results.append({"reg_number": str(row.get(reg_col,"")),
                                 "name":       str(row.get(name_col,"")),
                                 "date_registered": str(row.get("_dt",""))[:10],
                                 "website":    web, "status": "Active"})
    results.sort(key=lambda x: x.get("date_registered",""), reverse=True)
    out = {"charities": results, "count": len(results), "days": days, "cutoff": cutoff}
    cache_set(f"new_{days}", out)
    return jsonify(out)

# ── LATE ACCOUNTS ──────────────────────────────────────────────────────────────
@app.route("/api/late_accounts")
def api_late_accounts():
    cached = cache_get("late", max_age=120)
    if cached: return jsonify(cached)
    today = datetime.utcnow().date()
    results = []
    df = load_extract("parta")
    if df is not None and not df.empty:
        reg_col   = "registered_charity_number"
        due_col   = "ar_due_date"
        sub_col   = "latest_fin_period_submitted_ind"
        inc_col   = "total_gross_income"
        period_col= "fin_period_end_date"
        df["_sort"] = pd.to_datetime(df.get(period_col, pd.Series(dtype=str)), errors="coerce")
        latest = (df.sort_values("_sort", ascending=False)
                    .drop_duplicates(subset=[reg_col], keep="first").copy())
        latest["_due"] = pd.to_datetime(latest[due_col], errors="coerce")
        late = latest[latest["_due"].notna() & (latest["_due"].dt.date < today)].copy()
        if sub_col in late.columns:
            late = late[late[sub_col].astype(str).str.upper().str.strip().isin(["FALSE","0","NO","N"])]
        if inc_col in late.columns:
            late[inc_col] = pd.to_numeric(late[inc_col], errors="coerce").fillna(0)
        name_lookup = {}; name_lookup_nz = {}
        df_charity = load_extract("charity")
        if df_charity is not None and not df_charity.empty:
            creg  = find_col(df_charity, "registered_charity_number", "regno")
            cname = find_col(df_charity, "charity_name", "name")
            if creg and cname:
                ks = df_charity[creg].astype(str).str.strip()
                vs = df_charity[cname].astype(str).str.strip()
                name_lookup   = dict(zip(ks, vs))
                name_lookup_nz = dict(zip(ks.str.lstrip("0"), vs))
        print(f"Late accounts found: {len(late)}")
        for _, row in late.iterrows():
            due_d = row["_due"].date()
            ml    = round((today - due_d).days / 30.4, 1)
            reg   = str(row.get(reg_col,"")).strip()
            inc   = 0.0
            try: inc = float(row.get(inc_col,0) or 0)
            except: pass
            reg_nz = reg.lstrip("0")
            name = (name_lookup.get(reg,"") or name_lookup.get(reg_nz,"") or
                    name_lookup_nz.get(reg,"") or name_lookup_nz.get(reg_nz,"") or reg)
            if name in ("nan","None","NaN",""): name = reg
            results.append({"reg_number": reg, "name": name,
                            "due_date": str(due_d), "months_late": ml, "latest_income": inc})
    results.sort(key=lambda x: x.get("months_late",0), reverse=True)
    over12=len([r for r in results if r["months_late"]>12])
    b612  =len([r for r in results if 6<=r["months_late"]<=12])
    u6    =len([r for r in results if r["months_late"]<6])
    out = {"charities":results,"count":len(results),"over_12":over12,"bet_6_12":b612,"under_6":u6}
    cache_set("late", out)
    return jsonify(out)

# ── OLD CHARITIES (40+ years, zero statutory) ──────────────────────────────────
@app.route("/api/old_charities")
def api_old_charities():
    cached = cache_get("old_charities", max_age=120)
    if cached: return jsonify(cached)
    today = datetime.utcnow().date()
    cutoff_year = today.year - 40
    results = []
    df_charity = load_extract("charity")
    df_parta   = load_extract("parta")
    if df_charity is not None and not df_charity.empty:
        reg_col    = "registered_charity_number"
        name_col   = "charity_name"
        date_col   = "date_of_registration"
        status_col = "charity_registration_status"
        web_col    = "charity_contact_web"
        sub = df_charity.copy()
        sub = sub[sub[status_col].str.upper().str.strip().isin(["REGISTERED","R"])]
        sub["_dt"] = pd.to_datetime(sub[date_col].str[:10], errors="coerce")
        old = sub[sub["_dt"].notna() & (sub["_dt"].dt.year <= cutoff_year)].copy()
        print(f"Charities 40+ years old: {len(old)}")
        stat_lookup = {}
        if df_parta is not None and not df_parta.empty:
            df_p = df_parta.copy()
            df_p["_sort"] = pd.to_datetime(df_p.get("fin_period_end_date",pd.Series(dtype=str)),errors="coerce")
            lp = df_p.sort_values("_sort",ascending=False).drop_duplicates(subset=[reg_col],keep="first")
            for col in ["income_from_government_grants","income_from_government_contracts","total_gross_income"]:
                if col in lp.columns:
                    lp = lp.copy()
                    lp[col] = pd.to_numeric(lp[col],errors="coerce").fillna(0)
            for _,row in lp.iterrows():
                reg = str(row.get(reg_col,"")).strip()
                gg  = float(row.get("income_from_government_grants",0) or 0)
                gc  = float(row.get("income_from_government_contracts",0) or 0)
                ti  = float(row.get("total_gross_income",0) or 0)
                yr  = str(row.get("fin_period_end_date",""))[:4]
                stat_lookup[reg] = {"statutory":gg+gc,"total_income":ti,"fin_year":yr}
        for _,row in old.iterrows():
            reg = str(row.get(reg_col,"")).strip()
            if not reg: continue
            fin = stat_lookup.get(reg,{})
            if fin.get("statutory",0) > 0: continue
            web  = str(row.get(web_col,""))
            if web in ("nan","None","NaN",""): web=""
            name = str(row.get(name_col,""))
            if name in ("nan","None","NaN",""): name=reg
            reg_date = str(row.get("_dt",""))[:10]
            age_years = today.year - row["_dt"].year
            results.append({"reg_number":reg,"name":name,"website":web,
                            "date_registered":reg_date,"age_years":age_years,
                            "total_income":fin.get("total_income",0),
                            "fin_year":fin.get("fin_year","")})
    results.sort(key=lambda x:x.get("date_registered",""))
    out = {"charities":results,"count":len(results)}
    cache_set("old_charities", out)
    return jsonify(out)

# ── CALLED API ─────────────────────────────────────────────────────────────────
@app.route("/api/mark_called", methods=["POST"])
def mark_called():
    data = request.json or {}
    reg  = str(data.get("reg_number","")).strip()
    page = str(data.get("page","")).strip()
    name = str(data.get("name","")).strip()
    if not reg or not page: return jsonify({"ok":False}), 400
    key = f"{page}|{reg}"
    if key in _called_log:
        del _called_log[key]
        return jsonify({"ok":True,"marked":False})
    _called_log[key] = {"reg_number":reg,"name":name,"page":page,
                        "called_by":"Muhanna",
                        "timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
    return jsonify({"ok":True,"marked":True})

# ── COMMENT API ────────────────────────────────────────────────────────────────
@app.route("/api/comment", methods=["POST"])
def save_comment():
    data = request.json or {}
    reg  = str(data.get("reg_number","")).strip()
    page = str(data.get("page","")).strip()
    text = str(data.get("comment","")).strip()[:400]
    if not reg or not page: return jsonify({"ok":False}), 400
    _comments[f"{page}|{reg}"] = text
    return jsonify({"ok":True})

# ── STATUS API ─────────────────────────────────────────────────────────────────
@app.route("/api/status", methods=["POST"])
def save_status():
    data   = request.json or {}
    reg    = str(data.get("reg_number","")).strip()
    page   = str(data.get("page","")).strip()
    status = str(data.get("status","")).strip()
    if not reg or not page: return jsonify({"ok":False}), 400
    key = f"{page}|{reg}"
    if status:
        _statuses[key] = status
        if key not in _called_log:
            _called_log[key] = {"reg_number":reg,"page":page,"called_by":"Muhanna",
                                "timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
    else:
        _statuses.pop(key, None)
    return jsonify({"ok":True})

# ── SAVED SEARCHES ─────────────────────────────────────────────────────────────
@app.route("/api/saved_searches", methods=["GET"])
def get_saved_searches():
    return jsonify(list(_saved_searches.keys()))

@app.route("/api/saved_searches", methods=["POST"])
def save_search():
    data = request.json or {}
    name = str(data.get("name","")).strip()
    criteria = data.get("criteria",{})
    if not name: return jsonify({"ok":False,"error":"Name required"}), 400
    _saved_searches[name] = criteria
    return jsonify({"ok":True})

@app.route("/api/saved_searches/<name>", methods=["GET"])
def get_saved_search(name):
    if name not in _saved_searches: return jsonify({"error":"Not found"}), 404
    return jsonify(_saved_searches[name])

@app.route("/api/saved_searches/<name>", methods=["DELETE"])
def delete_saved_search(name):
    _saved_searches.pop(name, None)
    return jsonify({"ok":True})

# ── DOWNLOADS ──────────────────────────────────────────────────────────────────
PROSPECT_BASE_HDRS = ["Charity Number","Charity Name","Website","Prospect List",
    "Total Income","Total Expenditure","Gov Grants","Gov Contracts",
    "Statutory Total","Statutory %","Prev Statutory","Financial Year"]
ALL_CONTACT_HDRS = PROSPECT_BASE_HDRS + CONTACT_HDRS

@app.route("/download/prospects")
def dl_prospects():
    lf = request.args.get("list","all")
    cached = cache_get("prospects")
    if not cached: return "Load Prospects tab first.",400
    map_ = {"best":"Best Immediate","readiness":"Readiness Package",
            "contract":"Contract Growth","retention":"Retention/Replacement"}
    items = []
    if lf=="all":
        for k,l in map_.items():
            for c in cached["lists"].get(k,[]): items.append((c,l))
    else:
        l = map_.get(lf,lf)
        for c in cached["lists"].get(lf,[]): items.append((c,l))
    rows = []
    for c,label in items:
        f   = c.get("financials",{})
        reg = c.get("reg_number","")
        base = [reg,c.get("name",""),c.get("website",""),label,
                f.get("total_income",0),f.get("total_expenditure",0),
                f.get("gov_grants",0),f.get("gov_contracts",0),
                f.get("statutory",0),f.get("stat_pct",0),
                f.get("prev_statutory",0),f.get("fin_year","")]
        rows.append(base + contact_row(reg,"prospects"))
    return csv_response(rows, ALL_CONTACT_HDRS,
                        f"Statutory_Prospects_{lf}_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/download/new_charities")
def dl_new():
    days = int(request.args.get("days",30))
    cached = cache_get(f"new_{days}")
    if not cached: return "Load New Charities tab first.",400
    hdrs = ["Charity Number","Charity Name","Date Registered","Website","Status"] + CONTACT_HDRS
    rows = []
    for c in cached["charities"]:
        reg = c["reg_number"]
        rows.append([reg,c["name"],c["date_registered"],c["website"],c["status"]]
                    + contact_row(reg,"new"))
    return csv_response(rows, hdrs, f"New_Charities_{days}days_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/download/late_accounts")
def dl_late():
    cached = cache_get("late")
    if not cached: return "Load Late Accounts tab first.",400
    def sev(m): return "Severe" if m>12 else "Moderate" if m>=6 else "Recent"
    hdrs = ["Charity Number","Charity Name","Accounts Due Date","Months Late","Latest Income","Severity"] + CONTACT_HDRS
    rows = []
    for c in cached["charities"]:
        reg = c["reg_number"]
        rows.append([reg,c["name"],c["due_date"],c["months_late"],c["latest_income"],sev(c["months_late"])]
                    + contact_row(reg,"late"))
    return csv_response(rows, hdrs, f"Late_Accounts_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/download/old_charities")
def dl_old():
    cached = cache_get("old_charities")
    if not cached: return "Load Old Charities tab first.",400
    hdrs = ["Charity Number","Charity Name","Website","Date Registered","Age (Years)",
            "Latest Income","Financial Year"] + CONTACT_HDRS
    rows = []
    for c in cached["charities"]:
        reg = c["reg_number"]
        rows.append([reg,c["name"],c["website"],c["date_registered"],
                     c["age_years"],c["total_income"],c["fin_year"]]
                    + contact_row(reg,"old"))
    return csv_response(rows, hdrs,
                        f"Old_Charities_Zero_Statutory_{datetime.utcnow().strftime('%Y%m%d')}.csv")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
