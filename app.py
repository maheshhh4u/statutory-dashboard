import os, io, csv, zipfile, requests, threading, json
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

app = Flask(__name__)

API_KEY  = os.environ.get("CC_API_KEY", "80c5abfc86864f8d9330288c3521bb78")
BULK_BASE= "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/txt"
PARTA_URL  = f"{BULK_BASE}/publicextract.charity_annual_return_parta.zip"
CHARITY_URL= f"{BULK_BASE}/publicextract.charity.zip"
CLASSIF_URL= f"{BULK_BASE}/publicextract.charity_classification.zip"
CC_API_BASE = "https://api.charitycommission.gov.uk/register/api"
AREA_URL   = f"{BULK_BASE}/publicextract.charity_area_of_operation.zip"

# ─── Turso DB ─────────────────────────────────────────────────────────────────
TURSO_URL   = os.environ.get("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")
_db_lock = threading.Lock()
_db = None

def _new_conn():
    """Create a fresh Turso client per call — pure HTTP, no Rust runtime."""
    if not TURSO_URL or not TURSO_TOKEN: return None
    try:
        import libsql_client
        # Force HTTPS - libsql-client otherwise tries wss:// which Turso rejects with 505
        url = TURSO_URL.replace("libsql://", "https://", 1)
        return libsql_client.create_client_sync(url=url, auth_token=TURSO_TOKEN)
    except Exception as e:
        print(f"[Turso] connect failed: {e}")
        return None

def db_init():
    """Create tables if they don't exist."""
    client = _new_conn()
    if not client: return
    try:
        client.execute("""CREATE TABLE IF NOT EXISTS users (
            name TEXT PRIMARY KEY,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        client.execute("""CREATE TABLE IF NOT EXISTS called_log (
            key TEXT PRIMARY KEY,
            reg_number TEXT, page TEXT, name TEXT,
            called_by TEXT, timestamp TEXT, data TEXT
        )""")
        client.execute("""CREATE TABLE IF NOT EXISTS comments (
            key TEXT PRIMARY KEY, text TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        # Comment history — every comment ever added (for viewing past notes)
        client.execute("""CREATE TABLE IF NOT EXISTS comment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reg_number TEXT, page TEXT, text TEXT,
            caller TEXT, timestamp TEXT,
            synced_to_maximizer INTEGER DEFAULT 0
        )""")
        client.execute("CREATE INDEX IF NOT EXISTS idx_ch_reg ON comment_history(reg_number)")
        client.execute("""CREATE TABLE IF NOT EXISTS statuses (
            key TEXT PRIMARY KEY, status TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        client.execute("""CREATE TABLE IF NOT EXISTS saved_searches (
            name TEXT PRIMARY KEY, criteria TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        # Phone overrides — user-edited phone numbers per charity
        # (Table also holds email + website overrides — kept this name for migration compat)
        client.execute("""CREATE TABLE IF NOT EXISTS phone_overrides (
            reg_number TEXT PRIMARY KEY,
            phone TEXT,
            updated_by TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        # Idempotent column adds for email + website (no-op if already present)
        for _col in ("email","website"):
            try: client.execute(f"ALTER TABLE phone_overrides ADD COLUMN {_col} TEXT")
            except Exception: pass
        # Call log — every call made through dashboard
        client.execute("""CREATE TABLE IF NOT EXISTS call_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reg_number TEXT, page TEXT,
            phone_used TEXT, outcome TEXT, notes TEXT,
            duration_sec INTEGER DEFAULT 0,
            caller TEXT, timestamp TEXT,
            synced_to_maximizer INTEGER DEFAULT 0
        )""")
        client.execute("CREATE INDEX IF NOT EXISTS idx_call_reg ON call_log(reg_number)")
        # Seed default user if empty
        try:
            rs = client.execute("SELECT COUNT(*) FROM users")
            if rs.rows and rs.rows[0][0] == 0:
                client.execute("INSERT INTO users(name) VALUES(?)", ["Muhanna"])
        except: pass
        print("[Turso] Tables initialized")
    except Exception as e:
        print(f"[Turso] db_init error: {e}")
    finally:
        try: client.close()
        except: pass

def db_exec(sql, params=()):
    """Execute write with a fresh short-lived client."""
    client = _new_conn()
    if not client: return None
    try:
        client.execute(sql, list(params) if params else [])
        return True
    except Exception as e:
        print(f"[Turso] db_exec error on '{sql[:50]}': {e}")
        return None
    finally:
        try: client.close()
        except: pass

def db_query(sql, params=()):
    """Read query with a fresh short-lived client. Returns list of tuples."""
    client = _new_conn()
    if not client: return []
    try:
        rs = client.execute(sql, list(params) if params else [])
        return [tuple(r) for r in rs.rows]
    except Exception as e:
        print(f"[Turso] db_query error on '{sql[:50]}': {e}")
        return []
    finally:
        try: client.close()
        except: pass

# ─── Cache and load in-memory state from Turso ────────────────────────────────
_cache={}; _called_log={}; _comments={}; _statuses={}; _saved_searches={}
_contact_overrides={}   # {reg_number: {phone,email,website}} user-edited contact details
_bg_status={}; _bg_lock=threading.Lock()
_users=["Muhanna"]  # admin-managed caller list

def db_load_state():
    """Load all DB state into in-memory dicts. Clears existing state first."""
    global _users, _called_log, _comments, _statuses, _saved_searches, _contact_overrides
    if not TURSO_URL or not TURSO_TOKEN: return

    try:
        users = db_query("SELECT name FROM users ORDER BY added_at")
        if users:
            _users = [r[0] for r in users]
        print(f"[Turso] Loaded {len(_users)} users")

        # Clear existing state so removed rows are also removed from memory
        _called_log.clear()
        _comments.clear()
        _statuses.clear()
        _saved_searches.clear()

        rows = db_query("SELECT key, reg_number, page, name, called_by, timestamp, data FROM called_log")
        for r in rows:
            extra = json.loads(r[6]) if r[6] else {}
            _called_log[r[0]] = {"reg_number": r[1], "page": r[2], "name": r[3],
                                  "called_by": r[4], "timestamp": r[5], **extra}
        print(f"[Turso] Loaded {len(_called_log)} called_log entries")

        rows = db_query("SELECT key, text FROM comments")
        for r in rows: _comments[r[0]] = r[1]
        print(f"[Turso] Loaded {len(_comments)} comments")

        rows = db_query("SELECT key, status FROM statuses")
        for r in rows: _statuses[r[0]] = r[1]
        print(f"[Turso] Loaded {len(_statuses)} statuses")

        rows = db_query("SELECT name, criteria FROM saved_searches")
        for r in rows:
            try: _saved_searches[r[0]] = json.loads(r[1])
            except: _saved_searches[r[0]] = {}
        print(f"[Turso] Loaded {len(_saved_searches)} saved searches")

        _contact_overrides.clear()
        try:
            rows = db_query("SELECT reg_number, phone, email, website FROM phone_overrides")
            for r in rows:
                d = {}
                if r[1]: d["phone"]   = r[1]
                if r[2]: d["email"]   = r[2]
                if r[3]: d["website"] = r[3]
                if d: _contact_overrides[r[0]] = d
            print(f"[Turso] Loaded {len(_contact_overrides)} contact overrides")
        except Exception as e:
            # Fallback if email/website columns aren't there yet (very first deploy)
            rows = db_query("SELECT reg_number, phone FROM phone_overrides")
            for r in rows:
                if r[1]: _contact_overrides[r[0]] = {"phone": r[1]}
            print(f"[Turso] Loaded {len(_contact_overrides)} phone-only overrides (legacy schema)")
    except Exception as e:
        print(f"[Turso] db_load_state error: {e}")

# Initialize on import - never let DB issues crash the app
try:
    db_init()
    db_load_state()
except Exception as _e:
    print(f"[Turso] FATAL during init (continuing without DB): {_e}")
    import traceback; traceback.print_exc()

def cache_get(key,max_age=180):
    if key in _cache:
        ts,val=_cache[key]
        if (datetime.utcnow()-ts).total_seconds()<max_age*60: return val
    return None
def cache_set(key,val): _cache[key]=(datetime.utcnow(),val)
def flt(v):
    try: return float(v) if v else 0.0
    except: return 0.0

def stream_zip_csv(url, needed_cols=None):
    r=requests.get(url,timeout=180,stream=True)
    r.raise_for_status()
    buf=io.BytesIO()
    for chunk in r.iter_content(chunk_size=1024*1024): buf.write(chunk)
    buf.seek(0)
    print(f"  Downloaded {buf.getbuffer().nbytes//1024}KB from {url.split('/')[-1]}")
    with zipfile.ZipFile(buf) as z:
        inner=z.namelist()[0]
        with z.open(inner) as f:
            header=f.readline().decode('utf-8',errors='replace').rstrip('\r\n')
            all_cols=[c.strip().lower() for c in header.split('\t')]
            keep={i:c for i,c in enumerate(all_cols) if not needed_cols or c in needed_cols}
            count=0
            for raw in f:
                try:
                    fields=raw.decode('utf-8',errors='replace').rstrip('\r\n').split('\t')
                    row={c:(fields[i].strip() if i<len(fields) else '') for i,c in keep.items()}
                    row={k:('' if v.lower() in ('nan','none') else v) for k,v in row.items()}
                    yield row; count+=1
                except: continue
    print(f"  Streamed {count} rows")

def classify(ti,stat,pct,cont):
    l=[]
    if 250_000<=ti<=3_000_000 and 50_000<=stat<=300_000 and 10<=pct<=40: l.append("Best Immediate")
    if 150_000<=ti<=1_500_000 and 10_000<=stat<=100_000: l.append("Readiness Package")
    if 500_000<=ti<=5_000_000 and 100_000<=cont<=500_000 and (cont/ti*100<35 if ti>0 else False): l.append("Contract Growth")
    if 500_000<=ti<=5_000_000 and stat>=200_000 and 40<=pct<=70: l.append("Retention/Replacement")
    return l

def build_flags(ti,te,stat,pstat,pgc,cont):
    f=[]
    if ti>0 and te>=ti*0.95: f.append({"label":"Spend≥Income","cls":"red"})
    if pstat>0 and stat<pstat*0.9: f.append({"label":"Stat↓10%+","cls":"orange"})
    if pgc>0 and cont==0: f.append({"label":"Lost contracts","cls":"red"})
    return f

def bg_run(key,fn):
    with _bg_lock:
        if _bg_status.get(key)=="loading": return
        _bg_status[key]="loading"
    def _w():
        try:
            result=fn(); cache_set(key,result); print(f"BG {key}: done")
        except Exception as e:
            import traceback; print(f"BG {key} ERROR: {traceback.format_exc()}")
        finally:
            with _bg_lock: _bg_status[key]="idle"
    threading.Thread(target=_w,daemon=True).start()

def bg_check(key,empty):
    cached=cache_get(key,max_age=60)
    if cached: return jsonify(cached)
    with _bg_lock: s=_bg_status.get(key,"idle")
    if s=="loading": return jsonify({"status":"loading","message":"Loading…",**empty}),202
    return None

def compute_prospects():
    PC={"registered_charity_number","income_from_government_grants",
        "income_from_government_contracts","total_gross_income",
        "total_gross_expenditure","fin_period_end_date"}
    CC={"registered_charity_number","charity_name","charity_contact_web",
        "charity_registration_status","charity_contact_phone","charity_contact_email",
        "charity_contact_address3","charity_contact_address4"}
    latest={}; prev={}
    for row in stream_zip_csv(PARTA_URL,PC):
        reg=row.get("registered_charity_number","").strip()
        if not reg: continue
        yr=row.get("fin_period_end_date","")[:10]
        if reg not in latest or yr>latest[reg].get("fin_period_end_date",""):
            if reg in latest: prev[reg]=latest[reg]
            latest[reg]=row
        elif reg not in prev or yr>prev[reg].get("fin_period_end_date",""):
            prev[reg]=row

    # PRE-FILTER: figure out which charities pass financial criteria BEFORE loading contacts
    # This saves ~200MB by only storing contact info for ~5K prospects, not all 200K charities
    prospect_regs = set()
    for reg, row in latest.items():
        ti=flt(row.get("total_gross_income"))
        if ti<150_000 or ti>5_000_000: continue
        gg=flt(row.get("income_from_government_grants"))
        gc=flt(row.get("income_from_government_contracts"))
        stat=gg+gc; pct=round(stat/ti*100,1) if ti>0 else 0.0
        if classify(ti,stat,pct,gc):
            prospect_regs.add(reg)
            prospect_regs.add(reg.lstrip("0"))

    # Only load contact info for prospects (much smaller memory footprint)
    contacts={}
    for row in stream_zip_csv(CHARITY_URL,CC):
        reg=row.get("registered_charity_number","").strip()
        if reg in prospect_regs and row.get("charity_name",""):
            contacts[reg]={"name":row.get("charity_name",""),
                           "website":row.get("charity_contact_web",""),
                           "phone":row.get("charity_contact_phone",""),
                           "email":row.get("charity_contact_email",""),
                           "town":row.get("charity_contact_address3",""),
                           "county":row.get("charity_contact_address4",""),
                           "status":row.get("charity_registration_status","")}

    results={"best":[],"readiness":[],"contract":[],"retention":[]}
    for reg,row in latest.items():
        ti=flt(row.get("total_gross_income"))
        if ti<150_000 or ti>5_000_000: continue
        te=flt(row.get("total_gross_expenditure"))
        gg=flt(row.get("income_from_government_grants"))
        gc=flt(row.get("income_from_government_contracts"))
        stat=gg+gc; pct=round(stat/ti*100,1) if ti>0 else 0.0
        yr=row.get("fin_period_end_date","")[:4]
        pr=prev.get(reg,{}); pgg=flt(pr.get("income_from_government_grants")); pgc=flt(pr.get("income_from_government_contracts")); pstat=pgg+pgc
        lists=classify(ti,stat,pct,gc)
        if not lists: continue
        ct=contacts.get(reg,{}) or contacts.get(reg.lstrip("0"),{}) or {}
        fin={"total_income":ti,"total_expenditure":te,"gov_grants":gg,"gov_contracts":gc,
             "statutory":stat,"stat_pct":pct,"prev_grants":pgg,"prev_contracts":pgc,
             "prev_statutory":pstat,"fin_year":yr,"flags":build_flags(ti,te,stat,pstat,pgc,gc)}
        obj={"reg_number":reg,"name":ct.get("name",""),"website":ct.get("website",""),
             "phone":ct.get("phone",""),"email":ct.get("email",""),
             "town":ct.get("town",""),"county":ct.get("county",""),
             "removed":str(ct.get("status","")).upper().strip() in ("REMOVED","D"),
             "financials":fin,"lists":lists}
        if "Best Immediate" in lists: results["best"].append(obj)
        if "Readiness Package" in lists: results["readiness"].append(obj)
        if "Contract Growth" in lists: results["contract"].append(obj)
        if "Retention/Replacement" in lists: results["retention"].append(obj)
    return {"lists":results,"counts":{k:len(v) for k,v in results.items()},
            "total":sum(len(v) for v in results.values()),
            "generated":datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}

def compute_new(days):
    cutoff=(datetime.utcnow()-timedelta(days=int(days))).strftime("%Y-%m-%d")
    COLS={"registered_charity_number","charity_name","date_of_registration",
          "charity_registration_status","charity_contact_web",
          "charity_contact_phone","charity_contact_email",
          "charity_contact_address3","charity_contact_address4"}
    results=[]
    for row in stream_zip_csv(CHARITY_URL,COLS):
        if row.get("charity_registration_status","").upper().strip() not in ("REGISTERED","R"): continue
        dt=row.get("date_of_registration","")[:10]
        if dt>=cutoff:
            results.append({"reg_number":row.get("registered_charity_number",""),
                            "name":row.get("charity_name",""),"date_registered":dt,
                            "website":row.get("charity_contact_web",""),
                            "phone":row.get("charity_contact_phone",""),
                            "email":row.get("charity_contact_email",""),
                            "town":row.get("charity_contact_address3",""),
                            "county":row.get("charity_contact_address4",""),
                            "status":"Active"})
    results.sort(key=lambda x:x.get("date_registered",""),reverse=True)
    return {"charities":results,"count":len(results),"days":days,"cutoff":cutoff}

def compute_late():
    today=datetime.utcnow().date()
    PC={"registered_charity_number","ar_due_date","latest_fin_period_submitted_ind",
        "total_gross_income","fin_period_end_date"}
    NC={"registered_charity_number","charity_name","charity_contact_phone",
        "charity_contact_email","charity_contact_address3","charity_contact_address4",
        "charity_contact_web","charity_registration_status"}
    latest={}
    for row in stream_zip_csv(PARTA_URL,PC):
        reg=row.get("registered_charity_number","").strip()
        if not reg: continue
        yr=row.get("fin_period_end_date","")[:10]
        if reg not in latest or yr>latest[reg].get("fin_period_end_date",""): latest[reg]=row

    # PRE-FILTER: determine which charities are late, then only load contacts for those
    late_regs = set()
    late_data = {}
    for reg,row in latest.items():
        if row.get("latest_fin_period_submitted_ind","").upper().strip() not in ("FALSE","0","NO","N"): continue
        due_str=row.get("ar_due_date","")[:10]
        if not due_str: continue
        try: due_d=datetime.strptime(due_str,"%Y-%m-%d").date()
        except: continue
        if due_d>=today: continue
        ml=round((today-due_d).days/30.4,1)
        late_regs.add(reg)
        late_data[reg]={"due_date":str(due_d),"months_late":ml,
                        "latest_income":flt(row.get("total_gross_income",""))}

    # Only load contact info for late charities (much smaller dict)
    contacts={}
    for row in stream_zip_csv(CHARITY_URL,NC):
        reg=row.get("registered_charity_number","").strip()
        if reg in late_regs:
            contacts[reg]={"name":row.get("charity_name",""),
                           "phone":row.get("charity_contact_phone",""),
                           "email":row.get("charity_contact_email",""),
                           "town":row.get("charity_contact_address3",""),
                           "county":row.get("charity_contact_address4",""),
                           "website":row.get("charity_contact_web",""),
                           "status":row.get("charity_registration_status","")}

    results=[]
    for reg, d in late_data.items():
        ct=contacts.get(reg,{})
        results.append({"reg_number":reg,"name":ct.get("name",reg) or reg,
                        "due_date":d["due_date"],"months_late":d["months_late"],
                        "latest_income":d["latest_income"],
                        "phone":ct.get("phone",""),"email":ct.get("email",""),
                        "town":ct.get("town",""),"county":ct.get("county",""),
                        "website":ct.get("website",""),
                        "removed":str(ct.get("status","")).upper().strip() in ("REMOVED","D")})
    results.sort(key=lambda x:x.get("months_late",0),reverse=True)
    o12=len([r for r in results if r["months_late"]>12])
    b612=len([r for r in results if 6<=r["months_late"]<=12])
    u6=len([r for r in results if r["months_late"]<6])
    return {"charities":results,"count":len(results),"over_12":o12,"bet_6_12":b612,"under_6":u6}

def compute_old():
    today=datetime.utcnow().date(); cutoff_yr=today.year-40
    PC={"registered_charity_number","income_from_government_grants",
        "income_from_government_contracts","total_gross_income","fin_period_end_date"}
    CC={"registered_charity_number","charity_name","date_of_registration",
        "charity_registration_status","charity_contact_web",
        "charity_contact_phone","charity_contact_email",
        "charity_contact_address3","charity_contact_address4"}
    pl={}
    for row in stream_zip_csv(PARTA_URL,PC):
        reg=row.get("registered_charity_number","").strip()
        if not reg: continue
        yr=row.get("fin_period_end_date","")[:10]
        if reg not in pl or yr>pl[reg].get("fin_period_end_date",""): pl[reg]=row
    results=[]
    for row in stream_zip_csv(CHARITY_URL,CC):
        if row.get("charity_registration_status","").upper().strip() not in ("REGISTERED","R"): continue
        dt=row.get("date_of_registration","")[:10]
        if not dt or len(dt)<4: continue
        try: ry=int(dt[:4])
        except: continue
        if ry>cutoff_yr: continue
        reg=row.get("registered_charity_number","").strip()
        if not reg: continue
        fin=pl.get(reg,{})
        if flt(fin.get("income_from_government_grants"))+flt(fin.get("income_from_government_contracts"))>0: continue
        web=row.get("charity_contact_web","")
        results.append({"reg_number":reg,"name":row.get("charity_name","") or reg,
                        "website":web if web not in ("nan","None") else "",
                        "phone":row.get("charity_contact_phone",""),
                        "email":row.get("charity_contact_email",""),
                        "town":row.get("charity_contact_address3",""),
                        "county":row.get("charity_contact_address4",""),
                        "date_registered":dt,"age_years":today.year-ry,
                        "total_income":flt(fin.get("total_gross_income","")),
                        "fin_year":fin.get("fin_period_end_date","")[:4]})
    results.sort(key=lambda x:x.get("date_registered",""))
    return {"charities":results,"count":len(results)}

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/prospects")
def api_prospects():
    r=bg_check("prospects",{"lists":{"best":[],"readiness":[],"contract":[],"retention":[]},"counts":{},"total":0})
    if r: return r
    bg_run("prospects",compute_prospects)
    return jsonify({"status":"loading","message":"Loading charity data from Charity Commission…",
                    "lists":{"best":[],"readiness":[],"contract":[],"retention":[]},"counts":{},"total":0}),202

@app.route("/api/new_charities")
def api_new_charities():
    days=request.args.get("days","30"); key=f"new_{days}"
    r=bg_check(key,{"charities":[],"count":0,"days":days,"cutoff":""})
    if r: return r
    bg_run(key,lambda:compute_new(days))
    return jsonify({"status":"loading","message":"Loading newly registered charities…",
                    "charities":[],"count":0,"days":days,"cutoff":""}),202

@app.route("/api/late_accounts")
def api_late_accounts():
    r=bg_check("late",{"charities":[],"count":0,"over_12":0,"bet_6_12":0,"under_6":0})
    if r: return r
    bg_run("late",compute_late)
    return jsonify({"status":"loading","message":"Loading late accounts…",
                    "charities":[],"count":0,"over_12":0,"bet_6_12":0,"under_6":0}),202

@app.route("/api/old_charities")
def api_old_charities():
    r=bg_check("old_charities",{"charities":[],"count":0})
    if r: return r
    bg_run("old_charities",compute_old)
    return jsonify({"status":"loading","message":"Loading established charities…",
                    "charities":[],"count":0}),202

@app.route("/api/search")
def api_search():
    term=request.args.get("q","").strip()
    if not term: return jsonify([])
    COLS={"registered_charity_number","charity_name","charity_registration_status",
          "charity_contact_web","charity_contact_phone","charity_contact_email",
          "charity_contact_address3","charity_contact_address4","date_of_removal"}
    results=[]; is_num=term.isdigit()
    for row in stream_zip_csv(CHARITY_URL,COLS):
        reg=row.get("registered_charity_number","").strip()
        name=row.get("charity_name","")
        if is_num:
            if reg!=term: continue
        else:
            if term.upper() not in name.upper(): continue
        rem=row.get("date_of_removal","")
        results.append({"reg_number":reg,"name":name,"status":"Removed" if rem else "Active",
                        "phone":row.get("charity_contact_phone",""),"email":row.get("charity_contact_email",""),
                        "town":row.get("charity_contact_address3",""),"county":row.get("charity_contact_address4",""),
                        "website":row.get("charity_contact_web",""),"financials":{},"lists":[]})
        if len(results)>=25: break
    return jsonify(results)

@app.route("/api/advanced_search")
def advanced_search():
    nq=request.args.get("name","").upper().strip()
    rq=request.args.get("reg_number","").strip()
    pq=request.args.get("postcode","").upper().strip()
    cq=request.args.get("county","").upper().strip()
    wq=request.args.get("what","").upper().strip()
    sq=request.args.get("reg_status","registered").strip()
    imin=flt(request.args.get("inc_min","")); imax=flt(request.args.get("inc_max","")) or float("inf")
    df=request.args.get("reg_date_from","").strip(); dt=request.args.get("reg_date_to","").strip()
    tq=request.args.get("charity_type","").upper().strip()
    # Optional limit (default 50000 - allows full UK charity range, pagination handles UI rendering)
    try: limit = max(1, min(int(request.args.get("limit","50000")), 200000))
    except: limit = 50000
    COLS={"registered_charity_number","charity_name","charity_registration_status",
          "date_of_registration","date_of_removal","charity_contact_web",
          "charity_contact_phone","charity_contact_email",
          "charity_contact_address3","charity_contact_address4",
          "charity_contact_postcode","charity_activities","charity_type","latest_income"}
    results=[]
    truncated=False
    for row in stream_zip_csv(CHARITY_URL,COLS):
        st=row.get("charity_registration_status","").upper().strip()
        if sq=="registered" and st not in ("REGISTERED","R"): continue
        if sq=="removed" and st not in ("REMOVED","D"): continue
        name=row.get("charity_name","")
        if nq and nq not in name.upper(): continue
        reg=row.get("registered_charity_number","").strip()
        if rq and rq!=reg: continue
        pc=row.get("charity_contact_postcode","").upper()
        if pq and not pc.startswith(pq): continue
        county=row.get("charity_contact_address4","").upper()
        if cq and cq not in county: continue
        what=row.get("charity_activities","").upper()
        if wq and wq not in what: continue
        ctype=row.get("charity_type","").upper()
        if tq and tq not in ctype: continue
        inc=flt(row.get("latest_income",""))
        if imin and inc<imin: continue
        if imax<float("inf") and inc>imax: continue
        dtreg=row.get("date_of_registration","")[:10]
        if df and dtreg<df: continue
        if dt and dtreg>dt: continue
        results.append({"reg_number":reg,"name":name,"status":st,"latest_income":inc,
                        "date_registered":dtreg,"website":row.get("charity_contact_web",""),
                        "phone":row.get("charity_contact_phone",""),"email":row.get("charity_contact_email",""),
                        "town":row.get("charity_contact_address3",""),"county":row.get("charity_contact_address4",""),
                        "postcode":pc,"activities":row.get("charity_activities","")[:200],
                        "charity_type":row.get("charity_type",""),"financials":{},"lists":[]})
        if len(results)>=limit:
            truncated=True
            break
    return jsonify({"charities":results,"count":len(results),"truncated":truncated,"limit":limit})

@app.route("/api/mark_called",methods=["POST"])
def mark_called():
    data=request.json or {}; reg=str(data.get("reg_number","")).strip(); page=str(data.get("page","")).strip()
    name=str(data.get("name","")).strip()
    if not reg or not page: return jsonify({"ok":False}),400
    key=f"{page}|{reg}"
    if key in _called_log:
        del _called_log[key]
        db_exec("DELETE FROM called_log WHERE key=?", (key,))
        return jsonify({"ok":True,"marked":False})
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    _called_log[key]={"reg_number":reg,"name":name,"page":page,"called_by":"Muhanna","timestamp":ts}
    db_exec("INSERT OR REPLACE INTO called_log(key,reg_number,page,name,called_by,timestamp,data) VALUES(?,?,?,?,?,?,?)",
            (key, reg, page, name, "Muhanna", ts, "{}"))
    return jsonify({"ok":True,"marked":True})

@app.route("/api/comment",methods=["POST"])
def save_comment():
    data = request.json or {}
    reg  = str(data.get("reg_number","")).strip()
    page = str(data.get("page","")).strip()
    text = str(data.get("comment","")).strip()[:400]
    caller = str(data.get("caller","")).strip() or "Unknown"
    if not reg or not page: return jsonify({"ok":False}),400
    key = f"{page}|{reg}"

    # Save latest in comments (for dashboard display)
    _comments[key] = text
    db_exec("INSERT OR REPLACE INTO comments(key,text) VALUES(?,?)", (key, text))

    # Also save to history if non-empty
    synced = 0
    if text:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        # Push to Maximizer Note if entry exists
        try:
            found = mx_find_by_org_number(reg)
            if found and found.get("key"):
                resp = mx_create_note(found["key"], text, caller)
                if resp and resp.get("Code") == 0:
                    synced = 1
        except Exception as e:
            print(f"  Note sync error: {e}")
        db_exec("INSERT INTO comment_history(reg_number,page,text,caller,timestamp,synced_to_maximizer) VALUES(?,?,?,?,?,?)",
                (reg, page, text, caller, ts, synced))
    return jsonify({"ok":True, "synced_to_maximizer": synced})

# ── Phone overrides ───────────────────────────────────────────────────────────
@app.route("/api/phone", methods=["GET","POST"])
def phone_override():
    """GET ?reg=X → returns override; POST → saves override. Preserves email/website."""
    if request.method == "GET":
        reg = str(request.args.get("reg","")).strip()
        if not reg: return jsonify({"phone": ""})
        rows = db_query("SELECT phone FROM phone_overrides WHERE reg_number=?", (reg,))
        return jsonify({"reg_number": reg, "phone": rows[0][0] if rows else ""})
    # POST → route through the unified contact endpoint logic
    data = request.json or {}
    return contact_override.__wrapped__() if False else _save_contact(
        reg=str(data.get("reg_number","")).strip(),
        field="phone",
        val=str(data.get("phone","")).strip(),
        caller=str(data.get("caller","")).strip() or "Unknown",
    )

def _save_contact(reg, field, val, caller):
    """Internal helper used by /api/phone and /api/contact."""
    if not reg:
        return jsonify({"ok":False, "error":"Missing reg_number"}), 400
    if field not in MX_CONTACT_FIELD:
        return jsonify({"ok":False, "error":"Bad field"}), 400
    val = val[:CONTACT_LIMITS[field]]
    existing = db_query("SELECT phone,email,website FROM phone_overrides WHERE reg_number=?", (reg,))
    cur = {"phone": (existing[0][0] if existing else "") or "",
           "email": (existing[0][1] if existing else "") or "",
           "website":(existing[0][2] if existing else "") or ""}
    cur[field] = val
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if not any(cur.values()):
        db_exec("DELETE FROM phone_overrides WHERE reg_number=?", (reg,))
        _contact_overrides.pop(reg, None)
    else:
        db_exec("""INSERT OR REPLACE INTO phone_overrides
                   (reg_number,phone,email,website,updated_by,updated_at)
                   VALUES(?,?,?,?,?,?)""",
                (reg, cur["phone"], cur["email"], cur["website"], caller, ts))
        _contact_overrides[reg] = {k:v for k,v in cur.items() if v}
    synced = False
    try:
        found = mx_find_by_org_number(reg)
        if found and found.get("key"):
            payload = {"Key": found["key"], MX_CONTACT_FIELD[field]: val}
            r = mx_write_update(payload)
            if r and r.get("Code") == 0: synced = True
    except Exception as e:
        print(f"  contact→mx update error: {e}")
    # /api/phone callers expect this shape
    return jsonify({"ok": True, "phone": val if field=="phone" else None,
                    "field": field, "value": val, "synced_to_maximizer": synced})

@app.route("/api/phones/bulk")
def phones_bulk():
    """Return all phone overrides as a {reg_number: phone} dict for frontend caching."""
    rows = db_query("SELECT reg_number, phone FROM phone_overrides")
    return jsonify({r[0]: r[1] for r in rows if r[1]})

# ── Contact overrides (phone / email / website) — manual edits per charity ────
MX_CONTACT_FIELD = {"phone": "Phone1", "email": "Email1", "website": "WebSite"}
CONTACT_LIMITS   = {"phone": 30,       "email": 100,      "website": 200}

@app.route("/api/contacts/bulk")
def contacts_bulk():
    """All contact overrides as {reg: {phone,email,website}} for frontend bootstrap."""
    return jsonify(_contact_overrides)

@app.route("/api/contact", methods=["POST"])
def contact_override():
    """Save a manual phone/email/website override → Turso + Maximizer (if entry exists)."""
    data = request.json or {}
    return _save_contact(
        reg=str(data.get("reg_number","")).strip(),
        field=str(data.get("field","")).strip().lower(),
        val=str(data.get("value","")).strip(),
        caller=str(data.get("caller","")).strip() or "Unknown",
    )

# ── Call log ──────────────────────────────────────────────────────────────────
@app.route("/api/call", methods=["POST"])
def save_call():
    """Save a completed call. Pushes to Maximizer Notes if entry exists."""
    data = request.json or {}
    reg     = str(data.get("reg_number","")).strip()
    page    = str(data.get("page","")).strip()
    phone   = str(data.get("phone","")).strip()
    outcome = str(data.get("outcome","")).strip()[:40]
    notes   = str(data.get("notes","")).strip()[:800]
    caller  = str(data.get("caller","")).strip() or "Unknown"
    try: duration = max(0, int(data.get("duration_sec", 0) or 0))
    except: duration = 0
    if not reg or not outcome:
        return jsonify({"ok": False, "error": "Missing reg_number or outcome"}), 400
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Push to Maximizer as a Note
    synced = 0
    try:
        found = mx_find_by_org_number(reg)
        if found and found.get("key"):
            note_text = f"📞 Call: {outcome}"
            if duration: note_text += f" ({duration//60}m {duration%60}s)"
            if phone: note_text += f"\nPhone: {phone}"
            if notes: note_text += f"\n\n{notes}"
            resp = mx_create_note(found["key"], note_text, caller)
            if resp and resp.get("Code") == 0:
                synced = 1
    except Exception as e:
        print(f"  call→mx note error: {e}")

    db_exec("""INSERT INTO call_log
        (reg_number,page,phone_used,outcome,notes,duration_sec,caller,timestamp,synced_to_maximizer)
        VALUES(?,?,?,?,?,?,?,?,?)""",
        (reg, page, phone, outcome, notes, duration, caller, ts, synced))

    # Also mark called in called_log
    key = f"{page}|{reg}"
    _called_log[key] = {"reg_number": reg, "page": page,
                        "called_by": caller, "timestamp": ts}
    db_exec("INSERT OR REPLACE INTO called_log(key,reg_number,page,name,called_by,timestamp,data) VALUES(?,?,?,?,?,?,?)",
            (key, reg, page, "", caller, ts, "{}"))

    return jsonify({"ok": True, "synced_to_maximizer": synced})

@app.route("/api/call/history")
def call_history():
    """Get all calls for a charity, most recent first."""
    reg = str(request.args.get("reg","")).strip()
    if not reg: return jsonify({"calls": []})
    rows = db_query("""SELECT phone_used, outcome, notes, duration_sec, caller, timestamp, synced_to_maximizer
                       FROM call_log WHERE reg_number=? ORDER BY id DESC""", (reg,))
    return jsonify({"reg_number": reg, "calls": [
        {"phone": r[0], "outcome": r[1], "notes": r[2],
         "duration_sec": r[3], "caller": r[4],
         "timestamp": r[5], "synced": bool(r[6])}
        for r in rows
    ]})

@app.route("/api/comment/history")
def comment_history():
    """Get all past comments for a charity from both dashboard DB and Maximizer Notes."""
    import re as _re
    reg = str(request.args.get("reg","")).strip()
    if not reg: return jsonify({"history": []})

    # Local DB history
    rows = db_query("SELECT text, caller, timestamp, synced_to_maximizer FROM comment_history WHERE reg_number=? ORDER BY id DESC", (reg,))
    local = [
        {"text": r[0], "caller": r[1], "timestamp": r[2],
         "synced": bool(r[3]), "source": "dashboard"}
        for r in rows
    ]

    # Maximizer notes
    mx_notes = []
    try:
        found = mx_find_by_org_number(reg)
        if found and found.get("key"):
            notes = mx_read_notes(found["key"], top=50)
            for n in notes:
                rich = n.get("RichText","") or ""
                # Strip HTML for clean display
                clean = _re.sub(r"<br\s*/?>", "\n", rich)
                clean = _re.sub(r"</p>\s*<p[^>]*>", "\n", clean)
                clean = _re.sub(r"<[^>]+>", "", clean).strip()
                dt = n.get("DateTime","")
                # Try to extract caller name from "<strong>NAME</strong>" pattern
                caller_match = _re.search(r"<strong>([^<]+)</strong>", rich)
                caller = caller_match.group(1) if caller_match else (n.get("Creator","") or "Maximizer User")
                mx_notes.append({
                    "text": clean,
                    "caller": caller,
                    "timestamp": dt.replace("T"," ")[:19] if dt else "",
                    "synced": True,
                    "source": "maximizer",
                    "category": n.get("Category","")
                })
    except Exception as e:
        print(f"  history maximizer fetch error: {e}")

    # Merge - dashboard notes already in DB are duplicates of synced Maximizer notes
    # Deduplicate by matching text content
    seen_texts = set()
    merged = []
    # Show Maximizer notes first (they're the authoritative source for synced items)
    for item in mx_notes:
        key = item["text"].strip()[:100]
        seen_texts.add(key)
        merged.append(item)
    # Then any dashboard notes that aren't already represented
    for item in local:
        key = item["text"].strip()[:100]
        if key not in seen_texts:
            merged.append(item)
            seen_texts.add(key)

    # Sort by timestamp descending
    merged.sort(key=lambda x: x.get("timestamp",""), reverse=True)

    return jsonify({"reg_number": reg, "history": merged,
                    "dashboard_count": len(local),
                    "maximizer_count": len(mx_notes)})

@app.route("/api/status",methods=["POST"])
def save_status():
    data=request.json or {}; reg=str(data.get("reg_number","")).strip(); page=str(data.get("page","")).strip()
    status=str(data.get("status","")).strip()
    if not reg or not page: return jsonify({"ok":False}),400
    key=f"{page}|{reg}"
    if status:
        _statuses[key]=status
        db_exec("INSERT OR REPLACE INTO statuses(key,status) VALUES(?,?)", (key, status))
        if key not in _called_log:
            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
            _called_log[key]={"reg_number":reg,"page":page,"called_by":"Muhanna","timestamp":ts}
            db_exec("INSERT OR REPLACE INTO called_log(key,reg_number,page,name,called_by,timestamp,data) VALUES(?,?,?,?,?,?,?)",
                    (key, reg, page, "", "Muhanna", ts, "{}"))
    else:
        _statuses.pop(key,None)
        db_exec("DELETE FROM statuses WHERE key=?", (key,))
    return jsonify({"ok":True})

@app.route("/api/db/test_write")
def db_test_write():
    """Test writing and reading to confirm DB works end-to-end."""
    import time
    test_name = f"TEST_{int(time.time())}"
    result = {"test_name": test_name}

    # Write
    result["write_result"] = db_exec("INSERT OR IGNORE INTO users(name) VALUES(?)", (test_name,))

    # Read back
    rows = db_query("SELECT name FROM users WHERE name=?", (test_name,))
    result["read_back"] = [r[0] for r in rows] if rows else "NOT FOUND"

    # All users
    all_rows = db_query("SELECT name, added_at FROM users")
    result["all_users"] = [{"name": r[0], "added_at": r[1]} for r in all_rows]
    result["all_count"] = len(all_rows)

    # Cleanup
    db_exec("DELETE FROM users WHERE name=?", (test_name,))

    return jsonify(result)

@app.route("/api/db/stats")
def db_stats():
    """Show row counts for all DB tables."""
    if not TURSO_URL or not TURSO_TOKEN:
        return jsonify({"connected": False, "error": "TURSO_DATABASE_URL/TOKEN env vars not set"})
    stats = {"connected": True, "url": TURSO_URL[:60] + "…"}
    for tbl in ("users","called_log","comments","statuses","saved_searches"):
        try:
            r = db_query(f"SELECT COUNT(*) FROM {tbl}")
            stats[tbl] = r[0][0] if r else 0
        except Exception as e:
            stats[tbl] = f"err: {e}"
    return jsonify(stats)

@app.route("/api/db/export/<table>")
def db_export(table):
    """Export a table as JSON for backup or viewing."""
    if table not in ("users","called_log","comments","statuses","saved_searches"):
        return jsonify({"error": "Invalid table"}), 400
    # Get columns + rows via libsql_client ResultSet
    try:
        client = _new_conn()
        if not client:
            return jsonify({"error": "No DB connection"}), 500
        rs = client.execute(f"SELECT * FROM {table}")
        cols = list(rs.columns) if hasattr(rs, 'columns') else []
        rows = [dict(zip(cols, tuple(r))) for r in rs.rows]
        client.close()
        return jsonify({"table": table, "columns": cols,
                        "row_count": len(rows), "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e), "table": table}), 500

@app.route("/api/saved_searches",methods=["GET"])
def get_saved_searches(): return jsonify(list(_saved_searches.keys()))

@app.route("/api/saved_searches",methods=["POST"])
def save_search():
    data=request.json or {}; name=str(data.get("name","")).strip(); criteria=data.get("criteria",{})
    if not name: return jsonify({"ok":False}),400
    _saved_searches[name]=criteria
    db_exec("INSERT OR REPLACE INTO saved_searches(name,criteria) VALUES(?,?)", (name, json.dumps(criteria)))
    return jsonify({"ok":True})

@app.route("/api/saved_searches/<name>",methods=["GET"])
def get_saved_search(name):
    if name not in _saved_searches: return jsonify({"error":"Not found"}),404
    return jsonify(_saved_searches[name])

@app.route("/api/saved_searches/<name>",methods=["DELETE"])
def delete_saved_search(name):
    _saved_searches.pop(name,None)
    db_exec("DELETE FROM saved_searches WHERE name=?", (name,))
    return jsonify({"ok":True})

CONTACT_HDRS=["Phone","Email","Address 1","Address 2","Town","County","Postcode",
              "Activities","Call Status","Comment","Called By","Call Timestamp"]

def get_contact_details(reg):
    COLS={"registered_charity_number","charity_contact_phone","charity_contact_email",
          "charity_contact_address1","charity_contact_address2","charity_contact_address3",
          "charity_contact_address4","charity_contact_postcode","charity_activities"}
    for row in stream_zip_csv(CHARITY_URL,COLS):
        if row.get("registered_charity_number","").strip()==str(reg).strip():
            return {"phone":row.get("charity_contact_phone",""),"email":row.get("charity_contact_email",""),
                    "address1":row.get("charity_contact_address1",""),"address2":row.get("charity_contact_address2",""),
                    "town":row.get("charity_contact_address3",""),"county":row.get("charity_contact_address4",""),
                    "postcode":row.get("charity_contact_postcode",""),"activities":row.get("charity_activities","")}
    return {}

def contact_row(reg,page):
    d=get_contact_details(reg); key=f"{page}|{reg}"; cal=_called_log.get(key,{})
    return[d.get("phone",""),d.get("email",""),d.get("address1",""),d.get("address2",""),
           d.get("town",""),d.get("county",""),d.get("postcode",""),d.get("activities",""),
           _statuses.get(key,""),_comments.get(key,""),cal.get("called_by",""),cal.get("timestamp","")]

def csv_response(rows,headers,filename):
    def gen():
        buf=io.StringIO(); w=csv.writer(buf)
        w.writerow(headers); yield buf.getvalue(); buf.seek(0); buf.truncate()
        for row in rows:
            w.writerow(row); yield buf.getvalue(); buf.seek(0); buf.truncate()
    return Response(stream_with_context(gen()),mimetype="text/csv",
                    headers={"Content-Disposition":f"attachment; filename={filename}"})

@app.route("/download/prospects")
def dl_prospects():
    lf=request.args.get("list","all"); cached=cache_get("prospects")
    if not cached: return "Load Prospects tab first.",400
    map_={"best":"Best Immediate","readiness":"Readiness Package","contract":"Contract Growth","retention":"Retention/Replacement"}
    items=[]; seen=set()
    if lf=="all":
        for k,l in map_.items():
            for c in cached["lists"].get(k,[]):
                if c["reg_number"] not in seen: seen.add(c["reg_number"]); items.append((c,l))
    else:
        l=map_.get(lf,lf)
        for c in cached["lists"].get(lf,[]): items.append((c,l))
    hdrs=["Charity Number","Charity Name","Website","Prospect List","Total Income","Total Expenditure",
          "Gov Grants","Gov Contracts","Statutory Total","Statutory %","Prev Statutory","Financial Year"]+CONTACT_HDRS
    rows=[]
    for c,label in items:
        f=c.get("financials",{}); reg=c.get("reg_number","")
        rows.append([reg,c.get("name",""),c.get("website",""),label,f.get("total_income",0),
                     f.get("total_expenditure",0),f.get("gov_grants",0),f.get("gov_contracts",0),
                     f.get("statutory",0),f.get("stat_pct",0),f.get("prev_statutory",0),f.get("fin_year","")]
                    +contact_row(reg,"prospects"))
    return csv_response(rows,hdrs,f"Statutory_Prospects_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/download/new_charities")
def dl_new():
    days=request.args.get("days","30"); cached=cache_get(f"new_{days}")
    if not cached: return "Load New Charities tab first.",400
    hdrs=["Charity Number","Charity Name","Date Registered","Website","Status"]+CONTACT_HDRS
    rows=[[c["reg_number"],c["name"],c["date_registered"],c["website"],c["status"]]
          +contact_row(c["reg_number"],"new") for c in cached["charities"]]
    return csv_response(rows,hdrs,f"New_Charities_{days}days_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/download/late_accounts")
def dl_late():
    cached=cache_get("late")
    if not cached: return "Load Late Accounts tab first.",400
    def sev(m): return "Severe" if m>12 else "Moderate" if m>=6 else "Recent"
    hdrs=["Charity Number","Charity Name","Due Date","Months Late","Latest Income","Severity"]+CONTACT_HDRS
    rows=[[c["reg_number"],c["name"],c["due_date"],c["months_late"],c["latest_income"],sev(c["months_late"])]
          +contact_row(c["reg_number"],"late") for c in cached["charities"]]
    return csv_response(rows,hdrs,f"Late_Accounts_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/download/old_charities")
def dl_old():
    cached=cache_get("old_charities")
    if not cached: return "Load Old Charities tab first.",400
    hdrs=["Charity Number","Charity Name","Website","Date Registered","Age (Years)","Latest Income","Financial Year"]+CONTACT_HDRS
    rows=[[c["reg_number"],c["name"],c["website"],c["date_registered"],c["age_years"],c["total_income"],c["fin_year"]]
          +contact_row(c["reg_number"],"old") for c in cached["charities"]]
    return csv_response(rows,hdrs,f"Old_Charities_{datetime.utcnow().strftime('%Y%m%d')}.csv")

@app.route("/api/debug")
def debug(): return jsonify({"status":dict(_bg_status),"cache":list(_cache.keys())})

# ── Users / Callers API ────────────────────────────────────────────────────────
@app.route("/api/users", methods=["GET"])
def get_users():
    """Always fetch fresh from DB so manual DB edits show immediately."""
    rows = db_query("SELECT name FROM users ORDER BY added_at")
    if rows:
        # Update in-memory cache too so other code stays in sync
        global _users
        _users = [r[0] for r in rows]
    return jsonify(_users)

@app.route("/api/db/refresh", methods=["GET","POST"])
def db_refresh():
    """Reload all in-memory state from DB. Call this after editing the DB manually."""
    try:
        db_load_state()
        return jsonify({"ok": True, "users": len(_users),
                        "statuses": len(_statuses),
                        "comments": len(_comments),
                        "called_log": len(_called_log),
                        "saved_searches": len(_saved_searches)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/users", methods=["POST"])
def add_user():
    data = request.json or {}
    name = str(data.get("name","")).strip()
    if not name or len(name) > 60:
        return jsonify({"ok":False,"error":"Invalid name"}), 400
    if name not in _users:
        _users.append(name)
        db_exec("INSERT OR IGNORE INTO users(name) VALUES(?)", (name,))
    return jsonify({"ok":True,"users":_users})

@app.route("/api/users/<name>", methods=["DELETE"])
def delete_user(name):
    if name in _users and name != "Muhanna":
        _users.remove(name)
        db_exec("DELETE FROM users WHERE name=?", (name,))
    return jsonify({"ok":True,"users":_users})

# ── Called log now stores caller name ─────────────────────────────────────────
@app.route("/api/mark_called_by", methods=["POST"])
def mark_called_by():
    """Mark called with a specific caller name. toggle=False means always set (JS handles toggle)."""
    data   = request.json or {}
    reg    = str(data.get("reg_number","")).strip()
    page   = str(data.get("page","")).strip()
    name   = str(data.get("name","")).strip()
    caller = str(data.get("caller","")).strip() or "Unknown"
    do_toggle = data.get("toggle", True)
    if not reg or not page:
        return jsonify({"ok":False,"error":"Missing reg_number or page"}), 400
    key = f"{page}|{reg}"
    if do_toggle and key in _called_log:
        del _called_log[key]
        db_exec("DELETE FROM called_log WHERE key=?", (key,))
        return jsonify({"ok":True,"marked":False})
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    _called_log[key] = {
        "reg_number": reg, "name": name, "page": page,
        "called_by": caller, "timestamp": ts
    }
    db_exec("INSERT OR REPLACE INTO called_log(key,reg_number,page,name,called_by,timestamp,data) VALUES(?,?,?,?,?,?,?)",
            (key, reg, page, name, caller, ts, "{}"))
    return jsonify({"ok":True,"marked":True,"caller":caller})

# ── Milestone charities (approaching age anniversary with zero statutory) ──────
def compute_milestones(milestone_years):
    """Find charities that will reach milestone_years in the next 12 months."""
    today      = datetime.utcnow().date()
    # Target: charities whose registration anniversary = milestone_years
    # i.e. registered between (today - milestone_years - 1yr) and (today - milestone_years)
    cutoff_hi = today.replace(year=today.year - milestone_years)
    cutoff_lo = today.replace(year=today.year - milestone_years - 1)

    PC={"registered_charity_number","income_from_government_grants",
        "income_from_government_contracts","total_gross_income","fin_period_end_date"}
    CC={"registered_charity_number","charity_name","date_of_registration",
        "charity_registration_status","charity_contact_web",
        "charity_contact_phone","charity_contact_email",
        "charity_contact_address3","charity_contact_address4"}

    # Get parta latest per charity
    pl = {}
    for row in stream_zip_csv(PARTA_URL, PC):
        reg = row.get("registered_charity_number","").strip()
        if not reg: continue
        yr = row.get("fin_period_end_date","")[:10]
        if reg not in pl or yr > pl[reg].get("fin_period_end_date",""): pl[reg] = row

    results = []
    for row in stream_zip_csv(CHARITY_URL, CC):
        if row.get("charity_registration_status","").upper().strip() not in ("REGISTERED","R"): continue
        dt = row.get("date_of_registration","")[:10]
        if not dt or len(dt) < 10: continue
        try:
            reg_date = datetime.strptime(dt, "%Y-%m-%d").date()
        except: continue
        # Check if registration date falls in the milestone window
        if not (cutoff_lo < reg_date <= cutoff_hi): continue
        reg = row.get("registered_charity_number","").strip()
        if not reg: continue
        fin = pl.get(reg,{})
        gg  = float(fin.get("income_from_government_grants","") or 0)
        gc  = float(fin.get("income_from_government_contracts","") or 0)
        if gg + gc > 0: continue  # skip if has statutory income
        ti  = float(fin.get("total_gross_income","") or 0)
        # Calculate exact anniversary date
        try:
            anniversary = reg_date.replace(year=reg_date.year + milestone_years)
        except ValueError:
            anniversary = reg_date.replace(year=reg_date.year + milestone_years, day=28)
        months_to_go = round((anniversary - today).days / 30.4, 1)
        web = row.get("charity_contact_web","")
        results.append({
            "reg_number":   reg,
            "name":         row.get("charity_name","") or reg,
            "website":      web if web not in ("nan","None") else "",
            "phone":        row.get("charity_contact_phone",""),
            "email":        row.get("charity_contact_email",""),
            "town":         row.get("charity_contact_address3",""),
            "county":       row.get("charity_contact_address4",""),
            "date_registered": dt,
            "age_years":    today.year - reg_date.year,
            "milestone":    milestone_years,
            "anniversary_date": str(anniversary),
            "months_to_anniversary": months_to_go,
            "total_income": ti,
            "fin_year":     fin.get("fin_period_end_date","")[:4],
        })
    results.sort(key=lambda x: x.get("anniversary_date",""))
    return {"charities": results, "count": len(results),
            "milestone": milestone_years, "generated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}

# ── Daily Calling — top 100 priority-ranked contacts ────────────────────────────
import math as _math

def _called_today(reg):
    """True if this charity was called today (UTC), per call_log timestamps."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    for v in _called_log.values():
        if v.get("reg_number")==reg and str(v.get("timestamp","")).startswith(today):
            return True
    return False

def _ever_called(reg):
    return any(v.get("reg_number")==reg for v in _called_log.values())

def priority_score(c):
    """
    Composite 0–100 score blending Opportunity (40), Urgency (30), Freshness (30).
    Adapts to whatever fields a charity has — missing fields use neutral defaults
    so no list type is unfairly penalised. Returns (score, breakdown).
    """
    ti   = flt(c.get("total_income"))
    stat = flt(c.get("statutory"))
    pstat= flt(c.get("prev_statutory"))
    reg  = c.get("reg_number","")

    # ── Opportunity (0–40): deal size via log-scaled income + statutory exposure ──
    # £50k→~10pts, £500k→~25pts, £3m→~38pts (log curve). Statutory adds up to +12.
    opp = 0.0
    if ti > 0:
        opp = min(28.0, (_math.log10(max(ti,1)) - 4.0) * 14.0)   # log10(10k)=4 → 0
        opp = max(0.0, opp)
    if stat > 0:
        opp += min(12.0, (_math.log10(max(stat,1)) - 4.0) * 8.0)
        opp = min(40.0, opp)
    if ti <= 0 and stat <= 0:
        opp = 14.0   # neutral default for lists without financials (e.g. Newly Registered)

    # ── Urgency (0–30): declining trend, lateness, approaching anniversary ────────
    urg = 0.0
    if pstat > 0 and stat < pstat:                       # statutory funding declining
        drop = (pstat - stat) / pstat
        urg += min(16.0, drop * 16.0)
    ml = flt(c.get("months_late"))
    if ml > 0:                                           # late accounts
        urg += min(16.0, ml * 1.2)
    mta = c.get("months_to_anniversary")
    if mta is not None and mta != "":
        try:
            m = float(mta)
            if 0 <= m <= 12: urg += (12.0 - m) / 12.0 * 14.0   # closer = more urgent
        except: pass
    if c.get("spend_over_income"):
        urg += 8.0
    urg = min(30.0, urg)
    if urg == 0.0:
        urg = 6.0    # small neutral baseline

    # ── Freshness (0–30): uncalled floats to top; called-today drops ─────────────
    if _called_today(reg):
        fresh = 0.0
    elif _ever_called(reg):
        fresh = 12.0
    else:
        fresh = 30.0

    total = round(opp + urg + fresh, 1)
    return total, {"opportunity": round(opp,1), "urgency": round(urg,1), "freshness": fresh}

def _normalize_for_calling(c, source):
    """Map any list's charity dict into the common shape the calling page uses."""
    fin = c.get("financials",{}) or {}
    ti   = c.get("total_income", fin.get("total_income", c.get("latest_income","")))
    stat = fin.get("statutory","")
    pstat= fin.get("prev_statutory","")
    spend_over = any(f.get("label","").startswith("Spend") for f in fin.get("flags",[]))
    return {
        "reg_number": c.get("reg_number",""),
        "name": c.get("name",""),
        "phone": c.get("phone",""),
        "email": c.get("email",""),
        "website": c.get("website",""),
        "town": c.get("town",""),
        "county": c.get("county",""),
        "total_income": ti,
        "statutory": stat,
        "prev_statutory": pstat,
        "months_late": c.get("months_late",""),
        "months_to_anniversary": c.get("months_to_anniversary",""),
        "spend_over_income": spend_over,
        "removed": bool(c.get("removed", False)),
        "source": source,
    }

def _collect_from_cache():
    """Gather charities from all cached lists, tagged by source. Returns dict source→list."""
    pools = {}
    # Prospects (nested lists)
    p = cache_get("prospects", max_age=86400)
    if p and p.get("lists"):
        merged = {}
        for sub in ("best","readiness","contract","retention"):
            for c in p["lists"].get(sub,[]):
                merged[c.get("reg_number")] = c   # dedupe across sublists
        pools["Prospect Lists"] = list(merged.values())
    # Newly Registered (default 30d window)
    for k in ("new_30","new_60","new_90"):
        n = cache_get(k, max_age=86400)
        if n and n.get("charities"):
            pools["Newly Registered"] = n["charities"]; break
    # Late Accounts
    l = cache_get("late", max_age=86400)
    if l and l.get("charities"):
        pools["Late Accounts"] = l["charities"]
    # 40+ Zero Statutory
    o = cache_get("old_charities", max_age=86400)
    if o and o.get("charities"):
        pools["40+ Yrs Zero Statutory"] = o["charities"]
    # Anniversary Watch
    for k in ("milestones_40","milestones_20","milestones_25","milestones_50"):
        m = cache_get(k, max_age=86400)
        if m and m.get("charities"):
            pools["Anniversary Watch"] = m["charities"]; break
    return pools

CALLING_CATEGORIES = {
    "all": "All combined",
    "prospects": "Prospect Lists",
    "new": "Newly Registered",
    "late": "Late Accounts",
    "old": "40+ Yrs Zero Statutory",
    "milestones": "Anniversary Watch",
    "search": "Charity Search (saved)",
}

@app.route("/api/daily_calling")
def api_daily_calling():
    category = request.args.get("category","all").strip().lower()
    pools = _collect_from_cache()

    # Charity Search source = combine all saved-search result sets (if any saved)
    if category == "search":
        rows = []
        for crit in _saved_searches.values():
            if isinstance(crit, dict) and crit.get("results"):
                rows.extend(crit["results"])
        if not rows:
            return jsonify({"charities":[], "count":0, "category":category,
                            "category_label": CALLING_CATEGORIES.get(category,category),
                            "note":"No saved searches with results yet. Run a Charity Search and save it to feed this view."})
        src_map = {"Charity Search": rows}
    elif category == "all":
        src_map = pools
    else:
        label = CALLING_CATEGORIES.get(category)
        if label and label in pools:
            src_map = {label: pools[label]}
        else:
            # That list isn't cached yet — tell the UI to warm it up
            return jsonify({"charities":[], "count":0, "category":category,
                            "category_label": CALLING_CATEGORIES.get(category,category),
                            "status":"not_loaded",
                            "note":f"Open the '{CALLING_CATEGORIES.get(category,category)}' tab once to load its data, then return here."}), 200

    if not src_map:
        return jsonify({"charities":[], "count":0, "category":category,
                        "category_label": CALLING_CATEGORIES.get(category,category),
                        "status":"not_loaded",
                        "note":"No list data is loaded yet. Open the list tabs once to load them, then return here."}), 200

    # Normalize + dedupe across sources (keep first/highest-priority source seen)
    seen = {}
    for source, rows in src_map.items():
        for c in rows:
            reg = c.get("reg_number","")
            if not reg or reg in seen: continue
            if c.get("removed"): continue   # never call removed charities
            seen[reg] = _normalize_for_calling(c, source)

    scored = []
    for reg, c in seen.items():
        sc, breakdown = priority_score(c)
        c["priority"] = sc
        c["priority_breakdown"] = breakdown
        c["called_today"] = _called_today(reg)
        c["ever_called"] = _ever_called(reg)
        c["band"] = "high" if sc>=65 else ("med" if sc>=45 else "low")
        scored.append(c)

    scored.sort(key=lambda x: x["priority"], reverse=True)
    top = scored[:100]
    called_today_count = sum(1 for c in top if c["called_today"])

    return jsonify({
        "charities": top,
        "count": len(top),
        "total_pool": len(scored),
        "called_today": called_today_count,
        "category": category,
        "category_label": CALLING_CATEGORIES.get(category, category),
        "generated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    })

@app.route("/api/milestones")
def api_milestones():
    years = int(request.args.get("years","40"))
    key   = f"milestones_{years}"
    r = bg_check(key, {"charities":[],"count":0,"milestone":years})
    if r: return r
    bg_run(key, lambda: compute_milestones(years))
    return jsonify({"status":"loading","message":f"Loading {years}-year milestone charities…",
                    "charities":[],"count":0,"milestone":years}), 202

@app.route("/download/milestones")
def dl_milestones():
    years = int(request.args.get("years","40"))
    cached = cache_get(f"milestones_{years}")
    if not cached: return "Load milestones tab first.", 400
    hdrs = ["Charity Number","Charity Name","Website","Date Registered","Current Age (Yrs)",
            f"Milestone ({years} yrs)","Anniversary Date","Months Until Anniversary",
            "Latest Income","Fin Year"] + CONTACT_HDRS
    rows = []
    for c in cached["charities"]:
        rows.append([c["reg_number"],c["name"],c["website"],c["date_registered"],
                     c["age_years"],f"{years} years",c["anniversary_date"],
                     c["months_to_anniversary"],c["total_income"],c["fin_year"]]
                    + contact_row(c["reg_number"],"milestones"))
    return csv_response(rows, hdrs, f"Milestones_{years}yr_{datetime.utcnow().strftime('%Y%m%d')}.csv")

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)


if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)

# ═══════════════════════════════════════════════════════════════════════
# MAXIMIZER CRM SYNC — Octopus API
# ═══════════════════════════════════════════════════════════════════════
MX_TOKEN = os.environ.get("MAXIMIZER_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteHB5ZjV6MGFwbWpub29jO"
    "XM3NCIsImlhdCI6MTc3ODUyNzEyMywiZXhwIjoxODczMDY1NjAwLCJteC1jaWQiOiJEMzIx"
    "RDMxRS04QzRBLTQyRTMtQUU5Ny03NjMzRDk5Qjk5QjIiLCJteC13c2lkIjoiOENDNkVBRkY"
    "tQkFERi00RDY5LTgyRTktNzQxREFERUU2QjU0IiwibXgtZGIiOiJjZmM0MWQwY2Y0OWQ0MjV"
    "iYjQ0ZTQ0YzA3ZDdiMTBmZCIsIm14LXVpZCI6Ik1BSEVTSCIsIm14LXBsIjoiY2xvdWQifQ"
    ".hEwCX0Yg35m_QoPa1yXiCoumJ-_9tP1OL3YM8MgOjMU"
)
MX_DB   = "cfc41d0cf49d425bb44e44c07d7b10fd"
MX_BASE = os.environ.get("MAXIMIZER_BASE","https://api.maximizer.com/octopus")

# ── UDF option maps from UDFOptionList.xml ────────────────────────────────────
# TYPEID(111)=What, (112)=Who, (113)=How, (107)=Country, (108)=LocalAuth,
# (109)=Region, (261)=Policy, (264)=IsSync

WHAT_MAP = {
    "accommodation/housing":"1","amateur sport":"2","animals":"3",
    "armed forces/emergency service efficiency":"4",
    "arts/culture/heritage/science":"5","disability":"6",
    "economic/community development/employment":"7","education/training":"8",
    "environment/conservation/heritage":"9","general charitable purposes":"10",
    "human rights/religious or racial harmony/equality or diversity":"11",
    "other charitable purposes":"12","overseas aid/famine relief":"13",
    "recreation":"14","religious activities":"15",
    "the advancement of health or saving of lives":"16",
    "the prevention or relief of poverty":"17",
}
WHO_MAP = {
    "children/young people":"1","elderly/old people":"2",
    "other charities or voluntary bodies":"3","other defined groups":"4",
    "people of a particular ethnic or racial origin":"5",
    "people with disabilities":"6","the general public/mankind":"7",
}
HOW_MAP = {
    "acts as an umbrella or resource body":"1",
    "makes grants to individuals":"2","makes grants to organisations":"3",
    "other charitable activities":"4",
    "provides advocacy/advice/information":"5",
    "provides buildings/facilities/open space":"6",
    "provides human resources":"7","provides other finance":"8",
    "provides services":"9","sponsors or undertakes research":"10",
}
REGION_MAP = {
    "throughout england":"1","throughout england and wales":"2",
    "throughout london":"3","throughout wales":"4",
}
LOCAL_AUTH_MAP = {
    "barking and dagenham":"1","barnet":"2","barnsley":"3",
    "bath and north east somerset":"4","bedford":"5","bexley":"6",
    "birmingham city":"7","blackburn with darwen":"8","blackpool":"9",
    "blaenau gwent":"10","bolton":"11","bournemouth":"12",
    "bracknell forest":"13","bradford city":"14","brent":"15",
    "bridgend":"16","brighton and hove":"17","bristol city":"18",
    "bromley":"19","buckinghamshire":"20","bury":"21","caerphilly":"22",
    "calderdale":"23","cambridgeshire":"24","camden":"25","cardiff":"26",
    "carmarthenshire":"27","central bedfordshire":"28","ceredigion":"29",
    "cheshire east":"30","cheshire west & chester":"31",
    "city of london":"32","city of swansea":"33","city of wakefield":"34",
    "city of westminster":"35","city of york":"36","conwy":"37",
    "cornwall":"38","coventry city":"39","croydon":"40","cumbria":"41",
    "darlington":"42","denbighshire":"43","derby city":"44",
    "derbyshire":"45","devon":"46","doncaster":"47","dorset":"48",
    "dudley":"49","durham":"50","ealing":"51",
    "east riding of yorkshire":"52","east sussex":"53","enfield":"54",
    "essex":"55","flintshire":"56","gateshead":"57","gloucestershire":"58",
    "greenwich":"59","gwynedd":"60","hackney":"61","halton":"62",
    "hammersmith and fulham":"63","hampshire":"64","haringey":"65",
    "harrow":"66","hartlepool":"67","havering":"68","herefordshire":"69",
    "hertfordshire":"70","hillingdon":"71","hounslow":"72",
    "isle of anglesey":"73","isle of wight":"74","isles of scilly":"75",
    "islington":"76","kensington and chelsea":"77","kent":"78",
    "kingston upon hull city":"79","kingston upon thames":"80",
    "kirklees":"81","knowsley":"82","lambeth":"83","lancashire":"84",
    "leeds city":"85","leicester city":"86","leicestershire":"87",
    "lewisham":"88","lincolnshire":"89","liverpool city":"90","luton":"91",
    "manchester city":"92","medway":"93","merthyr tydfil":"94",
    "merton":"95","middlesbrough":"96","milton keynes":"97",
    "monmouthshire":"98","neath port talbot":"99",
    "newcastle upon tyne city":"100","newham":"101","newport city":"102",
    "norfolk":"103","north east lincolnshire":"104",
    "north lincolnshire":"105","north somerset":"106",
    "north tyneside":"107","north yorkshire":"108",
    "northamptonshire":"109","northumberland":"110",
    "nottingham city":"111","nottinghamshire":"112","oldham":"113",
    "oxfordshire":"114","pembrokeshire":"115","peterborough city":"116",
    "plymouth city":"117","poole":"118","portsmouth city":"119",
    "powys":"120","reading":"121","redbridge":"122",
    "redcar and cleveland":"123","rhondda cynon taff":"124",
    "richmond upon thames":"125","rochdale":"126","rotherham":"127",
    "rutland":"128","salford city":"129","sandwell":"130","sefton":"131",
    "sheffield city":"132","shropshire":"133","slough":"134",
    "solihull":"135","somerset":"136","south gloucestershire":"137",
    "south tyneside":"138","southampton city":"139",
    "southend-on-sea":"140","southwark":"141","st helens":"142",
    "staffordshire":"143","stockport":"144","stockton-on-tees":"145",
    "stoke-on-trent city":"146","suffolk":"147","sunderland":"148",
    "surrey":"149","sutton":"150","swindon":"151","tameside":"152",
    "telford & wrekin":"153","thurrock":"154","torbay":"155",
    "torfaen":"156","tower hamlets":"157","trafford":"158",
    "vale of glamorgan":"159","walsall":"160","waltham forest":"161",
    "wandsworth":"162","warrington":"163","warwickshire":"164",
    "west berkshire":"165","west sussex":"166","wigan":"167",
    "wiltshire":"168","windsor and maidenhead":"169","wirral":"170",
    "wokingham":"171","wolverhampton":"172","worcestershire":"173",
    "wrexham":"174",
}
POLICY_MAP = {
    "bullying and harassment policy and procedures":"1",
    "conflicting interests":"2",
    "financial reserves policy and procedures":"3",
    "internal charity financial controls policy and procedures":"4",
    "internal risk management policy and procedures":"5",
    "investing charity funds policy and procedures":"6","investment":"7",
    "risk management":"8","safeguarding policy and procedures":"9",
    "safeguarding vulnerable beneficiaries":"10",
    "serious incident reporting policy and procedures":"11",
    "trustee conflicts of interest policy and procedures":"12",
    "volunteer management":"13","complaints handling":"14",
    "paying staff":"15",
    "campaigns and political activity policy and procedures":"16",
    "complaints policy and procedures":"17",
    "engaging external speakers at charity events policy and procedures":"18",
    "social media policy and procedures":"19",
    "trustee expenses policy and procedures":"20",
}

def _lookup(mapping, text):
    if not text: return None
    return mapping.get(str(text).lower().strip())

def _multi_keys(mapping, text_or_list):
    """Return list of option keys for multiple CC values."""
    if not text_or_list: return []
    if isinstance(text_or_list, str):
        items = [x.strip() for x in text_or_list.replace(';',',').split(',') if x.strip()]
    else:
        items = list(text_or_list)
    return [k for k in (_lookup(mapping, i) for i in items) if k]

# CC classification cache — loaded lazily
_classif_cache = {}
_classif_loaded = False

def load_classif_cache():
    """Load charity classification (what/who/how) from CC bulk file into memory."""
    global _classif_cache, _classif_loaded
    if _classif_loaded: return
    try:
        cols = {"registered_charity_number","classification_code","classification_type"}
        for row in stream_zip_csv(CLASSIF_URL, cols):
            reg  = row.get("registered_charity_number","").strip()
            code = row.get("classification_code","").strip()
            ctype = row.get("classification_type","").strip().lower()
            if not reg or not code: continue
            if reg not in _classif_cache:
                _classif_cache[reg] = {"what":[],"who":[],"how":[]}
            if ctype == "what":
                _classif_cache[reg]["what"].append(code)
            elif ctype == "who":
                _classif_cache[reg]["who"].append(code)
            elif ctype == "how":
                _classif_cache[reg]["how"].append(code)
        _classif_loaded = True
        print(f"Classif cache loaded: {len(_classif_cache)} charities")
    except Exception as e:
        print(f"load_classif_cache error: {e}")
        _classif_loaded = True  # Don't retry on error

# CC classification code → human readable name maps
CC_WHAT_CODES = {
    "101":"accommodation/housing","102":"education/training",
    "103":"the prevention or relief of poverty","104":"overseas aid/famine relief",
    "105":"accommodation/housing","106":"religious activities",
    "107":"arts/culture/heritage/science","108":"amateur sport",
    "109":"animals","110":"environment/conservation/heritage",
    "111":"economic/community development/employment","112":"other charitable purposes",
    "113":"general charitable purposes","114":"the advancement of health or saving of lives",
    "115":"disability","116":"the prevention or relief of poverty",
    "117":"accommodation/housing","118":"religious activities",
    "119":"arts/culture/heritage/science","120":"amateur sport",
    "121":"animals","122":"environment/conservation/heritage",
    "123":"economic/community development/employment",
    "124":"human rights/religious or racial harmony/equality or diversity",
    "125":"recreation","200":"general charitable purposes",
    "201":"education/training","202":"the prevention or relief of poverty",
    "203":"overseas aid/famine relief","204":"accommodation/housing",
    "205":"religious activities","206":"arts/culture/heritage/science",
    "207":"amateur sport","208":"animals",
    "209":"environment/conservation/heritage",
    "210":"economic/community development/employment",
    "211":"other charitable purposes",
    "212":"the advancement of health or saving of lives","213":"disability",
    "214":"human rights/religious or racial harmony/equality or diversity",
    "215":"recreation",
    "301":"children/young people","302":"elderly/old people",
    "303":"people with disabilities","304":"people of a particular ethnic or racial origin",
    "305":"other charities or voluntary bodies","306":"other defined groups",
    "307":"the general public/mankind",
    "401":"makes grants to individuals","402":"makes grants to organisations",
    "403":"provides services","404":"provides facilities",
    "405":"provides buildings/facilities/open space","406":"provides human resources",
    "407":"provides other finance","408":"other charitable activities",
    "409":"acts as an umbrella or resource body","410":"provides advocacy/advice/information",
    "411":"sponsors or undertakes research",
}

def get_charity_classification(reg_no):
    """Get what/who/how for a charity from CC bulk classification data."""
    load_classif_cache()
    entry = _classif_cache.get(str(reg_no), {})
    if not entry:
        return {"what":"","who":"","how":""}
    # Convert codes to human-readable names
    def codes_to_names(codes):
        return ",".join(CC_WHAT_CODES.get(c, c) for c in codes if c)
    return {
        "what": codes_to_names(entry.get("what",[])),
        "who":  codes_to_names(entry.get("who",[])),
        "how":  codes_to_names(entry.get("how",[])),
    }


# ── CC API lookup ────────────────────────────────────────────────────────────
_cc_detail_cache = {}  # Cache CC API results by reg_number

def fetch_cc_charity(reg_no):
    """Fetch full charity details from CC API including what/who/how, address."""
    reg_no = str(reg_no)
    if reg_no in _cc_detail_cache:
        return _cc_detail_cache[reg_no]
    hdrs = {"Ocp-Apim-Subscription-Key": API_KEY}
    try:
        # Try suffix 0 first (main charity), then no suffix
        r = requests.get(f"{CC_API_BASE}/allcharitydetails/{reg_no}/0",
                         headers=hdrs, timeout=10)
        if r.status_code == 200:
            data = r.json()
            _cc_detail_cache[reg_no] = data
            return data
    except Exception as e:
        print(f"fetch_cc_charity({reg_no}): {e}")
    return {}

def enrich_charity_from_cc(c):
    """Enrich a charity dict with full CC API data."""
    reg_no = str(c.get("reg_number","")).strip()
    if not reg_no:
        return c
    cc = fetch_cc_charity(reg_no)
    if not cc:
        return c
    # Basic fields
    if not c.get("phone")   and cc.get("phone"):   c["phone"]   = cc["phone"]
    if not c.get("email")   and cc.get("email"):   c["email"]   = cc["email"]
    if not c.get("website") and cc.get("web"):     c["website"] = cc["web"]
    # Address
    if not c.get("address1"): c["address1"] = cc.get("address_line_one","")
    if not c.get("address2"): c["address2"] = cc.get("address_line_two","")
    c["postcode"] = cc.get("address_post_code","")
    # City/Town = address_line_five (London) or address_line_four (City of London)
    c["city"]   = cc.get("address_line_five","") or cc.get("address_line_four","")
    c["county"] = cc.get("address_line_four","")
    # Date of registration
    dor = cc.get("date_of_registration","")
    if dor: c["date_of_registration"] = dor[:10]  # YYYY-MM-DD
    # Organisation number
    c["organisation_number"] = str(cc.get("organisation_number",""))
    # Linked Charity: group_subsid_suffix != 0 means it's a linked charity
    c["linked_charity"] = "Yes" if cc.get("group_subsid_suffix",0) != 0 else ""
    # Latest income/expenditure
    c["latest_income"]      = cc.get("latest_income","")
    c["latest_expenditure"] = cc.get("latest_expenditure","")
    # Financial year dates
    c["fin_year_start"] = (cc.get("latest_acc_fin_year_start_date","") or "")[:10]
    c["fin_year_end"]   = (cc.get("latest_acc_fin_year_end_date","") or "")[:10]
    # What/Who/How from who_what_where
    wwh = cc.get("who_what_where", [])
    whats = [x["classification_desc"] for x in wwh if x.get("classification_type","").lower()=="what"]
    whos  = [x["classification_desc"] for x in wwh if x.get("classification_type","").lower()=="who"]
    hows  = [x["classification_desc"] for x in wwh if x.get("classification_type","").lower()=="how"]
    if whats: c["what"] = ",".join(whats)
    if whos:  c["who"]  = ",".join(whos)
    if hows:  c["how"]  = ",".join(hows)
    # Local Authority from CharityAoOLocalAuthority
    la_list = cc.get("CharityAoOLocalAuthority",[])
    if la_list: c["local_authority"] = la_list[0].get("local_authority","")
    return c

# ── CC API — full charity detail lookup ──────────────────────────────────────
_cc_detail_cache = {}

def fetch_cc_charity(reg_no):
    """Fetch full charity details from CC API: who/what/how, address, dates, etc."""
    reg_no = str(reg_no)
    if reg_no in _cc_detail_cache:
        return _cc_detail_cache[reg_no]
    hdrs = {"Ocp-Apim-Subscription-Key": API_KEY}
    try:
        r = requests.get(f"{CC_API_BASE}/allcharitydetails/{reg_no}/0",
                         headers=hdrs, timeout=15)
        if r.status_code == 200:
            data = r.json()
            _cc_detail_cache[reg_no] = data
            return data
        print(f"  fetch_cc_charity({reg_no}): HTTP {r.status_code}")
    except Exception as e:
        print(f"  fetch_cc_charity({reg_no}): {e}")
    return {}

def enrich_charity_from_cc(c):
    """Enrich a charity dict with full CC API data."""
    reg_no = str(c.get("reg_number","")).strip()
    if not reg_no:
        return c
    cc = fetch_cc_charity(reg_no)
    if not cc:
        return c
    # Basic contact fields
    if not c.get("phone")   and cc.get("phone"):   c["phone"]   = cc["phone"]
    if not c.get("email")   and cc.get("email"):   c["email"]   = cc["email"]
    if not c.get("website") and cc.get("web"):     c["website"] = cc["web"]
    # Address lines
    c["address1"]  = cc.get("address_line_one","")
    c["address2"]  = cc.get("address_line_two","")
    c["address3"]  = cc.get("address_line_three","")
    c["city"]      = cc.get("address_line_five","") or cc.get("address_line_four","")
    c["county"]    = cc.get("address_line_four","")
    c["postcode"]  = cc.get("address_post_code","")
    # Registration details
    dor = cc.get("date_of_registration","")
    if dor: c["date_of_registration"] = dor[:10]
    c["organisation_number"] = str(cc.get("organisation_number",""))
    c["linked_charity"] = "Yes" if cc.get("group_subsid_suffix",0) != 0 else "No"
    # Financials
    c["latest_income"]      = cc.get("latest_income","")
    c["latest_expenditure"] = cc.get("latest_expenditure","")
    c["fin_year_start"]     = (cc.get("latest_acc_fin_year_start_date","") or "")[:10]
    c["fin_year_end"]       = (cc.get("latest_acc_fin_year_end_date","") or "")[:10]
    # What / Who / How from who_what_where array
    wwh = cc.get("who_what_where", [])
    whats = [x["classification_desc"] for x in wwh
             if x.get("classification_type","").lower()=="what"]
    whos  = [x["classification_desc"] for x in wwh
             if x.get("classification_type","").lower()=="who"]
    hows  = [x["classification_desc"] for x in wwh
             if x.get("classification_type","").lower()=="how"]
    if whats: c["what"] = ",".join(whats)
    if whos:  c["who"]  = ",".join(whos)
    if hows:  c["how"]  = ",".join(hows)
    # Local Authority
    la_list = cc.get("CharityAoOLocalAuthority", [])
    if la_list: c["local_authority"] = la_list[0].get("local_authority","")
    print(f"  CC enriched: what={c.get('what','')[:30]} who={c.get('who','')[:20]} "
          f"la={c.get('local_authority','')} addr={c.get('address1','')[:20]}")
    return c

# ── CC classification cache (what/who/how) ────────────────────────────────────
_classif_cache = {}
_classif_loaded = False

def load_classif_cache():
    global _classif_cache, _classif_loaded
    if _classif_loaded: return
    try:
        cols = {"registered_charity_number","classification_code","classification_type"}
        for row in stream_zip_csv(CLASSIF_URL, cols):
            reg   = row.get("registered_charity_number","").strip()
            code  = row.get("classification_code","").strip()
            ctype = row.get("classification_type","").strip().lower()
            if not reg or not code: continue
            if reg not in _classif_cache:
                _classif_cache[reg] = {"what":[],"who":[],"how":[]}
            if "what" in ctype:   _classif_cache[reg]["what"].append(code)
            elif "who" in ctype:  _classif_cache[reg]["who"].append(code)
            elif "how" in ctype:  _classif_cache[reg]["how"].append(code)
        _classif_loaded = True
        print(f"Classif cache: {len(_classif_cache)} charities")
    except Exception as e:
        print(f"load_classif_cache: {e}")
        _classif_loaded = True

# CC classification code → name (from CC bulk schema)
CC_CODE_MAP = {
    # What
    "101":"general charitable purposes","102":"education/training",
    "103":"the prevention or relief of poverty","104":"overseas aid/famine relief",
    "105":"accommodation/housing","106":"religious activities",
    "107":"arts/culture/heritage/science","108":"amateur sport","109":"animals",
    "110":"environment/conservation/heritage",
    "111":"economic/community development/employment",
    "112":"the advancement of health or saving of lives","113":"disability",
    "114":"human rights/religious or racial harmony/equality or diversity",
    "115":"recreation","116":"other charitable purposes",
    # Who
    "201":"children/young people","202":"elderly/old people",
    "203":"people with disabilities",
    "204":"people of a particular ethnic or racial origin",
    "205":"other charities or voluntary bodies","206":"other defined groups",
    "207":"the general public/mankind",
    # How
    "301":"makes grants to individuals","302":"makes grants to organisations",
    "303":"provides services","304":"provides facilities",
    "305":"provides buildings/facilities/open space","306":"provides human resources",
    "307":"provides other finance","308":"other charitable activities",
    "309":"acts as an umbrella or resource body",
    "310":"provides advocacy/advice/information","311":"sponsors or undertakes research",
}

def get_charity_classification(reg_no):
    """Get what/who/how strings for a charity reg number."""
    load_classif_cache()
    entry = _classif_cache.get(str(reg_no), {})
    def codes_to_str(codes):
        return ",".join(CC_CODE_MAP.get(c, c) for c in codes if c)
    return {
        "what": codes_to_str(entry.get("what",[])),
        "who":  codes_to_str(entry.get("who",[])),
        "how":  codes_to_str(entry.get("how",[])),
    }

def mx_hdrs():
    return {"Authorization":f"Bearer {MX_TOKEN}",
            "Content-Type":"application/json","Accept":"application/json"}

def mx_call(endpoint, body):
    url = f"{MX_BASE.rstrip('/')}/{endpoint}"
    # Add Compatibility header required by Octopus API
    if "Compatibility" not in body:
        body["Compatibility"] = {"AbEntryKey": "2.0"}
    r = requests.post(url, headers=mx_hdrs(), json=body, timeout=20)
    if not r.ok: raise Exception(f"{r.status_code}: {r.text[:200]}")
    return r.json()

def mx_read(search_query=None, fields=None, top=1, entry_type=None):
    """Read AbEntry — includes Compatibility header to make Company entries visible."""
    sq = dict(search_query or {})
    if entry_type:
        sq["type"] = entry_type
    body = {
        "abEntry": {
            "criteria": {"searchQuery": sq, "top": top},
            "scope": {"fields": fields or {"key":1,"companyName":1}}
        },
        "Compatibility": {"AbEntryKey": "2.0"}
    }
    return mx_call("AbEntryRead", body)

def mx_write_create(data_dict):
    resp = mx_call("AbEntryCreate",{"AbEntry":{"Data":data_dict}})
    # Extract key from response: resp["AbEntry"]["Data"]["Key"]
    try:
        key = resp.get("AbEntry",{}).get("Data",{}).get("Key","")
        if key:
            print(f"  AbEntryCreate key: {key[:30]}")
            resp["_extracted_key"] = key
    except: pass
    return resp

def _extract_key_from_create_resp(resp):
    """Try every possible location for the key in an AbEntryCreate response."""
    import json
    # Log full response for debugging
    print(f"  Create resp: {json.dumps(resp)[:300]}")
    # Try all known locations
    candidates = [
        resp.get("AbEntry",{}).get("Data",{}).get("Key",""),
        resp.get("abEntry",{}).get("Data",{}).get("Key",""),
        resp.get("Data",{}).get("Key",""),
        resp.get("Key",""),
    ]
    # Also check if Data is a list
    data = resp.get("AbEntry",resp.get("abEntry",{})).get("Data",{})
    if isinstance(data, list) and data:
        candidates.append(data[0].get("Key",""))
    for c in candidates:
        if c: return c
    return ""

def mx_write_update(data_dict):
    return mx_call("AbEntryUpdate",{"AbEntry":{"Data":data_dict}})

def title_case(s):
    if not s: return s
    minor = {"a","an","the","and","or","but","of","in","on","at","to","for",
             "by","with","from","as","its"}
    acronyms = {"uk","cio","nhs","ymca","rspca","rspb","rnli","rnib","nspcc",
                "hmrc","plc","ltd","raf","rbl"}
    words = s.strip().split()
    result = []
    for i, w in enumerate(words):
        wl = w.lower().strip("',.")
        if wl in acronyms: result.append(w.upper())
        elif i==0 or wl not in minor: result.append(w.capitalize())
        else: result.append(w.lower())
    return " ".join(result)

# Set via env var once TYPEID is discovered via /probe_org_number_typeid
MX_ORG_NUM_TYPEID = os.environ.get("MX_ORG_NUM_TYPEID","")

def mx_find_by_org_number(reg_no):
    """Find Company entry by TYPEID(114) — confirmed working format."""
    import requests as req
    try:
        reg_int = int(str(reg_no).strip())
    except:
        return None
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    body = {
        "AbEntry": {
            "Scope": {"Fields": {"Key": 1, "CompanyName": 1, "/AbEntry/Udf/$TYPEID(114)": 1}},
            "Criteria": {"SearchQuery": {"$AND": [
                {"Type": {"$EQ": "Company"}},
                {"/AbEntry/Udf/$TYPEID(114)": {"$EQ": reg_int}}
            ]}}
        },
        "Configuration": {"Drivers": {"IAbEntrySearcher": "Maximizer.Model.Access.Sql.AbEntrySearcher"}},
        "Compatibility": {"AbEntryKey": "2.0"}
    }
    try:
        r = req.post(f"{MX_BASE}/AbEntryRead", headers=hdrs, json=body, timeout=15)
        d = r.json()
        items = d.get("AbEntry",{}).get("Data",[])
        if not isinstance(items, list): items = [items] if items else []
        if items:
            key  = items[0].get("Key","")
            name = items[0].get("CompanyName","")
            print(f"  Found by TYPEID(114): {name}")
            return {"key": key, "companyName": name}
    except Exception as e:
        print(f"  mx_find error: {e}")
    return None

def build_charity_data_full(c):
    """Build complete Company entry data with ALL UDFs for single create/update call."""
    name = title_case(c.get("name","") or "Unknown Charity")[:79]
    data = {"Type": "Company", "CompanyName": name}
    if c.get("phone"):   data["Phone1"]  = str(c["phone"])[:30]
    if c.get("email"):   data["Email1"]  = str(c["email"])[:100]
    if c.get("website"): data["WebSite"] = str(c["website"])[:200]

    # Address fields — all capped at 79 chars per Maximizer limit
    # From CCToMaximizerCRM.exe: AddressLine1 = lines 1+2+3 joined, City=line4, State=line5
    a1 = str(c.get("address1","") or "")
    a2 = str(c.get("address2","") or "")
    a3 = str(c.get("address3","") or "")
    combined = " ".join(p for p in [a1, a2, a3] if p).strip()
    # Split at word boundary before 79 chars
    if len(combined) <= 79:
        line1, line2 = combined, ""
    else:
        split_at = combined.rfind(" ", 0, 79)
        if split_at == -1: split_at = 79
        line1 = combined[:split_at].strip()
        line2 = combined[split_at:].strip()[:79]
    city    = str(c.get("county","") or "")[:79]   # address_line_four
    state   = str(c.get("city","") or "")[:79]     # address_line_five
    zipcode = str(c.get("postcode","") or "")[:10]
    if line1 or zipcode or city:
        addr_dict = {
            "AddressLine1": line1,
            "City":          city,
            "StateProvince": state,
            "ZipCode":       zipcode,
            "Country":       "England"   # All CC charities are in England/Wales
        }
        if line2:
            addr_dict["AddressLine2"] = line2
        data["Address"] = addr_dict

    fin = c.get("financials") or {}
    what = c.get("what","") or fin.get("what","")
    who  = c.get("who","")  or fin.get("who","")
    how  = c.get("how","")  or fin.get("how","")
    region = str(c.get("geo_area","") or c.get("region","") or "throughout england").strip()
    la = str(c.get("local_authority","") or c.get("town","") or "").strip()

    # Organisation Number = TYPEID(114) as integer (from CCToMaximizerCRM.exe)
    # Charity Suffix = TYPEID(263) = 0 for main charity
    reg = str(c.get("reg_number","")).strip()
    if reg:
        try:
            data["/AbEntry/Udf/$TYPEID(114)"] = int(reg)
            data["/AbEntry/Udf/$TYPEID(263)"] = 0
        except: pass

    # Multi-value table UDFs
    what_keys = _multi_keys(WHAT_MAP, what)
    if what_keys: data["/AbEntry/Udf/$TYPEID(111)"] = what_keys
    who_keys = _multi_keys(WHO_MAP, who)
    if who_keys: data["/AbEntry/Udf/$TYPEID(112)"] = who_keys
    how_keys = _multi_keys(HOW_MAP, how)
    if how_keys: data["/AbEntry/Udf/$TYPEID(113)"] = how_keys
    rk = _lookup(REGION_MAP, region)
    if rk: data["/AbEntry/Udf/$TYPEID(109)"] = rk
    lk = _lookup(LOCAL_AUTH_MAP, la)
    if lk: data["/AbEntry/Udf/$TYPEID(108)"] = lk
    # Linked Charity: "1"=No (main charity, suffix=0), "2"=Yes (linked, suffix>0)
    linked_value = "2" if int(c.get("charity_suffix", 0) or 0) > 0 else "1"
    data["/AbEntry/Udf/$TYPEID(264)"] = linked_value

    # Caller (TYPEID 378) - string field, who is calling from dashboard
    caller = str(c.get("caller","") or "").strip()
    if caller:
        data["/AbEntry/Udf/$TYPEID(378)"] = caller[:50]

    # Caller Status (TYPEID 379) - currently Yes/No, will be text after admin change
    # Map dashboard status to value. If status is set, send as string
    status = str(c.get("status","") or "").strip()
    if status:
        data["/AbEntry/Udf/$TYPEID(379)"] = status[:50]

    return data

def build_charity_base(c):
    """Alias for full data build."""
    return build_charity_data_full(c)

def _mx_find_key_by_creation_date(company_name, created_after_iso):
    """Find recently created Individual entry by lastName=reg_number."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    name_lower = company_name.lower()
    match_parts = [name_lower[:15], name_lower[:10], name_lower[:8]]

    bodies = [
        # ONLY working pattern: empty searchQuery, sorted by recent
        {"abEntry": {"criteria": {"searchQuery": {}, "top": 100},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1}},
                     "orderBy": {"fields": [{"lastModifyDate": "DESC"}]}}},
    ]

    for i, body in enumerate(bodies):
        try:
            r = req.post(f"{MX_BASE}/AbEntryRead", headers=hdrs, json=body, timeout=12)
            d = r.json()
            if d.get("Code") != 0:
                print(f"  Strategy {i+1}: Code={d.get('Code')} Msg={str(d.get('Msg',''))[:60]}")
                continue
            items = d.get("abEntry",{}).get("Data",[])
            print(f"  Strategy {i+1}: {len(items)} items returned")
            for item in items:
                cn = item.get("companyName","").lower()
                t  = item.get("type","")
                # Log first 5 items to see what's there
                if items.index(item) < 5:
                    print(f"    [{t}] '{cn[:35]}'")
                for mp in match_parts:
                    if mp and mp in cn:
                        print(f"  ✓ MATCH strategy {i+1}: '{item.get('companyName')}'")
                        return item.get("key","")
        except Exception as e:
            print(f"  Strategy {i+1} error: {e}")
    return ""

def build_charity_udfs(c, key):
    """Build list of individual UDF update dicts — each sent separately."""
    updates = []

    def upd(typeid, val):
        if val:
            updates.append({"Key": key,
                            f"/AbEntry/Udf/$TYPEID({typeid})": str(val)})

    # Pull what/who/how from financials if not directly on c
    fin = c.get("financials") or {}
    what = c.get("what","") or fin.get("what","")
    who  = c.get("who","")  or fin.get("who","")
    how  = c.get("how","")  or fin.get("how","")

    # Organisation Number = TYPEID(114), Suffix = TYPEID(263)
    reg = str(c.get("reg_number","")).strip()
    if reg:
        try:
            upd(114, int(reg))
            upd(263, 0)
        except: pass

    # Multi-value: pass full array to table UDFs
    what_keys = _multi_keys(WHAT_MAP, what)
    if what_keys: updates.append({"Key": key, "/AbEntry/Udf/$TYPEID(111)": what_keys})

    who_keys = _multi_keys(WHO_MAP, who)
    if who_keys: updates.append({"Key": key, "/AbEntry/Udf/$TYPEID(112)": who_keys})

    how_keys = _multi_keys(HOW_MAP, how)
    if how_keys: updates.append({"Key": key, "/AbEntry/Udf/$TYPEID(113)": how_keys})

    region = str(c.get("geo_area","") or c.get("region","") or "throughout england").strip()
    rk = _lookup(REGION_MAP, region)
    if rk: upd(109, rk)

    la = str(c.get("local_authority","") or c.get("town","") or "").strip()
    lk = _lookup(LOCAL_AUTH_MAP, la)
    if lk: upd(108, lk)

    policy = str(c.get("policy","") or "").strip()
    if policy:
        pk = _lookup(POLICY_MAP, policy)
        if pk: upd(261, pk)

    # Linked Charity: "1"=No, "2"=Yes based on suffix
    linked_v = "2" if int(c.get("charity_suffix", 0) or 0) > 0 else "1"
    upd(264, linked_v)

    # Caller and Caller Status
    caller = str(c.get("caller","") or "").strip()
    if caller: upd(378, caller[:50])
    status = str(c.get("status","") or "").strip()
    if status: upd(379, status[:50])

    return updates

def mx_apply_udfs(key, c):
    """Apply each UDF update separately using {Value: v} wrapper format."""
    results = []
    for upd_data in build_charity_udfs(c, key):
        try:
            # Wrap UDF values in {"Value": v} per UpdateCompanyUdfs format
            wrapped = {"Key": upd_data["Key"]}
            for k, v in upd_data.items():
                if k != "Key":
                    wrapped[k] = {"Value": v}
            r = mx_write_update(wrapped)
            field = [k for k in upd_data if k.startswith("/AbEntry")][0]
            ok = r.get("Code") == 0
            result = {"field":field,"val":upd_data[field],"Code":r.get("Code"),"ok":ok}
            if not ok:
                result["Msg"] = str(r.get("Msg",""))[:150]
            results.append(result)
        except Exception as e:
            results.append({"error":str(e)[:100]})
    return results

def _mx_get_all_keys():
    """Get Address Book entry keys — returns Companies too via address field."""
    try:
        r = mx_call("AbEntryRead", {
            "abEntry": {
                "criteria": {"searchQuery": {}, "top": 2000},
                "scope": {"fields": {"key":1, "companyName":1,
                                     "/AbEntry/Address/Key":1}},
                "orderBy": {"fields": [{"lastModifyDate": "DESC"}]}
            }
        })
        return {item["key"]: item.get("companyName","")
                for item in r.get("abEntry",{}).get("Data",[])}
    except Exception as e:
        print(f"  _mx_get_all_keys: {e}")
        return {}

def mx_create_note(entry_key, note_text, caller_name=""):
    """Create a Note on a Maximizer Address Book entry."""
    if not entry_key or not note_text: return None
    from datetime import datetime as _dt
    now_iso = _dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    html = f"<p>{note_text.replace(chr(10),'<br/>')}</p>"
    if caller_name:
        html = f"<p><strong>{caller_name}</strong> — {now_iso[:10]}</p>" + html
    body = {
        "Note": {
            "Data": {
                "Key": None,
                "ParentKey": entry_key,
                "DateTime": now_iso,
                "RichText": html,
                "Category": "Dashboard Note"
            }
        },
        "Compatibility": {"AbEntryKey": "2.0"}
    }
    try:
        resp = mx_call("Create", body)
        print(f"  mx_create_note: Code={resp.get('Code')} for {entry_key[:25]}")
        return resp
    except Exception as e:
        print(f"  mx_create_note error: {e}")
        return None

def mx_read_notes(entry_key, top=50):
    """Read all Notes for a given Address Book entry, newest first."""
    if not entry_key: return []
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    body = {
        "Note": {
            "Criteria": {
                "SearchQuery": {"ParentKey": {"$EQ": entry_key}}
            },
            "Scope": {
                "Fields": {
                    "Key": 1, "ParentKey": 1, "DateTime": 1,
                    "RichText": 1, "Category": 1, "Creator": 1
                }
            },
            "OrderBy": {"Fields": [{"DateTime": "DESC"}], "PageSize": top}
        },
        "Compatibility": {"AbEntryKey": "2.0"}
    }
    try:
        r = req.post(f"{MX_BASE}/Read", headers=hdrs, json=body, timeout=12)
        d = r.json()
        if d.get("Code") != 0:
            print(f"  mx_read_notes: Code={d.get('Code')} Msg={str(d.get('Msg',''))[:80]}")
            return []
        items = d.get("Note",{}).get("Data",[])
        if not isinstance(items, list): items = [items] if items else []
        return items
    except Exception as e:
        print(f"  mx_read_notes error: {e}")
        return []

@app.route("/api/maximizer/test_note")
def mx_test_note():
    """Test note creation directly. Pass ?reg=1202982&text=hello&caller=Mahesh."""
    reg    = str(request.args.get("reg","1202982")).strip()
    text   = str(request.args.get("text","Test note from API")).strip()
    caller = str(request.args.get("caller","TestUser")).strip()
    result = {"reg": reg, "text": text, "caller": caller}

    found = mx_find_by_org_number(reg)
    if not found or not found.get("key"):
        result["error"] = "Charity not found in Maximizer"
        return jsonify(result)

    entry_key = found["key"]
    result["entry_key"] = entry_key[:30]
    result["company_name"] = found.get("companyName","")

    # Try each endpoint and report full response
    from datetime import datetime as _dt
    now_iso = _dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    html = f"<p><strong>{caller}</strong> — {now_iso[:10]}</p><p>{text}</p>"
    body = {
        "Note": {
            "Data": {
                "Key": None,
                "ParentKey": entry_key,
                "DateTime": now_iso,
                "RichText": html,
                "Category": "Dashboard Note"
            }
        },
        "Compatibility": {"AbEntryKey": "2.0"}
    }

    result["attempts"] = {}
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    for endpoint in ("Create", "NoteCreate", "Note/Create", "NotesCreate"):
        try:
            r = req.post(f"{MX_BASE}/{endpoint}", headers=hdrs, json=body, timeout=12)
            try: resp_data = r.json()
            except: resp_data = r.text[:200]
            result["attempts"][endpoint] = {
                "status": r.status_code,
                "response": resp_data
            }
        except Exception as e:
            result["attempts"][endpoint] = {"error": str(e)[:200]}
    return jsonify(result)

def mx_create_entry(c, caller=""):
    """Create Company entry - uses TYPEID(114) for org number, gets key from response."""
    import time

    # Enrich with full CC data
    c = enrich_charity_from_cc(c)

    # Check for existing entry before creating (prevent duplicates)
    reg = str(c.get("reg_number","")).strip()
    if reg:
        existing = mx_find_by_org_number(reg)
        if existing:
            print(f"  Entry already exists — updating instead of creating")
            return mx_update_entry(existing.get("key",""), c, caller)
        print(f"  No existing entry found for reg {reg} — creating new")

    # Create with ALL UDFs in one call (Table types save correctly)
    # TYPEID(114) for org number is also in create call
    data = build_charity_data_full(c)
    print(f"  mx_create: {data.get('CompanyName','?')}")
    result = mx_write_create(data)
    print(f"  Code={result.get('Code')} Msg={str(result.get('Msg',''))[:60]}")
    if result.get("Code") != 0:
        return result

    # Get key from response (per CCToMaximizerCRM.exe: resp.AbEntry.Data.Key)
    key = result.get("_extracted_key","")
    if not key:
        # Fallback: search by TYPEID(114) = reg number
        time.sleep(1)
        found = mx_find_by_org_number(str(c.get("reg_number","")))
        if found: key = found.get("key","")

    if key:
        print(f"  Got key: {key[:25]}")
        # Apply UDFs that couldn't go in create (one at a time)
        mx_apply_udfs(key, c)
    else:
        print(f"  No key found — entry created but UDFs not applied separately")
    return result

def mx_update_entry(key, c, caller=""):
    """Update existing Company entry with all fields."""
    c = enrich_charity_from_cc(c)
    data = build_charity_data_full(c)
    data["Key"] = key
    data.pop("Type", None)
    try:
        r = mx_write_update(data)
        print(f"  mx_update: Code={r.get('Code')} {str(r.get('Msg',''))[:60]}")
        return r
    except Exception as e:
        print(f"  mx_update error: {e}")
        return {"Code": -1}

def mx_search_by_name(name):
    try:
        result = mx_read(search_query={"companyName":f"%{name[:25]}%"},
                         fields={"key":1,"companyName":1,"type":1}, top=5)
        return result.get("abEntry",{}).get("Data",[])
    except: return []

def _mx_find_company_key(name):
    """Find the key of a Company entry by name — tries multiple strategies."""
    import base64 as b64
    name_part = name[:30].lower()
    # Strategy 1: search with Company type filter
    try:
        r = mx_read(search_query={"companyName": name[:30]},
                    fields={"key":1,"companyName":1,"type":1},
                    top=20, entry_type="Company")
        for item in r.get("abEntry",{}).get("Data",[]):
            if name_part[:12] in item.get("companyName","").lower():
                return item["key"]
    except Exception as e:
        print(f"  _find_co strategy1: {e}")
    # Strategy 2: get all recent and filter
    try:
        r2 = mx_read(search_query={},
                     fields={"key":1,"companyName":1,"type":1}, top=100)
        for item in r2.get("abEntry",{}).get("Data",[]):
            if name_part[:12] in item.get("companyName","").lower():
                return item["key"]
    except Exception as e:
        print(f"  _find_co strategy2: {e}")
    # Strategy 3: Read with addresses field (deprecated but returns Companies)
    try:
        r3 = mx_call("AbEntryRead", {
            "abEntry": {
                "criteria": {"searchQuery": {"companyName": name[:30]}, "top": 10},
                "scope": {"fields": {"key":1,"companyName":1,"type":1,
                                     "/AbEntry/Address/Key":1}}
            }
        })
        for item in r3.get("abEntry",{}).get("Data",[]):
            if name_part[:12] in item.get("companyName","").lower():
                return item["key"]
    except Exception as e:
        print(f"  _find_co strategy3: {e}")
    return ""

def do_sync_one(reg, c, caller, page):
    """Sync one charity — fetch full CC data then create/update in Maximizer."""
    # Enrich with full CC API data (what/who/how, address, etc.)
    c = enrich_charity_from_cc(c)

    # Fallback classification from bulk file
    fin = c.get("financials") or {}
    if not c.get("what") and fin.get("what"): c["what"] = fin["what"]
    if not c.get("who")  and fin.get("who"):  c["who"]  = fin["who"]
    if not c.get("how")  and fin.get("how"):  c["how"]  = fin["how"]

    # Set caller and status from dashboard for Maximizer UDFs
    if caller: c["caller"] = caller
    # Get status from _statuses (per-page reg) - use the most recent across pages
    reg_str = str(c.get("reg_number","")).strip()
    if reg_str:
        for key, status in _statuses.items():
            if key.endswith(f"|{reg_str}") and status:
                c["status"] = status
                break
        # Get caller name from _called_log if set
        for key, log in _called_log.items():
            if key.endswith(f"|{reg_str}") and log.get("called_by"):
                c["caller"] = log["called_by"]
                break

    charity_name = title_case(c.get("name","") or "")
    existing = mx_find_by_org_number(reg_str) if reg_str else None
    existing_key = existing.get("key","") if existing else ""
    print(f"  {reg_str}: existing={'yes' if existing_key else 'no'} caller={c.get('caller','')} status={c.get('status','')}")

    if existing_key:
        print(f"  Updating: {charity_name[:30]}")
        mx_update_entry(existing_key, c, caller)
        return "updated", None
    else:
        print(f"  Creating: {charity_name[:30]}")
        mx_create_entry(c, caller)
        return "created", None

# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/api/maximizer/test")
def mx_test():
    server_ip = "unknown"
    try:
        r = requests.get("https://checkip.amazonaws.com",timeout=4)
        server_ip = r.text.strip()
    except: pass
    try:
        result = mx_read(fields={"key":1,"companyName":1},top=1)
        entries = result.get("abEntry",{}).get("Data",[])
        sample = entries[0].get("companyName","") if entries else ""
        return jsonify({"ok":True,"message":f"Connected! Sample: '{sample}'",
                        "database":MX_DB,"base":MX_BASE})
    except Exception as e:
        msg = str(e)
        hint = f"Render IP ({server_ip}) may not be whitelisted." if ("403" in msg or "whitelist" in msg.lower()) else ""
        return jsonify({"ok":False,"error":msg,"hint":hint,"server_ip":server_ip}), 200

@app.route("/api/maximizer/sync",methods=["POST"])
def mx_sync():
    global MX_BASE
    data      = request.json or {}
    charities = data.get("charities",[])
    page      = data.get("page","prospects")
    caller    = data.get("caller","")
    base_url  = data.get("base_url","").strip()
    if base_url: MX_BASE = base_url.rstrip("/")
    sync_key  = f"mx_sync_{page}"
    with _bg_lock:
        if _bg_status.get(sync_key)=="loading":
            return jsonify({"ok":False,"message":"Sync already running"}),409
    def _worker():
        with _bg_lock: _bg_status[sync_key]="loading"
        res={"created":0,"updated":0,"errors":0,"mx_callers_pulled":0,
             "error":"","error_details":[]}
        try:
            for c in charities[:100]:
                reg=c.get("reg_number","")
                if not reg: continue
                try:
                    action,_=do_sync_one(reg,c,caller,page)
                    if action=="created": res["created"]+=1
                    else: res["updated"]+=1
                except Exception as e:
                    err_msg = f"{reg}: {str(e)[:120]}"
                    print(f"  sync_err: {err_msg}")
                    res["errors"]+=1
                    res["error_details"].append(err_msg)
        except Exception as e:
            res["error"]=str(e)
        finally:
            cache_set(sync_key,res)
            with _bg_lock: _bg_status[sync_key]="idle"
        print(f"Mx sync done: {res}")
    threading.Thread(target=_worker,daemon=True).start()
    return jsonify({"ok":True,"status":"loading",
                    "message":f"Syncing {len(charities)} charities…"}),202

@app.route("/api/maximizer/sync_status")
def mx_sync_status():
    page=request.args.get("page","prospects")
    sync_key=f"mx_sync_{page}"
    with _bg_lock: status=_bg_status.get(sync_key,"idle")
    result=cache_get(sync_key,max_age=60) or {}
    # Include error details in response for debugging
    return jsonify({"status":status,"result":result,
                    "error_details":result.get("error_details",[])})

@app.route("/api/maximizer/config",methods=["GET","POST"])
def mx_config():
    global MX_BASE
    if request.method=="POST":
        d=request.json or {}
        if d.get("base_url"): MX_BASE=d["base_url"].rstrip("/")
    return jsonify({"ok":True,"base_url":MX_BASE,"database":MX_DB,
                    "user":"MAHESH","token_expires":"2029-05-10"})

@app.route("/api/maximizer/find")
def mx_find():
    reg=request.args.get("reg","")
    name=request.args.get("name","")
    try:
        if reg:
            entry=mx_find_by_org_number(reg)
            if entry: return jsonify({"found":True,"entry":entry})
            if name:
                entries=mx_search_by_name(name[:30])
                return jsonify({"found":bool(entries),"entries":entries,"note":"by name"})
            return jsonify({"found":False,"note":f"No entry for reg {reg}"})
        return jsonify({"error":"Provide ?reg= parameter"}),400
    except Exception as e:
        return jsonify({"error":str(e)}),500

@app.route("/api/maximizer/read_org_number_typeid")
def mx_read_org_number_typeid():
    """Read Juvenis entry UDFs in small batches to find Organisation Number TYPEID."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    results = {}
    juvenis_key = "Q29udGFjdAkyNDAzMjcyNTIyMzc1Nzk5MzU4MDJDCTE="

    # Read in small batches of 10 TYPEIDs to avoid errors
    all_udf_vals = {}
    for batch_start in range(1, 50, 10):
        batch = list(range(batch_start, min(batch_start+10, 50)))
        fields = {"key":1, "companyName":1}
        for i in batch:
            fields[f"/AbEntry/Udf/$TYPEID({i})"] = 1
        try:
            r = req.post(f"{MX_BASE}/AbEntryRead", headers=hdrs, json={
                "abEntry": {
                    "criteria": {"searchQuery": {"key": juvenis_key}, "top": 1},
                    "scope": {"fields": fields}
                }
            }, timeout=10)
            d = r.json()
            if d.get("Code") == 0:
                for item in d.get("abEntry",{}).get("Data",[]):
                    for k,v in item.items():
                        if k.startswith("/AbEntry/Udf") and v and v != [] and v != 0:
                            all_udf_vals[k] = v
        except Exception as e:
            results[f"batch_{batch_start}_err"] = str(e)

    results["non_null_udfs"] = all_udf_vals
    results["found_99999"] = {k:v for k,v in all_udf_vals.items()
                               if "99999" in str(v) or 99999 in (v if isinstance(v,list) else [v])}
    return jsonify(results)

@app.route("/api/maximizer/fix_org_number")
def mx_fix_org_number():
    """Find entry via TYPEID(264) search, then probe+set Organisation Number."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    reg = request.args.get("reg","1202982")
    results = {}

    # Step 1: Find entry - try multiple search strategies
    key = ""
    c = {"reg_number": reg}
    c = enrich_charity_from_cc(c)
    name_hint = title_case(c.get("name","")).lower()[:12]

    search_bodies = [
        # Strategy 1: type=Company filter (worked in probe=1 test!)
        {"abEntry": {"criteria": {"searchQuery": {"type": "Company"}, "top": 50},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1}}}},
        # Strategy 2: TYPEID(264)=2
        {"abEntry": {"criteria": {"searchQuery": {"/AbEntry/Udf/$TYPEID(264)": "2"}, "top": 20},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1}}}},
        # Strategy 3: empty search
        {"abEntry": {"criteria": {"searchQuery": {}, "top": 200},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1}}}},
    ]
    for i, body in enumerate(search_bodies):
        try:
            r = req.post(f"{MX_BASE}/AbEntryRead", headers=hdrs, json=body, timeout=12)
            d = r.json()
            items = d.get("abEntry",{}).get("Data",[])
            results[f"search_{i+1}"] = {"Code": d.get("Code"), "count": len(items),
                                         "first3": [x.get("companyName","") for x in items[:3]]}
            for item in items:
                cn = item.get("companyName","").lower()
                if name_hint[:8] in cn or reg in cn:
                    key = item.get("key","")
                    results["found_strategy"] = i+1
                    results["found_name"] = item.get("companyName")
                    break
            if key: break
        except Exception as e:
            results[f"search_{i+1}_err"] = str(e)

    if not key:
        results["error"] = "Entry not found via any search strategy"
        return jsonify(results)

    results["key"] = key[:30]

    # Step 2: Probe TYPEIDs to find Organisation Number field
    # Try setting the value and check which one accepts it
    found_typeid = None
    probe_results = {}
    # Skip known TYPEIDs and try numeric/alphanumeric ones
    skip = {107,108,109,111,112,113,243,261,264,2}
    for typeid in list(range(1,50)) + list(range(200,270)):
        if typeid in skip: continue
        try:
            r2 = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
                json={"AbEntry":{"Data":{"Key":key,
                      f"/AbEntry/Udf/$TYPEID({typeid})": reg}}},
                timeout=5)
            d2 = r2.json()
            if d2.get("Code") == 0:
                probe_results[f"TYPEID({typeid})"] = "✓ WORKS"
                if not found_typeid:
                    found_typeid = typeid
            elif "doesn't support" not in str(d2.get("Msg","")):
                probe_results[f"TYPEID({typeid})"] = f"diff: {str(d2.get('Msg',''))[:60]}"
        except: pass

    results["probe_results"] = probe_results
    results["found_org_typeid"] = found_typeid

    # Step 3: Also try TYPEID(2) as string (it was StringField)
    try:
        r3 = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
            json={"AbEntry":{"Data":{"Key":key,
                  "/AbEntry/Udf/$TYPEID(2)": str(reg)}}},
            timeout=5)
        d3 = r3.json()
        results["typeid2_result"] = {"Code": d3.get("Code"),
                                      "Msg": str(d3.get("Msg",""))[:100]}
    except Exception as e:
        results["typeid2_err"] = str(e)

    return jsonify(results)

@app.route("/api/maximizer/probe_org_number_typeid")
def mx_probe_org_number():
    """Find the TYPEID for Organisation Number UDF by probing."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    # Find the existing test entry
    existing = mx_find_by_org_number_by_name("1202982")
    if not existing:
        return jsonify({"error": "No test entry found — run /create_test_charity first"})
    key = existing.get("key","")
    results = {"key": key, "found_at": existing.get("companyName")}
    # Probe TYPEIDs in range likely for numeric/alphanumeric UDFs
    # We know: 107,108,109,111,112,113,243,261,264 are Table/Yes-No
    # Organisation Number (Numeric) and Organisation Number New (Alphanumeric)
    # are likely in a different range. Probe 1-50 and 200-270
    for typeid in list(range(1,50)) + list(range(200,270)):
        if typeid in [107,108,109,111,112,113,243,261,264]:
            continue
        try:
            r = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
                json={"AbEntry": {"Data": {"Key": key,
                      f"/AbEntry/Udf/$TYPEID({typeid})": "1202982"}}},
                timeout=5)
            d = r.json()
            if d.get("Code") == 0:
                results[f"TYPEID({typeid})"] = "✓ WORKS — likely Organisation Number!"
            elif "doesn't support" not in str(d.get("Msg","")):
                results[f"TYPEID({typeid})"] = f"Different error: {str(d.get('Msg',''))[:60]}"
        except Exception as e:
            pass  # Skip timeouts
    return jsonify(results)

def mx_find_by_org_number_by_name(reg_no):
    """Find entry by name containing reg number."""
    try:
        result = mx_read(
            search_query={"companyName": f"%{reg_no}%"},
            fields={"key":1,"companyName":1,"type":1}, top=5
        )
        items = result.get("abEntry",{}).get("Data",[])
        for item in items:
            if str(reg_no) in item.get("companyName",""):
                return item
        return None
    except: return None

@app.route("/api/maximizer/create_test_charity")
def mx_create_test_charity():
    """Create or update test entry — checks for duplicates by TYPEID(114)."""
    c = {"reg_number": "1202982",
         "name": "ST GEORGE'S INDIAN ORTHODOX CHURCH, LONDON",
         "caller": "Muhanna",
         "status": "Contacted"}
    c = enrich_charity_from_cc(c)
    result = {"cc_data": {k:v for k,v in c.items() if k not in ("financials",)}}

    try:
        # STEP 1: Check for existing entry by TYPEID(114) BEFORE create
        existing = mx_find_by_org_number("1202982")
        if existing and existing.get("key"):
            existing_key = existing["key"]
            result["action"] = "UPDATE (existing entry found)"
            result["existing_key"] = existing_key[:30]
            result["existing_name"] = existing.get("companyName","")
            # Build data without Type (can't change type on update)
            data = build_charity_data_full(c)
            data.pop("Type", None)
            data["Key"] = existing_key
            result["data_sent_address"] = data.get("Address",{})
            result["data_sent_company"] = data.get("CompanyName","")
            result["udfs_in_create"] = [k for k in data if k.startswith("/AbEntry")]
            resp = mx_call("AbEntryUpdate", {"AbEntry":{"Data":data}})
            result["update_code"] = resp.get("Code")
            result["update_msg"]  = str(resp.get("Msg",""))[:100]
            result["full_create_response"] = resp
            result["SUCCESS"] = resp.get("Code") == 0
            result["note"] = "UPDATED existing entry — no duplicate created"
            result["fields_set"] = list(resp.get("AbEntry",{}).get("Data",{}).keys())
        else:
            # STEP 2: Create new entry (only when no duplicate exists)
            result["action"] = "CREATE (no existing entry)"
            data = build_charity_data_full(c)
            result["type_check"] = data.get("Type","MISSING")
            result["udfs_in_create"] = [k for k in data if k.startswith("/AbEntry")]
            result["data_sent_address"] = data.get("Address",{})
            result["data_sent_company"] = data.get("CompanyName","")
            resp = mx_write_create(data)
            result["create_code"] = resp.get("Code")
            result["create_msg"]  = str(resp.get("Msg",""))[:100]
            result["full_create_response"] = resp
            result["SUCCESS"] = resp.get("Code") == 0
            if resp.get("Code") == 0:
                result["note"] = "CREATED new entry"
            else:
                result["note"] = f"CREATE FAILED: {str(resp.get('Msg',''))[:100]}"
            result["fields_set"] = list(resp.get("AbEntry",{}).get("Data",{}).keys())

        # STEP 3: Verify find works after operation
        import time; time.sleep(1)
        found = mx_find_by_org_number("1202982")
        result["find_after_create"] = found.get("companyName","NOT FOUND") if found else "NOT FOUND"

    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)

@app.route("/api/maximizer/probe_caller")
def mx_probe_caller():
    """Probe specifically near TYPEID(378) and test different value types."""
    import requests as req, time
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    results = {"attempts": {}}

    found = mx_find_by_org_number("1202982")
    if not found or not found.get("key"):
        return jsonify({"error": "No test entry."})
    key = found["key"]
    results["test_key"] = key[:30]

    # Try TYPEIDs near 378 with different value types
    # 378 is Caller (string). Caller Status was created with it - likely 377 or 379
    test_typeids = [370, 371, 372, 373, 374, 375, 376, 377, 379, 380, 381, 382, 383, 384, 385]

    for typeid in test_typeids:
        attempts = {}
        # Try string
        for val_type, val in [("str", "STATUSCHECK"), ("int", 1), ("table", ["1"]),
                               ("yn", "1"), ("yn2", "2")]:
            try:
                r = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
                    json={"AbEntry":{"Data":{"Key":key,
                          f"/AbEntry/Udf/$TYPEID({typeid})": val}},
                          "Compatibility":{"AbEntryKey":"2.0"}},
                    timeout=3)
                d = r.json()
                if d.get("Code") == 0:
                    attempts[val_type] = "WORKS"
                else:
                    msg = str(d.get("Msg",""))[:80]
                    if "Too Many" in msg: time.sleep(2)
                    elif "doesn't support" not in msg:
                        attempts[val_type] = f"err: {msg[:50]}"
            except: pass
            time.sleep(0.1)
        if attempts:
            results["attempts"][f"TYPEID({typeid})"] = attempts

    results["note"] = "Check Maximizer Caller Status field. Tell me what value it shows now."
    return jsonify(results)


@app.route("/api/maximizer/update_by_id")
def mx_update_by_id():
    """Apply UDFs to Company entry. Usage: ?id=IDENTIFICATION&reg=REG_NUMBER"""
    import base64 as b64, requests as req
    identification = request.args.get("id","").strip()
    reg = request.args.get("reg","1202982").strip()
    probe_org = request.args.get("probe","0") == "1"
    if not identification or identification in ("PASTE_ID_HERE","YOUR_IDENTIFICATION_HERE"):
        return jsonify({"error":"Paste the actual Identification number from Maximizer System Information"}), 400

    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    c = {"reg_number": reg}
    c = enrich_charity_from_cc(c)
    results = {"identification": identification, "reg": reg, "cc_fetched": bool(c.get("what"))}

    # Find real key via TYPEID(2) search (most reliable)
    found = mx_find_by_org_number(reg)
    if found:
        key = found.get("key","")
        results["key_source"] = "TYPEID(2) search"
    else:
        # Fallback: try all suffix variants with the identification
        key = ""
        for suffix in ["0","1","2","3",""]:
            try_key = b64.b64encode(f"Company	{identification}	{suffix}".encode()).decode()
            try:
                r = __import__('requests').post(
                    f"{MX_BASE}/AbEntryRead", headers=hdrs,
                    json={"abEntry":{"criteria":{"searchQuery":{},"top":1},
                                     "scope":{"fields":{"key":1,"companyName":1}}}},
                    timeout=5
                )
                # Just use suffix 0 as fallback
                key = b64.b64encode(f"Company	{identification}	0".encode()).decode()
                results["key_source"] = f"constructed suffix=0"
            except: pass
            break

    if not key:
        results["error"] = "Could not find entry"
        return jsonify(results)

    results["key"] = key[:30]+"..."

    # Apply each UDF separately
    udf_results = mx_apply_udfs(key, c)
    results["udf_results"] = udf_results

    # Probe for Organisation Number TYPEID if requested
    if probe_org:
        found_typeid = None
        for typeid in list(range(1,50)) + list(range(200,280)):
            if typeid in [107,108,109,111,112,113,243,261,264]: continue
            try:
                r = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
                    json={"AbEntry":{"Data":{"Key":key,
                          f"/AbEntry/Udf/$TYPEID({typeid})": reg}}},
                    timeout=5)
                d = r.json()
                if d.get("Code") == 0:
                    results[f"TYPEID_{typeid}"] = f"✓ WORKS for org number!"
                    found_typeid = typeid
                    break
                elif "doesn't support" not in str(d.get("Msg","")):
                    results[f"TYPEID_{typeid}"] = f"different error: {str(d.get('Msg',''))[:40]}"
            except: pass
        results["org_num_typeid"] = found_typeid
    else:
        results["hint"] = "Add &probe=1 to find the Organisation Number TYPEID"

    return jsonify(results)

@app.route("/api/maximizer/fill_latest_company")
def mx_fill_latest_company():
    """Find entry by companyName in top 100 recent, apply all UDFs."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    reg = request.args.get("reg","1202982")
    results = {}

    try:
        r = req.post(f"{MX_BASE}/AbEntryRead", headers=hdrs, json={
            "abEntry": {
                "criteria": {"searchQuery": {}, "top": 100},
                "scope": {"fields": {"key":1,"companyName":1,"type":1}},
                "orderBy": {"fields": [{"lastModifyDate": "DESC"}]}
            }
        }, timeout=12)
        d = r.json()
        items = d.get("abEntry",{}).get("Data",[])
        results["total"] = len(items)
        results["first_5"] = [{"n":x.get("companyName",""),"t":x.get("type","")} for x in items[:5]]

        # Get CC data for matching
        c = {"reg_number": reg}
        c = enrich_charity_from_cc(c)
        name_lc = title_case(c.get("name","")).lower()[:15]

        for item in items:
            cn = item.get("companyName","").lower()
            if name_lc[:10] in cn or reg in cn:
                key = item.get("key","")
                results["found"] = item.get("companyName")
                results["key"] = key[:30]
                results["udf_results"] = mx_apply_udfs(key, c)
                return jsonify(results)

        results["note"] = f"Not found. Looking for '{name_lc[:15]}' in {len(items)} entries"
    except Exception as e:
        results["error"] = str(e)
    return jsonify(results)

@app.route("/api/maximizer/apply_udfs_to_latest")
def mx_apply_to_latest():
    """Try all key variants for the given Identification, apply UDFs to working one."""
    import requests as req, base64 as b64
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    identification = request.args.get("id","").strip()
    reg = request.args.get("reg","1202982")
    results = {"identification": identification, "reg": reg}

    if not identification:
        return jsonify({"error": "Provide ?id=IDENTIFICATION&reg=REG_NUMBER"}), 400

    c = {"reg_number": reg}
    c = enrich_charity_from_cc(c)

    # Try every possible key variant, log ALL responses
    attempts = {}
    working_key = ""
    for type_str in ["Company", "Contact", "Individual"]:
        for suffix in ["0", "1", "2", "3", ""]:
            raw = f"{type_str}\t{identification}\t{suffix}"
            key = b64.b64encode(raw.encode()).decode()
            try:
                r = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
                    json={"AbEntry": {"Data": {"Key": key,
                          "/AbEntry/Udf/$TYPEID(264)": "2"}}},
                    timeout=6)
                d = r.json()
                label = f"{type_str}_{suffix or 'empty'}"
                attempts[label] = {
                    "Code": d.get("Code"),
                    "Msg": str(d.get("Msg",""))[:120]
                }
                if d.get("Code") == 0:
                    working_key = key
                    results["working_format"] = label
                    break
            except Exception as e:
                attempts[f"{type_str}_{suffix or 'empty'}"] = {"error": str(e)[:80]}
        if working_key:
            break

    results["attempts"] = attempts
    if not working_key:
        results["error"] = "No working key — see attempts for exact error messages"
        return jsonify(results)

    results["udf_results"] = mx_apply_udfs(working_key, c)
    return jsonify(results)

# ════════════════════════════════════════════════════════════════════════════════
# RINGCENTRAL WEBPHONE INTEGRATION (Phase 2 — WebRTC in-browser calls)
# ════════════════════════════════════════════════════════════════════════════════
# Flow: backend exchanges long-lived JWT → short-lived access_token → calls SIP
# provisioning endpoint to get sipInfo (one-day SIP credentials). Frontend uses
# sipInfo + the @ringcentral/web-phone SDK to register over WSS and place calls.
# Audio flows via WebRTC peer connection — no RC desktop app needed.

RC_SERVER_URL    = os.environ.get("RC_SERVER_URL", "https://platform.ringcentral.com").rstrip("/")
RC_CLIENT_ID     = os.environ.get("RC_CLIENT_ID", "")
RC_CLIENT_SECRET = os.environ.get("RC_CLIENT_SECRET", "")
RC_JWT           = os.environ.get("RC_JWT", "")
RC_FROM_EXT      = os.environ.get("RC_FROM_EXT", "")
RC_FROM_NUMBER   = os.environ.get("RC_FROM_NUMBER", "")
RC_CALLER_ID     = os.environ.get("RC_CALLER_ID", "")

_rc_token_cache  = {"access_token": "", "expires_at": 0.0}
_rc_token_lock   = threading.Lock()

def _rc_configured():
    return bool(RC_CLIENT_ID and RC_CLIENT_SECRET and RC_JWT)

def _rc_get_access_token():
    """Exchange the JWT credential for an OAuth access_token. Cached for ~50 min."""
    import time, base64 as _b64
    with _rc_token_lock:
        now = time.time()
        if _rc_token_cache["access_token"] and _rc_token_cache["expires_at"] > now + 60:
            return _rc_token_cache["access_token"]
        basic = _b64.b64encode(f"{RC_CLIENT_ID}:{RC_CLIENT_SECRET}".encode()).decode()
        r = requests.post(
            f"{RC_SERVER_URL}/restapi/oauth/token",
            headers={"Authorization": f"Basic {basic}",
                     "Content-Type": "application/x-www-form-urlencoded",
                     "Accept": "application/json"},
            data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                  "assertion": RC_JWT},
            timeout=30,
        )
        r.raise_for_status()
        d = r.json()
        _rc_token_cache["access_token"] = d.get("access_token", "")
        # Cache for up to 50 min (RC tokens last 3600s; refresh well before expiry)
        _rc_token_cache["expires_at"] = now + min(int(d.get("expires_in", 3600)) - 120, 3000)
        return _rc_token_cache["access_token"]

def _rc_provision_sip():
    """Provision a SIP device → returns sipInfo dict the WebPhone SDK consumes."""
    tok = _rc_get_access_token()
    r = requests.post(
        f"{RC_SERVER_URL}/restapi/v1.0/client-info/sip-provision",
        headers={"Authorization": f"Bearer {tok}",
                 "Content-Type": "application/json",
                 "Accept": "application/json"},
        json={"sipInfo": [{"transport": "WSS"}]},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

@app.route("/api/rc/status")
def api_rc_status():
    """Diagnostic — returns config + auth status, never secrets."""
    if not _rc_configured():
        missing = [k for k,v in {"RC_CLIENT_ID":RC_CLIENT_ID,
                                  "RC_CLIENT_SECRET":RC_CLIENT_SECRET,
                                  "RC_JWT":RC_JWT}.items() if not v]
        return jsonify({"configured": False, "missing_env_vars": missing})
    try:
        tok = _rc_get_access_token()
        return jsonify({
            "configured": True,
            "auth_ok": bool(tok),
            "server": RC_SERVER_URL,
            "from_ext": RC_FROM_EXT,
            "from_number": RC_FROM_NUMBER,
            "caller_id": RC_CALLER_ID,
        })
    except requests.HTTPError as e:
        body = ""
        try: body = e.response.text[:300]
        except: pass
        return jsonify({"configured": True, "auth_ok": False,
                        "error": f"HTTP {e.response.status_code}", "detail": body}), 200
    except Exception as e:
        return jsonify({"configured": True, "auth_ok": False, "error": str(e)[:300]}), 200

@app.route("/api/rc/sip")
def api_rc_sip():
    """Return sipInfo so the browser's WebPhone SDK can register over WSS."""
    if not _rc_configured():
        return jsonify({"ok": False, "error": "RingCentral not configured on the server"}), 400
    # Minimal identity guard — caller must have identified themselves in the dashboard.
    caller = (request.args.get("caller","") or "").strip()
    if not caller:
        return jsonify({"ok": False,
                        "error": "Select your name in the 'Calling as:' dropdown first."}), 400
    try:
        d = _rc_provision_sip()
        sip_info = (d.get("sipInfo") or [{}])[0]
        return jsonify({
            "ok": True,
            "sipInfo": sip_info,
            "device_id": (d.get("device") or {}).get("id"),
            "caller_id": RC_CALLER_ID,
            "from_number": RC_FROM_NUMBER,
            "from_ext": RC_FROM_EXT,
        })
    except requests.HTTPError as e:
        body = ""
        try: body = e.response.text[:400]
        except: pass
        return jsonify({"ok": False,
                        "error": f"RC API error: HTTP {e.response.status_code}",
                        "detail": body}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:400]}), 500


# ── Standalone WebPhone test page — verify WebRTC works before main integration ─
RC_TEST_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>9M · RingCentral WebPhone Test</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  *{box-sizing:border-box}
  body{font-family:-apple-system,Segoe UI,Inter,sans-serif;background:#f3f5f8;color:#1e3a5f;margin:0;padding:30px;line-height:1.5}
  .wrap{max-width:760px;margin:0 auto;background:#fff;border-radius:14px;padding:32px;box-shadow:0 4px 20px rgba(30,58,95,.10)}
  h1{margin:0 0 4px;font-size:24px}
  .sub{color:#7b8896;font-size:13px;margin-bottom:24px}
  .status-row{display:flex;align-items:center;gap:10px;margin-bottom:18px;padding:12px 16px;background:#f7f9fc;border-radius:8px;border-left:4px solid #ccc;font-size:14px}
  .status-row.ok{border-color:#2d7a3a;background:#eef9f1}
  .status-row.err{border-color:#c0392b;background:#fdecec}
  .status-row.busy{border-color:#e8b339;background:#fef8e8}
  .pill{font-size:11px;font-weight:700;padding:2px 8px;border-radius:99px;background:#e0e7ee;color:#1e3a5f;text-transform:uppercase;letter-spacing:.04em}
  .ok .pill{background:#2d7a3a;color:#fff}
  .err .pill{background:#c0392b;color:#fff}
  .busy .pill{background:#e8b339;color:#fff}
  .row{display:flex;gap:10px;margin-bottom:14px}
  input[type=tel]{flex:1;padding:12px 14px;border:1.5px solid #cfd6df;border-radius:8px;font-size:15px;font-family:inherit;color:#1e3a5f}
  input[type=tel]:focus{outline:none;border-color:#1e3a5f;box-shadow:0 0 0 3px rgba(232,179,57,.2)}
  button{padding:12px 22px;border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer;font-family:inherit;transition:all .12s}
  .btn-call{background:#1e3a5f;color:#fff}
  .btn-call:hover:not(:disabled){background:#152a44;transform:translateY(-1px);box-shadow:0 4px 10px rgba(30,58,95,.25)}
  .btn-hang{background:#c0392b;color:#fff}
  .btn-hang:hover:not(:disabled){background:#a02f23}
  button:disabled{opacity:.35;cursor:not-allowed;transform:none;box-shadow:none}
  .timer{font-family:'JetBrains Mono',Consolas,monospace;font-size:32px;text-align:center;color:#1e3a5f;font-weight:700;margin:20px 0;letter-spacing:.04em}
  .log{margin-top:24px;background:#0f1729;color:#a8c5e2;border-radius:8px;padding:14px 18px;height:260px;overflow-y:auto;font-family:'JetBrains Mono',Consolas,monospace;font-size:12px;line-height:1.7}
  .log .l-info{color:#a8c5e2}
  .log .l-warn{color:#e8b339}
  .log .l-err{color:#ff8a80}
  .log .l-ok{color:#69d68b}
  .log .l-evt{color:#7fb3ff}
  .log .ts{color:#5a7090;margin-right:8px}
  .footer-note{margin-top:18px;padding:10px 14px;background:#fef8e8;border:1px solid #f0d68a;border-radius:6px;font-size:12px;color:#8a6d1f}
</style>
</head>
<body>
<div class="wrap">
  <h1>📞 RingCentral WebPhone — Connection Test</h1>
  <div class="sub">Proves WebRTC + SIP works end-to-end before integration into the main dashboard.</div>

  <div id="status" class="status-row"><span class="pill" id="pill">Loading</span><span id="status-text">Initialising…</span></div>

  <div class="row" style="margin-bottom:8px">
    <label for="cidSelect" style="display:flex;align-items:center;font-size:12px;color:#7b8896;font-weight:600;padding-right:10px">Outbound caller ID:</label>
    <select id="cidSelect" style="flex:1;padding:10px;border:1.5px solid #cfd6df;border-radius:8px;font-size:13px;font-family:inherit;color:#1e3a5f">
      <option value="default">[Default — let RingCentral pick]</option>
      <option value="freephone">+44 800 208 4749 (company freephone)</option>
      <option value="direct">+44 121 818 4992 (her direct line)</option>
    </select>
  </div>
  <div class="row" style="margin-bottom:8px">
    <button id="micBtn" onclick="checkMic()" style="background:#7b8896;color:#fff;flex:1">🎤 Check microphone permission</button>
    <button id="inspectBtn" onclick="inspectWP()" style="background:#7b8896;color:#fff;flex:1">🔍 Inspect WebPhone state</button>
  </div>
  <div class="row">
    <input id="dest" type="tel" placeholder="Number to dial — e.g. +447700900123" value="">
    <button id="callBtn" class="btn-call" disabled onclick="placeCall()">📞 Call</button>
    <button id="hangBtn" class="btn-hang" disabled onclick="hangUp()">✕ Hang Up</button>
  </div>

  <div class="timer" id="timer">00:00</div>

  <div class="log" id="log"></div>

  <div class="footer-note">
    <strong>Tip:</strong> First time you click Call, the browser will ask for microphone permission — click Allow.
    Try calling your own mobile first to verify audio works both ways.
  </div>
</div>

<audio id="remoteAudio" autoplay></audio>

<script>
// ── Robust SDK loader — tries multiple CDN paths, logs what works ───────────
function loadSDK(){
  return new Promise((resolve, reject) => {
    if(typeof WebPhone !== 'undefined'){ log('WebPhone already present','l-ok'); return resolve(); }
    const urls = [
      'https://cdn.jsdelivr.net/npm/ringcentral-web-phone/dist/index.iife.js',
      'https://unpkg.com/ringcentral-web-phone/dist/index.iife.js',
      'https://cdn.jsdelivr.net/npm/ringcentral-web-phone@latest/dist/index.iife.js',
      'https://cdn.jsdelivr.net/npm/ringcentral-web-phone/dist/index.umd.js',
      'https://cdn.jsdelivr.net/npm/ringcentral-web-phone/dist/esm/index.umd.js'
    ];
    let i = 0;
    function tryNext(){
      if(i >= urls.length){ reject(new Error('all CDN URLs exhausted')); return; }
      const url = urls[i++];
      log('Trying: '+url,'l-info');
      const tag = document.createElement('script');
      tag.src = url;
      tag.onload = () => {
        // Locate the class — could be at any of several globals depending on bundle format
        const candidates = [
          window.WebPhone,
          window.RingCentralWebPhone,
          window.ringcentralWebPhone,
          (window['ringcentral-web-phone'] && window['ringcentral-web-phone'].default),
          (window.WebPhone && window.WebPhone.default),
          (window.WebPhone && window.WebPhone.WebPhone)
        ].filter(Boolean);
        if(candidates.length){
          window.WebPhone = candidates[0];
          log('SDK loaded · class found at global · '+url,'l-ok');
          resolve();
        }else{
          log('Script loaded but no WebPhone class found in window globals','l-warn');
          tryNext();
        }
      };
      tag.onerror = () => { log('Network failure for: '+url,'l-warn'); tryNext(); };
      document.head.appendChild(tag);
    }
    tryNext();
  });
}

let webPhone = null, callSession = null, callerName = '', timerInterval = null, callStart = 0;

function $(id){return document.getElementById(id);}
function ts(){return new Date().toTimeString().slice(0,8);}
function log(msg, cls){
  const line = document.createElement('div');
  line.innerHTML = `<span class="ts">${ts()}</span><span class="${cls||'l-info'}">${msg}</span>`;
  $('log').appendChild(line);
  $('log').scrollTop = $('log').scrollHeight;
}
function setStatus(txt, cls){
  $('status').className = 'status-row ' + (cls||'');
  $('pill').textContent = cls==='ok'?'Ready':cls==='busy'?'Busy':cls==='err'?'Error':'Loading';
  $('status-text').textContent = txt;
}
function tick(){
  if(!callStart) return;
  const s = Math.floor((Date.now()-callStart)/1000);
  $('timer').textContent = String(Math.floor(s/60)).padStart(2,'0')+':'+String(s%60).padStart(2,'0');
}
function startTimer(){callStart = Date.now(); timerInterval = setInterval(tick, 500);}
function stopTimer(){clearInterval(timerInterval); timerInterval=null; callStart=0; $('timer').textContent='00:00';}

async function init(){
  // 1) Read caller name from URL (?caller=Muhanna) or prompt
  const params = new URLSearchParams(window.location.search);
  callerName = (params.get('caller')||'').trim();
  if(!callerName) callerName = prompt('Your name (matches the "Calling as:" dropdown in the dashboard):','Muhanna') || '';
  if(!callerName){
    setStatus('No caller name — refresh and provide one.','err');
    log('Aborted: no caller name provided','l-err'); return;
  }
  log(`Caller identified as: ${callerName}`,'l-info');

  // 2) Verify server config
  setStatus('Checking server configuration…','busy');
  log('Fetching /api/rc/status …','l-info');
  try{
    const s = await fetch('/api/rc/status').then(r=>r.json());
    if(!s.configured){
      setStatus('Server missing RC env vars: '+(s.missing_env_vars||[]).join(', '),'err');
      log('Server not configured: '+JSON.stringify(s),'l-err'); return;
    }
    if(!s.auth_ok){
      setStatus('Server can\\'t authenticate to RingCentral.','err');
      log('Auth failure: '+(s.error||'unknown')+' — '+(s.detail||''),'l-err'); return;
    }
    log(`Server auth OK · from_ext=${s.from_ext} · caller_id=${s.caller_id}`,'l-ok');
  }catch(e){
    setStatus('Failed to reach /api/rc/status','err');
    log('Network error: '+e.message,'l-err'); return;
  }

  // 3) Fetch sipInfo
  setStatus('Provisioning SIP credentials…','busy');
  log('Fetching sipInfo from /api/rc/sip …','l-info');
  let sipResp;
  try{
    sipResp = await fetch('/api/rc/sip?caller='+encodeURIComponent(callerName)).then(r=>r.json());
  }catch(e){
    setStatus('Failed to fetch sipInfo','err'); log('Network: '+e.message,'l-err'); return;
  }
  if(!sipResp.ok){
    setStatus('SIP provisioning failed','err');
    log('Provisioning error: '+(sipResp.error||'')+' '+(sipResp.detail||''),'l-err'); return;
  }
  log('sipInfo received: domain='+(sipResp.sipInfo.domain||'?')+', transport='+(sipResp.sipInfo.transport||'?'),'l-ok');

  // 4) Load the WebPhone SDK from a CDN (try several known-good URLs)
  setStatus('Loading WebPhone SDK from CDN…','busy');
  try{
    await loadSDK();
  }catch(e){
    setStatus('Could not load WebPhone SDK from any CDN','err');
    log('All CDN sources failed: '+e.message,'l-err'); return;
  }

  setStatus('Registering with RingCentral SIP server…','busy');
  log('Initialising WebPhone instance …','l-info');

  // ── PATCH: inject STUN + TURN servers so WebRTC media can traverse NAT ───────
  // The v2 SDK only configures STUN, doesn't pick up RC's TURN credentials from
  // p-rc-ice-servers header. We wrap RTCPeerConnection so every PC the SDK creates
  // gets a full ICE config with public-TURN fallback (openrelay.metered.ca, free).
  (function(){
    const OriginalPC = window.RTCPeerConnection;
    if(!OriginalPC){ log('Browser has no RTCPeerConnection — cannot proceed','l-err'); return; }
    if(OriginalPC._9mPatched){ log('RTCPeerConnection already patched (skipping)','l-info'); return; }
    const InjectedServers = [
      {urls: 'stun:stun.l.google.com:19302'},
      {urls: 'stun:stun.cloudflare.com:3478'},
      {urls: ['turn:openrelay.metered.ca:80'],            username: 'openrelayproject', credential: 'openrelayproject'},
      {urls: ['turn:openrelay.metered.ca:443'],           username: 'openrelayproject', credential: 'openrelayproject'},
      {urls: ['turn:openrelay.metered.ca:443?transport=tcp'], username: 'openrelayproject', credential: 'openrelayproject'}
    ];
    function PatchedPC(config){
      config = config || {};
      const existing = Array.isArray(config.iceServers) ? config.iceServers : [];
      // Merge existing (SDK-provided) with our injected servers
      config.iceServers = existing.concat(InjectedServers);
      config.iceTransportPolicy = 'all';
      log('PC created · '+config.iceServers.length+' ICE servers · policy=all','l-info');
      return new OriginalPC(config);
    }
    PatchedPC.prototype = OriginalPC.prototype;
    Object.setPrototypeOf(PatchedPC, OriginalPC);
    PatchedPC._9mPatched = true;
    window.RTCPeerConnection = PatchedPC;
    log('RTCPeerConnection monkey-patched · TURN relay injected','l-ok');
  })();

  try{
    webPhone = new WebPhone({ sipInfo: sipResp.sipInfo });
    await webPhone.start();
    log('WebPhone registered successfully','l-ok');

    // Monkey-patch webPhone.emit to surface ALL events fired on the parent
    try{
      const _origEmit = webPhone.emit.bind(webPhone);
      webPhone.emit = function(ev, ...args){
        log('[webPhone] event: '+ev+(args.length?' · '+_briefArg(args[0]):''),'l-evt');
        return _origEmit(ev, ...args);
      };
      log('webPhone.emit hooked — all parent events will be logged','l-info');
    }catch(e){ log('Could not hook webPhone.emit: '+e.message,'l-warn'); }

    // Also listen for outboundCall (RC docs say this fires before await resolves)
    try{
      webPhone.on('outboundCall', (cs) => log('[webPhone] outboundCall fired · session keys: '+Object.keys(cs||{}).slice(0,12).join(','), 'l-ok'));
    }catch(e){}

    // Monkey-patch sipClient.emit (one level below webPhone) — this is where raw SIP signaling events live
    try{
      if(webPhone.sipClient && webPhone.sipClient.emit){
        const _origSip = webPhone.sipClient.emit.bind(webPhone.sipClient);
        webPhone.sipClient.emit = function(ev){
          const args = Array.prototype.slice.call(arguments, 1);
          log('[sipClient] '+ev+(args.length?' · '+_briefArg(args[0]):''),'l-evt');
          return _origSip.apply(webPhone.sipClient, arguments);
        };
        log('sipClient.emit hooked — raw SIP events will be logged','l-info');
        if(webPhone.sipClient.eventNames) log('sipClient event names registered: '+webPhone.sipClient.eventNames().join(',')||'(none)','l-info');
      }else{
        log('webPhone has no sipClient.emit to hook','l-warn');
      }
    }catch(e){ log('Could not hook sipClient: '+e.message,'l-warn'); }
    setStatus('Ready to make calls — enter a number above','ok');
    $('callBtn').disabled = false;
  }catch(e){
    setStatus('SIP registration failed','err');
    log('WebPhone error: '+(e.message||e),'l-err'); return;
  }

  // Save caller_id for outbound calls
  window._callerId = sipResp.caller_id || sipResp.from_number || '';
  log('Outbound caller_id will be: '+window._callerId,'l-info');
}

function _sipBriefLine(msg){
  // Extract first useful line of a SIP message for logging
  try{
    const txt = (typeof msg === 'string') ? msg : (msg && msg.toString ? msg.toString() : JSON.stringify(msg));
    const lines = txt.split(/\\r?\\n/).filter(Boolean);
    for(const ln of lines){
      if(/^SIP\/2\.0\s+\d{3}/.test(ln) || /^(INVITE|ACK|BYE|CANCEL|REGISTER|SUBSCRIBE|NOTIFY|OPTIONS|REFER|UPDATE|INFO)\s/.test(ln)) return ln.substring(0,160);
    }
    return (lines[0]||txt).substring(0,160);
  }catch(_){ return '?'; }
}

async function placeCall(){
  const dest = $('dest').value.trim();
  if(!dest){alert('Enter a phone number first'); return;}
  if(!webPhone){alert('WebPhone not ready'); return;}

  // Pre-flight: confirm mic permission BEFORE attempting call (silent mic = silent failure)
  log('Pre-flight: requesting microphone access…','l-info');
  try{
    const ms = await navigator.mediaDevices.getUserMedia({audio:true, video:false});
    log('Mic stream OK · tracks='+ms.getAudioTracks().length+' · device='+(ms.getAudioTracks()[0]||{}).label,'l-ok');
    ms.getTracks().forEach(t=>t.stop()); // release immediately; SDK will request again
  }catch(e){
    log('MIC ERROR: '+e.name+' — '+e.message,'l-err');
    setStatus('Microphone unavailable','err');
    return;
  }

  const cidChoice = $('cidSelect').value;
  let callerId = '';
  if(cidChoice === 'freephone') callerId = '+448002084749';
  else if(cidChoice === 'direct') callerId = '+441218184992';

  setStatus('Calling '+dest+' …','busy');
  log('Placing call to '+dest+' · callerId='+(callerId||'[default]'),'l-evt');
  $('callBtn').disabled = true;

  try{
    callSession = await webPhone.call(dest, callerId);
    log('webPhone.call() returned · session.id='+(callSession.callId||'?'),'l-info');
    log('Session keys: '+Object.keys(callSession).slice(0,20).join(','),'l-info');

    // Monkey-patch session.emit so we see EVERY event the session fires
    try{
      const _origEmit = callSession.emit.bind(callSession);
      callSession.emit = function(ev, ...args){
        log('[session] event: '+ev+(args.length?' · '+_briefArg(args[0]):''),'l-evt');
        return _origEmit(ev, ...args);
      };
      log('session.emit hooked — all events will be logged','l-info');
    }catch(e){ log('Could not hook session.emit: '+e.message,'l-warn'); }

    // Monitor the WebRTC peer connection — if ICE fails, calls die silently like this
    try{
      const pc = callSession.rtcPeerConnection;
      if(pc){
        log('RTC initial · conn='+pc.connectionState+' ice='+pc.iceConnectionState+' gather='+pc.iceGatheringState+' sig='+pc.signalingState,'l-info');
        pc.addEventListener('connectionstatechange', function(){ log('RTC connection → '+pc.connectionState, pc.connectionState==='failed'?'l-err':'l-evt'); });
        pc.addEventListener('iceconnectionstatechange', function(){ log('RTC ICE → '+pc.iceConnectionState, pc.iceConnectionState==='failed'?'l-err':'l-evt'); });
        pc.addEventListener('icegatheringstatechange', function(){ log('RTC gather → '+pc.iceGatheringState,'l-evt'); });
        pc.addEventListener('signalingstatechange', function(){ log('RTC signaling → '+pc.signalingState,'l-evt'); });
        let cands=0;
        pc.addEventListener('icecandidate', function(e){
          if(e.candidate){ cands++;
            const c = e.candidate.candidate || '';
            // Log only first few full candidates; then summarize
            if(cands<=4) log('ICE candidate #'+cands+': '+c.substring(0,140),'l-evt');
            else if(cands===5) log('ICE candidate #5+ (further candidates suppressed in log)','l-evt');
          }else log('ICE gathering DONE — total '+cands+' candidates','l-ok');
        });
        pc.addEventListener('icecandidateerror', function(e){
          log('ICE candidate ERROR: url='+(e.url||'?')+' code='+(e.errorCode||'?')+' text='+(e.errorText||'?'),'l-err');
        });

        // Log the configured ICE servers (TURN/STUN) — if empty, that's the bug
        try{
          const cfg = pc.getConfiguration();
          const servers = cfg.iceServers||[];
          log('RTCPeerConnection has '+servers.length+' ICE servers configured','l-info');
          servers.forEach(function(srv, idx){
            const urls = Array.isArray(srv.urls)?srv.urls:[srv.urls];
            urls.forEach(function(u){ log('  ICE server #'+idx+': '+u,'l-info'); });
          });
          if(servers.length===0) log('WARNING: No ICE servers — WebRTC cannot traverse NAT','l-err');
        }catch(e){ log('Could not read RTC config: '+e.message,'l-warn'); }

        // Examine the local SDP — count ICE candidates by type (host/srflx/relay)
        setTimeout(function(){
          try{
            const sdp = pc.localDescription && pc.localDescription.sdp;
            if(sdp){
              const lines = sdp.split(String.fromCharCode(10));
              const candLines = lines.filter(function(l){return l.indexOf('a=candidate:')===0;});
              log('Local SDP has '+candLines.length+' ICE candidates','l-info');
              const counts = {host:0, srflx:0, prflx:0, relay:0, other:0};
              candLines.forEach(function(l){
                const m = l.match(/typ\s+(\w+)/);
                if(m) counts[m[1]] = (counts[m[1]]||0) + 1; else counts.other++;
              });
              log('  Candidate types: host='+counts.host+'  srflx='+counts.srflx+'  prflx='+counts.prflx+'  relay='+counts.relay+'  other='+counts.other,'l-info');
              if(counts.relay===0 && counts.srflx===0) log('  → No public-reachable candidates (only local host). Media WILL fail through NAT.','l-err');
            }
          }catch(e){ log('Local SDP inspect failed: '+e.message,'l-warn'); }
        }, 500);

        // Poll RTP stats every 5s for 30s — detect whether any audio is flowing
        let statsPolls = 0;
        const statsInterval = setInterval(function(){
          statsPolls++;
          if(statsPolls > 6 || (callSession && callSession.state === 'disposed')){ clearInterval(statsInterval); return; }
          try{
            pc.getStats().then(function(report){
              let sent = 0, recv = 0, selectedPair = null;
              report.forEach(function(s){
                if(s.type==='outbound-rtp' && s.kind==='audio') sent = s.packetsSent || 0;
                if(s.type==='inbound-rtp'  && s.kind==='audio') recv = s.packetsReceived || 0;
                if(s.type==='candidate-pair' && s.nominated && s.state==='succeeded') selectedPair = s;
              });
              const pairInfo = selectedPair ? (' · pair=succeeded') : ' · NO SUCCEEDED CANDIDATE PAIR';
              log('Stats T+'+(statsPolls*5)+'s · RTP sent='+sent+' recv='+recv+pairInfo, sent>0&&recv>0?'l-ok':'l-warn');
            });
          }catch(_){}
        }, 5000);
      }else{
        log('No rtcPeerConnection on session — WebRTC not engaged','l-warn');
      }
    }catch(e){ log('RTC hook error: '+e.message,'l-err'); }

    // Print the outgoing SIP INVITE (the message the SDK built for this call)
    try{
      if(callSession.sipMessage){
        let txt = '';
        try{ txt = String(callSession.sipMessage); }catch(_){}
        if(txt){
          log('Outgoing SIP INVITE — first 14 lines:','l-info');
          const NL = String.fromCharCode(10);
          txt.split(NL).slice(0,14).forEach(function(ln){
            log('  ' + ln.substring(0, 200), 'l-info');
          });
        }else log('sipMessage present but could not stringify','l-warn');
      }else log('No sipMessage on session — INVITE may not have been constructed','l-warn');
    }catch(e){ log('SIP dump error: '+e.message,'l-warn'); }

    // Normal listeners (in case hook missed anything)
    ['answered','ringing','disposed','inboundMessage','outboundMessage','accepted','rejected','failed','progress','bye','canceled','terminated'].forEach(ev=>{
      try{ callSession.on(ev, (p) => log('  → on('+ev+') fired'+(p?' · '+_briefArg(p):''),'l-ok')); }catch(_){}
    });

    callSession.on('disposed', () => {
      setStatus('Ready to make calls','ok');
      $('callBtn').disabled = false; $('hangBtn').disabled = true;
      // keep callSession reference for inspection
      stopTimer();
    });
    callSession.on('answered', () => {
      setStatus('In call with '+dest,'ok');
      $('hangBtn').disabled = false;
      startTimer();
    });
  }catch(e){
    log('webPhone.call() threw: '+(e.message||e),'l-err');
    setStatus('Call failed — see log','err');
    $('callBtn').disabled = false; $('hangBtn').disabled = true;
  }
}

function _briefArg(a){
  try{
    if(a==null) return 'null';
    if(typeof a==='string') return a.length>180 ? a.substring(0,180)+'…' : a;
    if(typeof a==='object'){
      const keys = Object.keys(a).slice(0,8);
      // If it looks like a SIP message
      if(a.toString && a.toString.toString().indexOf('[native code]')<0){
        const t = String(a);
        if(t.startsWith('SIP') || /^[A-Z]+\s.*SIP/.test(t)) return t.split(/\\r?\\n/)[0].substring(0,160);
      }
      return '{' + keys.join(',') + '}';
    }
    return String(a).substring(0,180);
  }catch(_){ return '?'; }
}

async function checkMic(){
  log('Manual mic check…','l-info');
  try{
    const ms = await navigator.mediaDevices.getUserMedia({audio:true});
    const t = ms.getAudioTracks()[0];
    log('Mic OK · device="'+(t&&t.label)+'" · enabled='+(t&&t.enabled),'l-ok');
    ms.getTracks().forEach(t=>t.stop());
  }catch(e){
    log('Mic FAIL · '+e.name+' · '+e.message,'l-err');
  }
}

function inspectWP(){
  if(!webPhone){log('webPhone not initialised','l-warn'); return;}
  try{
    log('webPhone keys: '+Object.keys(webPhone).slice(0,30).join(','),'l-info');
    if(webPhone.eventNames) log('webPhone event names: '+webPhone.eventNames().join(','),'l-info');
    if(webPhone.userAgent) log('userAgent state: '+(webPhone.userAgent.state||'?'),'l-info');
  }catch(e){ log('inspect error: '+e.message,'l-err'); }
  if(callSession){
    log('callSession keys: '+Object.keys(callSession).slice(0,30).join(','),'l-info');
    if(callSession.state !== undefined) log('callSession.state: '+callSession.state,'l-info');
    if(callSession.status !== undefined) log('callSession.status: '+callSession.status,'l-info');
  }
}

async function hangUp(){
  if(!callSession) return;
  log('Hanging up…','l-evt');
  try{ await callSession.dispose(); }catch(e){ log('Hangup error: '+e.message,'l-err'); }
}

window.addEventListener('load', init);
</script>
</body>
</html>
"""

@app.route("/rc-test")
def rc_test_page():
    """Standalone test page — verifies WebRTC + SIP end-to-end before main integration."""
    from flask import render_template_string
    return render_template_string(RC_TEST_PAGE_HTML)
