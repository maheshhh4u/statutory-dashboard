import os, io, csv, zipfile, requests, threading
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, Response, stream_with_context

app = Flask(__name__)

API_KEY  = os.environ.get("CC_API_KEY", "80c5abfc86864f8d9330288c3521bb78")
BULK_BASE= "https://ccewuksprdoneregsadata1.blob.core.windows.net/data/txt"
PARTA_URL  = f"{BULK_BASE}/publicextract.charity_annual_return_parta.zip"
CHARITY_URL= f"{BULK_BASE}/publicextract.charity.zip"

_cache={}; _called_log={}; _comments={}; _statuses={}; _saved_searches={}
_bg_status={}; _bg_lock=threading.Lock()
_users=["Muhanna"]  # admin-managed caller list

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
        "charity_registration_status"}
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
    names={}; webs={}
    for row in stream_zip_csv(CHARITY_URL,CC):
        reg=row.get("registered_charity_number","").strip()
        if reg and row.get("charity_registration_status","").upper().strip() in ("REGISTERED","R"):
            names[reg]=row.get("charity_name",""); webs[reg]=row.get("charity_contact_web","")
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
        name=names.get(reg,"") or names.get(reg.lstrip("0"),"") or reg
        web=webs.get(reg,"") or webs.get(reg.lstrip("0"),"") or ""
        fin={"total_income":ti,"total_expenditure":te,"gov_grants":gg,"gov_contracts":gc,
             "statutory":stat,"stat_pct":pct,"prev_grants":pgg,"prev_contracts":pgc,
             "prev_statutory":pstat,"fin_year":yr,"flags":build_flags(ti,te,stat,pstat,pgc,gc)}
        obj={"reg_number":reg,"name":name,"website":web,"financials":fin,"lists":lists}
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
          "charity_registration_status","charity_contact_web"}
    results=[]
    for row in stream_zip_csv(CHARITY_URL,COLS):
        if row.get("charity_registration_status","").upper().strip() not in ("REGISTERED","R"): continue
        dt=row.get("date_of_registration","")[:10]
        if dt>=cutoff:
            results.append({"reg_number":row.get("registered_charity_number",""),
                            "name":row.get("charity_name",""),"date_registered":dt,
                            "website":row.get("charity_contact_web",""),"status":"Active"})
    results.sort(key=lambda x:x.get("date_registered",""),reverse=True)
    return {"charities":results,"count":len(results),"days":days,"cutoff":cutoff}

def compute_late():
    today=datetime.utcnow().date()
    PC={"registered_charity_number","ar_due_date","latest_fin_period_submitted_ind",
        "total_gross_income","fin_period_end_date"}
    NC={"registered_charity_number","charity_name"}
    latest={}
    for row in stream_zip_csv(PARTA_URL,PC):
        reg=row.get("registered_charity_number","").strip()
        if not reg: continue
        yr=row.get("fin_period_end_date","")[:10]
        if reg not in latest or yr>latest[reg].get("fin_period_end_date",""): latest[reg]=row
    names={row.get("registered_charity_number","").strip():row.get("charity_name","")
           for row in stream_zip_csv(CHARITY_URL,NC) if row.get("registered_charity_number","")}
    results=[]
    for reg,row in latest.items():
        if row.get("latest_fin_period_submitted_ind","").upper().strip() not in ("FALSE","0","NO","N"): continue
        due_str=row.get("ar_due_date","")[:10]
        if not due_str: continue
        try: due_d=datetime.strptime(due_str,"%Y-%m-%d").date()
        except: continue
        if due_d>=today: continue
        ml=round((today-due_d).days/30.4,1)
        results.append({"reg_number":reg,"name":names.get(reg,reg) or reg,
                        "due_date":str(due_d),"months_late":ml,
                        "latest_income":flt(row.get("total_gross_income",""))})
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
        "charity_registration_status","charity_contact_web"}
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
    COLS={"registered_charity_number","charity_name","charity_registration_status",
          "date_of_registration","date_of_removal","charity_contact_web",
          "charity_contact_phone","charity_contact_email",
          "charity_contact_address3","charity_contact_address4",
          "charity_contact_postcode","charity_activities","charity_type","latest_income"}
    results=[]
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
        if len(results)>=200: break
    return jsonify({"charities":results,"count":len(results)})

@app.route("/api/mark_called",methods=["POST"])
def mark_called():
    data=request.json or {}; reg=str(data.get("reg_number","")).strip(); page=str(data.get("page","")).strip()
    name=str(data.get("name","")).strip()
    if not reg or not page: return jsonify({"ok":False}),400
    key=f"{page}|{reg}"
    if key in _called_log: del _called_log[key]; return jsonify({"ok":True,"marked":False})
    _called_log[key]={"reg_number":reg,"name":name,"page":page,"called_by":"Muhanna",
                      "timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
    return jsonify({"ok":True,"marked":True})

@app.route("/api/comment",methods=["POST"])
def save_comment():
    data=request.json or {}; reg=str(data.get("reg_number","")).strip(); page=str(data.get("page","")).strip()
    text=str(data.get("comment","")).strip()[:400]
    if not reg or not page: return jsonify({"ok":False}),400
    _comments[f"{page}|{reg}"]=text; return jsonify({"ok":True})

@app.route("/api/status",methods=["POST"])
def save_status():
    data=request.json or {}; reg=str(data.get("reg_number","")).strip(); page=str(data.get("page","")).strip()
    status=str(data.get("status","")).strip()
    if not reg or not page: return jsonify({"ok":False}),400
    key=f"{page}|{reg}"
    if status:
        _statuses[key]=status
        if key not in _called_log:
            _called_log[key]={"reg_number":reg,"page":page,"called_by":"Muhanna",
                              "timestamp":datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
    else: _statuses.pop(key,None)
    return jsonify({"ok":True})

@app.route("/api/saved_searches",methods=["GET"])
def get_saved_searches(): return jsonify(list(_saved_searches.keys()))

@app.route("/api/saved_searches",methods=["POST"])
def save_search():
    data=request.json or {}; name=str(data.get("name","")).strip(); criteria=data.get("criteria",{})
    if not name: return jsonify({"ok":False}),400
    _saved_searches[name]=criteria; return jsonify({"ok":True})

@app.route("/api/saved_searches/<name>",methods=["GET"])
def get_saved_search(name):
    if name not in _saved_searches: return jsonify({"error":"Not found"}),404
    return jsonify(_saved_searches[name])

@app.route("/api/saved_searches/<name>",methods=["DELETE"])
def delete_saved_search(name): _saved_searches.pop(name,None); return jsonify({"ok":True})

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
    return jsonify(_users)

@app.route("/api/users", methods=["POST"])
def add_user():
    data = request.json or {}
    name = str(data.get("name","")).strip()
    if not name or len(name) > 60:
        return jsonify({"ok":False,"error":"Invalid name"}), 400
    if name not in _users:
        _users.append(name)
    return jsonify({"ok":True,"users":_users})

@app.route("/api/users/<name>", methods=["DELETE"])
def delete_user(name):
    if name in _users and name != "Muhanna":
        _users.remove(name)
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
    # If toggle=False, JS already toggled the UI — just mirror the current JS state
    # If toggle=True (legacy), do server-side toggle
    if do_toggle and key in _called_log:
        del _called_log[key]
        return jsonify({"ok":True,"marked":False})
    _called_log[key] = {
        "reg_number": reg, "name": name, "page": page,
        "called_by": caller,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    }
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
        "charity_registration_status","charity_contact_web"}

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

# ═══════════════════════════════════════════════════════════════════════
# MAXIMIZER CRM SYNC — Octopus API
# ═══════════════════════════════════════════════════════════════════════
MX_TOKEN = os.environ.get("MAXIMIZER_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteHB5ZjV6MGFwbWpub29jOXM3NCIsImlhdCI6MTc3ODUyNzEyMywiZXhwIjoxODczMDY1NjAwLCJteC1jaWQiOiJEMzIxRDMxRS04QzRBLTQyRTMtQUU5Ny03NjMzRDk5Qjk5QjIiLCJteC13c2lkIjoiOENDNkVBRkYtQkFERi00RDY5LTgyRTktNzQxREFERUU2QjU0IiwibXgtZGIiOiJjZmM0MWQwY2Y0OWQ0MjViYjQ0ZTQ0YzA3ZDdiMTBmZCIsIm14LXVpZCI6Ik1BSEVTSCIsIm14LXBsIjoiY2xvdWQifQ.hEwCX0Yg35m_QoPa1yXiCoumJ-_9tP1OL3YM8MgOjMU"
)
MX_DB    = os.environ.get("MAXIMIZER_DB",   "cfc41d0cf49d425bb44e44c07d7b10fd")
MX_BASE  = os.environ.get("MAXIMIZER_BASE", "https://api.maximizer.com/octopus")

# Caller UDF field — the field named "Caller" you created in Maximizer Address Book
# You can find this key in Maximizer Admin → User-Defined Fields → Address Book
MX_CALLER_UDF = os.environ.get("MX_CALLER_UDF", "Caller")

def mx_hdrs():
    return {
        "Authorization": f"Bearer {MX_TOKEN}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

def mx_call(endpoint, body):
    """POST to Maximizer Octopus API."""
    url = f"{MX_BASE.rstrip('/')}/{endpoint}"
    r = requests.post(url, headers=mx_hdrs(), json=body, timeout=20)
    if not r.ok:
        raise Exception(f"{r.status_code} {r.reason}: {r.text[:200]}")
    return r.json()

def mx_read(search_query=None, fields=None, top=1):
    """Read AbEntry using lowercase read syntax."""
    body = {
        "abEntry": {
            "criteria": {"searchQuery": search_query or {}, "top": top},
            "scope": {"fields": fields or {"key": 1, "companyName": 1, "type": 1}}
        }
    }
    return mx_call("AbEntryRead", body)

def mx_write_create(data_dict):
    """Create AbEntry using PascalCase Data wrapper syntax."""
    return mx_call("AbEntryCreate", {"AbEntry": {"Data": data_dict}})

def mx_write_update(data_dict):
    """Update AbEntry using PascalCase Data wrapper syntax."""
    return mx_call("AbEntryUpdate", {"AbEntry": {"Data": data_dict}})

def mx_find_by_org_number(reg_no):
    """Find Address Book entry by AccountNo (= CC Reg No.)."""
    try:
        # Search by AccountNo field (where we store the CC reg number)
        result = mx_read(
            search_query={"accountNo": str(reg_no)},
            fields={"key": 1, "companyName": 1, "type": 1, "accountNo": 1},
            top=3
        )
        items = result.get("abEntry", {}).get("Data", [])
        if items:
            print(f"  Found by accountNo: {items[0].get('companyName')}")
            return items[0]
        # Fallback: search by companyName containing reg (for old entries)
        result2 = mx_read(
            search_query={"companyName": f"%[{reg_no}]%"},
            fields={"key": 1, "companyName": 1, "type": 1},
            top=3
        )
        items2 = result2.get("abEntry", {}).get("Data", [])
        if items2:
            print(f"  Found by old suffix: {items2[0].get('companyName')}")
            return items2[0]
        print(f"  mx_find: no entry for reg {reg_no}")
        return None
    except Exception as e:
        print(f"mx_find_by_org_number({reg_no}): {e}")
        return None

def mx_get_caller(entry):
    """Extract caller from entry — stored in companyName or fetched separately."""
    # For now return empty — we set caller on create/update
    return ""

def title_case(s):
    """Convert ALL CAPS charity name to Title Case."""
    if not s:
        return s
    # Words to keep lowercase (unless first word)
    minor = {"a","an","the","and","or","but","of","in","on","at","to","for",
             "by","with","from","into","onto","up","as","its","it's"}
    words = s.strip().split()
    result = []
    for i, w in enumerate(words):
        # Keep acronyms uppercase (e.g. UK, CIO, NHS, YMCA)
        if len(w) <= 4 and w.isupper() and w.isalpha():
            result.append(w)
        elif i == 0 or w.lower() not in minor:
            result.append(w.capitalize())
        else:
            result.append(w.lower())
    return " ".join(result)

def mx_create_entry(c, caller=""):
    """Create Address Book Company entry using correct write syntax."""
    reg  = str(c.get("reg_number","")).strip()
    raw_name = (c.get("name","") or "Unknown Charity")
    name = title_case(raw_name)[:100]

    data = {
        "Type":        "Company",
        "CompanyName": name,
    }
    # Add optional fields if present
    if c.get("phone"):    data["Phone1"]   = str(c["phone"])[:30]
    if c.get("website"):  data["WebSite"]  = str(c["website"])[:200]
    if c.get("email"):    data["Email1"]   = str(c["email"])[:100]
    if c.get("town"):     data["City"]     = str(c["town"])[:60]
    if c.get("county"):   data["State"]    = str(c["county"])[:60]
    if c.get("address1"): data["Addr1"]    = str(c["address1"])[:100]
    if reg:               data["AccountNo"] = reg   # store reg in AccountNo field

    print(f"  mx_create: {name} (reg:{reg})")
    result = mx_write_create(data)
    print(f"  mx_create result: Code={result.get('Code')} Msg={result.get('Msg','')}")
    return result

def mx_update_entry(key, c, caller=""):
    """Update existing Maximizer entry."""
    data = {"Key": key}
    if c.get("name"):
        data["CompanyName"] = title_case(c["name"])[:100]
    if c.get("website"):  data["WebSite"] = str(c["website"])[:200]
    if c.get("phone"):    data["Phone1"]  = str(c["phone"])[:30]
    if c.get("email"):    data["Email1"]  = str(c["email"])[:100]
    reg = str(c.get("reg_number","")).strip()
    if reg:               data["AccountNo"] = reg
    print(f"  mx_update: key={key[:20]}...")
    return mx_write_update(data)

def mx_search_by_name(name):
    """Search by company name."""
    try:
        result = mx_read(
            search_query={"companyName": f"%{name[:30]}%"},
            fields={"key": 1, "companyName": 1, "type": 1},
            top=5
        )
        return result.get("abEntry", {}).get("Data", [])
    except Exception as e:
        print(f"mx_search_by_name: {e}")
        return []

def do_sync_one(reg, c, caller, page):
    """Sync one charity. Returns (action, mx_caller_pulled)."""
    existing = mx_find_by_org_number(reg)
    if existing:
        ab_key    = existing.get("key", "")
        mx_caller = mx_get_caller(existing)
        # Bidirectional: push our caller if set, else pull Maximizer's
        push_caller = caller if caller else mx_caller
        mx_update_entry(ab_key, c, push_caller)
        # If Maximizer had a caller we don't have locally, record it
        local_key = f"{page}|{reg}"
        if mx_caller and not _called_log.get(local_key):
            _called_log[local_key] = {
                "reg_number": reg, "page": page,
                "called_by": mx_caller,
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                "source": "maximizer"
            }
            return "updated", mx_caller
        return "updated", None
    else:
        mx_create_entry(c, caller)
        return "created", None

@app.route("/api/maximizer/test")
def mx_test():
    """Test connection to Maximizer Octopus API."""
    global MX_BASE
    # Get server IP for whitelist instructions
    server_ip = "unknown"
    try:
        r = requests.get("https://checkip.amazonaws.com", timeout=4)
        server_ip = r.text.strip()
    except Exception:
        pass
    try:
        result = mx_read(fields={"key": 1, "companyName": 1}, top=1)
        entries = result.get("abEntry", {}).get("Data", [])
        count   = len(entries)
        sample  = entries[0].get("companyName","") if entries else ""
        return jsonify({
            "ok": True,
            "message": f"Connected! Found entries in Maximizer. Sample: '{sample}'",
            "database": MX_DB, "base": MX_BASE
        })
    except Exception as e:
        msg = str(e)
        hint = ""
        if "403" in msg or "whitelist" in msg.lower():
            hint = (f"Render server IP ({server_ip}) is not whitelisted in Maximizer. "
                    f"Ask your Maximizer admin to add IP {server_ip} to the API allowlist.")
        elif "401" in msg:
            hint = "Token expired or invalid."
        return jsonify({"ok": False, "error": msg, "hint": hint,
                        "server_ip": server_ip}), 200

@app.route("/api/maximizer/test_create")
def mx_test_create():
    """Try several minimal bodies to find what Maximizer requires."""
    results = []
    # Attempt 1: Company with just companyName
    for body in [
        {"abEntry": {"type": "Company", "companyName": "TEST CHARITY BODY1"}},
        {"abEntry": {"type": "Company", "companyName": "TEST CHARITY BODY2", "lastName": ""}},
        {"abEntry": {"CompanyName": "TEST CHARITY BODY3", "Type": "Company"}},
        {"abEntry": {"type": "Individual", "lastName": "TestCharity", "firstName": "API"}},
    ]:
        try:
            r = mx_call("AbEntryCreate", body)
            results.append({"body_keys": list(body["abEntry"].keys()),
                            "response": r})
            break  # stop on first success
        except Exception as e:
            results.append({"body_keys": list(body["abEntry"].keys()),
                            "error": str(e)[:200]})
    return jsonify({"ok": True, "attempts": results})

@app.route("/api/maximizer/test_create2")
def mx_test_create2():
    """Try Ferret v1 style create."""
    try:
        # Ferret v1 uses different endpoint and body structure
        import requests as req
        url = "https://api.maximizer.com/ferret/v1/abEntry"
        hdrs = {"Authorization": f"Bearer {MX_TOKEN}",
                "Content-Type": "application/json"}
        body = {
            "data": {
                "type": "Company",
                "attributes": {
                    "companyName": "TEST FERRET CHARITY"
                }
            }
        }
        r = req.post(url, headers=hdrs, json=body, timeout=10)
        return jsonify({"status": r.status_code, "body": r.text[:500]})
    except Exception as e:
        return jsonify({"error": str(e)}), 200

@app.route("/api/maximizer/read_sample")
def mx_read_sample():
    """Read one existing entry to see exact field structure Maximizer uses."""
    try:
        # Request ALL fields to discover what Maximizer actually supports
        body = {
            "abEntry": {
                "criteria": {"searchQuery": {}, "top": 1},
                "scope": {
                    "fields": {
                        "key": 1,
                        "companyName": 1,
                        "type": 1,
                        "phone1": 1,
                        "email1": 1,
                        "address1": 1,
                        "website": 1,
                        "udf": 1,
                        "lastName": 1,
                        "firstName": 1,
                        "leadSource": 1,
                        "notes": 1,
                    }
                }
            }
        }
        result = mx_call("AbEntryRead", body)
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.route("/api/maximizer/read_full")
def mx_read_full():
    """Read the known entry key with ALL possible fields to discover structure."""
    known_key = "Q29udGFjdAkyNDAzMjcyNTIyMzc1Nzk5MzU4MDJDCTE="
    try:
        # Request every field we can think of
        body = {
            "abEntry": {
                "criteria": {
                    "searchQuery": {"key": known_key},
                    "top": 1
                },
                "scope": {
                    "fields": {
                        "key": 1,
                        "companyName": 1,
                        "lastName": 1,
                        "firstName": 1,
                        "type": 1,
                        "phone1": 1,
                        "phone2": 1,
                        "phone3": 1,
                        "email1": 1,
                        "email2": 1,
                        "website": 1,
                        "addr1Line1": 1,
                        "addr1Line2": 1,
                        "addr1City": 1,
                        "addr1State": 1,
                        "addr1Country": 1,
                        "addr1Zip": 1,
                        "addressLine1": 1,
                        "addressLine2": 1,
                        "city": 1,
                        "state": 1,
                        "country": 1,
                        "postalCode": 1,
                        "udf": 1,
                        "notes": 1,
                        "leadSource": 1,
                        "isLead": 1,
                    }
                }
            }
        }
        result = mx_call("AbEntryRead", body)
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 200

@app.route("/api/maximizer/create_test_charity")
def mx_create_test_charity():
    """Create charity 1202982 — discover valid fields by testing each one."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    reg  = "1202982"
    name = title_case("ST GEORGE'S INDIAN ORTHODOX CHURCH, LONDON")
    results = {}

    # Step 1: Create minimal entry (we know Type+CompanyName works)
    r = req.post(f"{MX_BASE}/AbEntryCreate", headers=hdrs,
        json={"AbEntry": {"Data": {"Type": "Company", "CompanyName": name}}}, timeout=10)
    resp = r.json()
    results["create"] = {"Code": resp.get("Code"), "Msg": str(resp.get("Msg",""))}
    if resp.get("Code") != 0:
        return jsonify(results)

    new_key = (resp.get("AbEntry") or resp.get("abEntry") or {}).get("Data",{}).get("Key","")
    results["key"] = new_key
    results["SUCCESS"] = True

    if not new_key:
        return jsonify(results)

    # Step 2: Probe each optional field name by updating one at a time
    field_candidates = [
        ("AccountNo",    reg),
        ("Phone1",       "07448976144"),
        ("Email1",       "st.georges.ioc.london@gmail.com"),
        ("WebSite",      "www.indianorthodox.london"),
        ("CityTown",     "City of London"),
        ("City",         "City of London"),
        ("StateProvince","London"),
        ("StProv",       "London"),
        ("Addr1",        "Rood Lane, Eastcheap"),
        ("Address1",     "Rood Lane, Eastcheap"),
        ("ZipCode",      "EC3M 5AD"),
        ("Zip",          "EC3M 5AD"),
        ("Country",      "United Kingdom"),
    ]
    results["fields"] = {}
    for field, val in field_candidates:
        try:
            r2 = req.post(f"{MX_BASE}/AbEntryUpdate", headers=hdrs,
                json={"AbEntry": {"Data": {"Key": new_key, field: val}}}, timeout=8)
            d = r2.json()
            ok = d.get("Code") == 0
            results["fields"][field] = "✓ VALID" if ok else f"✗ {str(d.get('Msg',''))[:60]}"
        except Exception as e:
            results["fields"][field] = f"ERR: {str(e)[:60]}"

    return jsonify(results)

@app.route("/api/maximizer/find")
def mx_find():
    """Find a charity in Maximizer by reg number or name — for debugging."""
    reg  = request.args.get("reg", "")
    name = request.args.get("name", "")
    try:
        if reg:
            entry = mx_find_by_org_number(reg)
            if entry:
                return jsonify({"found": True, "entry": entry})
            # Also try direct name search
            if name:
                entries = mx_search_by_name(name[:30])
                return jsonify({"found": bool(entries), "entries": entries,
                               "note": "Found by name (not by reg)"})
            # Try broader search
            broader = mx_search_by_name(reg)
            return jsonify({"found": bool(broader), "entries": broader,
                           "note": f"Searched for [{reg}] in companyName"})
        return jsonify({"error": "Provide ?reg= parameter"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/maximizer/sync", methods=["POST"])
def mx_sync():
    """Sync charities with Maximizer (background thread)."""
    global MX_BASE
    data      = request.json or {}
    charities = data.get("charities", [])
    page      = data.get("page", "prospects")
    caller    = data.get("caller", "")
    base_url  = data.get("base_url", "").strip()
    if base_url:
        MX_BASE = base_url.rstrip("/")
    sync_key  = f"mx_sync_{page}"

    with _bg_lock:
        if _bg_status.get(sync_key) == "loading":
            return jsonify({"ok": False, "message": "Sync already running"}), 409

    def _worker():
        with _bg_lock: _bg_status[sync_key] = "loading"
        result = {"created": 0, "updated": 0, "errors": 0, "mx_callers_pulled": 0, "error": ""}
        try:
            for c in charities[:100]:
                reg = c.get("reg_number", "")
                if not reg: continue
                try:
                    action, pulled_caller = do_sync_one(reg, c, caller, page)
                    if action == "created": result["created"] += 1
                    else: result["updated"] += 1
                    if pulled_caller: result["mx_callers_pulled"] += 1
                except Exception as e:
                    print(f"  sync_one({reg}): {e}")
                    result["errors"] += 1
        except Exception as e:
            result["error"] = str(e)
        finally:
            cache_set(sync_key, result)
            with _bg_lock: _bg_status[sync_key] = "idle"
        print(f"Maximizer sync done: {result}")

    threading.Thread(target=_worker, daemon=True).start()
    return jsonify({"ok": True, "status": "loading",
                    "message": f"Syncing {len(charities)} charities…"}), 202

@app.route("/api/maximizer/sync_status")
def mx_sync_status():
    page     = request.args.get("page", "prospects")
    sync_key = f"mx_sync_{page}"
    with _bg_lock:
        status = _bg_status.get(sync_key, "idle")
    result = cache_get(sync_key, max_age=60)
    return jsonify({"status": status, "result": result})

@app.route("/api/maximizer/config", methods=["GET", "POST"])
def mx_config():
    global MX_BASE
    if request.method == "POST":
        data = request.json or {}
        if data.get("base_url"):
            MX_BASE = data["base_url"].rstrip("/")
    return jsonify({"ok": True, "base_url": MX_BASE, "database": MX_DB,
                    "user": "MAHESH", "token_expires": "2029-05-10"})
