import os, io, csv, zipfile, requests, threading
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
        if reg and row.get("charity_name",""):  # include all charities for name lookup
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
        name=names.get(reg,"") or names.get(reg.lstrip("0"),"") or ""
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
    r = requests.post(url, headers=mx_hdrs(), json=body, timeout=20)
    if not r.ok: raise Exception(f"{r.status_code}: {r.text[:200]}")
    return r.json()

def mx_read(search_query=None, fields=None, top=1, entry_type=None):
    """Read AbEntry. entry_type can be 'Contact', 'Company', 'Individual' or None for all."""
    sq = dict(search_query or {})
    if entry_type:
        sq["type"] = entry_type
    body = {"abEntry":{"criteria":{"searchQuery":sq,"top":top},
                       "scope":{"fields":fields or {"key":1,"companyName":1}}}}
    return mx_call("AbEntryRead", body)

def mx_write_create(data_dict):
    return mx_call("AbEntryCreate",{"AbEntry":{"Data":data_dict}})

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
    """Find entry by reg number — searches all entries including Companies."""
    reg_no = str(reg_no)
    try:
        # Try TYPEID if known
        if MX_ORG_NUM_TYPEID:
            try:
                r2 = mx_read(
                    search_query={f"/AbEntry/Udf/$TYPEID({MX_ORG_NUM_TYPEID})": reg_no},
                    fields={"key":1,"companyName":1,"type":1}, top=3
                )
                items2 = r2.get("abEntry",{}).get("Data",[])
                if items2: return items2[0]
            except: pass
        # Search ALL entries including Companies via address key trick
        all_keys = _mx_get_all_keys()
        for key, cname in all_keys.items():
            if reg_no in cname:
                return {"key": key, "companyName": cname}
        return None
    except Exception as e:
        print(f"mx_find({reg_no}): {e}")
        return None

def build_charity_base(c):
    """Basic fields only for create."""
    name = title_case(c.get("name","") or "Unknown Charity")[:100]
    data = {"Type":"Company","CompanyName":name}
    if c.get("phone"):   data["Phone1"]  = str(c["phone"])[:30]
    if c.get("email"):   data["Email1"]  = str(c["email"])[:100]
    if c.get("website"): data["WebSite"] = str(c["website"])[:200]
    return data

def _mx_find_key_by_creation_date(company_name, created_after_iso):
    """Find recently created Company entry key using multiple strategies."""
    import requests as req
    hdrs = {"Authorization": f"Bearer {MX_TOKEN}", "Content-Type": "application/json"}
    name_lower = company_name.lower()
    # Try first 15 chars, then 10 chars for matching
    match_parts = [name_lower[:15], name_lower[:10], name_lower[:8]]

    bodies = [
        # Strategy 1: lastModifyDate DESC with Address/Key (returns Companies too)
        {"abEntry": {"criteria": {"searchQuery": {}, "top": 30},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1,
                                          "/AbEntry/Address/Key":1}},
                     "orderBy": {"fields": [{"lastModifyDate": "DESC"}]}}},
        # Strategy 2: companyName search (may not return Companies but worth trying)
        {"abEntry": {"criteria": {"searchQuery": {"companyName": company_name[:40]},
                                  "top": 10},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1,
                                          "/AbEntry/Address/Key":1}}}},
        # Strategy 3: companyName with partial match
        {"abEntry": {"criteria": {"searchQuery": {"companyName": f"%{company_name[:20]}%"},
                                  "top": 10},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1,
                                          "/AbEntry/Address/Key":1}}}},
        # Strategy 4: type=Company filter explicitly
        {"abEntry": {"criteria": {"searchQuery": {"type": "Company"}, "top": 50},
                     "scope": {"fields": {"key":1,"companyName":1,"type":1}}}},
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

    what_keys = _multi_keys(WHAT_MAP, what)
    if what_keys: upd(111, what_keys[0])

    who_keys = _multi_keys(WHO_MAP, who)
    if who_keys: upd(112, who_keys[0])

    how_keys = _multi_keys(HOW_MAP, how)
    if how_keys: upd(113, how_keys[0])

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

    upd(264, "2")  # IsSync = Yes

    if MX_ORG_NUM_TYPEID:
        reg = str(c.get("reg_number","")).strip()
        if reg: upd(MX_ORG_NUM_TYPEID, reg)

    return updates

def mx_apply_udfs(key, c):
    """Apply each UDF update separately."""
    results = []
    for upd_data in build_charity_udfs(c, key):
        try:
            r = mx_write_update(upd_data)
            field = [k for k in upd_data if k.startswith("/AbEntry")][0]
            results.append({"field":field,"val":upd_data[field],
                            "Code":r.get("Code"),"ok":r.get("Code")==0})
        except Exception as e:
            results.append({"error":str(e)[:80]})
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

def mx_create_entry(c, caller=""):
    """Create entry, find its key by diffing key lists before/after, then apply UDFs."""
    import time

    # Step 1: Get keys before create
    keys_before = _mx_get_all_keys()
    print(f"  Keys before: {len(keys_before)}")

    # Step 2: Create
    data = build_charity_base(c)
    print(f"  mx_create: {data.get('CompanyName','?')}")
    result = mx_write_create(data)
    print(f"  Code={result.get('Code')} Msg={result.get('Msg','')}")
    if result.get("Code") != 0:
        return result

    # Step 3: Wait and get keys after
    time.sleep(2)
    keys_after = _mx_get_all_keys()
    print(f"  Keys after: {len(keys_after)}")

    # Step 4: Find new key(s)
    new_keys = [k for k in keys_after if k not in keys_before]
    print(f"  New keys: {new_keys}")

    if new_keys:
        key = new_keys[0]
        mx_apply_udfs(key, c)
        print(f"  UDFs applied to key {key[:20]}…")
    else:
        # Fallback: try by name
        name = data.get("CompanyName","")
        for key, cname in keys_after.items():
            if name[:12].lower() in cname.lower():
                mx_apply_udfs(key, c)
                print(f"  UDFs applied by name match")
                break

    return result

def mx_update_entry(key, c, caller=""):
    """Update entry with all fields including UDFs."""
    if not c.get("what") and not c.get("who") and not c.get("how"):
        classif = get_charity_classification(str(c.get("reg_number","")))
        c.update(classif)
    data = build_charity_base(c)
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

    # Also try classification bulk file as fallback
    fin = c.get("financials") or {}
    if not c.get("what") and fin.get("what"): c["what"] = fin["what"]
    if not c.get("who")  and fin.get("who"):  c["who"]  = fin["who"]
    if not c.get("how")  and fin.get("how"):  c["how"]  = fin["how"]

    # Check existence — search via all-keys (includes Companies)
    name = title_case(c.get("name","") or "")
    all_keys = _mx_get_all_keys()
    existing_key = ""
    for key, cname in all_keys.items():
        if name[:15] and name[:15].lower() in cname.lower():
            existing_key = key; break

    if existing_key:
        print(f"  Updating: {name[:30]}")
        mx_update_entry(existing_key, c, caller)
        return "updated", None
    else:
        print(f"  Creating: {name[:30]}")
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
    """Delete old + create fresh test entry with correct CC data & UDF mappings."""
    # Remove existing test entry
    existing = mx_find_by_org_number_by_name("1202982")
    if existing:
        try: mx_call("AbEntryDelete",{"AbEntry":{"Data":{"Key":existing["key"]}}})
        except: pass

    c = {"reg_number": "1202982",
         "name": "ST GEORGE'S INDIAN ORTHODOX CHURCH, LONDON"}
    # Enrich with full CC API data
    c = enrich_charity_from_cc(c)
    result = {"cc_data": {k:v for k,v in c.items()
                          if k not in ("financials",)}}
    try:
        import time
        from datetime import datetime, timezone

        # Step 1: Create with basic fields only
        base = build_charity_base(c)
        result["base_sent"] = base
        created_after = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        resp = mx_write_create(base)
        result["create_code"] = resp.get("Code")
        result["SUCCESS"] = resp.get("Code") == 0
        if resp.get("Code") != 0:
            result["error"] = str(resp.get("Msg",""))
            return jsonify(result)

        # Step 2: Find key using creation date ordered search
        time.sleep(2)
        key = _mx_find_key_by_creation_date(base["CompanyName"], created_after)
        result["key"] = key

        if not key:
            result["note"] = "Created OK but key not found — UDFs not applied"
            return jsonify(result)

        # Step 3: Apply each UDF separately
        udf_results = mx_apply_udfs(key, c)
        result["udf_results"] = udf_results

    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)

@app.route("/api/maximizer/update_by_id")
def mx_update_by_id():
    """Legacy probe endpoint — kept for reference."""
    return jsonify({"message":"Field probing complete. Use /api/maximizer/create_test_charity"}),200
