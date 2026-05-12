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

# ═══════════════════════════════════════════════════════════════════════════════
# MAXIMIZER CRM SYNC
# ═══════════════════════════════════════════════════════════════════════════════
MX_TOKEN = os.environ.get("MAXIMIZER_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteHB5ZjV6MGFwbWpub"
    "29jOXM3NCIsImlhdCI6MTc3ODUyNzEyMywiZXhwIjoxODczMDY1NjAwLCJteC1jaWQiO"
    "iJEMzIxRDMxRS04QzRBLTQyRTMtQUU5Ny03NjMzRDk5Qjk5QjIiLCJteC13c2lkIjoiO"
    "ENDNkVBRkYtQkFERi00RDY5LTgyRTktNzQxREFERUU2QjU0IiwibXgtZGIiOiJjZmM0MW"
    "QwY2Y0OWQ0MjViYjQ0ZTQ0YzA3ZDdiMTBmZCIsIm14LXVpZCI6Ik1BSEVTSCIsIm14LX"
    "BsIjoiY2xvdWQifQ.hEwCX0Yg35m_QoPa1yXiCoumJ-_9tP1OL3YM8MgOjMU")
MX_DB   = os.environ.get("MAXIMIZER_DB",   "cfc41d0cf49d425bb44e44c07d7b10fd")
MX_BASE = os.environ.get("MAXIMIZER_BASE", "https://cloudhosted.maximizer.com/MaximizerWebData")

# UDF field keys discovered from the original exe (TYPEID numbers from UDFOptionList.xml)
# These match the fields synced by the original CCToMaximizerCRM tool
MX_UDF = {
    "what":             "/AbEntry/Udf/$TYPEID(111)",   # What charity does
    "who":              "/AbEntry/Udf/$TYPEID(112)",   # Who it helps
    "how":              "/AbEntry/Udf/$TYPEID(113)",   # How it helps
    "country":          "/AbEntry/Udf/$TYPEID(107)",   # Country
    "local_authority":  "/AbEntry/Udf/$TYPEID(108)",   # Local Authority
    "region":           "/AbEntry/Udf/$TYPEID(109)",   # Region
    "caller":           "/AbEntry/Udf/$TYPEID(243)",   # Caller (custom field you created)
    "is_sync":          "/AbEntry/Udf/$TYPEID(264)",   # Is Sync flag
}

def mx_headers():
    return {
        "Authorization": f"Bearer {MX_TOKEN}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

def mx_post(path, body):
    """POST to Maximizer API."""
    url = f"{MX_BASE}{path}"
    r = requests.post(url, headers=mx_headers(), json=body, timeout=20)
    r.raise_for_status()
    return r.json()

def mx_get(path, params=None):
    """GET from Maximizer API."""
    url = f"{MX_BASE}{path}"
    r = requests.get(url, headers=mx_headers(), params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

def mx_find_by_cc_number(cc_number):
    """Find a Maximizer Address Book entry by Charity Commission number (Organisation Number UDF)."""
    try:
        body = {
            "Database": MX_DB,
            "RequestAction": "Read",
            "Criteria": {
                "SearchQuery": {
                    "AbEntry": {
                        "Udf": {
                            "UserDefinedFields": [
                                {
                                    "FieldName": "/AbEntry/Udf/$TYPEID(CCID)",
                                    "Value": str(cc_number)
                                }
                            ]
                        }
                    }
                }
            },
            "Scope": {"Table": "AbEntry"},
            "Fields": {
                "AbEntry": [
                    "Key", "CompanyName", "Phone1", "Email1", "Website",
                    "Address1/Street1", "Address1/City", "Address1/Country",
                    "OrganizationNumber",
                    {"UdfList": list(MX_UDF.values())}
                ]
            },
            "Limit": 1
        }
        result = mx_post("/api/v1/AbEntry/read", body)
        items = result.get("Data", {}).get("AbEntry", [])
        return items[0] if items else None
    except Exception as e:
        print(f"mx_find_by_cc_number({cc_number}): {e}")
        return None

def mx_find_by_org_number(org_number):
    """Find by OrganizationNumber field (maps to CC Reg No.)."""
    try:
        body = {
            "Database": MX_DB,
            "RequestAction": "Read",
            "Criteria": {
                "SearchQuery": {
                    "AbEntry": {
                        "OrganizationNumber": str(org_number)
                    }
                }
            },
            "Scope": {"Table": "AbEntry"},
            "Fields": {
                "AbEntry": [
                    "Key", "CompanyName", "Phone1", "Email1", "Website",
                    "OrganizationNumber",
                    {"UdfList": list(MX_UDF.values())}
                ]
            },
            "Limit": 1
        }
        result = mx_post("/api/v1/AbEntry/read", body)
        items = result.get("Data", {}).get("AbEntry", [])
        return items[0] if items else None
    except Exception as e:
        print(f"mx_find_by_org_number({org_number}): {e}")
        return None

def mx_create_company(charity_data):
    """Create a new Address Book company entry in Maximizer."""
    reg  = charity_data.get("reg_number", "")
    name = charity_data.get("name", "")
    fin  = charity_data.get("financials", {})

    body = {
        "Database": MX_DB,
        "RequestAction": "Create",
        "AbEntry": {
            "CompanyName":        name,
            "OrganizationNumber": reg,
            "Phone1":             {"Value": charity_data.get("phone", "")},
            "Email1":             {"Value": charity_data.get("email", "")},
            "Website":            charity_data.get("website", ""),
            "Address1": {
                "Street1": charity_data.get("address1", ""),
                "Street2": charity_data.get("address2", ""),
                "City":    charity_data.get("town", ""),
                "Region":  charity_data.get("county", ""),
                "Country": "United Kingdom",
            },
            "Udf": {
                "UserDefinedFields": [
                    {"FieldName": MX_UDF["caller"],  "Value": charity_data.get("caller", "")},
                ]
            }
        }
    }
    return mx_post("/api/v1/AbEntry", body)

def mx_update_company(ab_key, charity_data, caller=None):
    """Update an existing Maximizer Address Book entry."""
    fin = charity_data.get("financials", {})
    udfs = []
    if caller:
        udfs.append({"FieldName": MX_UDF["caller"], "Value": caller})

    body = {
        "Database": MX_DB,
        "RequestAction": "Update",
        "AbEntry": {
            "Key":         ab_key,
            "CompanyName": charity_data.get("name", ""),
            "Website":     charity_data.get("website", ""),
            "Udf": {"UserDefinedFields": udfs} if udfs else {}
        }
    }
    return mx_post("/api/v1/AbEntry", body)

def mx_get_caller_for_entry(ab_entry):
    """Extract caller name from Maximizer UDF fields."""
    try:
        udfs = ab_entry.get("Udf", {}).get("UserDefinedFields", [])
        for udf in udfs:
            if udf.get("FieldName") == MX_UDF["caller"]:
                return udf.get("Value", "")
    except Exception:
        pass
    return ""

# ── Sync a single charity to/from Maximizer ───────────────────────────────────
def sync_one_charity(reg_number, charity_data, caller=None, page="prospects"):
    """
    Sync one charity with Maximizer.
    - Look up by OrganizationNumber (reg_number)
    - If found: update caller field; pull back their caller if set in Maximizer
    - If not found: create new entry
    Returns dict with status, ab_key, mx_caller
    """
    existing = mx_find_by_org_number(reg_number)

    if existing:
        ab_key = existing.get("Key", "")
        mx_caller = mx_get_caller_for_entry(existing)

        # Bidirectional caller sync:
        # If Maximizer has a caller and we don't locally → use Maximizer's
        # If we have a caller locally → push it to Maximizer
        key = f"{page}|{reg_number}"
        local_caller = _called_log.get(key, {}).get("called_by", "")

        if caller and caller != mx_caller:
            # Push our caller to Maximizer
            mx_update_company(ab_key, charity_data, caller=caller)
            mx_caller = caller
        elif mx_caller and not local_caller:
            # Pull caller from Maximizer into dashboard
            _called_log[key] = {
                "reg_number": reg_number,
                "page": page,
                "called_by": mx_caller,
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                "source": "maximizer"
            }
            _statuses[key] = _statuses.get(key, "Contacted")

        return {"status": "updated", "ab_key": ab_key,
                "mx_caller": mx_caller, "created": False}
    else:
        # Create new entry
        result = mx_create_company(charity_data)
        ab_key = result.get("Data", {}).get("AbEntry", [{}])[0].get("Key", "")
        if caller:
            mx_update_company(ab_key, charity_data, caller=caller)
        return {"status": "created", "ab_key": ab_key,
                "mx_caller": caller or "", "created": True}

# ── Sync API endpoints ─────────────────────────────────────────────────────────
@app.route("/api/maximizer/test")
def mx_test():
    """Test Maximizer API connection."""
    try:
        # Try a minimal read to verify credentials
        body = {
            "Database": MX_DB,
            "RequestAction": "Read",
            "Scope": {"Table": "AbEntry"},
            "Fields": {"AbEntry": ["Key", "CompanyName"]},
            "Limit": 1
        }
        result = mx_post("/api/v1/AbEntry/read", body)
        count = len(result.get("Data", {}).get("AbEntry", []))
        return jsonify({"ok": True, "message": f"Connected to Maximizer. Found {count} entries.",
                        "database": MX_DB})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/maximizer/sync", methods=["POST"])
def mx_sync():
    """
    Sync one or more charities with Maximizer.
    Body: { charities: [{reg_number, name, financials, ...}], page: "prospects", caller: "Muhanna" }
    Runs in background thread to avoid timeout.
    """
    data     = request.json or {}
    charities = data.get("charities", [])
    page     = data.get("page", "prospects")
    caller   = data.get("caller", "")
    sync_key = f"mx_sync_{page}"

    with _bg_lock:
        if _bg_status.get(sync_key) == "loading":
            return jsonify({"ok": False, "message": "Sync already in progress"}), 409

    def _do_sync():
        with _bg_lock:
            _bg_status[sync_key] = "loading"
        results = {"created": 0, "updated": 0, "errors": 0, "mx_callers_pulled": 0}
        try:
            for c in charities:
                reg = c.get("reg_number", "")
                if not reg:
                    continue
                try:
                    r = sync_one_charity(reg, c, caller=caller, page=page)
                    if r["created"]:
                        results["created"] += 1
                    else:
                        results["updated"] += 1
                    if r.get("mx_caller") and r["mx_caller"] != caller:
                        results["mx_callers_pulled"] += 1
                except Exception as e:
                    print(f"  Sync error for {reg}: {e}")
                    results["errors"] += 1
        finally:
            cache_set(sync_key, results)
            with _bg_lock:
                _bg_status[sync_key] = "done"
        print(f"Maximizer sync done: {results}")

    threading.Thread(target=_do_sync, daemon=True).start()
    return jsonify({"ok": True, "status": "loading",
                    "message": f"Syncing {len(charities)} charities with Maximizer…"}), 202

@app.route("/api/maximizer/sync_status")
def mx_sync_status():
    """Check sync progress."""
    page     = request.args.get("page", "prospects")
    sync_key = f"mx_sync_{page}"
    with _bg_lock:
        status = _bg_status.get(sync_key, "idle")
    result = cache_get(sync_key, max_age=60)
    return jsonify({"status": status, "result": result})

@app.route("/api/maximizer/pull_callers", methods=["POST"])
def mx_pull_callers():
    """
    Pull caller names FROM Maximizer for a list of reg numbers.
    Updates dashboard called_log if Maximizer has a caller we don't.
    """
    data     = request.json or {}
    reg_numbers = data.get("reg_numbers", [])
    page     = data.get("page", "prospects")
    pulled   = {}

    for reg in reg_numbers[:50]:  # limit to 50 per call
        try:
            existing = mx_find_by_org_number(reg)
            if existing:
                mx_caller = mx_get_caller_for_entry(existing)
                if mx_caller:
                    key = f"{page}|{reg}"
                    if key not in _called_log:
                        _called_log[key] = {
                            "reg_number": reg, "page": page,
                            "called_by": mx_caller,
                            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                            "source": "maximizer"
                        }
                    pulled[reg] = mx_caller
        except Exception as e:
            print(f"pull_caller({reg}): {e}")

    return jsonify({"ok": True, "pulled": pulled, "count": len(pulled)})

@app.route("/api/maximizer/config", methods=["GET", "POST"])
def mx_config():
    """Get or update Maximizer config (base URL override)."""
    global MX_BASE
    if request.method == "POST":
        data = request.json or {}
        if data.get("base_url"):
            MX_BASE = data["base_url"].rstrip("/")
        return jsonify({"ok": True, "base_url": MX_BASE, "database": MX_DB})
    return jsonify({"base_url": MX_BASE, "database": MX_DB,
                    "user": "MAHESH", "token_expires": "2029-05-10"})
