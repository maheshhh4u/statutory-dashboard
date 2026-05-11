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

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)
