# Statutory Prospecting Dashboard

A Flask web application that pulls live data from the UK Charity Commission
to display statutory prospect lists, newly registered charities, late accounts,
and analytics — all with CSV download buttons.

---

## Project Structure

```
statutory_app/
├── app.py               ← Flask backend (all API logic)
├── templates/
│   └── index.html       ← Frontend (single-page dashboard)
├── requirements.txt     ← Python dependencies
├── Procfile             ← For Render/Heroku deployment
├── render.yaml          ← Render auto-deploy config
└── README.md
```

---

## Run Locally (Windows or Mac)

### 1. Install Python 3.11+
Download from https://python.org if not installed.

### 2. Open a terminal in this folder, then:

```bash
# Create virtual environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the app
python app.py
```

### 3. Open your browser
Go to: http://localhost:5000

---

## Deploy to Render (Free Hosting — gets a public URL)

### Step 1 — Push to GitHub
1. Create a free account at https://github.com
2. Create a new repository (e.g. "statutory-dashboard")
3. Upload all files from this folder to the repo
   - You can drag-and-drop files in the GitHub web interface

### Step 2 — Deploy on Render
1. Go to https://render.com and sign up (free)
2. Click **New → Web Service**
3. Connect your GitHub account and select your repository
4. Render will auto-detect the settings from `render.yaml`
5. Click **Create Web Service**
6. Wait ~3 minutes for the build to finish
7. Your dashboard will be live at a URL like:
   `https://statutory-prospecting-dashboard.onrender.com`

### Step 3 — Environment Variables (already set in render.yaml)
- `CC_API_KEY` = your Charity Commission API key
- `PYTHON_VERSION` = 3.11.0

If you ever need to rotate your API key:
- Go to Render → your service → Environment → edit `CC_API_KEY`

---

## Data Sources

| Tab | Primary Source | Fallback |
|-----|---------------|---------|
| Prospect Lists | CC Bulk Extract + CC API financial history | CC API list endpoint |
| Newly Registered | CC Bulk Extract (date_of_registration) | CC API with date filter |
| Late Accounts | CC Bulk Extract (date_accounts_due) | CC API reporting_late param |
| Charity Search | CC API (name/number search) | — |

The bulk extract is downloaded from:
`https://ccewuksprpdata.blob.core.windows.net/extracts/RegPlusExtract_England_publicextract.zip`

It is updated daily by the Charity Commission and contains ALL registered charities.
Results are cached in memory for 2 hours to avoid repeated downloads.

---

## Prospect Screening Rules

| List | Income | Statutory | Dependency |
|------|--------|-----------|------------|
| Best Immediate | £250k–£3m | £50k–£300k | 10–40% |
| Readiness Package | £150k–£1.5m | £10k–£100k | Any |
| Contract Growth | £500k–£5m | Contracts £100k–£500k | Contracts <35% |
| Retention/Replacement | £500k–£5m | >£200k | 40–70% |

---

## Notes
- The app is for sales prospecting review only — not a final commercial decision tool
- Data attribution: Charity Commission Open Government Licence
- Free Render tier may sleep after 15 mins inactivity (first load takes ~30s to wake up)
  → Upgrade to Render Starter ($7/mo) for always-on hosting
