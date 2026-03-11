import pandas as pd
import os
import requests
import logging
import base64
from fastapi import FastAPI, Request, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime

# =========================
# LOGGING & APP CONFIG
# =========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Loan Risk Monitoring API")

# Persistent variable for the daily lock
last_run_date = None

# Config from Render Environment Variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_USER")  # MUST BE VERIFIED IN SENDGRID
EMAIL_TO = os.getenv("EMAIL_TO")

# =========================
# DATA LOADING & NORMALIZATION
# =========================
def load_and_normalize(filename):
    path = os.path.join(BASE_DIR, filename)
    if not os.path.exists(path):
        logger.error(f"Missing file: {filename}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Standardize column names
    df.columns = [" ".join(str(col).strip().lower().split()) for col in df.columns]
    logger.info(f"✓ Loaded {filename}")
    return df

# Load CSVs
agreement = load_and_normalize("agreement_details.csv")
bounce = load_and_normalize("bounce_details.csv")
payment = load_and_normalize("payment_details.csv")

def get_col(df, possible_names):
    if df.empty: return None
    for name in possible_names:
        if name in df.columns: return name
    for col in df.columns:
        if "agreement" in col: return col
    return None

# =========================
# SENDGRID API FUNCTION
# =========================
def send_via_sendgrid(body_text, csv_path=None):
    if not all([SENDGRID_API_KEY, EMAIL_FROM, EMAIL_TO]):
        logger.error("API ERROR: SENDGRID_API_KEY, EMAIL_USER, or EMAIL_TO is missing.")
        return

    attachments = []
    if csv_path and os.path.exists(csv_path):
        try:
            with open(csv_path, "rb") as f:
                data = f.read()
                encoded_file = base64.b64encode(data).decode()
            attachments.append({
                "content": encoded_file,
                "filename": "risk_report.csv",
                "type": "text/csv",
                "disposition": "attachment"
            })
        except Exception as e:
            logger.error(f"Attachment failed: {e}")

    payload = {
        "personalizations": [{"to": [{"email": EMAIL_TO}]}],
        "from": {"email": EMAIL_FROM},
        "subject": f"Loan Risk Alert - {datetime.now().strftime('%d %b %Y')}",
        "content": [{"type": "text/plain", "value": body_text}],
        "attachments": attachments
    }

    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post("https://api.sendgrid.com/v3/mail/send", headers=headers, json=payload, timeout=20)
        if response.status_code in [200, 201, 202]:
            logger.info("✓ EMAIL SENT SUCCESSFULLY via SendGrid API.")
        else:
            logger.error(f"SendGrid Error {response.status_code}: {response.text}")
    except Exception as e:
        logger.error(f"Could not connect to SendGrid: {e}")

# =========================
# RISK ENGINE
# =========================
def run_risk_analysis():
    logger.info("Starting Risk Analysis...")
    
    ag_main = get_col(agreement, ["agreement_no", "agreement_id"])
    ag_bounce = get_col(bounce, ["agreement_no", "agreement_id"])
    ag_pay = get_col(payment, ["agreement_no", "agreement_id"])

    if agreement.empty or not ag_main:
        logger.error("Risk engine aborted: Agreement data invalid.")
        return

    results = []
    for ag_id in agreement[ag_main]:
        # Logic: Bounce Count
        b_count = len(bounce[bounce[ag_bounce] == ag_id]) if ag_bounce else 0
        
        # Logic: DPD
        dpd = 0
        if ag_pay:
            p_sub = payment[payment[ag_pay] == ag_id]
            if not p_sub.empty:
                row = p_sub.iloc[0]
                try:
                    due_c = get_col(payment, ["due_date", "due date"])
                    paid_c = get_col(payment, ["payment_date", "payment date"])
                    due = pd.to_datetime(row[due_c], dayfirst=True)
                    paid = pd.to_datetime(row[paid_c], dayfirst=True)
                    dpd = (paid - due).days
                except: dpd = 0

        # Risk Classification
        if dpd > 10 or b_count >= 2:
            risk = "HIGH RISK" if dpd > 30 else "MEDIUM RISK"
            results.append({
                "agreement_no": ag_id,
                "DPD": max(0, dpd),
                "Bounce": b_count,
                "Risk": risk,
                "Action": "Legal Notice" if dpd > 30 else "Reminder Mail"
            })

    logger.info(f"Engine finished. Found {len(results)} risky cases.")

    if results:
        output_path = os.path.join(BASE_DIR, "risk_report.csv")
        pd.DataFrame(results).to_csv(output_path, index=False)
        
        body = f"Risk Analysis Complete.\nFound {len(results)} risky agreements.\n\nSummary (Top 10):\n"
        body += "\n".join([f"Ag: {r['agreement_no']} | DPD: {r['DPD']} | {r['Risk']}" for r in results[:10]])
        
        send_via_sendgrid(body, output_path)
    else:
        logger.info("No risk detected. Email not sent.")

# =========================
# API ROUTES & AUTOMATION
# =========================
@app.middleware("http")
async def monitor_docs_requests(request: Request, call_next):
    global last_run_date
    if request.url.path == "/docs":
        now = datetime.now()
        logger.info(f"[PING] UTC: {now.strftime('%H:%M:%S')}")

        # TARGET: 8:00 AM IST = 02:30 AM UTC
        # Window: 02:30 to 02:40 UTC to catch the 5-min pinger
        if now.hour == 2 and (30 <= now.minute <= 40):
            if last_run_date != now.date():
                logger.info("Morning schedule matched (08:00 AM IST). Auto-triggering...")
                run_risk_analysis()
                last_run_date = now.date()
    
    return await call_next(request)

@app.get("/")
def home():
    return {"status": "running", "engine": "ready", "target_ist": "08:00 AM"}

@app.get("/run-risk")
def manual_trigger(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_risk_analysis)
    return {"message": "Manual analysis started. Check logs for results."}
