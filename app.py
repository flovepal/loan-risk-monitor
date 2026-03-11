import pandas as pd
import os
import requests
import logging
from fastapi import FastAPI, Request, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime
import base64

# =========================
# LOGGING & APP CONFIG
# =========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Loan Risk Monitoring API")
last_run_date = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
EMAIL_USER = os.getenv("EMAIL_USER") # Your Verified Sender Email
EMAIL_TO = os.getenv("EMAIL_TO")

# =========================
# DATA LOADING
# =========================
def load_and_normalize(filename):
    path = os.path.join(BASE_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [" ".join(str(col).strip().lower().split()) for col in df.columns]
    logger.info(f"✓ Loaded {filename}")
    return df

agreement = load_and_normalize("agreement_details.csv")
bounce = load_and_normalize("bounce_details.csv")
payment = load_and_normalize("payment_details.csv")

def get_col(df, possible_names):
    for name in possible_names:
        if name in df.columns: return name
    for col in df.columns:
        if "agreement" in col: return col
    return None

# =========================
# NEW SENDGRID API FUNCTION
# =========================
def send_via_sendgrid(body_text, csv_path=None):
    if not SENDGRID_API_KEY:
        logger.error("SENDGRID_API_KEY missing in Environment Variables.")
        return

    # Prepare Attachment
    attachments = []
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            data = f.read()
            encoded_file = base64.b64encode(data).decode()
        
        attachments.append({
            "content": encoded_file,
            "filename": "daily_risk_report.csv",
            "type": "text/csv",
            "disposition": "attachment"
        })

    # Prepare JSON Payload for API
    data = {
        "personalizations": [{"to": [{"email": EMAIL_TO}]}],
        "from": {"email": EMAIL_USER},
        "subject": f"Daily Risk Alert - {datetime.now().strftime('%Y-%m-%d')}",
        "content": [{"type": "text/plain", "value": body_text}],
        "attachments": attachments
    }

    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post("https://api.sendgrid.com/v3/mail/send", headers=headers, json=data)
        if response.status_code in [200, 201, 202]:
            logger.info("✓ Email sent successfully via SendGrid API!")
        else:
            logger.error(f"SendGrid Error: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Failed to connect to SendGrid API: {e}")

# =========================
# RISK ENGINE
# =========================
def run_risk_analysis():
    logger.info("Starting Risk Analysis Engine...")
    ag_main = get_col(agreement, ["agreement_no", "agreement_id"])
    ag_bounce = get_col(bounce, ["agreement_no", "agreement_id"])
    ag_pay = get_col(payment, ["agreement_no", "agreement_id"])

    if not ag_main: return {"error": "Agreement column not found"}

    results = []
    for ag_id in agreement[ag_main]:
        b_count = len(bounce[bounce[ag_bounce] == ag_id]) if ag_bounce else 0
        dpd = 0
        if ag_pay:
            p_sub = payment[payment[ag_pay] == ag_id]
            if not p_sub.empty:
                row = p_sub.iloc[0]
                try:
                    due_col = get_col(payment, ["due_date", "due date"])
                    paid_col = get_col(payment, ["payment_date", "payment date"])
                    due = pd.to_datetime(row[due_col], dayfirst=True)
                    paid = pd.to_datetime(row[paid_col], dayfirst=True)
                    dpd = (paid - due).days
                except: dpd = 0

        if dpd > 10 or b_count >= 2:
            risk = "HIGH RISK" if dpd > 30 else "MEDIUM RISK"
            results.append({
                "agreement_no": ag_id, "DPD": max(0, dpd),
                "Bounce": b_count, "Risk": risk,
                "Action": "Legal Notice" if dpd > 30 else "Reminder Mail"
            })

    output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")
    if results:
        pd.DataFrame(results).to_csv(output_path, index=False)
        body = f"Risk Analysis Complete. Risky cases found: {len(results)}\n\n"
        body += "\n".join([f"Ag: {r['agreement_no']} | DPD: {r['DPD']} | Risk: {r['Risk']}" for r in results[:15]])
        send_via_sendgrid(body, output_path)
    else:
        logger.info("No risky agreements found.")
    
    return {"risky_agreements": len(results)}

# =========================
# API ROUTES
# =========================
@app.middleware("http")
async def monitor_docs_requests(request: Request, call_next):
    global last_run_date
    if request.url.path == "/docs":
        now = datetime.now()
        if now.hour == 16 and (45 <= now.minute <= 55): # 10:15 PM IST
            if last_run_date != now.date():
                run_risk_analysis()
                last_run_date = now.date()
    return await call_next(request)

@app.get("/")
def home(): return {"status": "running"}

@app.get("/run-risk")
def trigger_risk(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_risk_analysis)
    return {"message": "Analysis started in background via API."}
