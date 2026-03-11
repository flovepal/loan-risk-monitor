import pandas as pd
import os
import smtplib
import logging
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# =========================
# LOGGING CONFIGURATION
# =========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Loan Risk Monitoring API")
last_run_date = None

# =========================
# CONFIG & ENV
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

# =========================
# DATA LOADING & NORMALIZATION
# =========================
def load_and_normalize(filename):
    path = os.path.join(BASE_DIR, filename)
    if not os.path.exists(path):
        logger.error(f"File missing: {filename}")
        return pd.DataFrame()
    
    df = pd.read_csv(path)
    # Clean headers: lowercase, strip, and remove internal double spaces
    df.columns = [" ".join(str(col).strip().lower().split()) for col in df.columns]
    logger.info(f"✓ Loaded {filename}. Columns: {list(df.columns)}")
    return df

# Initial load
agreement = load_and_normalize("agreement_details.csv")
bounce = load_and_normalize("bounce_details.csv")
payment = load_and_normalize("payment_details.csv")
product = load_and_normalize("product_details.csv")
dealer = load_and_normalize("dealer_details.csv")
employee = load_and_normalize("employee_details.csv")

def get_col(df, possible_names):
    for name in possible_names:
        if name in df.columns: return name
    for col in df.columns:
        if "agreement" in col: return col
    return None

# =========================
# EMAIL FUNCTION (ROBUST VERSION)
# =========================
def send_via_gmail(body, csv_path=None):
    if not all([EMAIL_USER, EMAIL_PASS, EMAIL_TO]):
        logger.error("EMAIL ERROR: Credentials missing in Environment Variables.")
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"Daily Risk Alert - {datetime.now().strftime('%Y-%m-%d')}"
    msg.attach(MIMEText(body, "plain"))

    if csv_path and os.path.exists(csv_path):
        try:
            with open(csv_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(csv_path)}")
                msg.attach(part)
        except Exception as e:
            logger.error(f"Attachment error: {e}")

    # Try Port 465 (SSL) first, then Port 587 (TLS) if it fails
    try:
        logger.info("Attempting email via Port 465 (SSL)...")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        logger.info("✓ Email sent successfully via Port 465")
    except Exception as e1:
        logger.warning(f"Port 465 failed: {e1}. Retrying via Port 587 (TLS)...")
        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.send_message(msg)
            logger.info("✓ Email sent successfully via Port 587")
        except Exception as e2:
            logger.error(f"CRITICAL: Both SMTP ports unreachable. Error: {e2}")

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
                    # Added dayfirst=True to fix the warning in your logs
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
        send_via_gmail(body, output_path)
    
    return {"risky_agreements": len(results)}

# =========================
# API ENDPOINTS
# =========================
@app.middleware("http")
async def monitor_docs_requests(request: Request, call_next):
    global last_run_date
    if request.url.path == "/docs":
        now = datetime.now()
        logger.info(f"[DOCS HIT] UTC: {now.strftime('%H:%M:%S')}")
        # 16:45 - 16:55 UTC (10:15 - 10:25 PM IST)
        if now.hour == 16 and (45 <= now.minute <= 55):
            if last_run_date != now.date():
                logger.info("Triggering Scheduled Analysis...")
                run_risk_analysis()
                last_run_date = now.date()
    return await call_next(request)

@app.api_route("/", methods=["GET", "HEAD"])
def home(): return {"status": "running"}

@app.api_route("/run-risk", methods=["GET", "HEAD"])
def trigger_risk(request: Request, background_tasks: BackgroundTasks):
    if request.method == "HEAD": return {"status": "ok"}
    # Use background tasks so the web page doesn't timeout while email tries to send
    background_tasks.add_task(run_risk_analysis)
    return {"message": "Analysis started in background. Check logs for email status."}

@app.get("/test-risk")
def test_risk():
    return run_risk_analysis()
