import pandas as pd
import os
import smtplib
import logging
from fastapi import FastAPI, Request
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

# =========================
# FASTAPI APP
# =========================
app = FastAPI(title="Loan Risk Monitoring API")

# Global lock to prevent multiple runs on the same day
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
    # Normalize: strip spaces from headers and lowercase them
    df.columns = [str(col).strip().lower() for col in df.columns]
    logger.info(f"✓ Loaded {filename} ({len(df)} rows)")
    return df

# Initial load
agreement = load_and_normalize("agreement_details.csv")
product = load_and_normalize("product_details.csv")
dealer = load_and_normalize("dealer_details.csv")
employee = load_and_normalize("employee_details.csv")
bounce = load_and_normalize("bounce_details.csv")
payment = load_and_normalize("payment_details.csv")

# =========================
# HELPER FUNCTIONS
# =========================

class AgreementQuery(BaseModel):
    agreement_no: int

def safe_merge(left_df, right_df, left_key, right_key):
    if left_key not in left_df.columns or right_key not in right_df.columns:
        return left_df
    return left_df.merge(right_df, left_on=left_key, right_on=right_key, how="left")


def send_via_gmail(body, csv_path=None):
    if not all([EMAIL_USER, EMAIL_PASS, EMAIL_TO]):
        logger.warning("Email configuration missing in Render Environment Variables.")
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"Daily Risk Alert - {datetime.now().strftime('%Y-%m-%d')}"
    msg.attach(MIMEText(body, "plain"))

    if csv_path and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(csv_path)}")
            msg.attach(part)

    try:
        # Switching to SMTP_SSL and Port 465 for better compatibility with Render
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        logger.info("✓ Email sent successfully via Port 465")
    except Exception as e:
        logger.error(f"Final SMTP Error attempt: {e}")
# =========================
# RISK ENGINE
# =========================

def run_risk_analysis():
    logger.info("Starting Risk Analysis Engine...")
    results = []

    # Using normalized column names (lowercase)
    target_col = "agreement_no"
    
    if target_col not in agreement.columns:
        logger.error(f"Critical Error: '{target_col}' not found in agreement file.")
        return {"error": "Column mapping failed"}

    for ag in agreement[target_col]:
        # Filter bounce and payment data
        b_count = len(bounce[bounce[target_col] == ag])
        p_sub = payment[payment[target_col] == ag]

        dpd = 0
        if not p_sub.empty:
            row = p_sub.iloc[0]
            try:
                due = pd.to_datetime(row["due_date"])
                paid = pd.to_datetime(row["payment_date"])
                dpd = (paid - due).days
            except Exception:
                dpd = 0

        # Logic Gate
        if dpd > 10 or b_count >= 2:
            risk_level = "HIGH RISK" if dpd > 30 else "MEDIUM RISK"
            action = "Legal Notice Triggered" if dpd > 30 else "Reminder Mail Triggered"

            results.append({
                "agreement_no": ag,
                "DPD": dpd,
                "Bounce": b_count,
                "Risk": risk_level,
                "Action": action
            })

    logger.info(f"Analysis complete. Risky agreements found: {len(results)}")

    output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")
    if results:
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False)

        body = f"Daily Risk Report\nGenerated: {datetime.now()}\nTotal Risky Cases: {len(results)}\n"
        body += "\n" + "="*30 + "\n"
        for r in results[:20]: # Summarize first 20 in body
            body += f"Ag: {r['agreement_no']} | DPD: {r['DPD']} | Risk: {r['Risk']}\n"
        
        send_via_gmail(body, output_path)
    
    return {"risky_agreements": len(results)}

# =========================
# MIDDLEWARE
# =========================

@app.middleware("http")
async def monitor_docs_requests(request: Request, call_next):
    global last_run_date

    if request.url.path == "/docs":
        now = datetime.now()
        today = now.date()
        
        logger.info(f"[DOCS HIT] UTC Time: {now.strftime('%H:%M:%S')}")

        # Window: 10:15 PM IST to 10:20 PM IST (16:45 - 16:50 UTC)
        if now.hour == 16 and (45 <= now.minute <= 50):
            if last_run_date != today:
                logger.info("Automatic trigger window matched. Executing...")
                try:
                    run_risk_analysis()
                    last_run_date = today
                except Exception as e:
                    logger.error(f"Middleware execution error: {e}")
            else:
                logger.info("Analysis already performed today. Skipping auto-trigger.")

    return await call_next(request)

# =========================
# API ENDPOINTS
# =========================

@app.get("/")
@app.head("/")
def home():
    return {"service": "Loan Risk Monitoring API", "status": "running"}

@app.api_route("/run-risk", methods=["GET", "HEAD"])
def trigger_risk(request: Request):
    if request.method == "HEAD":
        return JSONResponse(content={"status": "alive"})
    
    # Logic for GET
    result = run_risk_analysis()
    return {"message": "Manual trigger successful", "result": result}

@app.get("/test-risk")
def test_risk():
    """Manual endpoint to test the engine without window restrictions"""
    result = run_risk_analysis()
    return {"status": "Test Complete", "data": result}

@app.post("/get_master")
def get_master(query: AgreementQuery):
    a = agreement[agreement["agreement_no"] == query.agreement_no]
    if a.empty:
        return JSONResponse(status_code=404, content={"error": "Agreement not found"})

    m = a.copy()
    m = safe_merge(m, product, "product_id", "product_id")
    m = safe_merge(m, dealer, "dealer_id", "dealer_id")
    m = safe_merge(m, employee, "employee_id", "employee_id")
    return m.to_dict(orient="records")[0]

@app.post("/get_bounce")
def get_bounce(query: AgreementQuery):
    count = len(bounce[bounce["agreement_no"] == query.agreement_no])
    return {"agreement_no": query.agreement_no, "bounce_count": int(count)}

@app.post("/get_dpd")
def get_dpd(query: AgreementQuery):
    p = payment[payment["agreement_no"] == query.agreement_no]
    if p.empty:
        return {"agreement_no": query.agreement_no, "dpd": 0}

    row = p.iloc[0]
    try:
        due = pd.to_datetime(row["due_date"])
        paid = pd.to_datetime(row["payment_date"])
        dpd = max(0, (paid - due).days)
    except:
        dpd = 0

    return {"agreement_no": query.agreement_no, "dpd": int(dpd)}

