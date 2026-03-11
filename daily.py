import pandas as pd
import requests
import smtplib
import os
import sys

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# =========================
# CONFIG
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

# =========================
# SAFE JSON HANDLER
# =========================

def safe_json(response):
    try:
        if response.status_code != 200:
            return None
        return response.json()
    except ValueError:
        return None

# =========================
# LOAD AGREEMENT DATA
# =========================

try:
    agreement = pd.read_csv(
        os.path.join(BASE_DIR, "agreement_details.csv")
    )
    print(f"✓ Loaded {len(agreement)} agreements")

except FileNotFoundError:
    print("✗ agreement_details.csv not found")
    sys.exit(1)

except Exception as e:
    print(f"✗ Error loading CSV: {e}")
    sys.exit(1)

results = []

print("\nProcessing agreements...")

# =========================
# MAIN PROCESSING
# =========================

for idx, ag in enumerate(agreement["agreement_no"], 1):

    try:

        print(f"[{idx}/{len(agreement)}] Processing {ag}...", end=" ")

        master_resp = requests.post(
            f"{API_URL}/get_master",
            json={"agreement_no": int(ag)},
            timeout=10
        )

        bounce_resp = requests.post(
            f"{API_URL}/get_bounce",
            json={"agreement_no": int(ag)},
            timeout=10
        )

        dpd_resp = requests.post(
            f"{API_URL}/get_dpd",
            json={"agreement_no": int(ag)},
            timeout=10
        )

        master = safe_json(master_resp)
        bounce_data = safe_json(bounce_resp)
        dpd_data = safe_json(dpd_resp)

        if not master or not bounce_data or not dpd_data:
            print("Invalid API response")
            continue

        bounce = bounce_data.get("bounce_count", 0)
        dpd = dpd_data.get("dpd", 0)

        if dpd > 10 or bounce >= 2:

            if dpd > 30:
                risk = "HIGH RISK"
                action = "Legal Notice Triggered"
            else:
                risk = "MEDIUM RISK"
                action = "Reminder Mail Triggered"

            results.append({
                "agreement_no": ag,
                "DPD": dpd,
                "Bounce": bounce,
                "Risk": risk,
                "Action": action
            })

            print(risk)

        else:
            print("OK")

    except requests.exceptions.ConnectionError:
        print("API server not reachable")

    except requests.exceptions.Timeout:
        print("API timeout")

    except Exception as e:
        print(f"Unexpected error: {e}")

print(f"\nFound {len(results)} risky agreements")

# =========================
# SAVE CSV
# =========================

output_path = os.path.join(BASE_DIR, "daily_risk_output.csv")

if results:

    try:
        pd.DataFrame(results).to_csv(output_path, index=False)
        print(f"Report saved → {output_path}")

    except Exception as e:
        print(f"CSV save error: {e}")

# =========================
# EMAIL FUNCTION
# =========================

def send_via_gmail(body, csv_path=None):

    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        print("Email credentials not configured")
        return

    msg = MIMEMultipart()

    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg["Subject"] = "Daily Risk Alert"

    msg.attach(MIMEText(body, "plain"))

    if csv_path and os.path.exists(csv_path):

        with open(csv_path, "rb") as f:

            part = MIMEBase("application", "octet-stream")

            part.set_payload(f.read())

            encoders.encode_base64(part)

            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(csv_path)}"
            )

            msg.attach(part)

    try:

        with smtplib.SMTP("smtp.gmail.com", 587) as server:

            server.starttls()

            server.login(EMAIL_USER, EMAIL_PASS)

            server.send_message(msg)

        print("Email sent successfully")

    except Exception as e:
        print(f"Email error: {e}")

# =========================
# SEND EMAIL
# =========================

if results:

    body = f"""Daily Risk Report

Date: {pd.Timestamp.now()}

Total Risky Agreements: {len(results)}

"""

    for r in results:

        body += f"""
Agreement No: {r['agreement_no']}
DPD: {r['DPD']}
Bounce Count: {r['Bounce']}
Risk Level: {r['Risk']}
Action: {r['Action']}
"""

    body += "\n--- Automated Risk Monitoring System ---"

    send_via_gmail(body, output_path)

else:
    print("No risky agreements")

print("\nScript finished")