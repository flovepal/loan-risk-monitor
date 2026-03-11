import pandas as pd
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="MCP Server")

# =========================
# BASE DIRECTORY
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# =========================
# LOAD CSV FILES
# =========================
agreement = pd.read_csv(os.path.join(BASE_DIR, "agreement_details.csv"))
product = pd.read_csv(os.path.join(BASE_DIR, "product_details.csv"))
dealer = pd.read_csv(os.path.join(BASE_DIR, "dealer_details.csv"))
employee = pd.read_csv(os.path.join(BASE_DIR, "employee_details.csv"))
bounce = pd.read_csv(os.path.join(BASE_DIR, "bounce_details.csv"))
payment = pd.read_csv(os.path.join(BASE_DIR, "payment_details.csv"))

# =========================
# REQUEST MODEL
# =========================
class AgreementQuery(BaseModel):
    agreement_no: int

# =========================
# SAFE MERGE FUNCTION
# =========================
def safe_merge(left_df, right_df, left_key, right_key):
    if left_key not in left_df.columns or right_key not in right_df.columns:
        return left_df
    return left_df.merge(
        right_df,
        left_on=left_key,
        right_on=right_key,
        how="left"
    )

# =========================
# MASTER DATA API
# =========================
@app.post("/get_master")
def get_master(query: AgreementQuery):

    a = agreement[agreement["agreement_no"] == query.agreement_no]

    if a.empty:
        return JSONResponse(
            status_code=404,
            content={"error": "Agreement not found"}
        )

    m = a.copy()

    m = safe_merge(m, product, "product_id", "product_id")
    m = safe_merge(m, dealer, "dealer_id", "dealer_id")
    m = safe_merge(m, employee, "employee_id", "employee_id")

    return m.to_dict(orient="records")[0]

# =========================
# BOUNCE COUNT API
# =========================
@app.post("/get_bounce")
def get_bounce(query: AgreementQuery):

    count = len(bounce[bounce["agreement_no"] == query.agreement_no])

    return {
        "agreement_no": query.agreement_no,
        "bounce_count": int(count)
    }

# =========================
# DPD API
# =========================
@app.post("/get_dpd")
def get_dpd(query: AgreementQuery):

    p = payment[payment["agreement_no"] == query.agreement_no]

    if p.empty:
        return JSONResponse(
            status_code=404,
            content={
                "agreement_no": query.agreement_no,
                "dpd": 0
            }
        )

    row = p.iloc[0]

    due = pd.to_datetime(row["due_date"])
    paid = pd.to_datetime(row["payment_date"])

    dpd = (paid - due).days

    return {
        "agreement_no": query.agreement_no,
        "dpd": int(dpd)
    }