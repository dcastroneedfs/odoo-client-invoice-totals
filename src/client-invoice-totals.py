import os
import time
import logging
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ──────────────────────────────
# 📋 Logging Setup
# ──────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.info
log_err = logging.error
log_warn = logging.warning

# ──────────────────────────────
# 🔐 Load & sanitize env vars
# ──────────────────────────────
def _env(name, default=None, required=False):
    v = os.getenv(name, default)
    if v is not None:
        v = v.strip()
    if required and not v:
        log_err(f"❌ Missing required env var: {name}")
        raise SystemExit(1)
    return v

NEON_DATABASE_URL = _env("NEON_DATABASE_URL", required=True)
ODOO_URL          = _env("ODOO_URL", required=True)              # e.g. https://needfstrial.odoo.com
ODOO_DB           = _env("ODOO_DB", required=True)               # e.g. needfstrial
ODOO_USERNAME     = _env("ODOO_USERNAME", required=True)
ODOO_PASSWORD     = _env("ODOO_PASSWORD", required=True)
ODOO_MODEL        = _env("ODOO_MODEL", "x_client_invoice_total")
ODOO_VENDOR_FIELD = _env("ODOO_VENDOR_FIELD", "x_studio_client_name")
ODOO_TOTAL_FIELD  = _env("ODOO_TOTAL_FIELD", "x_studio_total_invoice_amount")
ODOO_COUNT_FIELD  = _env("ODOO_COUNT_FIELD", "x_studio_invoice_count")  # optional; used if present

# ──────────────────────────────
# 🐘 Fetch invoice totals per vendor from Neon DB
# ──────────────────────────────
def fetch_invoice_totals():
    log("📡 Fetching invoice totals from Neon DB...")
    rows = []
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL, cursor_factory=RealDictCursor)
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT vendor_name,
                           COUNT(*) AS invoice_count,
                           SUM(invoice_amount) AS total_invoice_amount
                    FROM invoices
                    GROUP BY vendor_name
                    ORDER BY vendor_name;
                """)
                rows = cur.fetchall()
        conn.close()
        log(f"📦 Retrieved {len(rows)} vendors.")
    except Exception as e:
        log_err(f"❌ Failed to fetch from Neon DB: {e}")
    return rows

# ──────────────────────────────
# 🔐 Login to Odoo and return a Session (cookie-based)
# ──────────────────────────────
def login_to_odoo():
    log(f"🔐 Logging into Odoo... (db={ODOO_DB})")
    session = requests.Session()
    try:
        resp = session.post(
            f"{ODOO_URL}/web/session/authenticate",
            json={
                "params": {
                    "db": ODOO_DB,
                    "login": ODOO_USERNAME,
                    "password": ODOO_PASSWORD,
                }
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
    except Exception as e:
        log_err(f"❌ Exception during Odoo login request: {e}")
        return None

    # Some failures still return 200 status; check content
    if resp.status_code != 200:
        log_err(f"❌ Odoo login HTTP failure: {resp.status_code} {resp.text}")
        return None

    # try JSON parse
    try:
        data = resp.json()
    except Exception as e:
        log_err(f"❌ Could not parse Odoo login JSON: {e} | body={resp.text}")
        return None

    # Extract session cookie
    session_id = session.cookies.get("session_id")
    if not session_id:
        # See if error in body
        err = data.get("error")
        if err:
            log_err(f"❌ Odoo login error: {err}")
        else:
            log_err("❌ Odoo login failed: no session_id cookie returned.")
        return None

    log(f"✅ Odoo login succeeded. session_id={session_id}")
    return session

# ──────────────────────────────
# 🔍 Upsert record (search by vendor; update if exists else create)
# ──────────────────────────────
def upsert_vendor_record(session, vendor_name, total_amount, invoice_count):
    headers = {"Content-Type": "application/json"}
    url = f"{ODOO_URL}/jsonrpc"

    # 1. search_read for existing vendor
    search_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": ODOO_MODEL,
            "method": "search_read",
            "args": [[ [ODOO_VENDOR_FIELD, "=", vendor_name] ],
                     ["id", ODOO_VENDOR_FIELD, ODOO_TOTAL_FIELD, ODOO_COUNT_FIELD]],
            "kwargs": {"limit": 1},
        },
        "id": 1,
    }
    try:
        search_resp = session.post(url, json=search_payload, headers=headers, timeout=30)
        search_json = search_resp.json()
        existing = search_json.get("result", [])
    except Exception as e:
        log_err(f"❌ search_read error for vendor '{vendor_name}': {e}")
        return

    if existing:
        # update
        rec_id = existing[0]["id"]
        update_vals = {
            ODOO_TOTAL_FIELD: total_amount,
        }
        if ODOO_COUNT_FIELD:
            update_vals[ODOO_COUNT_FIELD] = invoice_count

        update_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": ODOO_MODEL,
                "method": "write",
                "args": [[rec_id], update_vals],
                "kwargs": {},
            },
            "id": 2,
        }
        try:
            update_resp = session.post(url, json=update_payload, headers=headers, timeout=30)
            if update_resp.status_code == 200 and update_resp.json().get("result"):
                log(f"🔁 Updated vendor '{vendor_name}' | total=${total_amount:,.2f} count={invoice_count}")
            else:
                log_err(f"❌ Failed to update '{vendor_name}': {update_resp.text}")
        except Exception as e:
            log_err(f"❌ Exception updating '{vendor_name}': {e}")
    else:
        # create
        create_vals = {
            ODOO_VENDOR_FIELD: vendor_name,
            ODOO_TOTAL_FIELD: total_amount,
        }
        if ODOO_COUNT_FIELD:
            create_vals[ODOO_COUNT_FIELD] = invoice_count

        create_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": ODOO_MODEL,
                "method": "create",
                "args": [create_vals],
                "kwargs": {},
            },
            "id": 3,
        }
        try:
            create_resp = session.post(url, json=create_payload, headers=headers, timeout=30)
            if create_resp.status_code == 200 and create_resp.json().get("result"):
                log(f"➕ Created vendor '{vendor_name}' | total=${total_amount:,.2f} count={invoice_count}")
            else:
                log_err(f"❌ Failed to create '{vendor_name}': {create_resp.text}")
        except Exception as e:
            log_err(f"❌ Exception creating '{vendor_name}': {e}")

# ──────────────────────────────
# 📦 Read back a few records for verification
# ──────────────────────────────
def debug_read_back_records(session, limit=10):
    headers = {"Content-Type": "application/json"}
    url = f"{ODOO_URL}/jsonrpc"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": ODOO_MODEL,
            "method": "search_read",
            "args": [[], ["id", ODOO_VENDOR_FIELD, ODOO_TOTAL_FIELD, ODOO_COUNT_FIELD]],
            "kwargs": {"limit": limit},
        },
        "id": 99,
    }
    try:
        resp = session.post(url, json=payload, headers=headers, timeout=30)
        data = resp.json()
        records = data.get("result", [])
        log(f"🧾 {len(records)} records currently in Odoo ({ODOO_MODEL}):")
        for rec in records:
            vendor = rec.get(ODOO_VENDOR_FIELD)
            total = rec.get(ODOO_TOTAL_FIELD)
            count = rec.get(ODOO_COUNT_FIELD)
            log(f"   • {vendor} | total=${total} | count={count}")
    except Exception as e:
        log_err(f"❌ debug_read_back_records error: {e}")

# ──────────────────────────────
# 🔁 Main loop
# ──────────────────────────────
if __name__ == "__main__":
    log("🐍 Starting client-invoice-totals.py...")
    while True:
        log("🔁 Starting sync cycle...")
