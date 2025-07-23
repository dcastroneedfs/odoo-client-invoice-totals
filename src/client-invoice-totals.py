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
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ──────────────────────────────
# 🔐 Load Environment Variables
# ──────────────────────────────
NEON_DB_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
ODOO_MODEL = os.getenv("ODOO_MODEL", "x_ap_dashboard")
ODOO_VENDOR_FIELD = os.getenv("ODOO_VENDOR_FIELD", "x_vendor_name")
ODOO_TOTAL_FIELD = os.getenv("ODOO_TOTAL_FIELD", "x_invoice_total")

if not NEON_DB_URL:
    logging.error("❌ NEON_DATABASE_URL is not set.")
    exit(1)

# ──────────────────────────────
# 🐘 Fetch invoice totals per vendor
# ──────────────────────────────
def fetch_invoice_totals():
    logging.info("📡 Fetching invoice totals from Neon DB...")
    try:
        conn = psycopg2.connect(NEON_DB_URL, cursor_factory=RealDictCursor)
        cur = conn.cursor()
        cur.execute("""
            SELECT vendor_name, SUM(invoice_amount) AS total_invoice_amount
            FROM invoices
            GROUP BY vendor_name
        """)
        rows = cur.fetchall()
        conn.close()
        logging.info(f"📦 Retrieved {len(rows)} vendors.")
        return rows
    except Exception as e:
        logging.error(f"❌ Failed to fetch from Neon DB: {e}")
        return []

# ──────────────────────────────
# 🔐 Login to Odoo and get session cookie
# ──────────────────────────────
def login_to_odoo():
    logging.info("🔐 Logging into Odoo...")
    try:
        response = requests.post(
            f"{ODOO_URL}/web/session/authenticate",
            json={
                "jsonrpc": "2.0",
                "params": {
                    "db": ODOO_DB,
                    "login": ODOO_USERNAME,
                    "password": ODOO_PASSWORD,
                },
            },
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 200:
            session_id = response.cookies.get("session_id")
            if session_id:
                logging.info("✅ Odoo login successful. Session established.")
                return session_id
        logging.error(f"❌ Odoo login failed. Status: {response.status_code}")
        logging.error(f"❌ Response: {response.text}")
    except Exception as e:
        logging.error(f"❌ Exception during login: {e}")
    return None

# ──────────────────────────────
# 🚀 Push data to Odoo
# ──────────────────────────────
def push_to_odoo(session_id, vendor_data):
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }
    url = f"{ODOO_URL}/jsonrpc"

    for row in vendor_data:
        vendor = row["vendor_name"]
        total = row["total_invoice_amount"]

        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": ODOO_MODEL,
                "method": "create",
                "args": [{
                    ODOO_VENDOR_FIELD: vendor,
                    ODOO_TOTAL_FIELD: total
                }],
                "kwargs": {},
            },
            "id": datetime.utcnow().timestamp()
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            if response.status_code == 200:
                logging.info(f"✅ Pushed: {vendor} | ${total:,.2f}")
            else:
                logging.error(f"❌ Failed to push {vendor}. Status {response.status_code}")
        except Exception as e:
            logging.error(f"❌ Exception pushing {vendor}: {e}")

# ──────────────────────────────
# 🔁 Loop
# ──────────────────────────────
if __name__ == "__main__":
    logging.info("🐍 Starting client-invoice-totals.py...")

    while True:
        logging.info("🔁 Starting sync cycle...")

        data = fetch_invoice_totals()
        if not data:
            logging.warning("⚠️ No data to sync.")
        else:
            session = login_to_odoo()
            if session:
                push_to_odoo(session, data)
            else:
                logging.warning("⚠️ Skipping push. Login failed.")

        logging.info("⏳ Sleeping for 60 seconds...\n")
        time.sleep(60)
