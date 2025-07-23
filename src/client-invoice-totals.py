import os
import time
import psycopg2
import requests
from datetime import datetime
from urllib.parse import urlparse

# 🔧 Load from ENV (Render dashboard)
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_LOGIN = os.getenv("ODOO_LOGIN")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
ODOO_MODEL = os.getenv("ODOO_MODEL")
ODOO_VENDOR_FIELD = os.getenv("ODOO_VENDOR_FIELD")
ODOO_AMOUNT_FIELD = os.getenv("ODOO_AMOUNT_FIELD")

# 🪵 Logging helper
def log(msg):
    print(f"{datetime.now().isoformat()} | {msg}", flush=True)

# 🔐 Authenticate with Odoo and return session
def login_to_odoo():
    log("🔐 Logging into Odoo with user credentials...")
    try:
        session = requests.Session()
        resp = session.post(f"{ODOO_URL}/web/session/authenticate", json={
            "params": {
                "db": ODOO_DB,
                "login": ODOO_LOGIN,
                "password": ODOO_PASSWORD
            }
        }, headers={"Content-Type": "application/json"})

        if resp.status_code == 200:
            session_id = session.cookies.get("session_id")
            if session_id:
                log(f"✅ Logged in! Session ID: {session_id}")
                return session
            else:
                log("❌ Login failed: No session ID in cookies.")
        else:
            log(f"❌ Login failed: Status {resp.status_code}, Body: {resp.text}")
    except Exception as e:
        log(f"❌ Exception during Odoo login: {e}")
    return None

# 🧮 Fetch invoice totals by vendor
def fetch_vendor_totals():
    log("📡 Connecting to Neon DB...")
    try:
        parsed = urlparse(NEON_DATABASE_URL)
        conn = psycopg2.connect(
            dbname=parsed.path[1:],
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=parsed.port,
            sslmode="require"
        )
        cur = conn.cursor()
        log("📊 Running SQL query for vendor totals...")
        cur.execute("""
            SELECT vendor_name, SUM(invoice_amount)::numeric(12,2)
            FROM invoices
            GROUP BY vendor_name
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        log(f"✅ Retrieved {len(results)} vendor totals from Neon DB.")
        return results
    except Exception as e:
        log(f"❌ Error fetching data from Neon DB: {e}")
        return []

# 🚚 Push vendor totals to Odoo
def sync_to_odoo(session, vendor_totals):
    headers = {"Content-Type": "application/json"}
    for vendor, amount in vendor_totals:
        log(f"🚚 Syncing vendor: {vendor} | Total: ${amount}")
        try:
            payload = {
                "jsonrpc": "2.0",
                "method": "call",
                "params": {
                    "model": ODOO_MODEL,
                    "method": "create",
                    "args": [{
                        ODOO_VENDOR_FIELD: vendor,
                        ODOO_AMOUNT_FIELD: float(amount)
                    }],
                    "kwargs": {}
                },
                "id": 1
            }
            resp = session.post(f"{ODOO_URL}/jsonrpc", json=payload, headers=headers)
            if resp.status_code == 200:
                log("✅ Successfully updated Odoo.")
            else:
                log(f"❌ Failed to update Odoo: {resp.status_code} - {resp.text}")
        except Exception as e:
            log(f"❌ Exception during Odoo sync: {e}")

# 🔁 Main ETL loop
def main_loop():
    log("🐍 Starting client-invoice-totals.py...")

    if not all([NEON_DATABASE_URL, ODOO_URL, ODOO_DB, ODOO_LOGIN, ODOO_PASSWORD, ODOO_MODEL, ODOO_VENDOR_FIELD, ODOO_AMOUNT_FIELD]):
        log("❌ Missing one or more required environment variables.")
        return

    log("✅ All environment variables loaded successfully.")

    while True:
        log("🔁 Starting sync cycle...")
        session = login_to_odoo()
        if session:
            vendor_totals = fetch_vendor_totals()
            if vendor_totals:
                sync_to_odoo(session, vendor_totals)
        log("⏳ Sleeping for 60 seconds...\n")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
