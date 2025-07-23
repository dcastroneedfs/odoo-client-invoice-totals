import os
import time
import logging
import psycopg2
import requests

# === Logging Configuration ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === Load environment variables ===
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

required_vars = ["NEON_DATABASE_URL", "ODOO_URL", "ODOO_DB", "ODOO_USERNAME", "ODOO_PASSWORD"]
for var in required_vars:
    if not os.getenv(var):
        logging.error(f"❌ Missing required env var: {var}")
        exit(1)

# === Odoo Login ===
def odoo_login():
    logging.info("🔐 Logging into Odoo...")
    try:
        response = requests.post(
            f"{ODOO_URL}/web/session/authenticate",
            json={
                "jsonrpc": "2.0",
                "params": {
                    "db": ODOO_DB,
                    "login": ODOO_USERNAME,
                    "password": ODOO_PASSWORD
                }
            },
            timeout=10
        )
        result = response.json()
        if "result" in result and "session_id" in response.cookies:
            session_id = response.cookies.get("session_id")
            logging.info("✅ Logged in to Odoo!")
            return session_id
        else:
            logging.error(f"❌ Odoo login failed. Status: {response.status_code}")
            logging.error(f"❌ Response: {response.text}")
            return None
    except Exception as e:
        logging.error(f"❌ Exception during Odoo login: {e}")
        return None

# === Get invoice totals from Neon ===
def fetch_invoice_totals():
    logging.info("📡 Fetching invoice totals from Neon DB...")
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            SELECT vendor_name, SUM(invoice_amount)::numeric(10,2)
            FROM invoices
            GROUP BY vendor_name
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        logging.info(f"📦 Retrieved {len(rows)} vendors.")
        return rows
    except Exception as e:
        logging.error(f"❌ Error fetching from Neon DB: {e}")
        return []

# === Update Odoo custom model ===
def update_odoo_totals(session_id, vendor_totals):
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }
    for vendor, total in vendor_totals:
        logging.info(f"🚚 Syncing vendor: {vendor} | Total: ${total}")
        try:
            data = {
                "jsonrpc": "2.0",
                "method": "call",
                "params": {
                    "model": "x_ap_dashboard",
                    "method": "create",
                    "args": [{
                        "x_client_name": vendor,
                        "x_studio_float_field_44o_1j0pl01m9": float(total)
                    }],
                    "kwargs": {}
                },
                "id": 1
            }
            response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=data, headers=headers)
            if response.ok and 'result' in response.json():
                logging.info("✅ Successfully updated Odoo.")
            else:
                logging.warning(f"⚠️ Failed to update vendor {vendor}. Response: {response.text}")
        except Exception as e:
            logging.error(f"❌ Error updating Odoo for vendor {vendor}: {e}")

# === Main loop ===
def main():
    logging.info("🐍 Starting client-invoice-totals.py...")
    while True:
        try:
            logging.info("🔁 Starting sync cycle...")
            session_id = odoo_login()
            if session_id:
                vendor_totals = fetch_invoice_totals()
                update_odoo_totals(session_id, vendor_totals)
            else:
                logging.warning("⚠️ Skipping push. Login failed.")
        except Exception as e:
            logging.error(f"❌ Unhandled error: {e}")
        logging.info("⏳ Sleeping for 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    main()
