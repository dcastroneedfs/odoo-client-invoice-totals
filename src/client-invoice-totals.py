import os
import time
import logging
import psycopg2
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Env vars from Render
DB_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
MODEL_NAME = "x_client_invoice_total"

# Login to Odoo and return session ID + cookies
def login_to_odoo():
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": ODOO_DB,
            "login": ODOO_USERNAME,
            "password": ODOO_PASSWORD
        },
        "id": 1
    }
    response = requests.post(f"{ODOO_URL}/web/session/authenticate", json=payload)
    result = response.json()
    if "result" in result:
        session_id = response.cookies.get("session_id")
        logging.info("✅ Logged in to Odoo!")
        return session_id, response.cookies
    else:
        logging.error(f"❌ Odoo login failed: {result}")
        return None, None

# Delete all existing records in the model
def clear_odoo_records(session_id, cookies):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL_NAME,
            "method": "search",
            "args": [[]],
        },
        "id": 1
    }
    res = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload, cookies=cookies)
    ids = res.json().get("result", [])
    if ids:
        delete_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": MODEL_NAME,
                "method": "unlink",
                "args": [ids],
            },
            "id": 2
        }
        del_res = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=delete_payload, cookies=cookies)
        logging.info(f"🗑️ Deleted {len(ids)} existing records.")
    else:
        logging.info("ℹ️ No existing records to delete.")

# Fetch vendors and invoice totals from Neon
def fetch_invoice_totals():
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_name, invoice_amount FROM client_invoice_totals")
        rows = cursor.fetchall()
        conn.close()
        logging.info(f"📦 Retrieved {len(rows)} vendors.")
        return rows
    except Exception as e:
        logging.error(f"❌ Database fetch error: {e}")
        return []

# Create new record in Odoo
def create_odoo_record(vendor_name, invoice_amount, session_id, cookies):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL_NAME,
            "method": "create",
            "args": [{
                "x_studio_client_name": vendor_name,
                "x_studio_total_invoice_amount": invoice_amount,
                "x_name": vendor_name  # Required field
            }],
        },
        "id": 3
    }
    response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload, cookies=cookies)
    if response.status_code == 200 and "result" in response.json():
        logging.info(f"✅ Synced vendor: {vendor_name} | Invoice: ${invoice_amount}")
    else:
        logging.warning(f"⚠️ Failed to sync {vendor_name}. Response: {response.text}")

# Main sync loop
def run_sync():
    logging.info("🐍 Starting client-invoice-totals.py...")
    while True:
        logging.info("🔁 Starting sync cycle...")
        session_id, cookies = login_to_odoo()
        if not session_id:
            logging.error("❌ Aborting sync due to failed login.")
            time.sleep(60)
            continue

        clear_odoo_records(session_id, cookies)
        vendor_rows = fetch_invoice_totals()
        for vendor_name, invoice_amount in vendor_rows:
            logging.info(f"🚚 Syncing vendor: {vendor_name} | Total: ${invoice_amount}")
            create_odoo_record(vendor_name, float(invoice_amount), session_id, cookies)

        logging.info("⏳ Sleeping for 60 seconds...\n")
        time.sleep(60)

if __name__ == "__main__":
    run_sync()
