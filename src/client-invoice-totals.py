import os
import psycopg2
import requests
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

# Load environment variables from Render (do not hardcode)
NEON_DB_URL = os.environ.get("NEON_DATABASE_URL")
ODOO_URL = os.environ.get("ODOO_URL")
ODOO_DB = os.environ.get("ODOO_DB")
ODOO_USERNAME = os.environ.get("ODOO_USERNAME")
ODOO_PASSWORD = os.environ.get("ODOO_PASSWORD")

# Odoo model and fields
ODOO_MODEL = "x_client_invoice_total"
ODOO_FIELDS = {
    "name": "x_name",
    "client": "x_studio_client_name",
    "amount": "x_studio_total_invoice_amount"
}

def get_invoice_totals():
    try:
        conn = psycopg2.connect(NEON_DB_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_name, invoice_amount FROM invoices")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"❌ Database fetch error: {e}")
        return []

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
    try:
        res = requests.post(f"{ODOO_URL}/web/session/authenticate", json=payload).json()
        return res["result"]["session_id"]
    except Exception as e:
        logging.error(f"❌ Odoo login error: {e}")
        return None

def call_odoo(session_id, method, model, args=None, kwargs=None):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": model,
            "method": method,
            "args": args or [],
            "kwargs": kwargs or {},
        },
        "id": 1
    }
    headers = {"Content-Type": "application/json", "X-Openerp-Session-Id": session_id}
    response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload, headers=headers)
    return response.json()

def clear_existing_records(session_id):
    response = call_odoo(session_id, "search", ODOO_MODEL, args=[[]])
    record_ids = response.get("result", [])
    if record_ids:
        call_odoo(session_id, "unlink", ODOO_MODEL, args=[record_ids])
        logging.info(f"🧹 Deleted {len(record_ids)} existing records.")
    else:
        logging.info("ℹ️ No existing records to delete.")

def sync_to_odoo(session_id, records):
    for vendor_name, amount in records:
        logging.info(f"🚚 Syncing vendor: {vendor_name} | Total: ${amount:.2f}")
        payload = {
            ODOO_FIELDS["name"]: vendor_name,  # Required "Description"
            ODOO_FIELDS["client"]: vendor_name,
            ODOO_FIELDS["amount"]: amount
        }
        res = call_odoo(session_id, "create", ODOO_MODEL, args=[payload])
        if "error" in res:
            logging.warning(f"⚠️ Failed to sync vendor {vendor_name}. Response: {res}")
        else:
            logging.info(f"✅ Synced vendor: {vendor_name} | Invoice: ${amount:.2f}")

if __name__ == "__main__":
    logging.info("🐍 Starting client-invoice-totals.py...")
    while True:
        logging.info("🔁 Starting sync cycle...")
        session_id = login_to_odoo()
        if not session_id:
            time.sleep(60)
            continue

        logging.info("✅ Logged in to Odoo!")
        clear_existing_records(session_id)

        logging.info("📡 Fetching invoice totals from Neon DB...")
        vendor_totals = get_invoice_totals()

        if vendor_totals:
            logging.info(f"📦 Retrieved {len(vendor_totals)} vendors.")
            sync_to_odoo(session_id, vendor_totals)
        else:
            logging.info("⚠️ No data found in Neon DB.")

        logging.info("⏳ Sleeping for 60 seconds...")
        time.sleep(60)
