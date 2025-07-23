import os
import time
import logging
import psycopg2
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

# Environment variables
NEON_DB_URL = os.environ.get("NEON_DATABASE_URL")
ODOO_URL = os.environ.get("ODOO_URL")
ODOO_DB = os.environ.get("ODOO_DB")
ODOO_USERNAME = os.environ.get("ODOO_USERNAME")
ODOO_PASSWORD = os.environ.get("ODOO_PASSWORD")

# Odoo model + fields
ODOO_MODEL = "x_client_invoice_total"
ODOO_FIELDS = {
    "name": "x_name",  # Required: "Description"
    "client": "x_studio_client_name",
    "amount": "x_studio_total_invoice_amount"
}

# Use persistent session with cookies
session = requests.Session()

def login_to_odoo():
    url = f"{ODOO_URL}/web/session/authenticate"
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
        response = session.post(url, json=payload).json()
        if "result" in response and response["result"].get("session_id"):
            logging.info("✅ Logged in to Odoo!")
            return True
        else:
            logging.error(f"❌ Login failed: {response}")
            return False
    except Exception as e:
        logging.error(f"❌ Exception during login: {e}")
        return False

def call_odoo(method, model, args=None, kwargs=None):
    url = f"{ODOO_URL}/web/dataset/call_kw"
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

    headers = {"Content-Type": "application/json"}
    try:
        res = session.post(url, json=payload, headers=headers)
        return res.json()
    except Exception as e:
        logging.error(f"❌ Request error: {e}")
        return {}

def clear_existing_records():
    response = call_odoo("search", ODOO_MODEL, args=[[]])
    record_ids = response.get("result", [])
    if record_ids:
        call_odoo("unlink", ODOO_MODEL, args=[record_ids])
        logging.info(f"🧹 Deleted {len(record_ids)} existing records.")
    else:
        logging.info("ℹ️ No existing records to delete.")

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

def sync_to_odoo(records):
    for vendor_name, amount in records:
        logging.info(f"🚚 Syncing vendor: {vendor_name} | Total: ${amount:.2f}")
        payload = {
            ODOO_FIELDS["name"]: vendor_name,  # Required field!
            ODOO_FIELDS["client"]: vendor_name,
            ODOO_FIELDS["amount"]: amount
        }
        res = call_odoo("create", ODOO_MODEL, args=[payload])
        if "error" in res:
            logging.warning(f"⚠️ Failed to sync vendor {vendor_name}. Response: {res}")
        else:
            logging.info(f"✅ Synced vendor: {vendor_name} | Invoice: ${amount:.2f}")

if __name__ == "__main__":
    logging.info("🐍 Starting client-invoice-totals.py...")
    while True:
        logging.info("🔁 Starting sync cycle...")

        if not login_to_odoo():
            time.sleep(60)
            continue

        clear_existing_records()

        logging.info("📡 Fetching invoice totals from Neon DB...")
        records = get_invoice_totals()

        if records:
            logging.info(f"📦 Retrieved {len(records)} vendors.")
            sync_to_odoo(records)
        else:
            logging.info("⚠️ No data found.")

        logging.info("⏳ Sleeping for 60 seconds...")
        time.sleep(60)
