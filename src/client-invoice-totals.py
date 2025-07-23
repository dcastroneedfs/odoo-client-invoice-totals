import os
import time
import logging
import psycopg2
import requests

# Configure logging
logging.basicConfig(
    format='%(asctime)s | %(message)s',
    level=logging.INFO
)

logging.info("🐍 Starting client-invoice-totals.py...")

# Environment Variables
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
ODOO_DB = os.getenv("ODOO_DB")

# Constants
MODEL_NAME = "x_client_invoice_total"
FIELD_CLIENT_NAME = "x_studio_client_name"
FIELD_INVOICE_AMOUNT = "x_studio_total_invoice_amount"

def fetch_invoice_totals():
    logging.info("📡 Fetching invoice totals from Neon DB...")
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_name, invoice_amount FROM invoices;")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        logging.info(f"📦 Retrieved {len(rows)} vendors.")
        return rows
    except Exception as e:
        logging.error(f"❌ Database fetch error: {e}")
        return []

def login_odoo():
    logging.info("🔐 Logging into Odoo...")
    login_url = f"{ODOO_URL}/web/session/authenticate"
    headers = {"Content-Type": "application/json"}
    payload = {
        "jsonrpc": "2.0",
        "params": {
            "db": ODOO_DB,
            "login": ODOO_USERNAME,
            "password": ODOO_PASSWORD
        }
    }
    response = requests.post(login_url, json=payload, headers=headers)
    if response.status_code == 200 and "session_id" in response.cookies:
        logging.info("✅ Logged in to Odoo!")
        return response.cookies.get("session_id")
    else:
        logging.error(f"❌ Odoo login failed. Status: {response.status_code}")
        logging.error(f"❌ Response: {response.text}")
        return None

def update_vendor_record(session_id, vendor_name, invoice_amount):
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }

    data = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL_NAME,
            "method": "create",
            "args": [{
                FIELD_CLIENT_NAME: vendor_name,
                FIELD_INVOICE_AMOUNT: invoice_amount
            }],
            "kwargs": {},
        },
        "id": 1,
    }

    response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=data, headers=headers)
    if response.status_code == 200:
        res_json = response.json()
        if "error" in res_json:
            logging.warning(f"⚠️ Failed to update vendor {vendor_name}. Response: {res_json}")
        else:
            logging.info(f"✅ Updated Odoo for vendor: {vendor_name}")
    else:
        logging.warning(f"⚠️ Failed HTTP request for vendor {vendor_name}. Status: {response.status_code}, Response: {response.text}")

def main():
    while True:
        logging.info("🔁 Starting sync cycle...")
        session_id = login_odoo()
        if not session_id:
            time.sleep(60)
            continue

        vendors = fetch_invoice_totals()
        for vendor_name, invoice_amount in vendors:
            logging.info(f"🚚 Syncing vendor: {vendor_name} | Total: ${invoice_amount}")
            update_vendor_record(session_id, vendor_name, float(invoice_amount))

        logging.info("⏳ Sleeping for 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    main()
