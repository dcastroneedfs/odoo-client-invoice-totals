import os
import time
import logging
import psycopg2
import requests

# Setup logging
logging.basicConfig(
    format='%(asctime)s | %(message)s',
    level=logging.INFO
)

# Environment variables
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# Odoo model & fields
MODEL_NAME = "x_client_invoice_total"
FIELD_CLIENT_NAME = "x_studio_client_name"
FIELD_INVOICE_AMOUNT = "x_studio_total_invoice_amount"

# Loop timing
SYNC_INTERVAL = 60  # seconds


def fetch_invoice_totals():
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT vendor_name, invoice_amount FROM invoices")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        logging.error(f"❌ Database fetch error: {e}")
        return []


def login_to_odoo():
    data = {
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
        response = requests.post(f"{ODOO_URL}/web/session/authenticate", json=data)
        response.raise_for_status()
        session_id = response.cookies.get("session_id")
        if session_id:
            logging.info("✅ Logged in to Odoo!")
            return session_id
        else:
            logging.error("❌ Login failed: session_id not found.")
            return None
    except Exception as e:
        logging.error(f"❌ Error logging in to Odoo: {e}")
        return None


def delete_existing_records(session_id):
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }

    # Search for all records
    search_data = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL_NAME,
            "method": "search",
            "args": [[]],
            "kwargs": {}
        },
        "id": 1
    }

    try:
        search_response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=search_data, headers=headers)
        ids = search_response.json().get("result", [])
        if not ids:
            logging.info("ℹ️ No existing records to delete.")
            return

        # Delete all records found
        delete_data = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": MODEL_NAME,
                "method": "unlink",
                "args": [ids],
                "kwargs": {}
            },
            "id": 1
        }

        delete_response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=delete_data, headers=headers)
        if "error" in delete_response.json():
            logging.error(f"❌ Failed to delete records: {delete_response.json()}")
        else:
            logging.info(f"🗑️ Deleted {len(ids)} existing records.")

    except Exception as e:
        logging.error(f"❌ Error during deletion: {e}")


def insert_vendor_record(session_id, vendor_name, invoice_amount):
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
                "x_name": str(vendor_name),  # Description field
                FIELD_CLIENT_NAME: str(vendor_name),
                FIELD_INVOICE_AMOUNT: float(invoice_amount)
            }],
            "kwargs": {}
        },
        "id": 1
    }

    try:
        response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=data, headers=headers)
        res_json = response.json()
        if "error" in res_json:
            logging.warning(f"⚠️ Failed to sync {vendor_name}: {res_json}")
        else:
            logging.info(f"✅ Synced vendor: {vendor_name} | Invoice: ${invoice_amount}")
    except Exception as e:
        logging.error(f"❌ Exception syncing vendor {vendor_name}: {e}")


def main():
    logging.info("🐍 Starting client-invoice-totals.py...")

    while True:
        logging.info("🔁 Starting sync cycle...")
        session_id = login_to_odoo()
        if not session_id:
            time.sleep(SYNC_INTERVAL)
            continue

        delete_existing_records(session_id)

        logging.info("📡 Fetching invoice totals from Neon DB...")
        records = fetch_invoice_totals()

        if not records:
            logging.info("⚠️ No records found.")
        else:
            logging.info(f"📦 Retrieved {len(records)} vendors.")
            for vendor_name, invoice_amount in records:
                logging.info(f"🚚 Syncing vendor: {vendor_name} | Total: ${invoice_amount}")
                insert_vendor_record(session_id, vendor_name, invoice_amount)

        logging.info(f"⏳ Sleeping for {SYNC_INTERVAL} seconds...\n")
        time.sleep(SYNC_INTERVAL)


if __name__ == "__main__":
    main()
