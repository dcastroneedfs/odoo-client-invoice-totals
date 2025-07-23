import os
import time
import logging
import requests
import psycopg2

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

# Environment variables (set in Render Dashboard)
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# Constants
MODEL_NAME = "x_client_invoice_total"
FIELD_NAME_CLIENT = "x_studio_client_name"
FIELD_NAME_AMOUNT = "x_studio_total_invoice_amount"
SYNC_INTERVAL_SECONDS = 60

def fetch_invoice_totals():
    try:
        logging.info("📡 Fetching invoice totals from Neon DB...")
        conn = psycopg2.connect(NEON_DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT client_name, total_invoice_amount FROM client_invoice_totals")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        logging.info(f"📦 Retrieved {len(rows)} vendors.")
        return rows
    except Exception as e:
        logging.error(f"❌ Database fetch error: {e}")
        return []

def odoo_login():
    try:
        login_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "db": ODOO_DB,
                "login": ODOO_USERNAME,
                "password": ODOO_PASSWORD
            }
        }
        response = requests.post(f"{ODOO_URL}/web/session/authenticate", json=login_payload)
        session_id = response.cookies.get("session_id")

        if session_id:
            logging.info("✅ Logged in to Odoo!")
            return session_id
        else:
            logging.error("❌ Odoo login failed. No session cookie returned.")
            logging.error(f"❌ Full response: {response.text}")
            return None

    except Exception as e:
        logging.error(f"❌ Odoo login exception: {e}")
        return None

def upsert_invoice_record(session_id, client_name, total_amount):
    headers = {
        "Content-Type": "application/json",
    }

    cookies = {"session_id": session_id}

    # Search for existing record
    search_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL_NAME,
            "method": "search_read",
            "args": [[(FIELD_NAME_CLIENT, '=', client_name)]],
            "kwargs": {"limit": 1},
        },
        "id": 1
    }

    search_response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=search_payload, headers=headers, cookies=cookies)
    existing_records = search_response.json().get("result", [])

    if existing_records:
        record_id = existing_records[0]["id"]
        update_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": MODEL_NAME,
                "method": "write",
                "args": [[record_id], {
                    FIELD_NAME_AMOUNT: total_amount
                }],
                "kwargs": {},
            },
            "id": 1
        }
        action = "🔄 Updated"
    else:
        update_payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": MODEL_NAME,
                "method": "create",
                "args": [{
                    FIELD_NAME_CLIENT: client_name,
                    FIELD_NAME_AMOUNT: total_amount
                }],
                "kwargs": {},
            },
            "id": 1
        }
        action = "🆕 Created"

    update_response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=update_payload, headers=headers, cookies=cookies)
    if update_response.status_code == 200 and "error" not in update_response.json():
        logging.info(f"{action} vendor: {client_name} | Total: ${total_amount}")
    else:
        logging.warning(f"⚠️ Failed to sync vendor {client_name}. Response: {update_response.text}")

def main_loop():
    logging.info("🐍 Starting client-invoice-totals.py...")
    while True:
        logging.info("🔁 Starting sync cycle...")
        session_id = odoo_login()
        if not session_id:
            logging.warning("⚠️ Could not login to Odoo. Skipping this cycle.")
            time.sleep(SYNC_INTERVAL_SECONDS)
            continue

        invoice_data = fetch_invoice_totals()
        for client_name, total_amount in invoice_data:
            upsert_invoice_record(session_id, client_name, float(total_amount))

        logging.info(f"⏳ Sleeping for {SYNC_INTERVAL_SECONDS} seconds...\n")
        time.sleep(SYNC_INTERVAL_SECONDS)

if __name__ == "__main__":
    main_loop()
