import os
import time
import logging
import psycopg2
import requests
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    format='%(asctime)s | %(message)s',
    level=logging.INFO
)

# ENV VARS REQUIRED
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# Odoo model and field names
ODOO_MODEL = "x_client_invoice_total"
ODOO_FIELD_CLIENT = "x_studio_client_name"
ODOO_FIELD_AMOUNT = "x_studio_total_invoice_amount"

if not all([NEON_DATABASE_URL, ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD]):
    missing = [k for k in ["NEON_DATABASE_URL", "ODOO_URL", "ODOO_DB", "ODOO_USERNAME", "ODOO_PASSWORD"] if not os.getenv(k)]
    logging.error(f"❌ Missing required environment variables: {', '.join(missing)}")
    exit(1)

# Connect to Neon DB and fetch invoice totals
def fetch_invoice_totals():
    try:
        parsed = urlparse(NEON_DATABASE_URL)
        conn = psycopg2.connect(
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
            host=parsed.hostname,
            port=parsed.port,
            sslmode="require"
        )
        cur = conn.cursor()
        cur.execute("SELECT vendor, SUM(amount) FROM invoices GROUP BY vendor")
        data = cur.fetchall()
        cur.close()
        conn.close()
        return [{"vendor": row[0], "total": float(row[1])} for row in data]
    except Exception as e:
        logging.error(f"❌ Error fetching invoice totals: {e}")
        return []

# Log in to Odoo and get session
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
        if response.status_code == 200 and response.json().get("result", {}).get("session_id"):
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

# Send data to Odoo
def send_to_odoo(session_id, vendor_data):
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }
    for vendor in vendor_data:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": ODOO_MODEL,
                "method": "create",
                "args": [[
                    {
                        ODOO_FIELD_CLIENT: vendor["vendor"],
                        ODOO_FIELD_AMOUNT: vendor["total"]
                    }
                ]],
                "kwargs": {}
            },
            "id": 1
        }
        logging.info(f"🚚 Syncing vendor: {vendor['vendor']} | Total: ${vendor['total']:.2f}")
        try:
            response = requests.post(f"{ODOO_URL}/web/dataset/call_kw", headers=headers, json=payload)
            if response.status_code != 200 or "error" in response.json():
                logging.warning(f"⚠️ Failed to update vendor {vendor['vendor']}. Response: {response.text}")
        except Exception as e:
            logging.error(f"❌ Exception sending to Odoo: {e}")

# Main sync loop
def main():
    logging.info("🐍 Starting client-invoice-totals.py...")
    while True:
        logging.info("🔁 Starting sync cycle...")
        session_id = odoo_login()
        if session_id:
            logging.info("📡 Fetching invoice totals from Neon DB...")
            vendors = fetch_invoice_totals()
            logging.info(f"📦 Retrieved {len(vendors)} vendors.")
            send_to_odoo(session_id, vendors)
        logging.info("⏳ Sleeping for 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    main()
