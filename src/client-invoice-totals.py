from datetime import datetime
import requests
import logging
import os

# Setup logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
log = logging.info

# ENV variables
NEON_API_URL = os.getenv("NEON_API_URL", "https://mock.neon.tech/api/invoices")
ODOO_URL = os.getenv("ODOO_URL", "https://needfstrial.odoo.com")
ODOO_MODEL = "x_client_invoice_total"
ODOO_VENDOR_FIELD = "x_studio_client_name"
ODOO_AMOUNT_FIELD = "x_studio_total_invoice_amount"
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "dcastro@needfs.com")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "admin12345678#")
ODOO_DB = os.getenv("ODOO_DB", "needfstrial")

def get_invoice_totals():
    log("📡 Fetching invoice totals from Neon API...")
    response = requests.get(NEON_API_URL)
    if response.status_code == 200:
        data = response.json()
        client_totals = {}
        for invoice in data:
            client = invoice.get("client_name", "").strip()
            amount = float(invoice.get("amount", 0))
            if client:
                client_totals[client] = client_totals.get(client, 0) + amount
        log(f"📦 Parsed client totals: {client_totals}")
        return client_totals
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")

def odoo_login():
    log("🔐 Logging into Odoo with user credentials...")
    session = requests.Session()
    payload = {
        "jsonrpc": "2.0",
        "params": {
            "db": ODOO_DB,
            "login": ODOO_USERNAME,
            "password": ODOO_PASSWORD
        }
    }
    response = session.post(f"{ODOO_URL}/web/session/authenticate", json=payload)
    data = response.json()
    if "result" in data and "session_id" in session.cookies:
        log(f"✅ Success! Session ID: {session.cookies['session_id']}")
        return session
    else:
        raise Exception(f"❌ Login failed: {data}")

def push_totals_to_odoo(session, totals):
    for client, amount in totals.items():
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": ODOO_MODEL,
                "method": "create",
                "args": [{
                    ODOO_VENDOR_FIELD: client,
                    ODOO_AMOUNT_FIELD: amount,
                }],
                "kwargs": {},
            },
            "id": datetime.now().isoformat()
        }
        response = session.post(f"{ODOO_URL}/jsonrpc", json=payload)
        if response.status_code == 200 and "result" in response.json():
            log(f"📤 Pushed {client}: ${amount}")
        else:
            log(f"❌ Failed to push {client}: {response.status_code} {response.text}")

def debug_read_back_records(session):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": ODOO_MODEL,
            "method": "search_read",
            "args": [[], ["id", ODOO_VENDOR_FIELD, ODOO_AMOUNT_FIELD]],
        },
        "id": 99
    }
    response = session.post(f"{ODOO_URL}/jsonrpc", json=payload)
    data = response.json()
    log("🧾 Records currently in Odoo:")
    for rec in data.get("result", []):
        log(f"   ➤ ID {rec['id']}: {rec[ODOO_VENDOR_FIELD]} - ${rec[ODOO_AMOUNT_FIELD]}")

def main():
    try:
        log("🐍 Starting client-invoice-totals.py...")
        totals = get_invoice_totals()
        session = odoo_login()
        push_totals_to_odoo(session, totals)
        debug_read_back_records(session)
    except Exception as e:
        log(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
