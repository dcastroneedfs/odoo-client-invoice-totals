import os
import psycopg2
import requests
import time
import urllib.parse as urlparse
from datetime import datetime

# Logging helper
def log(message):
    print(f"{datetime.now().isoformat()} | {message}")

# Load Odoo credentials from environment
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_LOGIN = os.getenv("ODOO_LOGIN")
ODOO_API_KEY = os.getenv("ODOO_API_KEY")

# Parse Neon DB URL
db_url = os.getenv("NEON_DATABASE_URL")
result = urlparse.urlparse(db_url)

db_conn_params = {
    "dbname": result.path[1:],
    "user": result.username,
    "password": result.password,
    "host": result.hostname,
    "port": result.port,
    "sslmode": "require"
}

# Step 1: Fetch invoice totals per vendor
def fetch_client_totals():
    try:
        log("Connecting to Neon DB...")
        conn = psycopg2.connect(**db_conn_params)
        cur = conn.cursor()
        cur.execute("""
            SELECT vendor_name, COUNT(*) AS invoice_count, SUM(invoice_amount) AS total_amount
            FROM invoices
            GROUP BY vendor_name;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        log(f"✅ Fetched {len(rows)} client totals from Neon.")
        return rows
    except Exception as e:
        log(f"❌ Error fetching data from Neon: {e}")
        return []

# Step 2: Authenticate with Odoo
def authenticate_odoo():
    log("🔐 Authenticating with Odoo...")
    response = requests.post(f"{ODOO_URL}/web/session/authenticate", json={
        "params": {
            "db": ODOO_DB,
            "login": ODOO_LOGIN,
            "password": ODOO_API_KEY
        }
    }, headers={"Content-Type": "application/json"})

    if response.status_code != 200:
        log(f"❌ Odoo auth failed: {response.text}")
        return None

    result = response.json().get("result", {})
    if result.get("session_id"):
        log("✅ Authenticated with Odoo.")
    else:
        log("❌ No session ID returned.")
    return result.get("session_id")

# Step 3: Push or update client data in Odoo
def sync_to_odoo(client_data, session_id):
    log("📡 Syncing to Odoo...")
    updated, created = 0, 0

    for vendor_name, invoice_count, total_amount in client_data:
        search_payload = {
            "model": "x_ap_client_invoice_total",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "domain": [["x_studio_client_name", "=", vendor_name]],
                "fields": ["id"]
            }
        }

        headers = {
            "Content-Type": "application/json",
            "Cookie": f"session_id={session_id}"
        }

        search_resp = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=search_payload, headers=headers)
        existing = search_resp.json().get("result")

        if existing:
            record_id = existing[0]["id"]
            update_payload = {
                "model": "x_ap_client_invoice_total",
                "method": "write",
                "args": [[record_id], {
                    "x_studio_invoice_count": invoice_count,
                    "x_studio_total_invoice_amount": total_amount
                }],
                "kwargs": {}
            }
            requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=update_payload, headers=headers)
            updated += 1
        else:
            create_payload = {
                "model": "x_ap_client_invoice_total",
                "method": "create",
                "args": [{
                    "x_studio_client_name": vendor_name,
                    "x_studio_invoice_count": invoice_count,
                    "x_studio_total_invoice_amount": total_amount
                }],
                "kwargs": {}
            }
            requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=create_payload, headers=headers)
            created += 1

    log(f"✅ Sync complete. Created: {created}, Updated: {updated}")

# Step 4: Loop every 60 seconds
if __name__ == "__main__":
    while True:
        log("🔁 Starting sync cycle...")
        session_id = authenticate_odoo()
        if session_id:
            data = fetch_client_totals()
            if data:
                sync_to_odoo(data, session_id)
        log("⏳ Waiting 60 seconds until next sync...\n")
        time.sleep(60)
