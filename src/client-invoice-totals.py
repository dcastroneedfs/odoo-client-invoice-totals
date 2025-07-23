import os
import psycopg2
import requests
import time
import urllib.parse as urlparse
from datetime import datetime

# Pretty logger
def log(msg):
    print(f"{datetime.now().isoformat()} | {msg}")

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

# 1. Fetch totals per client from Neon
def fetch_client_totals():
    try:
        log("📡 Fetching invoice totals from Neon DB...")
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
        log(f"💰 Retrieved totals for {len(rows)} clients.")
        return rows
    except Exception as e:
        log(f"❌ Error fetching data from Neon: {e}")
        return []

# 2. Authenticate with Odoo
def authenticate_odoo():
    log("🔐 Logging into Odoo...")
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
        log("✅ Logged into Odoo, beginning sync...")
    else:
        log("❌ No session ID returned.")
    return result.get("session_id")

# 3. Sync to Odoo
def sync_to_odoo(client_data, session_id):
    log("📤 Syncing client invoice totals to Odoo...")
    created, updated = 0, 0

    for vendor_name, invoice_count, total_amount in client_data:
        headers = {
            "Content-Type": "application/json",
            "Cookie": f"session_id={session_id}"
        }

        # Search for existing record
        search_payload = {
            "model": "x_ap_client_invoice_total",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "domain": [["x_studio_client_name", "=", vendor_name]],
                "fields": ["id"]
            }
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

    log(f"✅ Odoo sync complete! Created: {created} | Updated: {updated}")

# 4. Run in loop
if __name__ == "__main__":
    while True:
        log("🔁 Starting sync cycle...")
        session_id = authenticate_odoo()
        if session_id:
            totals = fetch_client_totals()
            if totals:
                sync_to_odoo(totals, session_id)
        log("⏳ Sleeping for 60 seconds...\n")
        time.sleep(60)
