import os
import time
import sys
import psycopg2
import requests
import urllib.parse as urlparse
from datetime import datetime

def log(msg):
    print(f"{datetime.now().isoformat()} | {msg}", flush=True)

# 🔍 Startup check
print("🐍 Starting client-invoice-totals.py...", flush=True)

# ✅ Required ENV vars
required_vars = [
    "NEON_DATABASE_URL", "ODOO_URL", "ODOO_DB", "ODOO_LOGIN", "ODOO_PASSWORD"
]
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    print(f"❌ Missing environment variables: {', '.join(missing)}", flush=True)
    sys.exit(1)

# 🌐 Load and parse Neon DB URL
try:
    db_url = os.getenv("NEON_DATABASE_URL")
    result = urlparse.urlparse(db_url)
    db_conn_params = {
        "dbname": result.path.lstrip("/"),
        "user": result.username,
        "password": result.password,
        "host": result.hostname,
        "port": result.port,
        "sslmode": "require"
    }
    print("✅ Parsed Neon DB URL", flush=True)
except Exception as e:
    print(f"❌ Failed to parse NEON_DATABASE_URL: {e}", flush=True)
    sys.exit(1)

# 🔐 Odoo credentials
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_LOGIN = os.getenv("ODOO_LOGIN")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")

# 📡 Fetch totals from Neon
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

# 🔐 Authenticate to Odoo with login + password
def authenticate_odoo():
    log("🔐 Logging into Odoo with user credentials...")
    try:
        resp = requests.post(f"{ODOO_URL}/web/session/authenticate", json={
            "params": {
                "db": ODOO_DB,
                "login": ODOO_LOGIN,
                "password": ODOO_PASSWORD
            }
        }, headers={"Content-Type": "application/json"})

        if resp.status_code != 200:
            log(f"❌ Odoo auth failed: {resp.status_code} - {resp.text}")
            return None

        session_id = resp.json().get("result", {}).get("session_id")
        if session_id:
            log("✅ Logged into Odoo.")
        else:
            log("❌ No session ID returned from Odoo.")
        return session_id
    except Exception as e:
        log(f"❌ Exception during Odoo login: {e}")
        return None

# 🔁 Push to Odoo
def sync_to_odoo(data, session_id):
    log("📤 Syncing to Odoo...")
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"session_id={session_id}"
    }

    created, updated = 0, 0

    for vendor_name, invoice_count, total_amount in data:
        try:
            # 🔎 Search
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
            result = search_resp.json().get("result", [])

            if result:
                # 🛠 Update
                record_id = result[0]["id"]
                update_payload = {
                    "model": "x_ap_client_invoice_total",
                    "method": "write",
                    "args": [[record_id], {
                        "x_studio_invoice_count": invoice_count,
                        "x_studio_total_invoice_amount": total_amount
                    }],
                    "kwargs": {}
                }
                update_resp = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=update_payload, headers=headers)
                if update_resp.status_code == 200:
                    updated += 1
                else:
                    log(f"❌ Failed to update {vendor_name}: {update_resp.text}")
            else:
                # ➕ Create
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
                create_resp = requests.post(f"{ODOO_URL}/web/dataset/call_kw", json=create_payload, headers=headers)
                if create_resp.status_code == 200:
                    created += 1
                else:
                    log(f"❌ Failed to create {vendor_name}: {create_resp.text}")
        except Exception as e:
            log(f"❌ Exception syncing {vendor_name}: {e}")

    log(f"✅ Odoo sync complete. Created: {created}, Updated: {updated}")

# 🔁 Main loop
if __name__ == "__main__":
    while True:
        log("🔁 Starting sync cycle...")
        session_id = authenticate_odoo()
        if session_id:
            data = fetch_client_totals()
            if data:
                sync_to_odoo(data, session_id)
        log("⏳ Sleeping for 60 seconds...\n")
        time.sleep(60)
