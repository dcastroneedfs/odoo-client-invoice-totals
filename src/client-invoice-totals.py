import os
import psycopg2
import requests
import time
import urllib.parse as urlparse

# Load Odoo credentials from environment
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_LOGIN = os.getenv("ODOO_LOGIN")
ODOO_API_KEY = os.getenv("ODOO_API_KEY")

# Parse Neon DB URL from full connection string
db_url = os.getenv("NEON_DATABASE_URL")
result = urlparse.urlparse(db_url)

db_conn_params = {
    "dbname": result.path[1:],  # remove leading slash
    "user": result.username,
    "password": result.password,
    "host": result.hostname,
    "port": result.port,
    "sslmode": "require"
}

# Step 1: Fetch invoice totals per vendor from Neon DB
def fetch_client_totals():
    try:
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
        return rows
    except Exception as e:
        print(f"❌ Error connecting to Neon: {e}")
        return []

# Step 2: Authenticate with Odoo using API key
def authenticate_odoo():
    response = requests.post(f"{ODOO_URL}/web/session/authenticate", json={
        "params": {
            "db": ODOO_DB,
            "login": ODOO_LOGIN,
            "password": ODOO_API_KEY
        }
    }, headers={"Content-Type": "application/json"})

    if response.status_code != 200:
        print(f"❌ Odoo auth failed: {response.text}")
        return None

    result = response.json().get("result", {})
    return result.get("session_id")

# Step 3: Push or update each vendor's totals into Odoo Studio model
def sync_to_odoo(client_data, session_id):
    for vendor_name, invoice_count, total_amount in client_data:
        # Prepare search request to find existing vendor record
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
            # Update existing record
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
        else:
            # Create new record
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

    print("✅ Sync complete.")

# Step 4: Run every 60 seconds in a loop
if __name__ == "__main__":
    while True:
        print("🔁 Running sync to Odoo...")
        session_id = authenticate_odoo()
        if session_id:
            data = fetch_client_totals()
            if data:
                sync_to_odoo(data, session_id)
        time.sleep(60)
