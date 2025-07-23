import requests
from datetime import datetime

ODOO_URL = "https://needfstrial.odoo.com"
ODOO_DB = "needfstrial"
ODOO_LOGIN = "dcastro@needfs.com"
ODOO_PASSWORD = "admin12345678#"

def log(msg):
    print(f"{datetime.now().isoformat()} | {msg}", flush=True)

def test_odoo_login():
    log("🔐 Testing Odoo login...")

    try:
        session = requests.Session()

        resp = session.post(f"{ODOO_URL}/web/session/authenticate", json={
            "params": {
                "db": ODOO_DB,
                "login": ODOO_LOGIN,
                "password": ODOO_PASSWORD
            }
        }, headers={"Content-Type": "application/json"})

        log(f"📬 Response Status: {resp.status_code}")
        data = resp.json()

        session_id = session.cookies.get("session_id")

        if session_id:
            log(f"✅ Success! Session ID: {session_id}")
        else:
            log("❌ Login failed: No session ID returned in cookies.")
    except Exception as e:
        log(f"❌ Exception during Odoo login: {e}")

if __name__ == "__main__":
    test_odoo_login()
