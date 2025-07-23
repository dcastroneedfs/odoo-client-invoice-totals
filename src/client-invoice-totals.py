import os
import psycopg2
import xmlrpc.client
import logging
from decimal import Decimal

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')

# Odoo credentials
ODOO_URL = 'https://your-odoo-instance.odoo.com'
ODOO_DB = 'your-odoo-db-name'
ODOO_USERNAME = 'your@email.com'
ODOO_PASSWORD = 'your-password'

# PostgreSQL (Neon) connection string
DATABASE_URL = os.getenv("DATABASE_URL")  # or hardcode for testing

def fetch_vendor_totals():
    """Connects to Neon and returns vendor totals as a list of (vendor, total)"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        logging.info("📡 Fetching invoice totals from Neon DB...")

        cur.execute("""
            SELECT vendor_name, SUM(invoice_amount)
            FROM vendor_invoices
            GROUP BY vendor_name
        """)
        results = cur.fetchall()
        logging.info(f"📦 Retrieved {len(results)} vendors.")

        cur.close()
        conn.close()
        return results
    except Exception as e:
        logging.error(f"❌ Error fetching from Neon: {e}")
        return []

def login_odoo():
    """Authenticates and returns uid, models"""
    try:
        logging.info("🔐 Logging into Odoo...")
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})
        if not uid:
            raise Exception("Login failed. Check credentials.")
        models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
        logging.info("✅ Logged in to Odoo!")
        return uid, models
    except Exception as e:
        logging.error(f"❌ Login failed: {e}")
        return None, None

def clear_ap_dashboard(models, uid):
    """Deletes all rows from x_ap_dashboard"""
    try:
        logging.info("🧹 Clearing old dashboard entries...")
        deleted = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            'x_ap_dashboard', 'unlink',
            [[['id', '!=', False]]]
        )
        logging.info(f"✅ Old entries removed: {deleted}")
    except Exception as e:
        logging.error(f"❌ Error clearing dashboard: {e}")

def push_totals(models, uid, vendor_totals):
    """Push vendor totals into Odoo"""
    for vendor, total in vendor_totals:
        try:
            logging.info(f"🚚 Syncing vendor: {vendor} | Total: ${float(total):,.2f}")
            models.execute_kw(
                ODOO_DB, uid, ODOO_PASSWORD,
                'x_ap_dashboard', 'create',
                [{
                    'name': vendor,
                    'x_studio_float_field_44o_1j0pl01m9': float(total)
                }]
            )
            logging.info(f"✅ Synced vendor: {vendor} | Invoice: ${float(total):,.2f}")
        except Exception as e:
            logging.error(f"❌ Error syncing {vendor}: {e}")

def main():
    vendor_totals = fetch_vendor_totals()
    if not vendor_totals:
        logging.warning("⚠️ No vendor totals found. Exiting.")
        return

    uid, models = login_odoo()
    if not uid or not models:
        return

    clear_ap_dashboard(models, uid)
    push_totals(models, uid, vendor_totals)

    logging.info("🎉 Sync complete.")

if __name__ == "__main__":
    main()
