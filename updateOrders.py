import os
import re
import time
import requests
import pymongo
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from datetime import datetime
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient, UpdateOne

load_dotenv()

# ------------------------------------------------------------------------------
# Initialize Mongo and Shopify API credentials
# ------------------------------------------------------------------------------
client = MongoClient(os.getenv('MONGO_URI'))
shopify_url = os.getenv('SHOPIFY_API_KEY')

db = client['test']
collection = db['orders']

# ------------------------------------------------------------------------------
# Utility functions for reading & writing the 'start_order_id.txt' file
# ------------------------------------------------------------------------------
def read_start_order_id(filepath="start_order_id.txt", default=6139281572123):
    """
    Reads the start_order_id from a local file.
    Returns 'default' if file does not exist or is empty.
    """
    if not os.path.exists(filepath):
        return default

    try:
        with open(filepath, "r") as f:
            data = f.read().strip()
            if data:
                return int(data)
            else:
                return default
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return default

def write_start_order_id(order_id, filepath="start_order_id.txt"):
    """
    Writes the given order_id to a local file. Overwrites any existing content.
    """
    try:
        with open(filepath, "w") as f:
            f.write(str(order_id))
    except Exception as e:
        print(f"Error writing {filepath}: {e}")

# ------------------------------------------------------------------------------
# Phone number standardization
# ------------------------------------------------------------------------------
def standardize_phone_number(phone):
    if not phone:
        return None
    
    # Remove all non-digits
    phone = re.sub(r'\D', '', phone)
    
    # Extract the last 10 digits (basic assumption for US/NA phone #s)
    return phone[-10:]

# ------------------------------------------------------------------------------
# Main function to fetch orders from Shopify and upsert into MongoDB
# ------------------------------------------------------------------------------
def update_orders(start_id):
    """
    Loops through Shopify orders starting from 'start_id' and upserts them into MongoDB.
    Continues until no more orders are returned from Shopify.
    """
    more_orders = True

    while more_orders:
        url = (
            f"https://{shopify_url}/admin/api/2024-04/orders.json"
            f"?since_id={start_id}&limit=250&status=any"
        )
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to retrieve data from the Shopify API. Status code: {response.status_code}")
            break

        data = response.json()
        orders = data.get('orders', [])

        if not orders:
            # No more orders to process
            more_orders = False
            break

        bulk_operations = []
        order_count = 0
        min_order_num = float('inf')
        max_order_num = -1

        for order in orders:
            # This becomes the new "since_id" for the next loop iteration
            start_id = order['id']
            order_count += 1

            # Build the document to upsert
            mongoorder = {
                "id": order['id'],
                "products": [],
                "order_number": int(order['order_number']),
                "created_at": isoparse(order['created_at']),
                "cancelled": isoparse(order['cancelled_at']) if order['cancelled_at'] else False,
                "price": float(order['current_total_price']),
                "fullfilment_status": order['fulfillment_status'],
                "fulfillments": order['fulfillments'],
                "financial_status": order['financial_status'],
                "status_url": order['order_status_url'].split('?')[0],
                "discount_codes": order['discount_codes'] if order['discount_codes'] else [],
            }

            # Customer info
            customer = order.get('customer', {})
            mongoorder["first_name"] = (customer.get('first_name') or '').strip()
            mongoorder["last_name"] = (customer.get('last_name') or '').strip()

            # Phone number logic
            phone = order['phone']  or  None
            if not phone:
                try:
                    phone = order['shipping_address']['phone'] or order['billing_address']['phone'] or None
                except:
                    phone = None
            mongoorder['phone'] = standardize_phone_number(phone)

            # Line items
            product_list = []
            for line_item in order.get('line_items', []):
                product_id = line_item.get('product_id')
                if product_id:
                    product_list.append({
                        "name": line_item['title'],
                        "sku": line_item.get('sku'),
                        "quantity": line_item.get('current_quantity', 0),
                        "id": product_id
                    })

            mongoorder['products'] = product_list

            # Track min/max order number for reference
            min_order_num = min(min_order_num, mongoorder['order_number'])
            max_order_num = max(max_order_num, mongoorder['order_number'])

            # Prepare an upsert operation
            bulk_operations.append(
                UpdateOne({"id": mongoorder["id"]}, {"$set": mongoorder}, upsert=True)
            )

        # Bulk write to MongoDB
        if bulk_operations:
            collection.bulk_write(bulk_operations, ordered=False)

        print(f"\nFetched {order_count} orders from Shopify.")
        print(f"Processed Order Number Range: [{min_order_num}, {max_order_num}]")

        # If Shopify returns fewer than 250, we've reached the last page
        if len(orders) < 250:
            more_orders = False

def main():
    IST = ZoneInfo("Asia/Kolkata")
    while True:
        try:
            local_system = os.getenv('LOCAL_SYSTEM')
            if local_system == 'True':
                start_id = 5501152657691 # order 1001
                update_orders(start_id)
                break
            
            start_id = read_start_order_id()
            print(f"\nStarting update from order ID: {start_id}")
            update_orders(start_id)
            three_months_ago = datetime.now(IST) - relativedelta(months=3)
            old_order = collection.find_one(
                {"created_at": {"$lte": three_months_ago}},
                sort=[("created_at", pymongo.DESCENDING)]
            )

            if old_order:
                write_start_order_id(old_order['id'])
                print(f"\nWrote start_order_id to file for: #{old_order['order_number']}")
            else:
                print("\nNo orders found older than 3 months. start_order_id file not updated.")
            
        except Exception as e:
            print(f"Exception occurred: {e}")

if __name__ == '__main__':
    main()
