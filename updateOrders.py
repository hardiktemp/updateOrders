import os
import re
import requests
from dotenv import load_dotenv
from dateutil.parser import isoparse
from pymongo import MongoClient , UpdateOne

load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))
shopify_url = os.getenv('SHOPIFY_API_KEY')

db = client['test']
collection = db['orders']

def standardize_phone_number(phone):
    if not phone :
        return None
  
    phone = re.sub(r'\D', '', phone)
    return phone[-10:]


def update_order(startId = 6139281572123):
    moreOrders = True
    
    while moreOrders:
        response = requests.get(f"https://{shopify_url}/admin/api/2024-04/orders.json?since_id={startId}&limit=250&status=any")
        if response.status_code == 200:
            data = response.json()
            orders = data['orders']
            if not orders:
                moreOrders = False
                break
            
            orderChunk = []
            bulk_operations = []
            min_order = 10**8
            max_order = 0
            for order in orders:
                startId = order['id']
                
                mongoorder = {"id": order['id'], 'products': [] , 'order_number': int(order['order_number']) , 'created_at': isoparse(order['created_at']),}
                mongoorder['cancelled'] = isoparse(order['cancelled_at']) if order['cancelled_at'] else False
                mongoorder['price'] = float(order['current_total_price'])
                mongoorder['fullfilment_status'] = order['fulfillment_status']
                mongoorder['fulfillments'] = order['fulfillments']
                mongoorder['financial_status'] = order['financial_status']
                mongoorder["status_url"] = order['order_status_url']
                if order['discount_codes']:
                    mongoorder['discount_codes'] = order['discount_codes']
                else:
                    mongoorder['discount_codes'] = []

                customer = order['customer']
                if customer and 'first_name' in customer and customer['first_name']:
                    mongoorder["first_name"] = customer['first_name'].strip()
                else:
                        mongoorder["first_name"] = ''
                
                if customer and 'last_name' in customer and customer['last_name']:
                        mongoorder["last_name"] = customer['last_name'].strip()
                else:
                    mongoorder["last_name"] = ''
                
                phone = order['phone']  or  None
                if not phone : 
                    try:
                        phone = order['shipping_address']['phone'] or order['billing_address']['phone'] or None
                    except:
                        phone = None
                mongoorder['phone'] = standardize_phone_number(phone)
                
                for line_item in order['line_items']:
                    product_id = line_item['product_id']
                    if product_id:
                        product_detail = {
                            "name": line_item['title'],
                            "sku": line_item.get('sku'),
                            "quantity": line_item['current_quantity'],
                            "id": product_id
                        }
                        mongoorder['products'].append(product_detail)
                
                min_order = min(min_order, mongoorder['order_number'])
                max_order = max(max_order, mongoorder['order_number'])

                orderChunk.append(mongoorder)
                bulk_operations.append(
                    UpdateOne({"id": mongoorder["id"]}, {"$set": mongoorder}, upsert=True)
                )
            
            print(f"Min Order: {min_order} \nMax Order: {max_order}")
            print(f"Processing {len(orderChunk)} orders\n")
            collection.bulk_write(bulk_operations)
            if len(data['orders']) < 250:
                moreOrders = False
        else:
            print('Failed to retrieve data from the Shopify API')
            break

def main():
    while(True):
        local_system = os.getenv('LOCAL_SYSTEM')
        if local_system == 'True':
            start_id = 5501152657691 # order 1001
            update_order(startId=start_id)
            break
        else:
            start_id = 6139281572123 # order 20000
            update_order(startId=start_id)

if __name__ == '__main__':
    main()