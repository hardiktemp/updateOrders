from pymongo import MongoClient , UpdateOne
import os
from dotenv import load_dotenv
import requests
import re

load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))
shopify_url = os.getenv('SHOPIFY_API_KEY')

db = client['orders']
collection = db['orders']

def standardize_phone_number(phone):
    if not phone :
        return None
  
    phone = re.sub(r'\D', '', phone)
    return phone[-10:]


def update_order():
    # startId = 6043926724891
    startId = 6000724476187
    # startId = 0
    moreOrders = True
    
    while moreOrders:
        response = requests.get(f"https://{shopify_url}/admin/api/2024-04/orders.json?since_id={startId}&limit=250&status=any")
        print(f"https://{shopify_url}/admin/api/2024-04/orders.json?since_id={startId}&limit=250&status=any")
        if response.status_code == 200:
            data = response.json()
            orders = data['orders']
            if not orders:
                moreOrders = False
                break
            
            orderChunk = []
            bulk_operations = []
            for order in orders:
                print(f"Processing order {order['order_number']}")
                startId = order['id']
                
                mongoorder = {"id": order['id'], 'products': [] , 'order_number': order['order_number']}
                mongoorder['cancelled'] = order['cancelled_at'] if order['cancelled_at'] else False
                mongoorder['price'] = order['current_subtotal_price']
                mongoorder['fullfilment_status'] = order['fulfillment_status']
                mongoorder['financial_status'] = order['financial_status']
                mongoorder['created_at'] = order['created_at']
                mongoorder["status_url"] = order['order_status_url']
                
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
                
                orderChunk.append(mongoorder)
                bulk_operations.append(
                    UpdateOne({"id": mongoorder["id"]}, {"$set": mongoorder}, upsert=True)
                )
            
            print(f"Inserting {len(orderChunk)} orders")
            collection.bulk_write(bulk_operations)
            print(f"Processed {len(orderChunk)} orders")
            if len(data['orders']) < 250:
                moreOrders = False
        else:
            print('Failed to retrieve data from the Shopify API')
            break

def main():
    while(True):
        update_order()

if __name__ == '__main__':
    main()
