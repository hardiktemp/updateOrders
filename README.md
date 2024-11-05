## Update Orders Script

GitHub Repository: [updateOrders](https://github.com/hardiktemp/updateOrders)

This Python script is designed to run continuously on Render and helps manage Shopify orders by storing them in a MongoDB database, as Shopify does not support searching orders via phone numbers.

### Features

- **Order Storage:** Stores all Shopify orders in a MongoDB database for easy retrieval and management.
- **Order Update:** The script updates orders based on an `order ID` (startId). It updates all orders starting from this ID and checks for changes in existing orders, updating them as needed.

### Running the Script Locally

To run the script locally, follow these steps:

1. **Install Requirements**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create a `.env` file**

   In the root directory, create a `.env` file with the following keys:
   ```
   MONGO_URI=<your_mongo_uri>
   SHOPIFY_API_KEY=<your_shopify_api_key>
   ```

3. **Run the Script**
   ```bash
   python updateOrders.py
   ```

### Usage

The main function, `update_order`, accepts a `startId` parameter, representing the starting order ID. The function will:

- Update all orders beginning from `startId`.
- Check for any changes in orders already present in the MongoDB database and update them accordingly.
me know if you'd like any further customization!
