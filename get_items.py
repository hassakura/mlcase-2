from tokenize import String
import requests
import csv
import logging
import os
import json
import datetime

from google.cloud import bigquery

# ---------- Logging ----------- 
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "search_items.log")

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ])

# ---------- /////// -----------

# ---------- Constants -----------

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))

SITE_ID = "MLA"  # ML Argentina

BASE_URL = "https://api.mercadolibre.com"

SEARCH_TERMS = ["malbec", "cabernet sauvignon", "merlot", "syrah", "pinot noir", "bonarda", "torrontes", "chardonnay", "sauvignon blanc", "semillon"]

OUTPUT_FILENAME_ITEMS_PATH = os.path.join(CURRENT_FOLDER, "items.csv")
OUTPUT_FILENAME_ATTRIBUTES_PATH = os.path.join(CURRENT_FOLDER, "items_attributes.csv")

CREDENTIALS_FILE_NAME_PATH = os.path.join(CURRENT_FOLDER, "credentials.json")

# ---------- ///////// -----------

insert_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_items(query: str, limit: int = 50) -> list:

    """
    Receives a query and a limit as the parameters for a GET request in the mercadolibre Item search API and returns the results from the response as a list.

    Returns None if an error occurs
    """
    
    params = {
        "q": query,
        "limit": limit
    }

    try:
        response = requests.get(f"{BASE_URL}/sites/{SITE_ID}/search", params = params)
        response.raise_for_status()
        return [item["id"] for item in response.json().get("results", [])]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting items with query '{query}'. Status code {response.status_code}: {e}")
        return []
    except Exception as e:
        logging.error(f"Failed getting data for '{query}': {e}")
        return []

def get_item_details(item_id: str) -> dict:

    """
    Receives a string item_id and returns the JSON response for the items GET request in the mercadolibre Item search API.

    Returns None if an error occurs
    """

    try:
        response = requests.get(f"{BASE_URL}/items/{item_id}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting item details for '{item_id}'. Status code {response.status_code}: {e}")
        return {}
    except Exception as e:
        logging.error(f"Failed getting data for item '{item_id}': {e}")
        return []

def extract_shipping_fields(data: dict) -> dict:

    """
    Receives a dictionary 'data' and returns a dict with the shipping data extracted from the JSON response.

    Returns None if an error occurs
    """
    
    shipping_extract = {}

    try:
        for sub_key, sub_value in data.items():
            shipping_key = f"shipping_{sub_key}"
            if isinstance(sub_value, (dict, list)):
                shipping_extract[shipping_key] = str(sub_value)
            else:
                shipping_extract[shipping_key] = sub_value
        return shipping_extract
    except Exception as e:
        logging.error(f"Failed extracting shipping data from dict: {e}")
        return {}

def extract_seller_address_fields(data: dict) -> dict:

    """
    Receives a dictionary 'data' and returns a dict with the seller's address data extracted from the JSON response.

    Returns None if an error occurs
    """
    
    SELLER_ADDRESS_KEY = "seller_address"
    seller_address_extract = {}

    try:
        if 'country' in data and isinstance(data['country'], dict) and 'name' in data['country']:
            seller_address_extract[f"{SELLER_ADDRESS_KEY}_country_name"] = data['country']['name']
        if 'state' in data and isinstance(data['state'], dict) and 'name' in data['state']:
            seller_address_extract[f"{SELLER_ADDRESS_KEY}_state_name"] = data['state']['name']
        if 'city' in data and isinstance(data['city'], dict) and 'name' in data['city']:
            seller_address_extract[f"{SELLER_ADDRESS_KEY}_city_name"] = data['city']['name']
        return seller_address_extract
    except Exception as e:
        logging.error(f"Failed extracting seller's address data from dict: {e}")
        return {}


def extract_fields(data: dict) -> dict:

    """
    Receives a dictionary 'data' and returns a dict with the shipping and seller's address data extracted from the JSON response.

    Returns None if an error occurs
    """

    extracted = {}

    try:
        for key, value in data.items():

            if isinstance(value, list): extracted[key] = str(value)
            if isinstance(value, dict):
            
                if key == 'shipping':
                    shipping_extracted = extract_shipping_fields(value)
                    for shipping_key, shipping_value in shipping_extracted.items():
                        extracted[shipping_key] = shipping_value

                elif key == 'seller_address':
                    seller_address_extracted = extract_seller_address_fields(value)
                    for seller_address_key, seller_address_value in seller_address_extracted.items():
                        extracted[seller_address_key] = seller_address_value

                else:
                    extracted[key] = str(value)
            else:
                extracted[key] = value
        return extracted
    except Exception as e:
        logging.error(f"Failed extracting shipping and seller's address data from dict: {e}")
        return {}

def get_items_attributes(item_id: str, data: dict) -> list:

    global insert_datetime

    attributes_list = []
    attributes = data.get("attributes", [])

    for attribute in attributes:
        if isinstance(attribute, dict):
            flat_attribute = {'item_id': item_id}
            for key, value in attribute.items():
                if isinstance(value,(dict,list)):
                    flat_attribute[key] = str(value)
                else:
                    flat_attribute[key] = value
            flat_attribute["inserted_ts"] = insert_datetime
            attributes_list.append(flat_attribute)
        else:
            logging.warning(f"Unexpected attribute format for item {item_id}: {attribute}")
    return attributes_list

def create_csv(data: list, filename: str) -> None:

    """
    Receives a dictionary 'data' and a string filename and creates a CSV file with the information extracted from mercadolibre's API.
    """
    
    if not data:
        logging.warning(f"No data for file: {filename}")
        return

    columns = set()
    for row in data:
        columns.update(row.keys())
    columns = list(columns)

    try:
        with open(filename, mode = "w", encoding = "utf-8", newline = "") as f:
            writer = csv.DictWriter(f, fieldnames = columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        logging.info(f"CSV {filename} created!")
    except Exception as e:
        logging.error(f"Couldn't write data to CSV {filename}: {e}")

def write_to_bigquery(data: list, credentials: String, dataset_name: String, table_name: String) -> None:

    """
    Receives a dictionary 'data', string credentials as the credentials source filename, string dataset_name as the target dataset name and a string table_name as the Table's name and sends the data extracted to BigQuery.
    """

    client = bigquery.Client.from_service_account_json(os.path.dirname(os.path.abspath(__file__)) + f'/{credentials}')
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    for row in data:
        for key, value in row.items():
            if isinstance(value, (list, dict)):
                row[key] = json.dumps(value)

    try:
        errors = client.insert_rows_json(table_ref, data)  # API request
        if errors == []:
            logging.info(f"Successfully wrote {len(data)} rows to BigQuery table {table_ref}.")
        else:
            logging.error(f"Encountered errors while writing to BigQuery: {errors}")
    except Exception as e:
        logging.error(f"Error writing to BigQuery table {table_ref}: {e}")



def main():
    
    all_items_data, all_attributes_data = [], []

    for term in SEARCH_TERMS:
        logging.info(f"Processing term: {term}")
        item_ids = get_items(term, limit = 50)

        if not item_ids:
            logging.warning(f"No items found for term: {term}")
            continue

        for item_id in item_ids:
            item_details = get_item_details(item_id)
            if item_details:
                extracted_item = extract_fields(item_details)
                extracted_item['search_term'] = term
                extracted_item['snapshot_ts'] = insert_datetime
                all_items_data.append(extracted_item)

                attributes_data = get_items_attributes(item_id, item_details)
                all_attributes_data.extend(attributes_data)

    create_csv(all_items_data, OUTPUT_FILENAME_ITEMS_PATH)
    create_csv(all_attributes_data, OUTPUT_FILENAME_ATTRIBUTES_PATH)
    write_to_bigquery(all_items_data, "credentials.json", "case2", "items")
    write_to_bigquery(all_attributes_data, "credentials.json", "case2", "items_attributes")

if __name__ == "__main__":
    main()