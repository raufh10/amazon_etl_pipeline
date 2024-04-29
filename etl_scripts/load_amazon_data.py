from pymongo import MongoClient

import os
import json

def load_amazon_data(file_path):

    # Create a client connection to your MongoDB instance
    client = MongoClient('localhost', 27017)
    print('Connected to MongoDB')

    # Specify the database to be used
    db = client['amazon_data']

    # Specify the collection to be used
    col = db['products_data']

    # Load the JSON data
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print('Data loaded')

    # Insert the data into the MongoDB collection
    col.insert_many(data)
    print('Data inserted into MongoDB')

    # Close the connection
    client.close()
    print('Connection closed')

def delete_file(file_path):
    # Use OS to delete the file job_data.json
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"The file {file_path} has been deleted.")
    else:
        print(f"The file {file_path} does not exist.")

file_path = r'/home/raufhamidy/Documents/amazon_etl_pipeline/products_data.json'

load_amazon_data(file_path)
print('Data loaded into MongoDB')

delete_file(file_path)
print('File deleted')

print('ETL process completed')
