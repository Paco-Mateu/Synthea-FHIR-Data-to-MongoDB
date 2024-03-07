import os
import json  # Make sure to import the json module
import pymongo
from dotenv import load_dotenv
import certifi

load_dotenv()

# Set SSL_CERT_FILE to use certificates from certifi
os.environ['SSL_CERT_FILE'] = certifi.where()

mongodb_connection_string = os.environ.get('MONGODB_CONNECTION_STRING')
database_name = os.getenv('DATABASE')

data_directory = 'Dataset/enrichedResources'

client = pymongo.MongoClient(mongodb_connection_string)
db = client[database_name]

def upload_collection(file_path, collection_name):
    print(f'Starting upload for: {collection_name}')
    with open(file_path, 'r') as file:
        # This line now works because the json module is imported
        data = [json.loads(line) for line in file]
    collection = db[collection_name]
    collection.insert_many(data)
    print(f'Uploaded {len(data)} documents to collection {collection_name}')

def main():
    total_files = len([name for name in os.listdir(data_directory) if name.endswith('.ndjson')])
    print(f'Total files to process: {total_files}')
    
    processed_files = 0
    for filename in os.listdir(data_directory):
        if filename.endswith('.ndjson'):
            collection_name, _ = os.path.splitext(filename)
            file_path = os.path.join(data_directory, filename)
            upload_collection(file_path, collection_name)
            processed_files += 1
            print(f'Processed {processed_files}/{total_files} files.')
    
    print('All data uploaded successfully.')

if __name__ == "__main__":
    main()
