import json
import os

output_folder_path = 'Dataset/mergedPatientsPerResourceType'
uuid_mapping_file_path = os.path.join(output_folder_path, 'uuid_to_url_mapping.json')

with open(uuid_mapping_file_path, 'r', encoding='utf-8') as mapping_file:
    uuid_to_url_mapping = json.load(mapping_file)

def update_references(resource, uuid_to_url_mapping):
    for key, value in resource.items():
        if isinstance(value, dict):
            update_references(value, uuid_to_url_mapping)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    update_references(item, uuid_to_url_mapping)
        elif key == 'reference' and value.startswith('urn:uuid:'):
            if value in uuid_to_url_mapping:
                resource[key] = uuid_to_url_mapping[value]

def process_and_update_references(output_folder_path, uuid_to_url_mapping):
    for filename in os.listdir(output_folder_path):
        file_path = os.path.join(output_folder_path, filename)
        if os.path.isfile(file_path) and file_path.endswith('.ndjson'):
            updated_resources = []
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    resource = json.loads(line)
                    update_references(resource, uuid_to_url_mapping)
                    updated_resources.append(resource)
            
            with open(file_path, 'w', encoding='utf-8') as file:
                for resource in updated_resources:
                    print(json.dumps(resource), file=file)
            print(f'Updated references in {filename}')

process_and_update_references(output_folder_path, uuid_to_url_mapping)
