import json
import os
from collections import defaultdict

base_dir = 'Dataset'

folder_path = os.path.join(base_dir, 'originalFHIRBundles')
output_folder_path = os.path.join(base_dir, 'mergedPatientsPerResourceType')
uuid_mapping_file_path = os.path.join(output_folder_path, 'uuid_to_url_mapping.json')

os.makedirs(output_folder_path, exist_ok=True)

uuid_to_url_mapping = {}

# Function to generate a new URL reference for a resource
def generate_new_url(resource_type, counter):
    return f"{resource_type}/{counter}"

# Function to process each JSON file and extract resources
def process_files(folder_path, output_folder_path, max_patients=2000):
    # A dictionary to hold file handles for each resource type
    file_handles = defaultdict(lambda: None)
    # Resource counter for generating unique IDs
    resource_counters = defaultdict(int)

    processed_patients = 0

    try:
        for filename in os.listdir(folder_path):
            if processed_patients >= max_patients:
                print(f"Reached the limit of {max_patients} patients.")
                break

            file_path = os.path.join(folder_path, filename)
            
            if os.path.isfile(file_path) and file_path.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    print(f'Processing file: {filename}')

                    for entry in data.get('entry', []):
                        resource = entry.get('resource')
                        if resource:
                            resource_type = resource.get('resourceType')
                            resource_counters[resource_type] += 1
                            new_url = generate_new_url(resource_type, resource_counters[resource_type])
                            if 'id' in resource:
                                uuid_to_url_mapping[f"urn:uuid:{resource['id']}"] = new_url
                            
                            resource['id'] = new_url

                            output_file_path = os.path.join(output_folder_path, f'{resource_type}.ndjson')
                            
                            if file_handles[resource_type] is None:
                                file_handles[resource_type] = open(output_file_path, 'a', encoding='utf-8')
                            
                            print(json.dumps(resource), file=file_handles[resource_type])
                    
                    processed_patients += 1
                    
    finally:
        for resource_type, fh in file_handles.items():
            if fh is not None:
                fh.close()
        with open(uuid_mapping_file_path, 'w', encoding='utf-8') as mapping_file:
            json.dump(uuid_to_url_mapping, mapping_file, indent=2)

        print(f'Processing completed. Processed {processed_patients} files in {output_folder_path}.')
        print(f'UUID to URL mappings saved in {uuid_mapping_file_path}.')

process_files(folder_path, output_folder_path, 2000)
