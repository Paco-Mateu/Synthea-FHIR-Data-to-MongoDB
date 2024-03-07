import json
import os
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
import base64
import glob

base_dir = 'Dataset'

input_folder_path = os.path.join(base_dir, 'mergedPatientsPerResourceType')
enriched_folder_path = os.path.join(base_dir, 'enrichedResources')  
uuid_mapping_file = os.path.join(input_folder_path, 'uuid_to_url_mapping.json')  

os.makedirs(enriched_folder_path, exist_ok=True)

load_dotenv()
openai_api_key = os.getenv('OPENAI_API_KEY')

clientOpenAI = OpenAI(api_key=openai_api_key)

# Define search parameters for each resource type (simplified for demonstration)
search_parameters_config = {
"AllergyIntolerance": [
    {"key": "patient", "attribute": "patient.reference"},
    {"key": "clinical-status", "attribute": "clinicalStatus.coding[0].code"},
    {"key": "verification-status", "attribute": "verificationStatus.coding[0].code"},
    {"key": "code", "attribute": "code.coding[0].code"},
    {"key": "onset", "attribute": "onsetDateTime"}
],
"CarePlan": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "date", "attribute": "period.start"},
    {"key": "category", "attribute": "category[0].coding[0].code"},
    {"key": "status", "attribute": "status"}
],
"CareTeam": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "status", "attribute": "status"},
    {"key": "encounter", "attribute": "encounter.reference"},
    {"key": "participant", "attribute": "participant.member.reference"}
],
"Condition": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "clinical-status", "attribute": "clinicalStatus.coding[0].code"},
    {"key": "code", "attribute": "code.coding[0].code"},
    {"key": "onset-date", "attribute": "onsetDateTime"}
],
"Device": [
    {"key": "patient", "attribute": "patient.reference"},
    {"key": "status", "attribute": "status"},
    {"key": "type", "attribute": "type.coding[0].code"},
    {"key": "identifier", "attribute": "identifier[0].value"}
],
"DiagnosticReport": [
    {"key": "basedOn", "attribute": "basedOn.reference"},
    {"key": "category", "attribute": "category.coding[0].code"},
    {"key": "code", "attribute": "code.coding[0].code"},
    {"key": "conclusion", "attribute": "conclusion"},
    {"key": "date", "attribute": "effectiveDateTime"},
    {"key": "encounter", "attribute": "encounter.reference"},
    {"key": "identifier", "attribute": "identifier[0].value"},
    {"key": "issued", "attribute": "issued"},
    {"key": "media", "attribute": "media[0].link"},
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "performer", "attribute": "performer[0].actor.reference"},
    {"key": "result", "attribute": "result[0].reference"},
    {"key": "resultsInterpreter", "attribute": "resultsInterpreter[0].reference"},
    {"key": "specimen", "attribute": "specimen[0].reference"},
    {"key": "status", "attribute": "status"},
    {"key": "study", "attribute": "imagingStudy[0].reference"},
    {"key": "subject", "attribute": "subject.reference"}
],
"DocumentReference": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "type", "attribute": "type.coding[0].code"},
    {"key": "date", "attribute": "date"},
    {"key": "category", "attribute": "category[0].coding[0].code"}
],
"Encounter": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "date", "attribute": "period.start"},
    {"key": "type", "attribute": "type[0].coding[0].code"},
    {"key": "status", "attribute": "status"}
],
"ImagingStudy": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "modality", "attribute": "series[0].modality.code"},
    {"key": "date", "attribute": "started"},
    {"key": "basedon", "attribute": "basedOn[0].reference"}
],
"Immunization": [
    {"key": "patient", "attribute": "patient.reference"},
    {"key": "date", "attribute": "occurrenceDateTime"},
    {"key": "status", "attribute": "status"},
    {"key": "vaccine-code", "attribute": "vaccineCode.coding[0].code"}
],
"Location": [
    {"key": "name", "attribute": "name"},
    {"key": "address", "attribute": "address.text"},
    {"key": "type", "attribute": "type[0].coding[0].code"},
    {"key": "status", "attribute": "status"}
],
"Medication": [
    {"key": "code", "attribute": "code.coding[0].code"},
    {"key": "status", "attribute": "status"},
    {"key": "form", "attribute": "form.coding[0].code"},
    {"key": "ingredient", "attribute": "ingredient[0].itemCodeableConcept.coding[0].code"}
],
"MedicationAdministration": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "status", "attribute": "status"},
    {"key": "effective-time", "attribute": "effectiveDateTime"},
    {"key": "medication", "attribute": "medicationCodeableConcept.coding[0].code"}
],
"MedicationRequest": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "status", "attribute": "status"},
    {"key": "intent", "attribute": "intent"},
    {"key": "medication", "attribute": "medicationCodeableConcept.coding[0].code"}
],
"MedicationStatement": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "status", "attribute": "status"},
    {"key": "effective", "attribute": "effectiveDateTime"},
    {"key": "medication", "attribute": "medicationCodeableConcept.coding[0].code"}
],
"Observation": [
    {"key": "subject", "attribute": "subject.reference"},
    {"key": "code", "attribute": "code.coding[0].code"},
    {"key": "date", "attribute": "effectiveDateTime"},
    {"key": "status", "attribute": "status"}
],
"Organization": [
    {"key": "name", "attribute": "name"},
    {"key": "active", "attribute": "active"},
    {"key": "type", "attribute": "type[0].coding[0].code"},
    {"key": "address", "attribute": "address[0].text"}
],
"Patient": [
    {"key": "identifier", "attribute": "identifier[0].value"},
    {"key": "name", "attribute": "name[0].family"},
    {"key": "family", "attribute": "name[0].family"},
    {"key": "given", "attribute": "name[0].given[0]"},
    {"key": "gender", "attribute": "gender"},
    {"key": "birthdate", "attribute": "birthDate"},
    {"key": "address", "attribute": "address[0].line[0]"},
    {"key": "address-city", "attribute": "address[0].city"},
    {"key": "address-state", "attribute": "address[0].state"},
    {"key": "address-postalcode", "attribute": "address[0].postalCode"},
    {"key": "phone", "attribute": "telecom[0].value"},
    {"key": "email", "attribute": "telecom[1].value"},
    {"key": "deceased", "attribute": "deceasedBoolean"},
    {"key": "language", "attribute": "communication[0].language.coding[0].code"}
],
"Practitioner": [
    {"key": "name", "attribute": "name[0].family"},
    {"key": "identifier", "attribute": "identifier[0].value"},
    {"key": "address", "attribute": "address[0].line[0]"},
    {"key": "gender", "attribute": "gender"}
],
"PractitionerRole": [
    {"key": "practitioner", "attribute": "practitioner.reference"},
    {"key": "organization", "attribute": "organization.reference"},
    {"key": "role", "attribute": "code[0].coding[0].code"},
    {"key": "service", "attribute": "healthcareService[0].reference"}
],
"Procedure": [
    {"key": "patient", "attribute": "subject.reference"},
    {"key": "date", "attribute": "performedDateTime"},
    {"key": "status", "attribute": "status"},
    {"key": "code", "attribute": "code.coding[0].code"}
]
}

# Add embeddings configuration according to your needs. In this current conf only DiagnosticReport generates them)
embeddings_config = {
    "DiagnosticReport": {
        "path": "presentedForm[].data",
        "encodedBase64": True,
        "model": "text-embedding-3-small"
    },
}

def extract_data_for_embedding(resource, config):
    #Extracts and decodes data from a resource based on the provided config
    data = resource
    for part in config["path"].split('.'):
        if '[]' in part:
            part = part.replace('[]', '')
            data = data.get(part, [])
            if not isinstance(data, list):
                return None  # Return None if the expected list structure is not found
            # If the structure is a list of dictionaries with 'data' fields, proceed to process each element
            decoded_texts = []
            for item in data:
                if config.get("encodedBase64", False):
                    # Check if item is a dictionary and has a 'data' field
                    if isinstance(item, dict) and 'data' in item:
                        try:
                            # Decode the base64 text within the 'data' field individually
                            decoded_text = base64.b64decode(item['data']).decode('utf-8')
                            decoded_texts.append(decoded_text)
                        except TypeError as e:
                            print(f"Error decoding base64: {e}")
                            return None
                    else:
                        print(f"Expected a dictionary with a 'data' field but got: {type(item)}")
                        return None
                else:
                    # If not base64 encoded, just append the item (or handle differently if needed)
                    decoded_texts.append(item)
            # Join all decoded texts into one string
            data = " ".join(decoded_texts)
            break  # Assuming only one list in the path for simplicity
        else:
            data = data.get(part, None)

    return data

def get_embedding(resourceType, resource, model="text-embedding-3-small"):
    config = embeddings_config.get(resourceType)
    if not config:
        return None  
    
    text = extract_data_for_embedding(resource, config)
    if not text:
        return None 
    
    try:
        text = text.replace("\n", " ")
        # Ensure the input is passed as a list
        response = clientOpenAI.embeddings.create(input=[text], model="text-embedding-3-small")
        embedding = response['choices'][0]['embedding'] if isinstance(response, dict) else response.data[0].embedding
    except Exception as e:
        print(f"Failed to get embedding: {e}")
        embedding = None

    return embedding

def extract_search_parameter_values(resource):
    resource_type = resource.get("resourceType")
    params_config = search_parameters_config.get(resource_type, [])

    extracted_values = []
    for param in params_config:
        attribute_path = param["attribute"].split('.')
        value = resource
        for part in attribute_path:
            # Check if part is attempting to index a list
            if '[' in part and ']' in part:
                part, index = part.rstrip(']').split('[')
                index = int(index)
                # Ensure value is a dictionary and part exists before accessing it
                if isinstance(value, dict) and part in value:
                    value = value.get(part)
                    # Check if value is a list and has the index
                    if isinstance(value, list) and len(value) > index:
                        value = value[index]
                    else:
                        value = None
                else:
                    value = None
            else:
                # Ensure value is a dictionary before attempting to get part
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
            
            # Break the loop if value is None at any part
            if value is None:
                break

        if value is not None:
            extracted_values.append({"key": param["key"], "value": value})

    return extracted_values



def extract_data(resource, config):
    path_elements = config["path"].split('.')
    data = resource
    for element in path_elements:
        if '[]' in element:
            element = element.replace('[]', '')
            data = data.get(element, [])
            # Assuming the data to be directly used without further processing
            break  # Assuming only one list in the path for simplicity
        else:
            data = data.get(element, None)
    return data

def load_and_reverse_uuid_mapping(mapping_file_path):
    with open(mapping_file_path, 'r') as file:
        uuid_to_url_mapping = json.load(file)
    # Reverse the mapping to map from URL (e.g., "CareTeam/12") to UUID
    url_to_uuid_mapping = {v: k for k, v in uuid_to_url_mapping.items()}
    return url_to_uuid_mapping


def enrich_and_save_resources(input_folder_path, enriched_folder_path, uuid_mapping_file, embeddings_total=25):
    url_to_uuid_mapping = load_and_reverse_uuid_mapping(uuid_mapping_file)
    embeddings_counter = {}  # A dictionary to keep count of embeddings per resource type

    # Clear existing enriched files
    for f in glob.glob(enriched_folder_path + '/enriched_*.ndjson'):
        try:
            os.remove(f)
            print(f'Removed {f}')
        except OSError as e:
            print(f"Error deleting file {f}: {e.strerror}")

    for filename in os.listdir(input_folder_path):
        file_path = os.path.join(input_folder_path, filename)
        if os.path.isfile(file_path) and file_path.endswith('.ndjson'):
            enriched_resources = []

            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    resource = json.loads(line)
                    resource_type = resource.get("resourceType")

                    # Initialize counter for this resource type if it doesn't exist
                    if resource_type not in embeddings_counter:
                        embeddings_counter[resource_type] = 0

                    search_parameters = extract_search_parameter_values(resource)
                    uuid = url_to_uuid_mapping.get(resource.get("id"), "Unknown UUID")
                    
                    enriched_resource = {
                        "metadata": {
                            "documentVersion": "1.0",
                            "fhirVersion": "4.0.1",
                            "lastUpdate": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "tenant_id": "TenantA",
                            "uuid": uuid,
                            "searchParameters": search_parameters,
                        },
                        "resource": resource
                    }

                    # Add embeddings only if the counter for this type is less than the limit
                    if resource_type in embeddings_config and embeddings_counter[resource_type] < embeddings_total:
                        enriched_resource["metadata"]["vectorSearchEmbeddings"] = {
                            "model": embeddings_config[resource_type]["model"],
                            "vector": get_embedding(resource_type, resource)
                        }
                        embeddings_counter[resource_type] += 1

                    enriched_resources.append(enriched_resource)

            # Write enriched resources to a new file
            if enriched_resources:
                enriched_file_path = os.path.join(enriched_folder_path, f'{filename}')
                with open(enriched_file_path, 'w', encoding='utf-8') as file:
                    for enriched_resource in enriched_resources:
                        print(json.dumps(enriched_resource), file=file)
                print(f'Enriched resources saved in {enriched_file_path}')


enrich_and_save_resources(input_folder_path, enriched_folder_path, uuid_mapping_file, embeddings_total=25)



