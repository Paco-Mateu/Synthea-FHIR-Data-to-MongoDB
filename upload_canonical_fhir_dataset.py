import argparse
import json
import os

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload canonical FHIR NDJSON documents into a single polymorphic MongoDB collection."
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional .env file to load before resolving MongoDB settings.",
    )
    parser.add_argument(
        "--input-file",
        default=os.path.join("Dataset", "build", "fhir_resources_runtime.ndjson"),
        help="Canonical NDJSON file produced by build_canonical_fhir_dataset.py.",
    )
    parser.add_argument(
        "--mongo-uri",
        default="",
        help="MongoDB connection string. Defaults to MONGODB_CONNECTION_STRING after env loading.",
    )
    parser.add_argument(
        "--database",
        default="",
        help="Target MongoDB database name. Defaults to DATABASE or hc_fhir_synthea_r4_sep2019_canonical after env loading.",
    )
    parser.add_argument(
        "--collection",
        default="",
        help="Target MongoDB collection name. Defaults to COLLECTION or fhir_resources after env loading.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Bulk upsert batch size.",
    )
    parser.add_argument(
        "--drop-collection",
        action="store_true",
        help="Drop the target collection before uploading.",
    )
    return parser.parse_args()


def ensure_identity_index(collection):
    collection.create_index(
        [("datasetLabel", 1), ("logicalRef", 1)],
        unique=True,
        name="uq_dataset_logical_ref",
    )


def ensure_secondary_indexes(collection):
    collection.create_index(
        [("datasetLabel", 1), ("resourceType", 1), ("resourceId", 1)],
        name="dataset_type_id",
    )
    collection.create_index([("resourceType", 1)], name="resource_type")
    collection.create_index([("logicalRef", 1)], name="logical_ref")
    collection.create_index(
        [("resourceType", 1), ("compartments.patientRefs", 1)],
        name="type_patient_compartment",
    )
    collection.create_index(
        [("resourceType", 1), ("compartments.encounterRefs", 1)],
        name="type_encounter_compartment",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.reference.value", 1)],
        name="type_reference_value",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.reference.targetType", 1), ("searchIndex.reference.targetId", 1)],
        name="type_reference_target",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.token.system", 1), ("searchIndex.token.code", 1)],
        name="type_token_system_code",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.token.code", 1)],
        name="type_token_code",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.string.valueNormalized", 1)],
        name="type_string_value_normalized",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.date.value", 1)],
        name="type_date_value",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.quantity.code", 1), ("searchIndex.quantity.value", 1)],
        name="type_quantity_code_value",
    )
    collection.create_index(
        [("resourceType", 1), ("searchIndex.number.value", 1)],
        name="type_number_value",
    )
    collection.create_index(
        [("hashes.normalizedResourceSha256", 1)],
        name="normalized_resource_sha256",
    )


def flush_upsert_batch(collection, batch):
    if not batch:
        return {"inserted": 0, "matched": 0, "modified": 0, "upserted": 0}
    result = collection.bulk_write(batch, ordered=False)
    return {
        "inserted": 0,
        "matched": int(result.matched_count),
        "modified": int(result.modified_count),
        "upserted": int(result.upserted_count),
    }


def flush_insert_batch(collection, batch):
    if not batch:
        return {"inserted": 0, "matched": 0, "modified": 0, "upserted": 0}
    result = collection.insert_many(batch, ordered=False)
    return {
        "inserted": len(result.inserted_ids),
        "matched": 0,
        "modified": 0,
        "upserted": 0,
    }


def main():
    load_dotenv()
    args = parse_args()
    if args.env_file:
        load_dotenv(args.env_file, override=False)

    mongo_uri = args.mongo_uri or os.getenv("MONGODB_CONNECTION_STRING", "")
    database_name = args.database or os.getenv("DATABASE", "hc_fhir_synthea_r4_sep2019_canonical")
    collection_name = args.collection or os.getenv("COLLECTION", "fhir_resources")

    if not mongo_uri:
        raise RuntimeError("Missing MongoDB connection string.")
    if not os.path.exists(args.input_file):
        raise FileNotFoundError(f"Input NDJSON file does not exist: {args.input_file}")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=30000)
    collection = client[database_name][collection_name]
    if args.drop_collection:
        collection.drop()
    collection_was_empty = collection.estimated_document_count() == 0
    if not collection_was_empty:
        ensure_identity_index(collection)

    batch = []
    insert_mode = collection_was_empty
    totals = {"inserted": 0, "matched": 0, "modified": 0, "upserted": 0, "linesRead": 0}
    for line_number, raw_line in enumerate(open(args.input_file, "r", encoding="utf-8"), start=1):
        line = raw_line.strip()
        if not line:
            continue
        document = json.loads(line)
        totals["linesRead"] += 1
        if insert_mode:
            batch.append(document)
        else:
            batch.append(
                UpdateOne(
                    {
                        "datasetLabel": document.get("datasetLabel"),
                        "logicalRef": document.get("logicalRef"),
                    },
                    {"$set": document},
                    upsert=True,
                )
            )
        if len(batch) >= max(1, int(args.batch_size)):
            batch_summary = (
                flush_insert_batch(collection, batch)
                if insert_mode
                else flush_upsert_batch(collection, batch)
            )
            for key, value in batch_summary.items():
                totals[key] += value
            batch = []
            print({"processedThroughLine": line_number, **batch_summary})

    batch_summary = (
        flush_insert_batch(collection, batch)
        if insert_mode
        else flush_upsert_batch(collection, batch)
    )
    for key, value in batch_summary.items():
        totals[key] += value
    ensure_identity_index(collection)
    ensure_secondary_indexes(collection)
    print(
        {
            "database": database_name,
            "collection": collection_name,
            **totals,
            "collectionCount": collection.count_documents({}),
        }
    )
    client.close()


if __name__ == "__main__":
    main()
