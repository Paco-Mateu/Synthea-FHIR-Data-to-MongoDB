import argparse
import os

from dotenv import load_dotenv

from synthea_mongo.canonical_pipeline import build_canonical_dataset


def parse_csvish_set(value):
    return {item.strip() for item in str(value or "").split(",") if item.strip()}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build a canonical polymorphic FHIR NDJSON dataset from Synthea patient bundle JSON files."
    )
    parser.add_argument(
        "--input-dir",
        default=os.path.join("Dataset", "source_fhir_r4_sep2019", "fhir"),
        help="Directory containing patient-level FHIR Bundle JSON files.",
    )
    parser.add_argument(
        "--output-file",
        default=os.path.join("Dataset", "build", "fhir_resources_runtime.ndjson"),
        help="Output NDJSON path for canonical FHIR resources.",
    )
    parser.add_argument(
        "--dataset-label",
        default="synthea_fhir_r4_sep2019_runtime",
        help="Dataset label stored on every resource document.",
    )
    parser.add_argument(
        "--include-raw-resource",
        action="store_true",
        help="Include rawResource in the canonical output document for archive-grade exports.",
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional .env file to load before resolving OpenAI settings.",
    )
    parser.add_argument(
        "--enable-embeddings",
        action="store_true",
        help="Generate embeddings for narrative-heavy resources.",
    )
    parser.add_argument(
        "--embedding-model",
        default="text-embedding-3-small",
        help="Embedding model when --enable-embeddings is used.",
    )
    parser.add_argument(
        "--embedding-limit-per-type",
        type=int,
        default=25,
        help="Maximum number of embeddings to generate per resource type.",
    )
    parser.add_argument(
        "--embedding-resource-types",
        default="DiagnosticReport,DocumentReference",
        help="Comma-separated resource types eligible for embeddings when --enable-embeddings is used.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    load_dotenv()
    if args.env_file:
        load_dotenv(args.env_file, override=False)
    manifest = build_canonical_dataset(
        input_dir=args.input_dir,
        output_ndjson_path=args.output_file,
        dataset_label=args.dataset_label,
        include_raw_resource=bool(args.include_raw_resource),
        enable_embeddings=args.enable_embeddings,
        embedding_model=args.embedding_model,
        embedding_limit_per_type=max(0, int(args.embedding_limit_per_type)),
        vector_resource_types=parse_csvish_set(args.embedding_resource_types),
    )
    print(manifest)


if __name__ == "__main__":
    main()
