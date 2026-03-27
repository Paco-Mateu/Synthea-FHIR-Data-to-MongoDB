# Synthea FHIR Data to MongoDB

This project shows a production-shaped way to store Synthea FHIR data in MongoDB without losing the original FHIR resources.

The target model is:

`FHIR bundle JSON -> canonical polymorphic NDJSON -> MongoDB single collection -> downstream search and analytics`

## Goal

Use one canonical MongoDB collection, `fhir_resources`, to store all FHIR resource types while preserving:

- the full normalized FHIR `resource`
- optional `rawResource` for archive-grade exports
- cardinality-safe `searchIndex` arrays for FHIR-search-oriented querying
- derived `compartments` metadata for fast patient and encounter scoped access
- optional OpenAI embeddings only for configured narrative-heavy resource types

This repository is a best-practice storage and indexing pattern for FHIR on MongoDB. It is not a FHIR API server.

## Design Rules

- One collection for all FHIR resources.
- Keep `resourceType` as a first-class top-level field.
- Preserve the logical FHIR `id`; do not rewrite resource ids.
- Resolve bundle-local references into normalized `resource.reference` values without mutating the raw payload.
- Model search projections as arrays so cardinality-N fields remain queryable.
- Put FHIR-scope style metadata in `compartments` to avoid repeated cross-resource lookups.
- Keep semantic helpers such as narrative summaries and embeddings outside the raw-truth contract.

## 4-Step Workflow

### 1. Install and configure

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

`.env` supports:

- `MONGODB_CONNECTION_STRING`
- `DATABASE`
- `COLLECTION`
- `OPENAI_API_KEY`

Default Mongo targets:

- database: `hc_fhir_synthea_r4_sep2019_canonical`
- collection: `fhir_resources`

### 2. Download the official public Synthea FHIR archive

```bash
python download_fhir_sample_data.py
```

This downloads the public Synthea FHIR R4 ZIP into `Dataset/downloads/` and extracts it into `Dataset/source_fhir_r4_sep2019/`.

If you already have another Synthea FHIR bundle directory, skip this step and point the builder at that directory with `--input-dir`.

### 3. Build the canonical polymorphic dataset

The default build is the runtime export used by downstream applications:

```bash
python build_canonical_fhir_dataset.py
```

Default outputs:

- input directory: `Dataset/source_fhir_r4_sep2019/fhir`
- output file: `Dataset/build/fhir_resources_runtime.ndjson`
- manifest: `Dataset/build/fhir_resources_runtime.manifest.json`

To create an archive-grade export that also keeps `rawResource`:

```bash
python build_canonical_fhir_dataset.py \
  --output-file Dataset/build/fhir_resources_archive.ndjson \
  --dataset-label synthea_fhir_r4_sep2019_archive \
  --include-raw-resource
```

To enable OpenAI embeddings for configured resource types:

```bash
python build_canonical_fhir_dataset.py \
  --env-file .env \
  --enable-embeddings \
  --embedding-model text-embedding-3-small \
  --embedding-resource-types DiagnosticReport,DocumentReference \
  --embedding-limit-per-type 25
```

### 4. Upload into the single canonical MongoDB collection

```bash
python upload_canonical_fhir_dataset.py --env-file .env
```

You can also point directly at another environment file:

```bash
python upload_canonical_fhir_dataset.py \
  --env-file /path/to/project/.env \
  --database hc_fhir_synthea_r4_sep2019_canonical \
  --collection fhir_resources
```

The uploader uses:

- fast `insert_many` on the first load
- idempotent bulk upserts on later loads
- canonical identity and search-oriented indexes

## Canonical Document Model

Each MongoDB document represents one FHIR resource and includes:

- `datasetLabel`
- `pipelineVersion`
- `ingestedAt`
- `resourceType`
- `resourceId`
- `logicalRef`
- `resource`
- optional `rawResource`
- `searchIndex`
- `compartments`
- `source`
- `hashes`
- optional `narrative.summaryText`
- optional `narrative.embedding`

Example shape:

```json
{
  "datasetLabel": "synthea_fhir_r4_sep2019_runtime",
  "pipelineVersion": "2.0.0",
  "resourceType": "Observation",
  "resourceId": "obs-1",
  "logicalRef": "Observation/obs-1",
  "resource": {
    "resourceType": "Observation",
    "id": "obs-1",
    "subject": { "reference": "Patient/patient-1" }
  },
  "searchIndex": {
    "reference": [
      {
        "path": "subject.reference",
        "value": "Patient/patient-1",
        "targetType": "Patient",
        "targetId": "patient-1"
      }
    ],
    "token": [
      {
        "path": "status",
        "code": "final"
      }
    ]
  },
  "compartments": {
    "patientRefs": ["Patient/patient-1"]
  }
}
```

## Search-Friendly Shape

The canonical documents are intentionally shaped so a future FHIR search compiler can query Mongo efficiently against a known dataset.

Current `searchIndex` buckets:

- `searchIndex.reference`
- `searchIndex.token`
- `searchIndex.string`
- `searchIndex.date`
- `searchIndex.quantity`
- `searchIndex.number`

Important details:

- repeated codings, identifiers, references, and other cardinality-N values stay queryable
- string values are stored with `valueNormalized` for case-insensitive matching
- reference items carry both `targetType` and `targetId`
- patient and encounter scope can be resolved through `compartments` instead of repeated joins

## Indexing Strategy

The uploader creates indexes aligned with the canonical search shape, including:

- unique identity: `datasetLabel + logicalRef`
- `datasetLabel + resourceType + resourceId`
- `resourceType`
- `logicalRef`
- `resourceType + compartments.patientRefs`
- `resourceType + compartments.encounterRefs`
- `resourceType + searchIndex.reference.value`
- `resourceType + searchIndex.reference.targetType + searchIndex.reference.targetId`
- `resourceType + searchIndex.token.system + searchIndex.token.code`
- `resourceType + searchIndex.token.code`
- `resourceType + searchIndex.string.valueNormalized`
- `resourceType + searchIndex.date.value`
- `resourceType + searchIndex.quantity.code + searchIndex.quantity.value`
- `resourceType + searchIndex.number.value`

This is the recommended pattern in this repo: preserve the full resource for exchange, and add a stable query-optimized companion shape for search and analytics.

## What This Demo Delivers

- a single polymorphic MongoDB collection for all FHIR resource types
- preserved FHIR resources ready for exchange or reprocessing
- cardinality-safe search projections
- derived compartment metadata for patient-scoped retrieval
- optional narrative summaries and OpenAI embeddings for configured resource types
- a strong foundation for downstream clinical applications such as longitudinal views, facts materialization, and trial matching

## What This Demo Does Not Deliver

- a FHIR REST API
- full FHIR Search semantics or all search parameters
- disease-specific clinical normalization
- oncology eligibility logic

Those are downstream layers. This repository’s responsibility is the canonical storage and indexing contract.
