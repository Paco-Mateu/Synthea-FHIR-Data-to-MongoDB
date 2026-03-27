import json
import os
import tempfile
import unittest

from synthea_mongo.canonical_pipeline import (
    build_bundle_reference_map,
    build_canonical_dataset,
    build_canonical_document,
    build_compartments,
    build_search_index,
    resolve_bundle_references,
)


class CanonicalPipelineTests(unittest.TestCase):
    def test_bundle_reference_map_preserves_original_logical_ids(self):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {"resourceType": "Patient", "id": "patient-1"},
                },
                {
                    "fullUrl": "urn:uuid:obs-1",
                    "resource": {"resourceType": "Observation", "id": "obs-1"},
                },
            ],
        }

        mapping = build_bundle_reference_map(bundle)

        self.assertEqual(mapping["urn:uuid:patient-1"], "Patient/patient-1")
        self.assertEqual(mapping["urn:uuid:obs-1"], "Observation/obs-1")

    def test_resolve_bundle_references_normalizes_urn_uuid_without_mutating_ids(self):
        resource = {
            "resourceType": "Observation",
            "id": "obs-1",
            "subject": {"reference": "urn:uuid:patient-1"},
        }

        normalized = resolve_bundle_references(resource, {"urn:uuid:patient-1": "Patient/patient-1"})

        self.assertEqual(resource["subject"]["reference"], "urn:uuid:patient-1")
        self.assertEqual(normalized["subject"]["reference"], "Patient/patient-1")
        self.assertEqual(normalized["id"], "obs-1")

    def test_search_index_keeps_multiple_codings_and_references(self):
        resource = {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "subject": {"reference": "Patient/p-1"},
            "performer": [{"reference": "Practitioner/doc-1"}, {"reference": "Practitioner/doc-2"}],
            "code": {
                "text": "Tumor marker",
                "coding": [
                    {"system": "http://loinc.org", "code": "33732-9", "display": "Cancer Ag 15-3"},
                    {"system": "http://snomed.info/sct", "code": "12345", "display": "Marker"},
                ],
            },
        }

        search_index = build_search_index(resource)

        self.assertEqual(len(search_index["reference"]), 3)
        self.assertEqual(len(search_index["token"]), 3)
        self.assertEqual(search_index["reference"][0]["value"], "Patient/p-1")
        self.assertEqual(search_index["reference"][0]["targetId"], "p-1")

    def test_search_index_normalizes_string_values_for_case_insensitive_search(self):
        resource = {
            "resourceType": "DiagnosticReport",
            "id": "report-1",
            "conclusion": "HER2 Positive",
        }

        search_index = build_search_index(resource)

        self.assertEqual(search_index["string"][0]["value"], "HER2 Positive")
        self.assertEqual(search_index["string"][0]["valueNormalized"], "her2 positive")

    def test_compartments_include_self_for_patient_and_referenced_practitioner(self):
        compartments = build_compartments(
            "Patient/p-1",
            "Patient",
            [
                {"value": "Practitioner/doc-1", "targetType": "Practitioner"},
                {"value": "Encounter/e-1", "targetType": "Encounter"},
            ],
        )

        self.assertIn("Patient/p-1", compartments["patientRefs"])
        self.assertIn("Practitioner/doc-1", compartments["practitionerRefs"])
        self.assertIn("Encounter/e-1", compartments["encounterRefs"])

    def test_build_canonical_document_preserves_resource_and_adds_search_index(self):
        bundle = {
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {"resourceType": "Patient", "id": "patient-1"},
                },
                {
                    "fullUrl": "urn:uuid:obs-1",
                    "resource": {
                        "resourceType": "Observation",
                        "id": "obs-1",
                        "status": "final",
                        "subject": {"reference": "urn:uuid:patient-1"},
                        "code": {"text": "Tumor marker"},
                    },
                    "request": {"method": "POST", "url": "Observation"},
                },
            ],
        }
        reference_map = build_bundle_reference_map(bundle)

        document = build_canonical_document(
            entry=bundle["entry"][1],
            bundle_doc=bundle,
            reference_map=reference_map,
            dataset_label="synthea_test",
            source_file="/tmp/sample.json",
            ingested_at="2026-03-27T00:00:00Z",
            include_raw_resource=True,
            enable_embeddings=False,
            embedding_model="text-embedding-3-small",
            embedding_limit_per_type=0,
            embedding_counts={},
        )

        self.assertEqual(document["logicalRef"], "Observation/obs-1")
        self.assertEqual(document["resource"]["subject"]["reference"], "Patient/patient-1")
        self.assertTrue(document["searchIndex"]["reference"])
        self.assertTrue(document["searchIndex"]["token"])

    def test_build_canonical_dataset_can_emit_runtime_docs_without_raw_resource(self):
        bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {"resourceType": "Patient", "id": "patient-1", "gender": "female"},
                },
                {
                    "fullUrl": "urn:uuid:obs-1",
                    "resource": {
                        "resourceType": "Observation",
                        "id": "obs-1",
                        "status": "final",
                        "subject": {"reference": "urn:uuid:patient-1"},
                        "code": {"text": "Mammogram"},
                    },
                },
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "bundle.json")
            output_path = os.path.join(temp_dir, "runtime.ndjson")
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(bundle, handle)

            manifest = build_canonical_dataset(
                input_dir=temp_dir,
                output_ndjson_path=output_path,
                dataset_label="synthea_runtime_test",
                include_raw_resource=False,
            )

            with open(output_path, "r", encoding="utf-8") as handle:
                documents = [json.loads(line) for line in handle if line.strip()]

            self.assertEqual(manifest["documentCount"], 2)
            self.assertFalse(manifest["includeRawResource"])
            self.assertTrue(all("rawResource" not in document for document in documents))

    def test_build_canonical_dataset_deduplicates_shared_logical_refs_across_bundles(self):
        patient_one_bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [
                {
                    "fullUrl": "urn:uuid:org-1",
                    "resource": {"resourceType": "Organization", "id": "org-1", "name": "Shared Org"},
                }
            ],
        }
        patient_two_bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [
                {
                    "fullUrl": "urn:uuid:org-1",
                    "resource": {"resourceType": "Organization", "id": "org-1", "name": "Shared Org"},
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            with open(os.path.join(temp_dir, "bundle-1.json"), "w", encoding="utf-8") as handle:
                json.dump(patient_one_bundle, handle)
            with open(os.path.join(temp_dir, "bundle-2.json"), "w", encoding="utf-8") as handle:
                json.dump(patient_two_bundle, handle)

            output_path = os.path.join(temp_dir, "runtime.ndjson")
            manifest = build_canonical_dataset(
                input_dir=temp_dir,
                output_ndjson_path=output_path,
                dataset_label="synthea_runtime_test",
                include_raw_resource=False,
            )

            with open(output_path, "r", encoding="utf-8") as handle:
                documents = [json.loads(line) for line in handle if line.strip()]

            self.assertEqual(len(documents), 1)
            self.assertEqual(manifest["documentCount"], 1)
            self.assertEqual(manifest["duplicateLogicalRefsSkipped"], 1)
            self.assertEqual(
                manifest["embeddingResourceTypes"],
                ["DiagnosticReport", "DocumentReference"],
            )

    def test_build_canonical_dataset_allows_custom_embedding_resource_types(self):
        bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {"resourceType": "Patient", "id": "patient-1"},
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "bundle.json")
            output_path = os.path.join(temp_dir, "runtime.ndjson")
            with open(input_path, "w", encoding="utf-8") as handle:
                json.dump(bundle, handle)

            manifest = build_canonical_dataset(
                input_dir=temp_dir,
                output_ndjson_path=output_path,
                dataset_label="synthea_runtime_test",
                include_raw_resource=False,
                vector_resource_types={"Patient"},
            )

            self.assertEqual(manifest["embeddingResourceTypes"], ["Patient"])


if __name__ == "__main__":
    unittest.main()
