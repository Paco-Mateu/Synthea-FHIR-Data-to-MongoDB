from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any


PIPELINE_VERSION = "2.0.0"
DEFAULT_VECTOR_RESOURCE_TYPES = {"DiagnosticReport", "DocumentReference"}
SIMPLE_TOKEN_FIELDS = {
    "active",
    "deceasedBoolean",
    "gender",
    "intent",
    "multipleBirthBoolean",
    "status",
    "use",
}
SIMPLE_DATE_FIELDS = {
    "abatementDateTime",
    "authoredOn",
    "birthDate",
    "date",
    "deceasedDateTime",
    "effectiveDateTime",
    "issued",
    "occurrenceDateTime",
    "onsetDateTime",
    "performedDateTime",
    "recordedDate",
    "started",
    "valueDateTime",
}
SIMPLE_STRING_FIELDS = {
    "city",
    "conclusion",
    "country",
    "description",
    "display",
    "div",
    "family",
    "given",
    "line",
    "name",
    "postalCode",
    "state",
    "text",
    "title",
    "valueString",
}
QUANTITY_FIELD_NAMES = {"valueQuantity"}
NUMERIC_FIELD_NAMES = {"valueDecimal", "valueInteger", "valuePositiveInt", "valueUnsignedInt"}
COMPARTMENT_TARGET_TO_FIELD = {
    "Device": "deviceRefs",
    "Encounter": "encounterRefs",
    "Organization": "organizationRefs",
    "Patient": "patientRefs",
    "Practitioner": "practitionerRefs",
    "RelatedPerson": "relatedPersonRefs",
}
NARRATIVE_TEXT_PATHS = [
    "text.div",
    "code.text",
    "type.text",
    "category[].text",
    "note[].text",
    "conclusion",
    "description",
    "title",
    "valueString",
]
EMBEDDING_TEXT_PATHS = ["presentedForm[]"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_space(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def strip_html(value: Any) -> str:
    text_value = normalize_space(value)
    if not text_value:
        return ""
    text_value = re.sub(r"<[^>]+>", " ", text_value)
    return normalize_space(unescape(text_value))


def stable_json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def dedupe_list(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped = []
    seen = set()
    for item in items:
        key = json.dumps(item, sort_keys=True, ensure_ascii=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def iter_bundle_files(input_dir: str | os.PathLike[str]) -> list[Path]:
    root = Path(input_dir)
    if not root.exists():
        raise FileNotFoundError(f"Input directory does not exist: {root}")
    return sorted([path for path in root.rglob("*.json") if path.is_file()])


def load_json_file(path: str | os.PathLike[str]) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_bundle_reference_map(bundle_doc: dict[str, Any]) -> dict[str, str]:
    reference_map: dict[str, str] = {}
    for entry in bundle_doc.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        resource = entry.get("resource") or {}
        resource_type = normalize_space(resource.get("resourceType"))
        resource_id = normalize_space(resource.get("id"))
        full_url = normalize_space(entry.get("fullUrl"))
        if not (resource_type and resource_id and full_url):
            continue
        reference_map[full_url] = f"{resource_type}/{resource_id}"
    return reference_map


def resolve_bundle_references(value: Any, reference_map: dict[str, str]) -> Any:
    if isinstance(value, dict):
        resolved = {}
        for key, child in value.items():
            if key == "reference" and isinstance(child, str) and child in reference_map:
                resolved[key] = reference_map[child]
            else:
                resolved[key] = resolve_bundle_references(child, reference_map)
        return resolved
    if isinstance(value, list):
        return [resolve_bundle_references(item, reference_map) for item in value]
    return copy.deepcopy(value)


def extract_path_values(node: Any, path_spec: str) -> list[Any]:
    values = [node]
    for raw_segment in path_spec.split("."):
        next_values = []
        is_many = raw_segment.endswith("[]")
        segment = raw_segment[:-2] if is_many else raw_segment
        for current in values:
            if isinstance(current, list):
                candidates = current
            else:
                candidates = [current]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                child = candidate.get(segment)
                if child is None:
                    continue
                if is_many and isinstance(child, list):
                    next_values.extend(child)
                elif is_many:
                    next_values.append(child)
                else:
                    next_values.append(child)
        values = next_values
    return values


def _append_reference_item(items: list[dict[str, Any]], current_path: tuple[str, ...], reference_value: str) -> None:
    cleaned = normalize_space(reference_value)
    if not cleaned:
        return
    target_type = cleaned.split("/", 1)[0] if "/" in cleaned else None
    target_id = cleaned.split("/", 1)[1] if "/" in cleaned else None
    items.append(
        {
            "name": current_path[-1] if current_path else "reference",
            "path": ".".join(current_path + ("reference",)),
            "value": cleaned,
            "targetType": target_type,
            "targetId": target_id,
        }
    )


def _append_token_item(items: list[dict[str, Any]], current_path: tuple[str, ...], value: Any, *, system: str | None = None, display: str | None = None) -> None:
    code_value = normalize_space(value)
    if not code_value:
        return
    items.append(
        {
            "name": current_path[-1] if current_path else "token",
            "path": ".".join(current_path),
            "code": code_value,
            "system": normalize_space(system) or None,
            "display": normalize_space(display) or None,
        }
    )


def _append_string_item(items: list[dict[str, Any]], current_path: tuple[str, ...], value: Any) -> None:
    cleaned = strip_html(value)
    if not cleaned:
        return
    items.append(
        {
            "name": current_path[-1] if current_path else "string",
            "path": ".".join(current_path),
            "value": cleaned,
            "valueNormalized": cleaned.casefold(),
        }
    )


def _append_date_item(items: list[dict[str, Any]], current_path: tuple[str, ...], value: Any) -> None:
    cleaned = normalize_space(value)
    if not cleaned:
        return
    items.append(
        {
            "name": current_path[-1] if current_path else "date",
            "path": ".".join(current_path),
            "value": cleaned,
        }
    )


def _append_quantity_item(items: list[dict[str, Any]], current_path: tuple[str, ...], quantity_value: dict[str, Any]) -> None:
    if not isinstance(quantity_value, dict) or quantity_value.get("value") is None:
        return
    items.append(
        {
            "name": current_path[-1] if current_path else "quantity",
            "path": ".".join(current_path),
            "value": quantity_value.get("value"),
            "unit": normalize_space(quantity_value.get("unit")) or None,
            "system": normalize_space(quantity_value.get("system")) or None,
            "code": normalize_space(quantity_value.get("code")) or None,
        }
    )


def _append_number_item(items: list[dict[str, Any]], current_path: tuple[str, ...], value: Any) -> None:
    if value is None:
        return
    items.append(
        {
            "name": current_path[-1] if current_path else "number",
            "path": ".".join(current_path),
            "value": value,
        }
    )


def _collect_search_items(node: Any, current_path: tuple[str, ...], buckets: dict[str, list[dict[str, Any]]]) -> None:
    if isinstance(node, dict):
        for key, child in node.items():
            next_path = current_path + (key,)
            if key == "reference" and isinstance(child, str):
                _append_reference_item(buckets["reference"], current_path, child)
                continue
            if key == "coding" and isinstance(child, list):
                for coding in child:
                    if not isinstance(coding, dict):
                        continue
                    _append_token_item(
                        buckets["token"],
                        current_path,
                        coding.get("code"),
                        system=coding.get("system"),
                        display=coding.get("display"),
                    )
                continue
            if key in SIMPLE_TOKEN_FIELDS and isinstance(child, (str, bool, int, float)):
                _append_token_item(buckets["token"], next_path, child)
            if key in SIMPLE_DATE_FIELDS and isinstance(child, str):
                _append_date_item(buckets["date"], next_path, child)
            if key in {"start", "end"} and current_path and current_path[-1] in {"period", "performedPeriod"}:
                _append_date_item(buckets["date"], next_path, child)
            if key in SIMPLE_STRING_FIELDS and isinstance(child, str):
                _append_string_item(buckets["string"], next_path, child)
            if key in QUANTITY_FIELD_NAMES and isinstance(child, dict):
                _append_quantity_item(buckets["quantity"], next_path, child)
            if key in NUMERIC_FIELD_NAMES and isinstance(child, (int, float)):
                _append_number_item(buckets["number"], next_path, child)
            if isinstance(child, (dict, list)):
                _collect_search_items(child, next_path, buckets)
    elif isinstance(node, list):
        for item in node:
            _collect_search_items(item, current_path, buckets)


def build_search_index(resource: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {
        "date": [],
        "number": [],
        "quantity": [],
        "reference": [],
        "string": [],
        "token": [],
    }
    _collect_search_items(resource, tuple(), buckets)
    return {key: dedupe_list(value) for key, value in buckets.items() if value}


def build_compartments(logical_ref: str, resource_type: str, reference_items: list[dict[str, Any]]) -> dict[str, list[str]]:
    buckets: dict[str, set[str]] = defaultdict(set)
    if resource_type == "Patient":
        buckets["patientRefs"].add(logical_ref)
    if resource_type == "Encounter":
        buckets["encounterRefs"].add(logical_ref)
    for item in reference_items or []:
        target_type = normalize_space(item.get("targetType"))
        target_ref = normalize_space(item.get("value"))
        if not target_type or not target_ref:
            continue
        compartment_field = COMPARTMENT_TARGET_TO_FIELD.get(target_type)
        if compartment_field:
            buckets[compartment_field].add(target_ref)
    return {key: sorted(list(value)) for key, value in buckets.items() if value}


def decode_attachment_texts(resource: dict[str, Any]) -> list[str]:
    decoded = []
    for attachment in extract_path_values(resource, "presentedForm[]"):
        if not isinstance(attachment, dict):
            continue
        raw_data = attachment.get("data")
        if not raw_data:
            continue
        try:
            decoded_text = base64.b64decode(str(raw_data)).decode("utf-8", errors="ignore")
        except Exception:
            continue
        cleaned = normalize_space(decoded_text)
        if cleaned:
            decoded.append(cleaned)
    return decoded


def build_narrative_summary(resource: dict[str, Any], *, max_chars: int = 12000) -> str:
    snippets = []
    for path_spec in NARRATIVE_TEXT_PATHS:
        for value in extract_path_values(resource, path_spec):
            cleaned = strip_html(value)
            if cleaned:
                snippets.append(cleaned)
    snippets.extend(decode_attachment_texts(resource))
    deduped = []
    seen = set()
    for snippet in snippets:
        if not snippet or snippet in seen:
            continue
        seen.add(snippet)
        deduped.append(snippet)
    return normalize_space(" ".join(deduped))[:max_chars]


def maybe_build_embedding(
    *,
    resource_type: str,
    summary_text: str,
    enable_embeddings: bool,
    embedding_model: str,
    embedding_limit_per_type: int,
    embedding_counts: Counter,
    vector_resource_types: set[str],
) -> dict[str, Any] | None:
    if not enable_embeddings:
        return None
    if resource_type not in vector_resource_types:
        return None
    if not summary_text:
        return None
    if embedding_limit_per_type > 0 and embedding_counts[resource_type] >= embedding_limit_per_type:
        return None

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required when embeddings are enabled.")

    from openai import OpenAI  # local import so the pipeline can run without OpenAI when embeddings are disabled

    client = OpenAI(api_key=api_key)
    response = client.embeddings.create(model=embedding_model, input=[summary_text])
    embedding_counts[resource_type] += 1
    return {
        "model": embedding_model,
        "vector": response.data[0].embedding,
        "dimensions": len(response.data[0].embedding),
    }


def build_canonical_document(
    *,
    entry: dict[str, Any],
    bundle_doc: dict[str, Any],
    reference_map: dict[str, str],
    dataset_label: str,
    source_file: str,
    ingested_at: str,
    include_raw_resource: bool,
    enable_embeddings: bool,
    embedding_model: str,
    embedding_limit_per_type: int,
    embedding_counts: Counter,
    vector_resource_types: set[str] | None = None,
) -> dict[str, Any] | None:
    raw_resource = entry.get("resource")
    if not isinstance(raw_resource, dict):
        return None

    normalized_resource = resolve_bundle_references(raw_resource, reference_map)
    resource_type = normalize_space(normalized_resource.get("resourceType"))
    resource_id = normalize_space(normalized_resource.get("id"))
    if not resource_type:
        return None
    if not resource_id:
        resource_id = stable_json_hash(raw_resource)[:32]
        normalized_resource["id"] = resource_id

    logical_ref = f"{resource_type}/{resource_id}"
    search_index = build_search_index(normalized_resource)
    compartments = build_compartments(logical_ref, resource_type, search_index.get("reference") or [])
    narrative_summary = build_narrative_summary(normalized_resource)
    resolved_vector_resource_types = set(vector_resource_types or DEFAULT_VECTOR_RESOURCE_TYPES)
    embedding_payload = maybe_build_embedding(
        resource_type=resource_type,
        summary_text=narrative_summary,
        enable_embeddings=enable_embeddings,
        embedding_model=embedding_model,
        embedding_limit_per_type=embedding_limit_per_type,
        embedding_counts=embedding_counts,
        vector_resource_types=resolved_vector_resource_types,
    )

    document: dict[str, Any] = {
        "datasetLabel": dataset_label,
        "pipelineVersion": PIPELINE_VERSION,
        "ingestedAt": ingested_at,
        "resourceType": resource_type,
        "resourceId": resource_id,
        "logicalRef": logical_ref,
        "resource": normalized_resource,
        "searchIndex": search_index,
        "compartments": compartments,
        "source": {
            "bundleType": normalize_space(bundle_doc.get("type")) or None,
            "entryFullUrl": normalize_space(entry.get("fullUrl")) or None,
            "fileName": os.path.basename(source_file),
            "filePath": source_file,
            "requestMethod": normalize_space(((entry.get("request") or {}).get("method"))) or None,
            "requestUrl": normalize_space(((entry.get("request") or {}).get("url"))) or None,
        },
        "hashes": {
            "rawResourceSha256": stable_json_hash(raw_resource),
            "normalizedResourceSha256": stable_json_hash(normalized_resource),
        },
    }
    if include_raw_resource:
        document["rawResource"] = raw_resource
    if narrative_summary:
        document["narrative"] = {"summaryText": narrative_summary}
        if embedding_payload:
            document["narrative"]["embedding"] = embedding_payload
    return document


def build_canonical_dataset(
    *,
    input_dir: str | os.PathLike[str],
    output_ndjson_path: str | os.PathLike[str],
    dataset_label: str,
    include_raw_resource: bool = True,
    enable_embeddings: bool = False,
    embedding_model: str = "text-embedding-3-small",
    embedding_limit_per_type: int = 25,
    vector_resource_types: set[str] | None = None,
) -> dict[str, Any]:
    source_paths = iter_bundle_files(input_dir)
    output_path = Path(output_ndjson_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = output_path.with_suffix(".manifest.json")
    ingested_at = utc_now_iso()
    resource_type_counts: Counter = Counter()
    embedding_counts: Counter = Counter()
    source_file_count = 0
    unique_patient_refs = set()
    document_count = 0
    duplicate_logical_refs_skipped = 0
    conflicting_duplicate_logical_refs = 0
    logical_ref_hashes: dict[str, str] = {}

    resolved_vector_resource_types = set(vector_resource_types or DEFAULT_VECTOR_RESOURCE_TYPES)
    with open(output_path, "w", encoding="utf-8") as handle:
        for bundle_path in source_paths:
            bundle_doc = load_json_file(bundle_path)
            if normalize_space(bundle_doc.get("resourceType")) != "Bundle":
                continue
            source_file_count += 1
            reference_map = build_bundle_reference_map(bundle_doc)
            for entry in bundle_doc.get("entry") or []:
                if not isinstance(entry, dict):
                    continue
                document = build_canonical_document(
                    entry=entry,
                    bundle_doc=bundle_doc,
                    reference_map=reference_map,
                    dataset_label=dataset_label,
                    source_file=str(bundle_path),
                    ingested_at=ingested_at,
                    include_raw_resource=include_raw_resource,
                    enable_embeddings=enable_embeddings,
                    embedding_model=embedding_model,
                    embedding_limit_per_type=embedding_limit_per_type,
                    embedding_counts=embedding_counts,
                    vector_resource_types=resolved_vector_resource_types,
                )
                if not document:
                    continue
                logical_ref = str(document.get("logicalRef") or "").strip()
                normalized_hash = (
                    ((document.get("hashes") or {}).get("normalizedResourceSha256"))
                    or stable_json_hash(document.get("resource") or {})
                )
                if logical_ref:
                    existing_hash = logical_ref_hashes.get(logical_ref)
                    if existing_hash:
                        if existing_hash == normalized_hash:
                            duplicate_logical_refs_skipped += 1
                        else:
                            conflicting_duplicate_logical_refs += 1
                        continue
                    logical_ref_hashes[logical_ref] = normalized_hash
                handle.write(json.dumps(document, ensure_ascii=True))
                handle.write("\n")
                document_count += 1
                resource_type_counts[document["resourceType"]] += 1
                for patient_ref in (document.get("compartments") or {}).get("patientRefs") or []:
                    unique_patient_refs.add(patient_ref)

    manifest = {
        "datasetLabel": dataset_label,
        "pipelineVersion": PIPELINE_VERSION,
        "generatedAt": ingested_at,
        "inputDirectory": str(Path(input_dir).resolve()),
        "outputFile": str(output_path.resolve()),
        "sourceFileCount": source_file_count,
        "documentCount": document_count,
        "resourceTypeCounts": dict(resource_type_counts),
        "uniquePatientRefs": len(unique_patient_refs),
        "duplicateLogicalRefsSkipped": duplicate_logical_refs_skipped,
        "conflictingDuplicateLogicalRefsSkipped": conflicting_duplicate_logical_refs,
        "includeRawResource": bool(include_raw_resource),
        "embeddingsEnabled": bool(enable_embeddings),
        "embeddingCounts": dict(embedding_counts),
        "embeddingResourceTypes": sorted(resolved_vector_resource_types),
    }
    with open(manifest_path, "w", encoding="utf-8") as manifest_handle:
        json.dump(manifest, manifest_handle, indent=2)
        manifest_handle.write("\n")
    return manifest
