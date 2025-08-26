from typing import Any, Dict, List

from .atlas import _atlas_get, _atlas_post
from .config import logger


CUSTOM_TYPE_NAME = "unstructured_dataset"


def ensure_types_and_relationships() -> None:
    existing_ds = _atlas_get("/datamap/api/atlas/v2/types/entitydef/name/unstructured_datasource")
    existing_ud = _atlas_get(f"/datamap/api/atlas/v2/types/typedef/name/{CUSTOM_TYPE_NAME}")

    unstructured_dataset_def = {
        "name": "unstructured_dataset",
        "superTypes": ["DataSet"],
        "attributeDefs": [
            {"name": "type", "typeName": "string", "isOptional": True},
            {"name": "parametersParameters", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersValues", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersTypes", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersMatchStrategies", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersOperators", "typeName": "array<string>", "isOptional": True},
            {"name": "parametersGroupIds", "typeName": "array<int>", "isOptional": True},
            {"name": "parametersGroupOrders", "typeName": "array<int>", "isOptional": True},
            {"name": "datasourceIds", "typeName": "array<string>", "isOptional": True},
            {"name": "datasourceNames", "typeName": "array<string>", "isOptional": True},
            {"name": "dxrTenant", "typeName": "string", "isOptional": True},
            {"name": "tagId", "typeName": "string", "isOptional": True},
            {"name": "hitDatasourceIds", "typeName": "array<string>", "isOptional": True},
            {"name": "hitDatasourceNames", "typeName": "array<string>", "isOptional": True},
            {"name": "hitDocumentCounts", "typeName": "array<int>", "isOptional": True},
            {"name": "statsUpdatedAt", "typeName": "string", "isOptional": True},
        ],
    }

    unstructured_datasource_def = {
        "name": "unstructured_datasource",
        "superTypes": ["DataSet"],
        "attributeDefs": [
            {"name": "datasourceId", "typeName": "string", "isOptional": False},
            {"name": "connectorTypeName", "typeName": "string", "isOptional": True},
            {"name": "dxrTenant", "typeName": "string", "isOptional": True},
            {"name": "hitLabelIds", "typeName": "array<string>", "isOptional": True},
            {"name": "hitLabelNames", "typeName": "array<string>", "isOptional": True},
            {"name": "hitDocumentCounts", "typeName": "array<int>", "isOptional": True},
            {"name": "statsUpdatedAt", "typeName": "string", "isOptional": True},
        ],
    }

    rel_ds_has_dataset = {
        "name": "unstructured_datasource_has_unstructured_dataset",
        "relationshipCategory": "ASSOCIATION",
        "endDef1": {"type": "unstructured_datasource", "name": "datasets", "isContainer": False, "cardinality": "SET"},
        "endDef2": {"type": "unstructured_dataset", "name": "datasources", "isContainer": False, "cardinality": "SET"},
        "propagateTags": "NONE",
        "attributeDefs": [
            {"name": "documentCount", "typeName": "int", "isOptional": True},
            {"name": "statsUpdatedAt", "typeName": "string", "isOptional": True},
        ],
    }

    rel_ds_hits_dataset = {
        "name": "unstructured_datasource_hits_unstructured_dataset",
        "relationshipCategory": "ASSOCIATION",
        "endDef1": {"type": "unstructured_datasource", "name": "hitDatasets", "isContainer": False, "cardinality": "SET"},
        "endDef2": {"type": "unstructured_dataset", "name": "hitDatasources", "isContainer": False, "cardinality": "SET"},
        "propagateTags": "NONE",
        "attributeDefs": [
            {"name": "documentCount", "typeName": "int", "isOptional": True},
            {"name": "statsUpdatedAt", "typeName": "string", "isOptional": True},
        ],
    }

    defs_to_submit: List[Dict[str, Any]] = []
    if existing_ds.status_code == 200:
        try:
            cur = existing_ds.json() or {}
            existing_names = {a.get("name") for a in (cur.get("attributeDefs") or [])}
        except Exception:
            existing_names = set()
        new_attrs = [a for a in unstructured_datasource_def["attributeDefs"] if a["name"] not in existing_names]
        if new_attrs:
            merged = dict(cur)
            merged["attributeDefs"] = (cur.get("attributeDefs") or []) + new_attrs
            defs_to_submit.append({
                "name": unstructured_datasource_def["name"],
                "superTypes": unstructured_datasource_def["superTypes"],
                "attributeDefs": merged["attributeDefs"],
            })
    else:
        defs_to_submit.append(unstructured_datasource_def)

    if existing_ud.status_code == 200:
        try:
            cur_ud = existing_ud.json() or {}
            existing_ud_names = {a.get("name") for a in (cur_ud.get("attributeDefs") or [])}
        except Exception:
            existing_ud_names = set()
        new_ud_attrs = [a for a in unstructured_dataset_def["attributeDefs"] if a["name"] not in existing_ud_names]
        if new_ud_attrs:
            merged_ud = dict(cur_ud)
            merged_ud["attributeDefs"] = (cur_ud.get("attributeDefs") or []) + new_ud_attrs
            defs_to_submit.append({
                "name": unstructured_dataset_def["name"],
                "superTypes": unstructured_dataset_def["superTypes"],
                "attributeDefs": merged_ud["attributeDefs"],
            })
    else:
        defs_to_submit.append(unstructured_dataset_def)

    if defs_to_submit:
        payload = {"entityDefs": defs_to_submit, "relationshipDefs": [rel_ds_has_dataset, rel_ds_hits_dataset]}
        resp = _atlas_post("/datamap/api/atlas/v2/types/typedefs", payload)
        if resp.status_code in (200, 201):
            logger.info("Ensured/updated custom types & relationships")
        elif resp.status_code == 409:
            logger.info("Type defs already exist (409). Ensuring relationships only.")
            payload_rel = {"relationshipDefs": [rel_ds_has_dataset, rel_ds_hits_dataset]}
            r2 = _atlas_post("/datamap/api/atlas/v2/types/typedefs", payload_rel)
            if r2.status_code not in (200, 201, 409):
                logger.error("Failed to ensure relationships: %s %s", r2.status_code, r2.text[:300])
                r2.raise_for_status()
        else:
            logger.error("Failed to create/update typedefs: %s %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()
    else:
        payload = {"relationshipDefs": [rel_ds_has_dataset, rel_ds_hits_dataset]}
        r = _atlas_post("/datamap/api/atlas/v2/types/typedefs", payload)
        if r.status_code not in (200, 201, 409):
            logger.error("Failed to ensure relationships: %s %s", r.status_code, r.text[:300])
            r.raise_for_status()

