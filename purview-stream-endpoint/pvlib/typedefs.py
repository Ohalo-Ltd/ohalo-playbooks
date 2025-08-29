from typing import Any, Dict, List

from .atlas import _atlas_get, _atlas_post
from .config import logger


CUSTOM_TYPE_NAME = "unstructured_dataset"


def ensure_types_and_relationships() -> None:
    # Read existing entity definitions directly from the entitydef endpoint for consistency
    existing_ds = _atlas_get("/datamap/api/atlas/v2/types/entitydef/name/unstructured_datasource")
    existing_ud = _atlas_get(f"/datamap/api/atlas/v2/types/entitydef/name/{CUSTOM_TYPE_NAME}")
    existing_proc = _atlas_get("/datamap/api/atlas/v2/types/entitydef/name/dxr_process")

    # Keep this entity type lean: only the current attributes we actively use.
    # Note: We do not attempt to remove legacy attributes from Purview once created,
    # because that generally requires elevated permissions and can fail if entities exist.
    # Pruning here simply stops (re)registering unused attributes going forward.
    unstructured_dataset_def = {
        "name": "unstructured_dataset",
        "superTypes": ["DataSet"],
        "attributeDefs": [
            # Core label metadata from the new DXR classifications API
            {"name": "labelId", "typeName": "string", "isOptional": True},
            {"name": "labelType", "typeName": "string", "isOptional": True},
            {"name": "labelSubtype", "typeName": "string", "isOptional": True},
            {"name": "createdAt", "typeName": "string", "isOptional": True},
            {"name": "updatedAt", "typeName": "string", "isOptional": True},
            {"name": "test", "typeName": "string", "isOptional": True},
            # Clickable search URL in DXR (Purview UI typically renders http(s) as links)
            {"name": "dxrSearchLink", "typeName": "string", "isOptional": True},
            # Minimal legacy/compat for continuity
            {"name": "dxrTenant", "typeName": "string", "isOptional": True},
            {"name": "tagId", "typeName": "string", "isOptional": True},
        ],
    }

    # Custom Process subtype to carry a clickable DXR search link on lineage nodes
    dxr_process_def = {
        "name": "dxr_process",
        "superTypes": ["Process"],
        "attributeDefs": [
            {"name": "dxrSearchLink", "typeName": "string", "isOptional": True},
            {"name": "labelType", "typeName": "string", "isOptional": True},
            {"name": "labelSubtype", "typeName": "string", "isOptional": True},
            {"name": "createdAt", "typeName": "string", "isOptional": True},
            {"name": "updatedAt", "typeName": "string", "isOptional": True},
            {"name": "datasourceId", "typeName": "string", "isOptional": True},
            {"name": "datasourceName", "typeName": "string", "isOptional": True},
            {"name": "connectorTypeName", "typeName": "string", "isOptional": True},
            {"name": "connectorId", "typeName": "string", "isOptional": True},
            {"name": "connectorName", "typeName": "string", "isOptional": True},
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

    # hits relationship is disabled in environments without permissions to register it

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
            logger.info("Adding attributes to unstructured_datasource: %s", ", ".join(a["name"] for a in new_attrs))
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
            logger.info("Adding attributes to unstructured_dataset: %s", ", ".join(a["name"] for a in new_ud_attrs))
            defs_to_submit.append({
                "name": unstructured_dataset_def["name"],
                "superTypes": unstructured_dataset_def["superTypes"],
                "attributeDefs": merged_ud["attributeDefs"],
            })
    else:
        defs_to_submit.append(unstructured_dataset_def)

    # dxr_process ensure/merge
    if existing_proc.status_code == 200:
        try:
            cur_p = existing_proc.json() or {}
            existing_p_names = {a.get("name") for a in (cur_p.get("attributeDefs") or [])}
        except Exception:
            existing_p_names = set()
        new_p_attrs = [a for a in dxr_process_def["attributeDefs"] if a["name"] not in existing_p_names]
        if new_p_attrs:
            merged_p = dict(cur_p)
            merged_p["attributeDefs"] = (cur_p.get("attributeDefs") or []) + new_p_attrs
            logger.info("Adding attributes to dxr_process: %s", ", ".join(a["name"] for a in new_p_attrs))
            defs_to_submit.append({
                "name": dxr_process_def["name"],
                "superTypes": dxr_process_def["superTypes"],
                "attributeDefs": merged_p["attributeDefs"],
            })
    else:
        defs_to_submit.append(dxr_process_def)

    if defs_to_submit:
        # Try a combined submit first (no hits relationship in restricted tenants)
        payload = {"entityDefs": defs_to_submit, "relationshipDefs": [rel_ds_has_dataset]}
        resp = _atlas_post("/datamap/api/atlas/v2/types/typedefs", payload)
        if resp.status_code in (200, 201):
            logger.info("Ensured/updated custom types & relationships")
        elif resp.status_code == 409:
            logger.info("Type defs returned 409; attempting entityDefs and relationshipDefs separately.")
            # Ensure entityDefs separately
            ent_payload = {"entityDefs": defs_to_submit}
            r_ent = _atlas_post("/datamap/api/atlas/v2/types/typedefs", ent_payload)
            if r_ent.status_code not in (200, 201, 409):
                logger.error("Failed to ensure entity type defs: %s %s", r_ent.status_code, r_ent.text[:300])
                r_ent.raise_for_status()
            # Ensure relationships separately (ignore hits failures in tenants without that typedef permission)
            rel_payload = {"relationshipDefs": [rel_ds_has_dataset]}
            r_rel = _atlas_post("/datamap/api/atlas/v2/types/typedefs", rel_payload)
            if r_rel.status_code not in (200, 201, 409):
                logger.warning("Relationship defs ensure returned %s: %s", r_rel.status_code, r_rel.text[:300])
        else:
            logger.error("Failed to create/update typedefs: %s %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()
    else:
        # No entity updates needed; still ensure the relationship exists
        payload = {"relationshipDefs": [rel_ds_has_dataset]}
        r = _atlas_post("/datamap/api/atlas/v2/types/typedefs", payload)
        if r.status_code not in (200, 201, 409):
            logger.error("Failed to ensure relationships: %s %s", r.status_code, r.text[:300])
            r.raise_for_status()
