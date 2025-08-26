from typing import Any, Dict
import time

from . import atlas as pvatlas
from .config import logger


def create_relationship(rel_type: str, end1_qn: str, end1_type: str, end2_qn: str, end2_type: str) -> None:
    payload = {
        "typeName": rel_type,
        "end1": {"typeName": end1_type, "uniqueAttributes": {"qualifiedName": end1_qn}},
        "end2": {"typeName": end2_type, "uniqueAttributes": {"qualifiedName": end2_qn}},
    }
    resp = pvatlas._atlas_post("/datamap/api/atlas/v2/relationship", payload)
    if resp.status_code in (200, 201, 409):
        return
    logger.debug("Relationship create failed (%s): %s", resp.status_code, resp.text[:200])


def _get_entity_relationship_map_for_label(label_guid: str, *, attempts: int = 5, delay: float = 1.0) -> Dict[str, str]:
    rels: Dict[str, str] = {}
    for _ in range(max(1, attempts)):
        # First attempt: relationshipAttributes on the entity
        # Ask Atlas to include relationship attributes where supported
        r = pvatlas._atlas_get(
            f"/datamap/api/atlas/v2/entity/guid/{label_guid}",
        )
        if r.status_code == 200:
            try:
                data = r.json() or {}
                ra = data.get("relationshipAttributes") or {}
                ds_rel = ra.get("hitDatasources") or ra.get("datasources") or ra.get("datasource")
                items = ds_rel if isinstance(ds_rel, list) else ([ds_rel] if isinstance(ds_rel, dict) else [])
                for it in items:
                    ds_guid = it.get("guid") or (it.get("entity") or {}).get("guid")
                    rel_guid = it.get("relationshipGuid") or it.get("relationshipId") or (it.get("relationship") or {}).get("guid")
                    if ds_guid and rel_guid:
                        rels[str(ds_guid)] = str(rel_guid)
            except Exception:
                pass
        if rels:
            return rels
        # Fallback: list relationships endpoint and filter hits relationship type
        r2 = pvatlas._atlas_get(f"/datamap/api/atlas/v2/entity/guid/{label_guid}/relationships")
        if r2.status_code == 200:
            try:
                data2 = r2.json() or {}
                for rel in data2.get("relations", []) or data2.get("list", []) or []:
                    if rel.get("typeName") != "unstructured_datasource_hits_unstructured_dataset":
                        continue
                    end1 = rel.get("end1") or {}
                    end2 = rel.get("end2") or {}
                    for end in (end1, end2):
                        if (end.get("typeName") or end.get("type")) == "unstructured_datasource":
                            ds_guid = end.get("guid") or (end.get("entity") or {}).get("guid")
                            if ds_guid:
                                rels[str(ds_guid)] = str(rel.get("guid") or rel.get("relationshipGuid") or "")
            except Exception:
                pass
        if rels:
            return rels
        time.sleep(delay)
    return rels


def _create_or_update_relationship_with_attrs(
    rel_type: str,
    end1_qn: str,
    end1_type: str,
    end2_qn: str,
    end2_type: str,
    attrs: Dict[str, Any],
) -> None:
    payload = {
        "typeName": rel_type,
        "attributes": attrs,
        "end1": {"typeName": end1_type, "uniqueAttributes": {"qualifiedName": end1_qn}},
        "end2": {"typeName": end2_type, "uniqueAttributes": {"qualifiedName": end2_qn}},
    }
    resp = pvatlas._atlas_post("/datamap/api/atlas/v2/relationship", payload)
    if resp.status_code in (200, 201):
        return
    if resp.status_code == 409:
        # Resolve existing relationship and PUT attributes
        label_guid = pvatlas.resolve_guid_by_qn(end2_type, end2_qn) if end2_type == "unstructured_dataset" else pvatlas.resolve_guid_by_qn(end1_type, end1_qn)
        if not label_guid:
            return
        rels = _get_entity_relationship_map_for_label(label_guid)
        ds_qn = end1_qn if end1_type == "unstructured_datasource" else end2_qn
        ds_guid = pvatlas.resolve_guid_by_qn("unstructured_datasource", ds_qn)
        if not ds_guid:
            return
        rel_guid = rels.get(ds_guid)
        if not rel_guid:
            return
        r = pvatlas._atlas_get(f"/datamap/api/atlas/v2/relationship/guid/{rel_guid}")
        if r.status_code != 200:
            return
        try:
            rel_obj = r.json() or {}
        except Exception:
            rel_obj = {}
        if rel_obj.get("typeName") and rel_obj.get("typeName") != rel_type:
            return
        rel_obj["attributes"] = rel_obj.get("attributes", {}) | attrs
        pr = pvatlas._atlas_put(f"/datamap/api/atlas/v2/relationship/guid/{rel_guid}", rel_obj)
        if pr.status_code not in (200, 201):
            logger.debug("Relationship attribute update failed: %s %s", pr.status_code, pr.text[:200])
        return
    logger.debug("Relationship create failed (%s): %s", resp.status_code, resp.text[:200])


def _delete_relationship_by_guid(rel_guid: str) -> None:
    r = pvatlas._atlas_delete(f"/datamap/api/atlas/v2/relationship/guid/{rel_guid}")
    if r.status_code not in (200, 204):
        logger.debug("Delete relationship %s -> %s %s", rel_guid, r.status_code, r.text[:200])

    
