from typing import Any, Dict, Optional
import time

from . import atlas as pvatlas
from .config import logger
from azure.purview.datamap import DataMapClient
from azure.purview.datamap.models import AtlasEntity, AtlasEntitiesWithExtInfo


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

    
def create_relationship_retry(
    rel_type: str,
    end1_qn: str,
    end1_type: str,
    end2_qn: str,
    end2_type: str,
    *,
    attempts: int = 6,
    delay_seconds: float = 1.2,
) -> bool:
    """Create relationship by resolving GUIDs first and posting by GUID with retries.

    More reliable than uniqueAttributes during indexing delays; returns True on success/exists.
    """
    end1_guid = None
    end2_guid = None
    for _ in range(max(1, attempts)):
        if not end1_guid:
            end1_guid = pvatlas.resolve_guid_by_qn(end1_type, end1_qn, attempts=1, delay_seconds=delay_seconds)
        if not end2_guid:
            end2_guid = pvatlas.resolve_guid_by_qn(end2_type, end2_qn, attempts=1, delay_seconds=delay_seconds)
        if end1_guid and end2_guid:
            break
        time.sleep(delay_seconds)
    if not (end1_guid and end2_guid):
        logger.debug(
            "create_relationship_retry: unable to resolve GUIDs: %s(%s), %s(%s)",
            end1_qn,
            end1_type,
            end2_qn,
            end2_type,
        )
        return False

    payload = {
        "typeName": rel_type,
        "end1": {"typeName": end1_type, "guid": end1_guid},
        "end2": {"typeName": end2_type, "guid": end2_guid},
    }
    r = None
    for i in range(max(1, attempts)):
        r = pvatlas._atlas_post("/datamap/api/atlas/v2/relationship", payload)
        if r.status_code in (200, 201, 409):
            if i > 0:
                logger.debug(
                    "Relationship %s created/exists after %d retries: %s -> %s",
                    rel_type,
                    i,
                    end1_qn,
                    end2_qn,
                )
            return True
        time.sleep(delay_seconds)
    if r is not None:
        logger.debug(
            "Relationship %s create failed after %d attempts: status=%s body=%s",
            rel_type,
            attempts,
            r.status_code,
            r.text[:300],
        )
    return False

def _parse_tenant_and_ids_from_qn(ds_qn: str, tag_qn: str) -> Dict[str, str]:
    # Expected formats: dxrds://<tenant>/<dsid>, dxrtag://<tenant>/<labelId>
    out: Dict[str, str] = {"tenant": "default", "dsid": "", "labelId": ""}
    try:
        t1 = ds_qn.split("//", 1)[1]
        parts1 = t1.split("/")
        out["tenant"] = parts1[0]
        out["dsid"] = parts1[1] if len(parts1) > 1 else ""
    except Exception:
        pass
    try:
        t2 = tag_qn.split("//", 1)[1]
        parts2 = t2.split("/")
        if not out.get("tenant"):
            out["tenant"] = parts2[0]
        out["labelId"] = parts2[1] if len(parts2) > 1 else ""
    except Exception:
        pass
    return out


def create_lineage_between_datasource_and_dataset(ds_qn: str, tag_qn: str) -> bool:
    """Create or update a Process lineage from datasource -> tag dataset.

    Returns True on success or already-exists, False otherwise.
    """
    ds_guid = pvatlas.resolve_guid_by_qn("unstructured_datasource", ds_qn)
    tag_guid = pvatlas.resolve_guid_by_qn("unstructured_dataset", tag_qn)
    if not (ds_guid and tag_guid):
        logger.debug("Lineage: unable to resolve GUIDs for %s -> %s", ds_qn, tag_qn)
        return False

    parts = _parse_tenant_and_ids_from_qn(ds_qn, tag_qn)
    tenant = parts.get("tenant", "default")
    dsid = parts.get("dsid", "")
    label_id = parts.get("labelId", "")
    proc_qn = f"dxrproc://{tenant}/{dsid}->{label_id}"
    name = f"DXR lineage: {dsid} -> {label_id}"

    entity = {
        "typeName": "Process",
        "attributes": {
            "qualifiedName": proc_qn,
            "name": name,
        },
        "relationshipAttributes": {
            "inputs": [{"guid": ds_guid}],
            "outputs": [{"guid": tag_guid}],
        },
    }
    payload = {"entities": [entity]}
    r = pvatlas._atlas_post("/datamap/api/atlas/v2/entity/bulk", payload)
    if r.status_code in (200, 201):
        return True
    if r.status_code == 409:
        # Update existing process to ensure edges
        guid = pvatlas.resolve_guid_by_qn("Process", proc_qn) or ""
        if not guid:
            return True  # treat as exists
        try:
            # Merge relationship attributes
            er = pvatlas._atlas_get(f"/datamap/api/atlas/v2/entity/guid/{guid}")
            cur = er.json() if er.status_code == 200 else {}
        except Exception:
            cur = {}
        ra = cur.get("relationshipAttributes") or {}
        inputs = ra.get("inputs") or []
        outputs = ra.get("outputs") or []
        if not any((i.get("guid") == ds_guid) for i in inputs):
            inputs.append({"guid": ds_guid})
        if not any((o.get("guid") == tag_guid) for o in outputs):
            outputs.append({"guid": tag_guid})
        update_payload = {
            "guid": guid,
            "typeName": "Process",
            "attributes": {"qualifiedName": proc_qn, "name": name},
            "relationshipAttributes": {"inputs": inputs, "outputs": outputs},
        }
        pr = pvatlas._atlas_put(f"/datamap/api/atlas/v2/entity/guid/{guid}", update_payload)
        return pr.status_code in (200, 201)
    logger.debug("Lineage process upsert failed (%s): %s", r.status_code, r.text[:300])
    return False


def create_lineage_with_client(
    client: DataMapClient,
    ds_qn: str,
    tag_qn: str,
    *,
    collection_id: Optional[str] = None,
    process_name: Optional[str] = None,
    process_description: Optional[str] = None,
    process_search_link: Optional[str] = None,
    label_type: Optional[str] = None,
    label_subtype: Optional[str] = None,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    datasource_id: Optional[str] = None,
    datasource_name: Optional[str] = None,
    connector_type_name: Optional[str] = None,
    connector_id: Optional[str] = None,
    connector_name: Optional[str] = None,
) -> bool:
    """Create lineage using SDK with collection context to avoid 403.

    This mirrors create_lineage_between_datasource_and_dataset but uses the
    DataMapClient so we can pass collection_id (curator scope).
    """
    ds_guid = pvatlas.resolve_guid_by_qn("unstructured_datasource", ds_qn)
    tag_guid = pvatlas.resolve_guid_by_qn("unstructured_dataset", tag_qn)
    if not (ds_guid and tag_guid):
        logger.debug("Lineage(SDK): unresolved GUIDs for %s -> %s", ds_qn, tag_qn)
        return False

    parts = _parse_tenant_and_ids_from_qn(ds_qn, tag_qn)
    tenant = parts.get("tenant", "default")
    dsid = parts.get("dsid", "")
    label_id = parts.get("labelId", "")
    proc_qn = f"dxrproc://{tenant}/{dsid}->{label_id}"
    name = process_name or f"DXR lineage: {dsid} -> {label_id}"

    attrs: Dict[str, Any] = {"qualifiedName": proc_qn, "name": name}
    if process_description:
        attrs["description"] = process_description
    if process_search_link:
        attrs["dxrSearchLink"] = process_search_link
    if label_type:
        attrs["labelType"] = label_type
    if label_subtype:
        attrs["labelSubtype"] = label_subtype
    if created_at:
        attrs["createdAt"] = created_at
    if updated_at:
        attrs["updatedAt"] = updated_at
    if datasource_id:
        attrs["datasourceId"] = datasource_id
    if datasource_name:
        attrs["datasourceName"] = datasource_name
    if connector_type_name:
        attrs["connectorTypeName"] = connector_type_name
    if connector_id:
        attrs["connectorId"] = connector_id
    if connector_name:
        attrs["connectorName"] = connector_name

    # Prefer custom subtype to carry clickable link attribute
    entity = AtlasEntity(
        type_name="dxr_process",
        attributes=attrs,
        relationship_attributes={"inputs": [{"guid": ds_guid}], "outputs": [{"guid": tag_guid}]},
    )
    payload = AtlasEntitiesWithExtInfo(entities=[entity])
    try:
        if collection_id:
            client.entity.batch_create_or_update(payload, collection_id=collection_id)
        else:
            client.entity.batch_create_or_update(payload)
        return True
    except Exception as e:
        try:
            from azure.core.exceptions import HttpResponseError
            if isinstance(e, HttpResponseError) and getattr(e, "response", None) is not None:
                status = getattr(e.response, "status_code", None)
                body = getattr(e.response, "text", "")
                logger.debug("Lineage(SDK) upsert failed: status=%s body=%s", status, str(body)[:300])
        except Exception:
            pass
        # Fallback 1: try base Process without custom attribute
        try:
            attrs_fallback = {k: v for k, v in attrs.items() if k != "dxrSearchLink"}
            entity2 = AtlasEntity(
                type_name="Process",
                attributes=attrs_fallback,
                relationship_attributes={"inputs": [{"guid": ds_guid}], "outputs": [{"guid": tag_guid}]},
            )
            payload2 = AtlasEntitiesWithExtInfo(entities=[entity2])
            if collection_id:
                client.entity.batch_create_or_update(payload2, collection_id=collection_id)
            else:
                client.entity.batch_create_or_update(payload2)
            return True
        except Exception:
            pass
        # Fallback 2: if the process already exists, update its attributes and edges
        parts = _parse_tenant_and_ids_from_qn(ds_qn, tag_qn)
        tenant = parts.get("tenant", "default")
        dsid = parts.get("dsid", "")
        label_id = parts.get("labelId", "")
        proc_qn = f"dxrproc://{tenant}/{dsid}->{label_id}"
        # Try resolve as dxr_process first, then Process
        guid = pvatlas.resolve_guid_by_qn("dxr_process", proc_qn) or pvatlas.resolve_guid_by_qn("Process", proc_qn) or ""
        if not guid:
            return create_lineage_between_datasource_and_dataset(ds_qn, tag_qn)
        er = pvatlas._atlas_get(f"/datamap/api/atlas/v2/entity/guid/{guid}")
        cur = er.json() if er.status_code == 200 else {}
        existing_type = cur.get("typeName") or cur.get("type") or "Process"
        ra = cur.get("relationshipAttributes") or {}
        inputs = ra.get("inputs") or []
        outputs = ra.get("outputs") or []
        if not any((i.get("guid") == ds_guid) for i in inputs):
            inputs.append({"guid": ds_guid})
        if not any((o.get("guid") == tag_guid) for o in outputs):
            outputs.append({"guid": tag_guid})
        upd_attrs: Dict[str, Any] = {"qualifiedName": proc_qn, "name": name}
        if process_description:
            upd_attrs["description"] = process_description
        # Only include custom fields when existing type supports them
        if existing_type == "dxr_process":
            if process_search_link:
                upd_attrs["dxrSearchLink"] = process_search_link
            if label_type:
                upd_attrs["labelType"] = label_type
            if label_subtype:
                upd_attrs["labelSubtype"] = label_subtype
            if created_at:
                upd_attrs["createdAt"] = created_at
            if updated_at:
                upd_attrs["updatedAt"] = updated_at
            if datasource_id:
                upd_attrs["datasourceId"] = datasource_id
            if datasource_name:
                upd_attrs["datasourceName"] = datasource_name
            if connector_type_name:
                upd_attrs["connectorTypeName"] = connector_type_name
            if connector_id:
                upd_attrs["connectorId"] = connector_id
            if connector_name:
                upd_attrs["connectorName"] = connector_name
        update_payload = {
            "guid": guid,
            "typeName": existing_type,
            "attributes": upd_attrs,
            "relationshipAttributes": {"inputs": inputs, "outputs": outputs},
        }
        pr = pvatlas._atlas_put(f"/datamap/api/atlas/v2/entity/guid/{guid}", update_payload)
        return pr.status_code in (200, 201)
