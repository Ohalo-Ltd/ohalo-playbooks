from typing import Any, Dict, List, Tuple
import os
import time
from collections import defaultdict

from azure.purview.datamap import DataMapClient
from azure.purview.datamap.models import AtlasEntity, AtlasEntitiesWithExtInfo

from . import atlas as pvatlas
from .config import logger
from .typedefs import CUSTOM_TYPE_NAME


def upsert_unstructured_datasets(
    client: DataMapClient,
    labels: List[Dict[str, Any]],
    ds_map: Dict[str, str],
    *,
    collection_id: str | None = None,
) -> Dict[str, str]:
    if not labels:
        logger.info("No tags to upsert this cycle.")
        return {}

    def _to_qualified_name(tag: Dict[str, Any]) -> str:
        tenant = tag.get("tenant") or os.environ.get("DXR_TENANT", "default")
        tag_id = str(tag.get("id"))
        return f"dxrtag://{tenant}/{tag_id}"

    def build_entities_payload_for_tags(tags: List[Dict[str, Any]], ds_map_in: Dict[str, str]) -> AtlasEntitiesWithExtInfo:
        entities: List[AtlasEntity] = []
        dxr_tenant = os.environ.get("DXR_TENANT", "default")
        for tag in tags:
            qn = _to_qualified_name(tag)
            tag_id = str(tag.get("id"))
            name = tag.get("name") or f"dxr_label_{tag_id}"
            description = tag.get("description")
            label_type = tag.get("type") or "LABEL"
            label_subtype = tag.get("subtype") or ""
            created_at = tag.get("createdAt")
            updated_at = tag.get("updatedAt")
            attrs = {
                "qualifiedName": qn,
                "name": name,
                "description": description,
                # New label metadata
                "labelId": tag_id,
                "labelType": label_type,
                "labelSubtype": label_subtype,
                "createdAt": created_at,
                "updatedAt": updated_at,
                # Keep tenant & legacy tagId for continuity
                "dxrTenant": tag.get("tenant", dxr_tenant),
                "tagId": tag_id,
            }
            entities.append(AtlasEntity(type_name=CUSTOM_TYPE_NAME, attributes=attrs))
        return AtlasEntitiesWithExtInfo(entities=entities)

    payload = build_entities_payload_for_tags(labels, ds_map)

    def _resolve_guids_by_qn(candidates: List[Dict[str, Any]]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for tag in candidates:
            qn = _to_qualified_name(tag)
            guid = pvatlas.resolve_guid_by_qn(CUSTOM_TYPE_NAME, qn, attempts=8, delay_seconds=1.5)
            if guid:
                out[qn] = guid
        return out

    try:
        target_collection = collection_id or os.environ.get("PURVIEW_COLLECTION_ID")
        if target_collection:
            result = client.entity.batch_create_or_update(payload, collection_id=target_collection)
        else:
            result = client.entity.batch_create_or_update(payload)
        guid_map: Dict[str, str] = {}
        if isinstance(result, dict):
            for sec in ("CREATE", "UPDATE"):
                for ent in result.get("mutatedEntities", {}).get(sec, []):
                    qn = ent.get("attributes", {}).get("qualifiedName")
                    guid = ent.get("guid")
                    if qn and guid:
                        guid_map[qn] = guid
        if not guid_map:
            guid_map = _resolve_guids_by_qn(labels)
        return guid_map
    except Exception as e:
        try:
            from azure.core.exceptions import HttpResponseError
            if isinstance(e, HttpResponseError) and getattr(e, "response", None) is not None:
                status = getattr(e.response, "status_code", None)
                body = getattr(e.response, "text", "")
                logger.error("Upsert unstructured_datasets failed: status=%s body=%s", status, str(body)[:300])
        except Exception:
            pass
        return _resolve_guids_by_qn(labels)


def upsert_unstructured_datasources(
    client: DataMapClient,
    tenant: str,
    ds_full: List[Dict[str, Any]],
    *,
    by_collection: Dict[str, List[Dict[str, Any]]] | None = None,
    collection_id: str | None = None,
) -> Dict[str, str]:
    def _qn_datasource(tenant_in: str, dsid: str) -> str:
        return f"dxrds://{tenant_in}/{dsid}"

    def _build_entities(items: List[Dict[str, Any]]) -> AtlasEntitiesWithExtInfo:
        entities: List[AtlasEntity] = []
        for item in items:
            dsid = str(item.get("id"))
            name = item.get("name", dsid)
            ctype = item.get("connectorTypeName") or item.get("connectorType") or ""
            attrs = {
                "qualifiedName": _qn_datasource(tenant, dsid),
                "name": name,
                "datasourceId": dsid,
                "connectorTypeName": ctype,
                "dxrTenant": tenant,
            }
            entities.append(AtlasEntity(type_name="unstructured_datasource", attributes=attrs))
        return AtlasEntitiesWithExtInfo(entities=entities)

    if not ds_full:
        return {}

    def _resolve_guids_by_qn(items: List[Dict[str, Any]]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for it in items:
            dsid = str(it.get("id"))
            qn = _qn_datasource(tenant, dsid)
            resp = pvatlas._atlas_get(
                "/datamap/api/atlas/v2/entity/uniqueAttribute/type/unstructured_datasource",
                params={"attr:qualifiedName": qn},
            )
            if resp.status_code == 200:
                try:
                    guid = resp.json().get("guid")
                    if guid:
                        out[qn] = guid
                except Exception:
                    continue
        return out

    guid_map: Dict[str, str] = {}
    try:
        if by_collection:
            for coll, items in by_collection.items():
                if not items:
                    continue
                payload = _build_entities(items)
                result = client.entity.batch_create_or_update(payload, collection_id=coll)
                if isinstance(result, dict):
                    for sec in ("CREATE", "UPDATE"):
                        for ent in result.get("mutatedEntities", {}).get(sec, []):
                            qn = ent.get("attributes", {}).get("qualifiedName")
                            gid = ent.get("guid")
                            if qn and gid:
                                guid_map[qn] = gid
        else:
            payload = _build_entities(ds_full)
            cid = collection_id or os.environ.get("PURVIEW_COLLECTION_ID")
            if cid:
                result = client.entity.batch_create_or_update(payload, collection_id=cid)
            else:
                result = client.entity.batch_create_or_update(payload)
            if isinstance(result, dict):
                for sec in ("CREATE", "UPDATE"):
                    for ent in result.get("mutatedEntities", {}).get(sec, []):
                        qn = ent.get("attributes", {}).get("qualifiedName")
                        gid = ent.get("guid")
                        if qn and gid:
                            guid_map[qn] = gid
        if not guid_map:
            guid_map = _resolve_guids_by_qn(ds_full)
        return guid_map
    except Exception as e:
        logger.warning("batch_create_or_update(datasources) raised (%s). Falling back to unique-attribute lookups.", e)
        return _resolve_guids_by_qn(ds_full)
