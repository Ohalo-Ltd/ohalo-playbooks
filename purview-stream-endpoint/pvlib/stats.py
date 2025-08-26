from typing import Any, Dict, List
import os
import time
from collections import defaultdict

from azure.purview.datamap import DataMapClient
from azure.purview.datamap.models import AtlasEntity, AtlasEntitiesWithExtInfo

from . import atlas as pvatlas
from . import dxr as pvdxr
from . import entities as pvent
from . import relationships as pvrels
from .typedefs import CUSTOM_TYPE_NAME, ensure_types_and_relationships
from .config import logger


def _to_qualified_name(tag: Dict[str, Any]) -> str:
    tenant = tag.get("tenant") or os.environ.get("DXR_TENANT", "default")
    tag_id = str(tag.get("id"))
    return f"dxrtag://{tenant}/{tag_id}"


def _qn_datasource(tenant: str, dsid: str) -> str:
    return f"dxrds://{tenant}/{dsid}"


def update_unstructured_datasource_hit_stats(
    client: DataMapClient,
    tenant: str,
    ds_items: List[Dict[str, Any]],
    child_collections: Dict[str, str] | None = None,
) -> None:
    if not ds_items:
        return
    # Ensure types and datasource entities exist so updates don't fail on missing required attributes
    try:
        ensure_types_and_relationships()
    except Exception:
        pass
    try:
        pvent.upsert_unstructured_datasources(client, tenant, ds_items)
    except Exception:
        pass
    per_coll: Dict[str, List[AtlasEntity]] = defaultdict(list)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for item in ds_items:
        dsid = str(item.get("id"))
        qn = _qn_datasource(tenant, dsid)
        stats = pvdxr.get_label_statistics_for_datasource(dsid, int(os.getenv("LABEL_STATS_LIMIT", "100000")))
        lbl_ids: List[str] = []
        lbl_names: List[str] = []
        lbl_counts: List[int] = []
        for row in stats or []:
            try:
                cnt = int(row.get("documentCount", 0))
            except Exception:
                cnt = 0
            if cnt > 0:
                lbl_ids.append(str(row.get("labelId")))
                lbl_names.append(str(row.get("labelName", "")))
                lbl_counts.append(cnt)
        attrs = {
            "qualifiedName": qn,
            # include required fields so the entity can be created if missing
            "name": item.get("name", dsid),
            "datasourceId": dsid,
            "connectorTypeName": item.get("connectorTypeName") or item.get("connectorType") or "",
            "dxrTenant": tenant,
            "hitLabelIds": lbl_ids,
            "hitLabelNames": lbl_names,
            "hitDocumentCounts": lbl_counts,
            "statsUpdatedAt": now_iso,
        }
        per_coll[(child_collections or {}).get(dsid) or os.environ.get("PURVIEW_COLLECTION_ID") or ""].append(
            AtlasEntity(type_name="unstructured_datasource", attributes=attrs)
        )
    for coll, entities in per_coll.items():
        if not entities:
            continue
        payload = AtlasEntitiesWithExtInfo(entities=entities)
        try:
            if coll:
                client.entity.batch_create_or_update(payload, collection_id=coll)
            else:
                client.entity.batch_create_or_update(payload)
        except Exception as e:
            logger.warning("Update hit stats upsert failed for collection=%s: %s", coll or "(none)", e)

    # Post-upsert verification with up to 2 retries (handles propagation/latency)
    def _verify_once(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        missing: List[Dict[str, Any]] = []
        for item in items:
            dsid = str(item.get("id"))
            qn = _qn_datasource(tenant, dsid)
            g = pvatlas.resolve_guid_by_qn("unstructured_datasource", qn, attempts=3, delay_seconds=0.8)
            if not g:
                missing.append(item)
                continue
            r = pvatlas._atlas_get(f"/datamap/api/atlas/v2/entity/guid/{g}")
            if r.status_code != 200:
                missing.append(item)
                continue
            try:
                attrs = (r.json() or {}).get("attributes") or {}
            except Exception:
                attrs = {}
            if not isinstance(attrs.get("hitLabelIds"), list):
                missing.append(item)
        return missing

    to_check = list(ds_items)
    for attempt in range(2):
        missing = _verify_once(to_check)
        if not missing:
            break
        # Re-upsert only missing entities explicitly (no explicit collection to keep minimal permissions)
        entities_fix: List[AtlasEntity] = []
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for item in missing:
            dsid = str(item.get("id"))
            qn = _qn_datasource(tenant, dsid)
            rows = pvdxr.get_label_statistics_for_datasource(dsid, int(os.getenv("LABEL_STATS_LIMIT", "100000")))
            lbl_ids = [str(r.get("labelId")) for r in rows or [] if int(r.get("documentCount", 0)) > 0]
            lbl_names = [str(r.get("labelName", "")) for r in rows or [] if int(r.get("documentCount", 0)) > 0]
            lbl_counts = [int(r.get("documentCount", 0)) for r in rows or [] if int(r.get("documentCount", 0)) > 0]
            attrs = {
                "qualifiedName": qn,
                "name": item.get("name", dsid),
                "datasourceId": dsid,
                "connectorTypeName": item.get("connectorTypeName") or item.get("connectorType") or "",
                "dxrTenant": tenant,
                "hitLabelIds": lbl_ids,
                "hitLabelNames": lbl_names,
                "hitDocumentCounts": lbl_counts,
                "statsUpdatedAt": now_iso,
            }
            entities_fix.append(AtlasEntity(type_name="unstructured_datasource", attributes=attrs))
        try:
            payload_fix = AtlasEntitiesWithExtInfo(entities=entities_fix)
            client.entity.batch_create_or_update(payload_fix)
        except Exception as e:
            logger.debug("Retry upsert for datasource stats failed: %s", e)
        time.sleep(1.2)


def update_unstructured_dataset_hit_stats_and_relationships(
    client: DataMapClient,
    tags: List[Dict[str, Any]],
    ds_items: List[Dict[str, Any]],
    ds_name_map: Dict[str, str],
    ud_collection: str | None,
) -> None:
    if not tags:
        return
    try:
        ensure_types_and_relationships()
    except Exception:
        pass
    # Ensure DS exist for linkage
    try:
        tenant = os.getenv("DXR_TENANT", "default")
        if ds_items:
            pvent.upsert_unstructured_datasources(client, tenant, ds_items, collection_id=ud_collection)
    except Exception:
        pass

    # Build per-label reverse map of hits
    per_label: Dict[str, Dict[str, Any]] = {}
    for item in ds_items or []:
        dsid = str(item.get("id"))
        rows = pvdxr.get_label_statistics_for_datasource(dsid, int(os.getenv("LABEL_STATS_LIMIT", "100000")))
        for row in rows or []:
            try:
                cnt = int(row.get("documentCount", 0))
            except Exception:
                cnt = 0
            if cnt <= 0:
                continue
            lid = str(row.get("labelId"))
            lname = str(row.get("labelName", ""))
            inv = per_label.setdefault(lid, {"labelName": lname, "by_ds": {}})
            inv["by_ds"][dsid] = cnt

    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    # Upsert label-side arrays (create-if-missing: include required fields)
    entities: List[AtlasEntity] = []
    for tag in tags:
        tag_id = str(tag.get("id"))
        qn = _to_qualified_name(tag)
        inv = per_label.get(tag_id) or {"by_ds": {}, "labelName": tag.get("name", "")}
        dsids = list(inv["by_ds"].keys())
        counts = [inv["by_ds"][d] for d in dsids]
        dsnames = [ds_name_map.get(d, "") for d in dsids]
        attrs = {
            "qualifiedName": qn,
            "name": tag.get("name") or tag.get("labelName") or f"dxr_label_{tag_id}",
            "type": tag.get("type") or "SMART",
            "dxrTenant": tag.get("tenant") or os.getenv("DXR_TENANT", "default"),
            "tagId": tag_id,
            "hitDatasourceIds": dsids,
            "hitDatasourceNames": dsnames,
            "hitDocumentCounts": counts,
            "statsUpdatedAt": now_iso,
        }
        entities.append(AtlasEntity(type_name=CUSTOM_TYPE_NAME, attributes=attrs))
    if entities:
        payload = AtlasEntitiesWithExtInfo(entities=entities)
        try:
            if ud_collection:
                client.entity.batch_create_or_update(payload, collection_id=ud_collection)
            else:
                client.entity.batch_create_or_update(payload)
        except Exception as e:
            logger.warning("Label-side stats upsert failed: %s", e)

    # Create/update hit relationships and prune absent
    for tag in tags:
        tag_id = str(tag.get("id"))
        qn_tag = _to_qualified_name(tag)
        tag_guid = pvatlas.resolve_guid_by_qn(CUSTOM_TYPE_NAME, qn_tag)
        if not tag_guid:
            continue
        current = (per_label.get(tag_id) or {}).get("by_ds", {})
        tenant = os.getenv("DXR_TENANT", "default")
        # create/update
        dsid_to_guid = {}
        for dsid, cnt in current.items():
            ds_qn = _qn_datasource(tenant, dsid)
            guid = pvatlas.resolve_guid_by_qn("unstructured_datasource", ds_qn)
            dsid_to_guid[dsid] = guid
            if not guid:
                continue
            attrs = {"documentCount": int(cnt), "statsUpdatedAt": now_iso}
            pvrels._create_or_update_relationship_with_attrs(
                "unstructured_datasource_hits_unstructured_dataset",
                ds_qn,
                "unstructured_datasource",
                qn_tag,
                CUSTOM_TYPE_NAME,
                attrs,
            )
        # prune
        existing = pvrels._get_entity_relationship_map_for_label(tag_guid)
        keep_guids = {g for g in dsid_to_guid.values() if g}
        for dsg, relg in existing.items():
            if dsg not in keep_guids:
                rr = pvatlas._atlas_get(f"/datamap/api/atlas/v2/relationship/guid/{relg}")
                if rr.status_code == 200 and (rr.json() or {}).get("typeName") == "unstructured_datasource_hits_unstructured_dataset":
                    pvrels._delete_relationship_by_guid(relg)

    # Post-enrichment verification with up to 2 retries
    def _verify_labels_and_rels_once() -> bool:
        ok = True
        tenant = os.getenv("DXR_TENANT", "default")
        for tag in tags:
            tag_id = str(tag.get("id"))
            qn_tag = _to_qualified_name(tag)
            tag_guid = pvatlas.resolve_guid_by_qn(CUSTOM_TYPE_NAME, qn_tag, attempts=3, delay_seconds=0.8)
            if not tag_guid:
                ok = False
                continue
            # label attrs
            r = pvatlas._atlas_get(f"/datamap/api/atlas/v2/entity/guid/{tag_guid}")
            if r.status_code != 200:
                ok = False
                continue
            try:
                attrs = (r.json() or {}).get("attributes") or {}
            except Exception:
                attrs = {}
            if not isinstance(attrs.get("hitDatasourceIds"), list):
                ok = False
            # rels presence for hit datasources
            current = (per_label.get(tag_id) or {}).get("by_ds", {})
            rels = pvrels._get_entity_relationship_map_for_label(tag_guid, attempts=3, delay=0.8)
            for dsid, cnt in current.items():
                if cnt <= 0:
                    continue
                ds_qn = _qn_datasource(tenant, dsid)
                ds_guid = pvatlas.resolve_guid_by_qn("unstructured_datasource", ds_qn, attempts=3, delay_seconds=0.8)
                if not ds_guid or ds_guid not in rels:
                    ok = False
        return ok

    for attempt in range(2):
        if _verify_labels_and_rels_once():
            break
        # Re-apply minimal upserts for tags (attributes) and recreate relationships for any missing
        try:
            # Re-upsert only the tags subset we processed
            payload_fix = []
            now_iso2 = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            for tag in tags:
                tag_id = str(tag.get("id"))
                qn = _to_qualified_name(tag)
                inv = per_label.get(tag_id) or {"by_ds": {}}
                dsids = list((inv.get("by_ds") or {}).keys())
                dsnames = [ds_name_map.get(d, "") for d in dsids]
                counts = [int((inv.get("by_ds") or {}).get(d, 0)) for d in dsids]
                payload_fix.append(AtlasEntity(type_name=CUSTOM_TYPE_NAME, attributes={
                    "qualifiedName": qn,
                    "name": tag.get("name") or tag.get("labelName") or f"dxr_label_{tag_id}",
                    "type": tag.get("type") or "SMART",
                    "dxrTenant": tag.get("tenant") or os.getenv("DXR_TENANT", "default"),
                    "tagId": tag_id,
                    "hitDatasourceIds": dsids,
                    "hitDatasourceNames": dsnames,
                    "hitDocumentCounts": counts,
                    "statsUpdatedAt": now_iso2,
                }))
            if payload_fix:
                client.entity.batch_create_or_update(AtlasEntitiesWithExtInfo(entities=payload_fix))
        except Exception:
            pass
        # Recreate relationships for any missing
        try:
            tenant = os.getenv("DXR_TENANT", "default")
            for tag in tags:
                tag_id = str(tag.get("id"))
                qn_tag = _to_qualified_name(tag)
                current = (per_label.get(tag_id) or {}).get("by_ds", {})
                for dsid, cnt in current.items():
                    if cnt <= 0:
                        continue
                    ds_qn = _qn_datasource(tenant, dsid)
                    pvrels._create_or_update_relationship_with_attrs(
                        "unstructured_datasource_hits_unstructured_dataset",
                        ds_qn,
                        "unstructured_datasource",
                        qn_tag,
                        CUSTOM_TYPE_NAME,
                        {"documentCount": int(cnt), "statsUpdatedAt": now_iso2},
                    )
        except Exception:
            pass
        time.sleep(1.2)
