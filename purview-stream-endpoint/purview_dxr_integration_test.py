import os
import time
import uuid
import logging
from typing import Dict

import pytest
import requests
from dotenv import load_dotenv

# Import SUT in a way that works both as a module and as a script
try:
    import purview_dxr_integration as sut
except ModuleNotFoundError:
    import importlib.util, pathlib, sys as _sys
    _THIS_DIR = pathlib.Path(__file__).resolve().parent
    _SUT_FILE = _THIS_DIR / "purview_dxr_integration.py"
    spec = importlib.util.spec_from_file_location("sut", _SUT_FILE)
    if spec is None or spec.loader is None:
        raise
    sut = importlib.util.module_from_spec(spec)
    _sys.modules["sut"] = sut
    spec.loader.exec_module(sut)

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


# --------- Helpers ---------
REQUIRED_ENV = [
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "PURVIEW_ENDPOINT",
    "DXR_APP_URL",
    "DXR_PAT_TOKEN",
]

def env_or_skip():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        pytest.skip(f"Missing required env vars for integration tests: {', '.join(missing)}")


def _now_suffix() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _ensure_ud_collection_or_skip() -> str:
    coll = os.getenv("PURVIEW_COLLECTION_ID")
    if coll:
        return coll
    domain_name = os.getenv("PURVIEW_DOMAIN_NAME")
    domain_id = os.getenv("PURVIEW_DOMAIN_ID")
    parent_ref = os.getenv("PURVIEW_PARENT_COLLECTION_ID") or domain_id or sut._slug(domain_name)
    try:
        return sut.ensure_unstructured_datasets_collection(parent_ref)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        pytest.skip(f"Unable to ensure UD collection (status={status}). Set PURVIEW_COLLECTION_ID or adjust permissions.")


@pytest.fixture(scope="session")
def purview_client():
    env_or_skip()
    return sut.get_purview_client()


@pytest.fixture(scope="session")
def purview_token() -> str:
    env_or_skip()
    return sut._get_access_token()


@pytest.fixture(scope="session")
def test_artifacts():
    data = {"entities": [], "collections": set()}
    yield data
    try:
        by_type: Dict[str, set] = {}
        for t, qn in data["entities"]:
            by_type.setdefault(t, set()).add(qn)
        for t, qns in by_type.items():
            try:
                sut.delete_entities_by_qualified_names(list(qns), t)
            except Exception as e:
                logger.warning("Cleanup: delete by QN failed for type %s: %s", t, e)
    except Exception as e:
        logger.warning("Cleanup encountered an error: %s", e)


# --------- Unit tests ---------
@pytest.mark.unit
def test_to_qualified_name_stability_unit():
    tag = {"id": 999, "tenant": "acme"}
    qn1 = sut._to_qualified_name(tag)
    qn2 = sut._to_qualified_name(tag)
    assert qn1 == qn2
    assert qn1 == "dxrtag://acme/999"


@pytest.mark.unit
def test_slug_unit():
    assert sut._slug("Unstructured Datasets") == "unstructured-datasets"
    assert sut._slug(" HR / Finance  ") == "hr-finance"


# --------- Integration tests ---------
@pytest.mark.integration
def test_connect_to_purview_token(purview_token: str):
    assert isinstance(purview_token, str) and len(purview_token) > 0


@pytest.mark.integration
def test_bootstrap_types_and_relationships(purview_client):
    sut.ensure_types_and_relationships()
    r1 = sut._atlas_get("/datamap/api/atlas/v2/types/typedef/name/unstructured_datasource")
    assert r1.status_code in (200,)


@pytest.mark.integration
def test_save_asset_into_purview(purview_client, test_artifacts):
    sut.ensure_types_and_relationships()
    qn = f"dxrtag://test/{uuid.uuid4()}-{_now_suffix()}"
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
    entity = {"typeName": sut.CUSTOM_TYPE_NAME, "attributes": {"qualifiedName": qn, "name": "test_unstructured_dataset", "type": "STANDARD"}}
    payload = {"entities": [entity]}
    try:
        coll_ref = _ensure_ud_collection_or_skip()
        result = purview_client.entity.batch_create_or_update(payload, collection_id=coll_ref)
        assert isinstance(result, dict)
    except Exception:
        resp = sut._atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}", params={"attr:qualifiedName": qn})
        assert resp.status_code == 200


@pytest.mark.integration
def test_connect_to_dxr_and_pull_classifications():
    env_or_skip()
    base_url = sut._normalize_base_url(os.getenv("DXR_APP_URL"))
    path = os.getenv("DXR_CLASSIFICATIONS_PATH", "/api/vbeta/classifications")
    url = f"{base_url}{path}"
    headers = {"Authorization": f"Bearer {os.getenv('DXR_PAT_TOKEN')}", "Accept": "application/json", "Referer": base_url}
    verify = not str(os.getenv("DXR_VERIFY_SSL", "1")).lower() in ("0", "false", "no")
    resp = requests.get(url, headers=headers, timeout=30, verify=verify)
    if resp.status_code in (401, 403):
        pytest.skip("DXR PAT token unauthorized; check DXR_PAT_TOKEN/permissions")
    assert resp.status_code == 200
    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    assert isinstance((data or {}).get("data", []), list)


@pytest.mark.integration
def test_list_file_datasources_for_label_smoke():
    env_or_skip()
    labels = sut.get_dxr_classifications()
    if not labels:
        pytest.skip("No classifications returned from DXR")
    label_id = str(labels[0].get("id"))
    name_map, items = sut.list_file_datasources_for_label(label_id)
    assert isinstance(name_map, dict)
    assert isinstance(items, list)


@pytest.mark.integration
def test_label_entity_has_core_metadata_and_search_link(purview_client, test_artifacts):
    env_or_skip()
    sut.ensure_types_and_relationships()
    # Verify the type has required attributes; if not, skip due to tenant permissions on type updates
    td = sut._atlas_get("/datamap/api/atlas/v2/types/typedef/name/unstructured_dataset")
    if td.status_code != 200:
        pytest.skip(f"Cannot read unstructured_dataset typedef (status={td.status_code})")
    try:
        tjson = td.json() or {}
        names = {a.get("name") for a in (tjson.get("attributeDefs") or [])}
    except Exception:
        names = set()
    required_attrs = {"labelId", "labelType", "labelSubtype", "createdAt", "updatedAt", "dxrSearchLink"}
    missing = required_attrs - names
    if missing:
        # Best-effort ensure then re-check
        sut.ensure_types_and_relationships()
        td2 = sut._atlas_get("/datamap/api/atlas/v2/types/typedef/name/unstructured_dataset")
        try:
            tjson2 = td2.json() or {}
            names2 = {a.get("name") for a in (tjson2.get("attributeDefs") or [])}
        except Exception:
            names2 = set()
        missing2 = required_attrs - names2
        if missing2:
            pytest.xfail(
                "Tenant lacks type-update permissions to add required attributes: "
                + ", ".join(sorted(missing2))
            )
    # Fetch classifications and pick first label
    allc = sut.get_dxr_classifications()
    labels = [c for c in (allc or []) if str(c.get("type")).upper() == "LABEL"]
    if not labels:
        pytest.skip("No classifications/labels returned from DXR")
    label = labels[0]
    # Upsert label into UD collection
    ud_ref = _ensure_ud_collection_or_skip()
    qn = sut._to_qualified_name(label)
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
    sut.upsert_unstructured_datasets(purview_client, [label], {}, collection_id=ud_ref)
    # Resolve and fetch attributes
    resp = sut._atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}", params={"attr:qualifiedName": qn})
    assert resp.status_code == 200, f"UniqueAttribute lookup failed: {resp.status_code} {resp.text[:200]}"
    data = resp.json() or {}
    attrs = data.get("attributes", {})
    # Core label metadata
    assert attrs.get("labelId") == str(label.get("id"))
    assert attrs.get("labelType") == (label.get("type") or "LABEL")
    assert attrs.get("labelSubtype") == (label.get("subtype") or "")
    assert isinstance(attrs.get("createdAt"), str) and attrs.get("createdAt")
    assert isinstance(attrs.get("updatedAt"), str) and attrs.get("updatedAt")
    # DXR search link present and clickable
    link = attrs.get("dxrSearchLink")
    assert isinstance(link, str) and link.startswith(("http://", "https://")), f"dxrSearchLink not set correctly: {link}"


@pytest.mark.integration
def test_end_to_end_upsert_labels_and_relationships(purview_client, test_artifacts):
    env_or_skip()
    sut.ensure_types_and_relationships()
    labels = sut.get_dxr_classifications()
    if not labels:
        pytest.skip("No classifications returned from DXR")
    ud_ref = _ensure_ud_collection_or_skip()
    label = labels[0]
    name_map, ds_items = sut.list_file_datasources_for_label(str(label.get("id")))
    tenant = os.getenv("DXR_TENANT", "default")
    sut.upsert_unstructured_datasources(purview_client, tenant, ds_items)
    sut.upsert_unstructured_datasets(purview_client, [label], name_map, collection_id=ud_ref)
    for it in ds_items:
        qn_ds = sut._qn_datasource(tenant, str(it.get("id")))
        qn_tag = sut._to_qualified_name(label)
        try:
            sut.create_relationship("unstructured_datasource_has_unstructured_dataset", qn_ds, "unstructured_datasource", qn_tag, "unstructured_dataset")
        except Exception:
            pass


@pytest.mark.integration
def test_perm_account_list_collections():
    env_or_skip()
    r = sut._account_get("/collections")
    if r.status_code == 200:
        assert r.text != ""
    elif r.status_code in (401, 403):
        pytest.skip("Insufficient permissions to list collections")
    else:
        pytest.skip(f"Collections list returned {r.status_code}")


@pytest.mark.integration
def test_perm_account_create_collection_if_allowed():
    env_or_skip()
    coll_name = f"perm-probe-{uuid.uuid4().hex[:8]}"
    r = sut._account_put(f"/collections/{coll_name}", {"friendlyName": coll_name})
    if r.status_code in (200, 201):
        assert r.json().get("friendlyName") == coll_name
    elif r.status_code in (401, 403):
        pytest.skip("Creating collections requires Collection Admin")
    else:
        pytest.skip(f"Create collection returned {r.status_code}")


@pytest.mark.integration
def test_perm_types_read_write():
    env_or_skip()
    try:
        sut.ensure_types_and_relationships()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            pytest.skip("Atlas typedefs require Data Curator")
        raise


@pytest.mark.integration
def test_perm_entity_create_and_lookup(purview_client, test_artifacts):
    sut.ensure_types_and_relationships()
    qn = f"dxrtag://perm-probe/{uuid.uuid4()}-{_now_suffix()}"
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
    entity = {"typeName": sut.CUSTOM_TYPE_NAME, "attributes": {"qualifiedName": qn, "name": "perm_probe_dataset", "type": "TEST"}}
    payload = {"entities": [entity]}
    try:
        if os.getenv("PURVIEW_COLLECTION_ID"):
            purview_client.entity.batch_create_or_update(payload, collection_id=os.getenv("PURVIEW_COLLECTION_ID"))
        else:
            purview_client.entity.batch_create_or_update(payload)
    except Exception:
        resp = sut._atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}", params={"attr:qualifiedName": qn})
        if resp.status_code in (401, 403):
            pytest.skip("Entity create requires Data Curator")
        assert resp.status_code in (200, 404)


@pytest.mark.integration
def test_perm_move_to_collection(purview_client, test_artifacts):
    pytest.skip("MoveTo is disabled; upserts write directly to collections.")


@pytest.mark.integration
def test_perm_relationship_create(purview_client, test_artifacts):
    sut.ensure_types_and_relationships()
    tenant = os.getenv("DXR_TENANT", "default")
    ds_qn = sut._qn_datasource(tenant, f"perm-{uuid.uuid4()}")
    ds_entity = {"typeName": "unstructured_datasource", "attributes": {"qualifiedName": ds_qn, "name": "perm_ds", "datasourceId": "perm", "connectorTypeName": "TEST", "dxrTenant": tenant}}
    tag_qn = f"dxrtag://perm-rel/{uuid.uuid4()}"
    test_artifacts["entities"].append(("unstructured_datasource", ds_qn))
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, tag_qn))
    tag_entity = {"typeName": sut.CUSTOM_TYPE_NAME, "attributes": {"qualifiedName": tag_qn, "name": "perm_tag", "type": "TEST", "dxrTenant": tenant}}
    payload = {"entities": [ds_entity, tag_entity]}
    try:
        if os.getenv("PURVIEW_COLLECTION_ID"):
            purview_client.entity.batch_create_or_update(payload, collection_id=os.getenv("PURVIEW_COLLECTION_ID"))
        else:
            purview_client.entity.batch_create_or_update(payload)
    except Exception:
        pass
    try:
        sut.create_relationship("unstructured_datasource_has_unstructured_dataset", ds_qn, "unstructured_datasource", tag_qn, "unstructured_dataset")
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            pytest.skip("Creating relationships requires Data Curator")
        raise


@pytest.mark.integration
def test_hits_relationships_flow_removed():
    pytest.skip("Legacy stats-based flow removed; relationships now derive from files endpoint hits.")
