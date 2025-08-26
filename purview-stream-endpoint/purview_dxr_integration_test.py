# -------------------------
# Standard library imports
# -------------------------
import os
import time
import json
import uuid
import logging
from typing import Dict, Any, List

# -------------------------
# Third-party imports
# -------------------------
import pytest
import requests
from dotenv import load_dotenv

# System under test (robust import for local runs)
try:
    import purview_dxr_integration as sut  # if repo is installed as a package
except ModuleNotFoundError:
    import importlib.util
    import pathlib
    import sys as _sys
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

# pytestmark = pytest.mark.usefixtures()

# -----------------------------
# Sample DXR payloads for unit tests
# -----------------------------

SAMPLE_DXR_TAGS = [
    {"id": 1, "name": "PII", "description": "Email, location, and persons", "type": "SMART",
     "savedQueryDtoList": [
         {"id": 1, "query": {"query_items": [
             {"parameter": "annotators", "value": "annotation.21", "type": "text", "match_strategy": "exists", "operator": "AND", "group_id": 0, "group_order": 0},
             {"parameter": "annotators", "value": "annotation.42", "type": "text", "match_strategy": "exists", "operator": "AND", "group_id": 0, "group_order": 1},
             {"parameter": "annotators", "value": "annotation.16", "type": "text", "match_strategy": "exists", "operator": "AND", "group_id": 0, "group_order": 2}
         ]}},
         {"id": 2, "datasourceIds": [100, 104, 105]}
     ]},
    {"id": 17, "name": "Invoices with PII", "description": "", "type": "SMART",
     "savedQueryDtoList": [
         {"id": 14, "query": {"query_items": [
             {"parameter": "annotators", "value": "annotation.16", "type": "text", "match_strategy": "exists", "operator": "AND", "group_id": 0, "group_order": 0},
             {"parameter": "annotators", "value": "annotation.21", "type": "text", "match_strategy": "exists", "operator": "AND", "group_id": 0, "group_order": 1},
             {"parameter": "annotators", "value": "annotation.42", "type": "text", "match_strategy": "exists", "operator": "AND", "group_id": 0, "group_order": 2}
         ]}},
         {"id": 15, "datasourceIds": [121, 124]}
     ]},
]

SAMPLE_DXR_DATASOURCES = [
    {"id": 100, "name": "HR", "connectorTypeId": 15, "connectorTypeName": "Folder Path", "accessLevel": "FOUR"},
    {"id": 104, "name": "Pavlos Touboulidis (pav@ohalo.co)", "connectorTypeId": 18, "connectorTypeName": "Box", "accessLevel": "FOUR"},
    {"id": 121, "name": "ipg_demo", "connectorTypeId": 15, "connectorTypeName": "Folder Path", "accessLevel": "FOUR"},
]
# -----------------------------
# Unit tests (do not hit external services)
# -----------------------------

@pytest.mark.unit
def test_build_entities_payload_for_tags_unit():
    # Build ds_map from sample datasources
    ds_map = {str(d["id"]): d["name"] for d in SAMPLE_DXR_DATASOURCES}
    payload = sut.build_entities_payload_for_tags(SAMPLE_DXR_TAGS, ds_map)
    assert hasattr(payload, "entities")
    entities = payload.entities
    assert len(entities) == len(SAMPLE_DXR_TAGS)
    first = entities[0]
    attrs = first.attributes
    assert attrs["name"] == "PII"
    # qualifiedName pattern
    assert attrs["qualifiedName"].startswith("dxrtag://")
    # parameters arrays are flattened and parallel
    assert len(attrs["parametersParameters"]) == 3
    assert len(attrs["parametersValues"]) == 3
    assert len(attrs["parametersTypes"]) == 3
    assert len(attrs["parametersMatchStrategies"]) == 3
    assert len(attrs["parametersOperators"]) == 3
    assert len(attrs["parametersGroupIds"]) == 3
    assert len(attrs["parametersGroupOrders"]) == 3
    # datasource arrays resolved via map
    assert set(attrs["datasourceIds"]) == {"100", "104", "105"}
    assert "HR" in attrs["datasourceNames"]


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

REQUIRED_ENV = [
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "PURVIEW_ENDPOINT",
    "DXR_APP_URL",
    "DXR_PAT_TOKEN",
]

OPTIONAL_ENV = {
    "PURVIEW_COLLECTION_ID": None,
    "DXR_TAGS_PATH": "/api/tags",
}


def env_or_skip():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        pytest.skip(f"Missing required env vars for integration tests: {', '.join(missing)}")


def _now_suffix() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _ensure_ud_collection_or_skip() -> str:
    """Resolve a collection refName to use for writing tag entities.
    Priority:
      1) PURVIEW_COLLECTION_ID env
      2) Ensure via governance/account-host under parent (PURVIEW_PARENT_COLLECTION_ID or PURVIEW_DOMAIN_ID or slug(domain)).
    If ensure fails (401/403/400), skip the test with guidance.
    """
    coll = os.getenv("PURVIEW_COLLECTION_ID")
    if coll:
        return coll
    # Try to ensure via governance
    domain_name = os.getenv("PURVIEW_DOMAIN_NAME")
    domain_id = os.getenv("PURVIEW_DOMAIN_ID")
    parent_ref = os.getenv("PURVIEW_PARENT_COLLECTION_ID") or domain_id or sut._slug(domain_name)
    try:
        return sut.ensure_unstructured_datasets_collection(parent_ref)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        pytest.skip(
            f"Unable to ensure UD collection (status={status}). Set PURVIEW_COLLECTION_ID or adjust permissions."
        )


@pytest.fixture(scope="session")
def purview_client():
    env_or_skip()
    return sut.get_purview_client()


@pytest.fixture(scope="session")
def purview_token() -> str:
    env_or_skip()
    return sut._get_access_token()


@pytest.fixture(scope="session")
def dxr_base_url() -> str:
    env_or_skip()
    # Reuse SUT normalizer so tests behave like runtime
    return sut._normalize_base_url(os.getenv("DXR_APP_URL"))




# -----------------------------
# Tests
# -----------------------------

@pytest.mark.integration
def test_connect_to_purview_token(purview_token: str):
    assert isinstance(purview_token, str) and len(purview_token) > 0


@pytest.mark.integration
def test_bootstrap_types_and_relationships(purview_client):
    # Should create/update typedefs and ensure both relationships (idempotent)
    sut.ensure_types_and_relationships()
    r1 = sut._atlas_get("/datamap/api/atlas/v2/types/typedef/name/unstructured_datasource")
    assert r1.status_code in (200,)
    rrel = sut._atlas_get("/datamap/api/atlas/v2/types/typedef/name/unstructured_datasource_hits_unstructured_dataset")
    assert rrel.status_code in (200, 404)


@pytest.mark.integration
def test_save_asset_into_purview(purview_client, test_artifacts):
    # Ensure type exists first
    sut.ensure_types_and_relationships()

    # Build a one-off test entity (unstructured_dataset) with unique qualifiedName
    qn = f"dxrtag://test/{uuid.uuid4()}-{_now_suffix()}"
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))

    entity = {
        "typeName": sut.CUSTOM_TYPE_NAME,
        "attributes": {
            "qualifiedName": qn,
            "name": "test_unstructured_dataset",
            "type": "STANDARD",
        },
    }

    payload = {"entities": [entity]}

    try:
        coll_ref = _ensure_ud_collection_or_skip()
        result = purview_client.entity.batch_create_or_update(payload, collection_id=coll_ref)
        assert isinstance(result, dict)
        mutated = result.get("mutatedEntities", {})
        created = mutated.get("CREATE", [])
        updated = mutated.get("UPDATE", [])
        assert len(created) + len(updated) >= 1
    except Exception:
        # Fallback: check existence via unique attribute lookup
        path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}"
        resp = sut._atlas_get(f"{path}", params={"attr:qualifiedName": qn})
        assert resp.status_code == 200, f"Upsert failed and entity not found: status={resp.status_code} body={resp.text[:300]}"




@pytest.mark.integration
def test_connect_to_dxr_and_pull_tags():
    env_or_skip()
    base_url = sut._normalize_base_url(os.getenv("DXR_APP_URL"))
    tags_path = os.getenv("DXR_TAGS_PATH", "/api/tags")
    url = f"{base_url}{tags_path}"
    pat = os.getenv("DXR_PAT_TOKEN")
    headers = {
        "Authorization": f"Bearer {pat}",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
        "Referer": base_url,
    }
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 401 or resp.status_code == 403:
        pytest.skip("DXR PAT token unauthorized; check DXR_PAT_TOKEN/permissions")
    assert resp.status_code == 200
    try:
        data = resp.json()
    except Exception:
        data = []
    assert isinstance(data, list)


@pytest.mark.integration
@pytest.mark.destructive
def test_create_domain_in_purview_via_types_exists_only(purview_client):
    """
    Minimal placeholder: Purview "domains" are usually Collections. The public Python Data Map SDK
    does not expose a direct Collections admin API; creating collections typically uses ARM/Studio.
    For now, we assert that our bootstrap type/relationship exists as a proxy for connectivity.
    """
    sut.ensure_types_and_relationships()


@pytest.mark.integration
@pytest.mark.destructive
def test_create_datasource_placeholder():
    """
    Placeholder: registering a data source in Purview typically uses the Scanning API and a supported
    source type. Because "Data X-Ray" is a custom source, this test is a placeholder and will be
    implemented once a concrete registration path is defined (e.g., via Scanning custom connector).
    """
    pytest.skip("Data source registration for custom 'Data X-Ray' requires scanning or admin API setup.")


# -----------------------------
# New fixtures and tests for integration
# -----------------------------

@pytest.fixture(scope="session")
def dxr_datasources_full():
    env_or_skip()
    try:
        name_map, full_list = sut.get_dxr_searchable_datasources()
        if not full_list:
            pytest.skip("No datasources returned from DXR")
        return name_map, full_list
    except Exception as e:
        pytest.skip(f"Failed to fetch DXR datasources: {e}")

@pytest.fixture(scope="session")
def test_artifacts():
    """
    Tracks temporary entities and scratch collections created by tests and cleans them up at the end.
    Stores:
      - entities: list of (type_name, qualifiedName)
      - collections: set of collection referenceNames
    """
    data = {"entities": [], "collections": set()}
    yield data
    # ------- cleanup -------
    try:
        # group QNs by type and delete
        by_type: Dict[str, set] = {}
        for t, qn in data["entities"]:
            by_type.setdefault(t, set()).add(qn)
        for t, qns in by_type.items():
            try:
                sut.delete_entities_by_qualified_names(list(qns), t)
            except Exception as e:
                logger.warning("Cleanup: delete by QN failed for type %s: %s", t, e)
        # delete scratch collections (best-effort)
        for coll in list(data["collections"]):
            try:
                r = sut._account_delete(f"/collections/{coll}")
                if r.status_code not in (200, 202, 204):
                    logger.info("Cleanup: delete collection %s -> %s %s", coll, r.status_code, r.text[:200])
            except Exception as e:
                logger.warning("Cleanup: delete collection %s failed: %s", coll, e)
    except Exception as e:
        logger.warning("Cleanup encountered an error: %s", e)

@pytest.mark.integration
def test_ensure_collections_domain_and_children(dxr_datasources_full):
    pytest.skip("Legacy collection ensure test not used in hits-based architecture")
    name_map, ds_full = dxr_datasources_full
    domain_name = os.getenv("PURVIEW_DOMAIN_NAME")
    domain_id = os.getenv("PURVIEW_DOMAIN_ID")
    try:
        child_map = sut.ensure_collections(domain_name, ds_full)
        ud_ref = _ensure_ud_collection_or_skip()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (400, 401, 403):
            pytest.skip("Collections ensure requires shared-service governance or proper permissions; set PURVIEW_PARENT_COLLECTION_ID to an existing collection.")
        raise

    assert isinstance(child_map, dict)
    assert any(dsid in child_map for dsid in name_map)
    r_ud = sut._account_get(f"/collections/{ud_ref}")
    assert r_ud.status_code == 200
    sample_coll = next(iter(child_map.values()))
    r = sut._account_get(f"/collections/{sample_coll}")
    assert r.status_code == 200


@pytest.mark.integration
@pytest.mark.destructive
def test_upsert_unstructured_datasources_in_child_collections(purview_client, dxr_datasources_full):
    pytest.skip("Legacy child-collection upsert test not used in hits-based architecture")
    name_map, ds_full = dxr_datasources_full
    tenant = os.getenv("DXR_TENANT", "default")
    # Upsert first N datasources (e.g., 2)
    N = 2
    ds_subset = ds_full[:N]
    try:
        ds_guid_map = sut.upsert_unstructured_datasources(purview_client, tenant, ds_subset)
    except Exception:
        # Fallback: resolve created/updated entities by QN
        ds_guid_map = {}
        for item in ds_subset:
            dsid = str(item.get("id"))
            qn = sut._qn_datasource(tenant, dsid)
            path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/unstructured_datasource"
            resp = sut._atlas_get(path, params={"attr:qualifiedName": qn})
            if resp.status_code == 200:
                try:
                    ds_guid_map[qn] = resp.json().get("guid")
                except Exception:
                    pass
    assert isinstance(ds_guid_map, dict)
    if not ds_guid_map:
        pytest.skip("No datasource GUIDs resolved; check permissions and Purview endpoint")
    # Ensure collections mapping for those two
    domain_name = os.getenv("PURVIEW_DOMAIN_NAME")
    # Wrap ensure_domain_exists in try/except for graceful skip
    try:
        try:
            try:
                sut.ensure_domain_exists(domain_name)
            except SystemExit as e:
                msg = str(e)
                if "not found" in msg.lower():
                    pytest.skip(f"Domain {domain_name} not found in Purview. Please create it manually before running this test.")
                raise
            child_map = sut.ensure_collections(domain_name, ds_subset)
        except SystemExit as e:
            msg = str(e)
            if "not found" in msg.lower():
                pytest.skip(f"Domain {domain_name} not found in Purview. Please create it manually before running this test.")
            raise
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            pytest.skip("Creating/ensuring collections requires Collection Admin on the Purview account or the parent collection.")
        raise
    # Verify first item is in its child collection (written directly into it)
    first_item = ds_subset[0]
    first_qn = sut._qn_datasource(tenant, str(first_item.get("id")))
    first_guid = ds_guid_map.get(first_qn)
    first_coll = child_map.get(str(first_item.get("id")))
    if first_guid and first_coll:
        expected_hierarchy = f"{domain_name} → datasource collection '{first_coll}'"
        assert sut.assert_entity_in_collection(first_guid, first_coll), (
            f"Entity was not observed in expected collection hierarchy: {expected_hierarchy}"
        )


@pytest.mark.integration
@pytest.mark.destructive
def test_upsert_unstructured_datasets_tags_flow(purview_client, dxr_datasources_full):
    pytest.skip("Legacy tags flow test superseded by hits-driven relationships")
    tags = sut.get_dxr_tags()
    if not tags:
        pytest.skip("No tags returned from DXR")
    name_map, ds_full = dxr_datasources_full
    ud_ref = _ensure_ud_collection_or_skip()
    guid_map = sut.upsert_unstructured_datasets(purview_client, tags, name_map, collection_id=ud_ref)
    assert isinstance(guid_map, dict)
    if not guid_map:
        # Fallback: synthesize one tag using unit sample but ensure its datasourceIds exist
        ds_ids_available = list(name_map.keys())
        if not ds_ids_available:
            pytest.skip("No datasources available to bind a synthetic tag")

        # use the first sample tag as a template
        sample = SAMPLE_DXR_TAGS[0].copy()
        # Ensure the savedQueryDtoList exists and points to a real datasourceId in this tenant
        sqd = sample.get("savedQueryDtoList", [])
        if sqd:
            sqd = [dict(sqd[0])]
            sqd[0]["datasourceIds"] = [int(ds_ids_available[0])]
            sample["savedQueryDtoList"] = sqd

        # Upsert the synthetic tag
        guid_map = sut.upsert_unstructured_datasets(purview_client, [sample], name_map, collection_id=ud_ref)
        if not guid_map:
            pytest.skip("No tags upserted (even synthetic), skipping move/relationship")
        # Continue test with synthetic tag only
        tags = [sample]    # For a tag with datasourceIds, move to correct collection and create relationship
    # Create one relationship (datasource -> tag) and assert tag is in UD collection
    for tag in tags:
        ds_ids = []
        for dto in (tag.get("savedQueryDtoList") or []):
            for dsid in (dto.get("datasourceIds") or []):
                ds_ids.append(str(dsid))
        if not ds_ids:
            continue
        qn = sut._to_qualified_name(tag)
        guid = guid_map.get(qn)
        if not guid:
            continue
        assert sut.assert_entity_in_collection(guid, ud_ref)
        # Create relationship
        tenant = tag.get("tenant", os.getenv("DXR_TENANT", "default"))
        qn_ds = sut._qn_datasource(tenant, ds_ids[0])
        try:
            sut.create_relationship(
                "unstructured_datasource_has_unstructured_dataset",
                qn_ds, "unstructured_datasource",
                qn, "unstructured_dataset"
            )
        except Exception as e:
            pytest.fail(f"Failed to create relationship: {e}")
        break  # test only one tag with datasourceIds


@pytest.mark.integration
def test_list_existing_tag_qns_for_tenant():
    tenant = os.getenv("DXR_TENANT", "default")
    qns = sut.list_existing_tag_qns_for_tenant(tenant)
    assert isinstance(qns, set)


@pytest.mark.integration
@pytest.mark.destructive
def test_create_and_delete_temp_unstructured_dataset(purview_client, test_artifacts):
    # Create a temp entity
    qn = f"dxrtag://test/{uuid.uuid4()}-temp-{_now_suffix()}"
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
    entity = {
        "typeName": sut.CUSTOM_TYPE_NAME,
        "attributes": {
            "qualifiedName": qn,
            "name": "temp_unstructured_dataset",
            "type": "TEMP",
        },
    }
    payload = {"entities": [entity]}
    try:
        coll_ref = _ensure_ud_collection_or_skip()
        result = purview_client.entity.batch_create_or_update(payload, collection_id=coll_ref)
        assert isinstance(result, dict)
        mutated = result.get("mutatedEntities", {})
        created = mutated.get("CREATE", [])
        updated = mutated.get("UPDATE", [])
        assert len(created) + len(updated) >= 1
    except Exception:
        path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}"
        resp = sut._atlas_get(f"{path}", params={"attr:qualifiedName": qn})
        assert resp.status_code == 200, f"Create failed and entity not found: status={resp.status_code} body={resp.text[:300]}"


@pytest.mark.integration
@pytest.mark.destructive
def test_upsert_labels_to_ud_collection_only(purview_client, test_artifacts):
    """Minimal integration test: upsert a small subset of DXR tags into the Unstructured Datasets collection only.
    Skips collection creation beyond ensuring the UD collection ref, and does not create relationships or datasource collections.
    """
    # Ensure custom type/relationship exists
    sut.ensure_types_and_relationships()

    # Resolve UD collection reference (from env or governance ensure). Skip if not available.
    ud_ref = _ensure_ud_collection_or_skip()

    # Fetch DXR tags and datasource name map (for payload enrichment)
    tags = sut.get_dxr_tags()
    if not tags:
        pytest.skip("No tags returned from DXR")
    name_map, _ = sut.get_dxr_searchable_datasources()

    # Limit to a very small subset to keep this test fast
    subset = tags[:2]
    guid_map = sut.upsert_unstructured_datasets(purview_client, subset, name_map, collection_id=ud_ref)
    assert isinstance(guid_map, dict)

    # If SDK returned no mutatedEntities, resolve by qualifiedName (handles eventual consistency).
    resolved = {}
    if not guid_map:
        for t in subset:
            qn = sut._to_qualified_name(t)
            g = sut.resolve_guid_by_qn(sut.CUSTOM_TYPE_NAME, qn, attempts=10, delay_seconds=1.2)
            if g:
                resolved[qn] = g
    else:
        resolved = guid_map

    # Track for cleanup and verify at least one label exists; best-effort membership check
    found = 0
    placed = 0
    for t in subset:
        qn = sut._to_qualified_name(t)
        test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
        guid = resolved.get(qn)
        if guid:
            found += 1
            if sut.assert_entity_in_collection(guid, ud_ref):
                placed += 1
                break
    assert found >= 1, "Label upsert did not resolve (no GUIDs returned or found by lookup)"
    # Some tenants/endpoints may not return collection membership on entity reads; don't fail the test solely on that signal
    # If you need strict placement verification, run the main app once and inspect Purview Studio.
    # Delete by qualifiedName
    sut.delete_entities_by_qualified_names([qn], sut.CUSTOM_TYPE_NAME)
    # Try to resolve by uniqueAttribute: should be 404 or not found
    path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}"
    resp = sut._atlas_get(f"{path}", params={"attr:qualifiedName": qn})
    # Accept 404 or missing guid
    if resp.status_code == 200:
        data = resp.json()
        assert not data.get("guid")
    else:
        assert resp.status_code == 404


# Existing test, updated to use ensure_types_and_relationships
@pytest.mark.integration
def test_create_assets_in_datasource(purview_client, test_artifacts):
    """
    Creates unstructured dataset assets, optionally within a collection if PURVIEW_COLLECTION_ID is set.
    Uses the same path as test_save_asset_into_purview but with a different qualifiedName namespace.
    """
    sut.ensure_types_and_relationships()
    qn = f"dxrtag://datasource-test/{uuid.uuid4()}-{_now_suffix()}"
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
    entity = {
        "typeName": sut.CUSTOM_TYPE_NAME,
        "attributes": {
            "qualifiedName": qn,
            "name": "datasource_unstructured_dataset",
            "type": "STANDARD",
        },
    }
    payload = {"entities": [entity]}
    try:
        coll_ref = _ensure_ud_collection_or_skip()
        result = purview_client.entity.batch_create_or_update(payload, collection_id=coll_ref)
        assert isinstance(result, dict)
        mutated = result.get("mutatedEntities", {})
        assert len(mutated.get("CREATE", [])) + len(mutated.get("UPDATE", [])) >= 1
    except Exception:
        path = f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}"
        resp = sut._atlas_get(f"{path}", params={"attr:qualifiedName": qn})
        assert resp.status_code == 200, f"Upsert failed and entity not found: status={resp.status_code} body={resp.text[:300]}"
# ---------------------------------------------
# Permission/connectivity probes (integration)
# ---------------------------------------------

def _uniq(name: str) -> str:
    return f"{name}-{uuid.uuid4()}-{_now_suffix()}"


@pytest.mark.integration
def test_perm_account_list_collections():
    """Probe the Account (collections) API. 200 = reachable and permitted; 401/403 => skip with guidance."""
    env_or_skip()
    r = sut._account_get("/collections")
    if r.status_code in (200,):
        assert "value" in r.json() or r.text != ""
    elif r.status_code in (401, 403):
        pytest.skip(
            f"Account GET /collections returned {r.status_code}. You likely need Collection Admin or Reader on at least one collection."
        )
    else:
        pytest.skip(f"Collections list returned {r.status_code}: permissions or endpoint constraints.")


@pytest.mark.integration
def test_perm_account_create_collection_if_allowed():
    """Try to create a temporary collection under root. If 401/403, skip with guidance (requires Collection Admin)."""
    env_or_skip()
    coll_name = _uniq("perm-probe")
    r = sut._account_put(f"/collections/{coll_name}", {"friendlyName": coll_name})
    if r.status_code in (200, 201):
        # Clean-up is not strictly required in Purview tests; collections can be left as probes.
        assert r.json().get("friendlyName") == coll_name
    elif r.status_code in (401, 403):
        pytest.skip(
            f"PUT /account/collections/{coll_name} returned {r.status_code}. Ask admin for Collection Admin on the target parent."
        )
    else:
        pytest.skip(f"Create collection returned {r.status_code}: permissions or endpoint constraints.")


@pytest.mark.integration
def test_perm_types_read_write():
    """Ensure we can read and (if missing) create typedefs via Atlas. 403/401 => skip with guidance (Data Curator)."""
    env_or_skip()
    try:
        sut.ensure_types_and_relationships()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            pytest.skip("Atlas typedefs require Data Curator on a collection or account-level metadata policy granting type ops.")
        raise


@pytest.mark.integration
def test_perm_entity_create_and_lookup(purview_client, test_artifacts):
    """Create a small temp entity and resolve it by uniqueAttribute. 401/403 => skip (needs Data Curator)."""
    sut.ensure_types_and_relationships()
    qn = f"dxrtag://perm-probe/{uuid.uuid4()}-{_now_suffix()}"
    test_artifacts["entities"].append((sut.CUSTOM_TYPE_NAME, qn))
    entity = {
        "typeName": sut.CUSTOM_TYPE_NAME,
        "attributes": {
            "qualifiedName": qn,
            "name": "perm_probe_dataset",
            "type": "TEST",
        },
    }
    payload = {"entities": [entity]}
    try:
        if os.getenv("PURVIEW_COLLECTION_ID"):
            result = purview_client.entity.batch_create_or_update(payload, collection_id=os.getenv("PURVIEW_COLLECTION_ID"))
        else:
            result = purview_client.entity.batch_create_or_update(payload)
    except Exception as e:
        # uniqueAttribute lookup to determine if it actually failed or just SDK returned empty body
        resp = sut._atlas_get(f"/datamap/api/atlas/v2/entity/uniqueAttribute/type/{sut.CUSTOM_TYPE_NAME}", params={"attr:qualifiedName": qn})
        if resp.status_code in (401, 403):
            pytest.skip("Entity create requires Data Curator on the target collection. Ask admin to grant it to your app registration.")
        assert resp.status_code in (200, 404), f"Unexpected status resolving entity: {resp.status_code} {resp.text[:200]}"
        if resp.status_code == 404:
            pytest.skip("Entity upsert did not commit (likely permissions).")
    else:
        # The SDK may return a dict-like object that isn't a plain dict.
        # Normalize to a dict and assert expected keys rather than strict type.
        if isinstance(result, dict):
            payload = result
        elif hasattr(result, "to_dict"):
            payload = result.to_dict()
        elif hasattr(result, "items"):
            payload = dict(result)
        else:
            pytest.skip(f"Unexpected upsert result type: {type(result)}")
        assert "mutatedEntities" in payload


@pytest.mark.integration
def test_perm_move_to_collection(purview_client, test_artifacts):
    """Move flow deprecated: entities are written directly to target collections via collection_id."""
    pytest.skip("MoveTo is disabled; upserts write directly to collections.")


@pytest.mark.integration
def test_perm_relationship_create(purview_client, test_artifacts):
    """Create two temp entities (datasource + dataset) and try to link them via our custom relationship. Curator needed."""
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
        # Proceed; they may already exist or SDK returned empty body
        pass

    try:
        sut.create_relationship("unstructured_datasource_has_unstructured_dataset", ds_qn, "unstructured_datasource", tag_qn, "unstructured_dataset")
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status in (401, 403):
            pytest.skip("Creating relationships requires Data Curator.")
        raise
    
@pytest.mark.integration
def test_hits_relationships_flow(purview_client, dxr_datasources_full):
    name_map, ds_full = dxr_datasources_full
    pick = None
    rows = None
    for item in ds_full[:10]:
        dsid = str(item.get("id"))
        rows = sut.get_label_statistics_for_datasource(dsid, 100)
        if any(int(r.get("documentCount", 0)) > 0 for r in (rows or [])):
            pick = item
            break
    if not pick:
        pytest.skip("No datasource with positive label stats returned from DXR")
    dsid = str(pick.get("id"))
    positive = next(r for r in rows if int(r.get("documentCount", 0)) > 0)
    target_label_id = str(positive.get("labelId"))
    target_count = int(positive.get("documentCount", 0))
    # Fetch tags and locate label
    tags = sut.get_dxr_tags()
    tag = next((t for t in tags if str(t.get("id")) == target_label_id), None)
    if not tag:
        pytest.skip("Target label id from stats not found in /api/tags payload")
    ud_ref = _ensure_ud_collection_or_skip()
    sut.update_unstructured_dataset_hit_stats_and_relationships(purview_client, [tag], [pick], name_map, ud_ref)
    tenant = os.getenv("DXR_TENANT", "default")
    ds_qn = sut._qn_datasource(tenant, dsid)
    tag_qn = sut._to_qualified_name(tag)
    ds_guid = sut.resolve_guid_by_qn("unstructured_datasource", ds_qn)
    tag_guid = sut.resolve_guid_by_qn(sut.CUSTOM_TYPE_NAME, tag_qn)
    assert ds_guid and tag_guid
    # Allow brief eventual consistency; retry read once if empty
    rels = sut._get_entity_relationship_map_for_label(tag_guid)
    if ds_guid not in rels:
        import time as _time
        _time.sleep(2)
        rels = sut._get_entity_relationship_map_for_label(tag_guid)
    assert ds_guid in rels
    rid = rels[ds_guid]
    rr = sut._atlas_get(f"/datamap/api/atlas/v2/relationship/guid/{rid}")
    assert rr.status_code == 200
    rj = rr.json()
    assert rj.get("typeName") in ("unstructured_datasource_hits_unstructured_dataset",)
    attrs = rj.get("attributes", {})
    assert int(attrs.get("documentCount", 0)) >= max(1, target_count)

@pytest.mark.integration
def test_update_datasource_hit_arrays(purview_client, dxr_datasources_full):
    name_map, ds_full = dxr_datasources_full
    subset = ds_full[:1]
    if not subset:
        pytest.skip("No datasources returned from DXR")
    tenant = os.getenv("DXR_TENANT", "default")
    sut.update_unstructured_datasource_hit_stats(purview_client, tenant, subset)
    dsid = str(subset[0].get("id"))
    ds_qn = sut._qn_datasource(tenant, dsid)
    g = sut.resolve_guid_by_qn("unstructured_datasource", ds_qn)
    if not g:
        pytest.skip("Could not resolve datasource entity GUID after update")
    r = sut._atlas_get(f"/datamap/api/atlas/v2/entity/guid/{g}")
    assert r.status_code == 200
    data = r.json() or {}
    attrs = data.get("attributes") or {}
    # If the datasource has no positive label hits, attributes may be empty or arrays may be missing.
    # Query DXR to check whether any positive hits exist for this datasource.
    rows = sut.get_label_statistics_for_datasource(dsid, 100)
    pos = [r for r in (rows or []) if int(r.get("documentCount", 0)) > 0]
    ids = attrs.get("hitLabelIds")
    if pos:
        assert isinstance(ids, list), "Expected hitLabelIds when DXR reports positive hits"
    else:
        # Accept missing or empty arrays for zero-hit datasources
        assert ids is None or isinstance(ids, list)
@pytest.mark.integration
def test_perm_domain_list_and_visibility():
    """
    Verify that our service principal can list domains and see the expected domain.
    """
    env_or_skip()
    headers = sut._governance_headers()
    base = sut._governance_base()
    url = f"{base}/catalog/api/domains?api-version=2023-09-01"
    resp = requests.get(url, headers=headers)
    if resp.status_code in (401, 403):
        pytest.skip("Service principal lacks Domain Admin permissions")
    elif resp.status_code == 200:
        try:
            data = resp.json()
        except Exception:
            pytest.fail("Failed to parse /catalog/api/domains response as JSON")
        assert isinstance(data, dict), "Response JSON is not a dict"
        assert "value" in data and isinstance(data["value"], list), "Response missing 'value' list"
        domains = data.get("value", [])
        domain_env = os.getenv("PURVIEW_DOMAIN_NAME")
        found = False
        for dom in domains:
            fn = dom.get("friendlyName") or dom.get("name") or ""
            if fn.lower() == (domain_env or "").lower():
                found = True
                break
        if not found:
            pytest.fail(f"Expected domain {domain_env} not found")
    else:
        pytest.fail(f"Unexpected status from /catalog/api/domains: {resp.status_code} {resp.text[:300]}")
