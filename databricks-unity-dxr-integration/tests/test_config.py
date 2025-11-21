from databricks_unity_dxr_integration.config import JobConfig, load_config


def _seed_required_env(monkeypatch):
    monkeypatch.setenv("VOLUME_CATALOG", "governance")
    monkeypatch.setenv("VOLUME_SCHEMA", "dxr")
    monkeypatch.setenv("VOLUME_NAME", "raw")
    monkeypatch.setenv("VOLUME_BASE_PATH", "/tmp/volumes")
    monkeypatch.setenv("VOLUME_PREFIX", "incoming")

    monkeypatch.setenv("METADATA_CATALOG", "governance")
    monkeypatch.setenv("METADATA_SCHEMA", "dxr")
    monkeypatch.setenv("METADATA_TABLE", "file_metadata")

    monkeypatch.setenv("DXR_BASE_URL", "https://dxr.example.com")
    monkeypatch.setenv("DXR_DATASOURCE_ID", "42")
    monkeypatch.setenv("DXR_POLL_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("DXR_MAX_BYTES_PER_JOB", "1024")

    monkeypatch.setenv("DXR_SECRET_SCOPE", "dxr")
    monkeypatch.setenv("DXR_SECRET_KEY", "api-token")


def test_load_config_from_environment(monkeypatch):
    _seed_required_env(monkeypatch)

    config = load_config(env_file=None)
    assert isinstance(config, JobConfig)
    assert config.volume.catalog == "governance"
    assert config.volume.base_path == "/tmp/volumes"
    assert config.volume.prefix == "incoming"
    assert config.metadata_table.table == "file_metadata"
    assert config.secret.scope == "dxr"
    assert config.dxr.base_url == "https://dxr.example.com"
    assert config.dxr.poll_interval_seconds == 5
    assert config.dxr.max_bytes_per_job == 1024
    assert config.dxr.verify_ssl is True
    assert config.dxr.ca_bundle_path is None
    assert config.dxr.api_prefix == "/api"


def test_load_config_disables_ssl_verification(monkeypatch):
    _seed_required_env(monkeypatch)
    monkeypatch.setenv("DXR_VERIFY_SSL", "false")
    monkeypatch.setenv("DXR_CA_BUNDLE_PATH", "/dbfs/FileStore/custom-ca.pem")

    config = load_config(env_file=None)

    assert config.dxr.verify_ssl is False
    assert config.dxr.ca_bundle_path == "/dbfs/FileStore/custom-ca.pem"


def test_load_config_normalizes_api_prefix(monkeypatch):
    _seed_required_env(monkeypatch)
    monkeypatch.setenv("DXR_API_PREFIX", "v1 ")

    config = load_config(env_file=None)

    assert config.dxr.api_prefix == "/v1"
