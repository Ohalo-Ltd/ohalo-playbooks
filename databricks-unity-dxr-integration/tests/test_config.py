from databricks_unity_dxr_integration.config import load_config


def test_load_config_from_environment(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://dbc-example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi123")
    monkeypatch.setenv("DATABRICKS_CATALOG", "governance")
    monkeypatch.setenv("DATABRICKS_SCHEMA", "dxr")
    monkeypatch.setenv("DATABRICKS_SOURCE_VOLUME", "raw")
    monkeypatch.setenv("DATABRICKS_LABEL_VOLUME_PREFIX", "label_")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "warehouse-123")
    monkeypatch.setenv("DATABRICKS_CHECKPOINT_TABLE", "dxr_metadata.file_checkpoints")
    monkeypatch.setenv("DATABRICKS_JOB_LEDGER_TABLE", "dxr_metadata.job_ledger")
    monkeypatch.setenv("DATABRICKS_METADATA_TABLE", "dxr_metadata.file_metadata")

    monkeypatch.setenv("DXR_BASE_URL", "https://dxr.example.com/api")
    monkeypatch.setenv("DXR_API_KEY", "token")
    monkeypatch.setenv("DXR_DATASOURCE_ID", "42")
    monkeypatch.setenv("DXR_POLL_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("DXR_MAX_BYTES_PER_JOB", "1024")

    config = load_config(env_file=None)

    assert config.databricks.host == "https://dbc-example.cloud.databricks.com"
    assert config.databricks.label_volume_prefix == "label_"
    assert config.databricks.warehouse_id == "warehouse-123"
    assert config.databricks.checkpoint_table == "dxr_metadata.file_checkpoints"
    assert config.databricks.job_ledger_table == "dxr_metadata.job_ledger"
    assert config.databricks.metadata_table == "dxr_metadata.file_metadata"
    assert config.data_xray.base_url == "https://dxr.example.com/api"
    assert config.data_xray.poll_interval_seconds == 5
    assert config.data_xray.max_bytes_per_job == 1024
