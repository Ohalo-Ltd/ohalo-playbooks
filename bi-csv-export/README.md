# BI CSV Export — Data X-Ray to Analysis-Ready Tables

Export your entire Data X-Ray catalogue to five flat CSV files that load directly into Tableau, Power BI, Excel, or any SQL-based analytics tool — no cloud infrastructure required.

## Why this playbook?

`GET /api/v1/files` returns a rich JSONL stream where labels, annotations, and extracted metadata are nested inside each file record.  BI tools expect flat tables.  This playbook splits one JSONL stream into five normalised tables so every BI query is a simple join or filter.

## Output tables

| File | Rows | Description |
|------|------|-------------|
| `files.csv` | 1 per file | Scalar file properties: path, size, MIME type, timestamps, datasource, owner |
| `labels.csv` | 1 per file × label | Smart labels and manual tags applied to each file |
| `annotations.csv` | 1 per file × annotator × value | Every detected entity: person names, email addresses, phone numbers, SSNs, etc. |
| `extracted_metadata.csv` | 1 per file × extractor field | LLM extractor output: booleans, classifications, free-text summaries |
| `dlp_labels.csv` | 1 per file × DLP label | DLP policy hits with confidence scores |

All tables share `file_id` as the join key.

### Example — files with PII label by datasource

```sql
SELECT
    f.datasource_name,
    COUNT(*) AS pii_file_count,
    SUM(f.size_bytes) / 1e9 AS total_gb
FROM files f
JOIN labels l ON l.file_id = f.file_id
WHERE l.label_name = 'PII'
GROUP BY f.datasource_name
ORDER BY pii_file_count DESC;
```

### Example — top annotated entities across the estate

```sql
SELECT annotator_name, COUNT(*) AS hits
FROM annotations
GROUP BY annotator_name
ORDER BY hits DESC;
```

### Example — LLM extractor output for HR documents

```sql
SELECT f.file_name, em.field_name, em.value
FROM files f
JOIN extracted_metadata em ON em.file_id = f.file_id
WHERE f.path LIKE '/HR/%'
ORDER BY f.file_name, em.field_name;
```

## Quickstart

### Prerequisites

- Python 3.10+
- `pip install requests` (only external dependency)
- A Data X-Ray Personal Access Token (PAT)

### Install

```bash
git clone https://github.com/ohalo-ltd/ohalo-playbooks.git
cd ohalo-playbooks/bi-csv-export
pip install .
```

### Configure

```bash
cp .env.example .env
# Edit .env — set DXR_BASE_URL and DXR_BEARER_TOKEN
```

Or export directly:

```bash
export DXR_BASE_URL=https://your-dxr-instance.example.com
export DXR_BEARER_TOKEN=pat_abc123...
```

### Run

```bash
python scripts/export_to_csv.py --output-dir ./output
```

**Output:**

```
✓ Export complete — 42,831 files → ./output/
  files.csv                      42831 rows
  labels.csv                     68940 rows
  annotations.csv               312475 rows
  extracted_metadata.csv         85662 rows
  dlp_labels.csv                  9104 rows
```

### Options

| Argument | Env variable | Default | Description |
|----------|--------------|---------|-------------|
| `--url` | `DXR_BASE_URL` | — | Data X-Ray base URL (required) |
| `--token` | `DXR_BEARER_TOKEN` | — | Bearer token / PAT (required) |
| `--output-dir` | `DXR_OUTPUT_DIR` | `./output` | CSV output directory |
| `--query` | `DXR_QUERY` | — | KQL filter on `/api/v1/files` |
| `--record-cap` | `DXR_RECORD_CAP` | 0 (no cap) | Stop after N records (testing) |
| `--no-verify-ssl` | `DXR_NO_VERIFY_SSL` | off | Skip TLS verification |
| `--http-timeout` | `DXR_HTTP_TIMEOUT` | 120 | HTTP timeout (seconds) |
| `--verbose` | — | off | Debug logging |

### Filter by datasource or label

```bash
# Only files from one datasource
python scripts/export_to_csv.py \
  --output-dir ./output \
  --query "datasource.id:123"

# Only PII-labelled files
python scripts/export_to_csv.py \
  --output-dir ./output \
  --query "labels.name:PII"

# Sample the first 500 records (fast smoke-test)
python scripts/export_to_csv.py \
  --output-dir ./sample \
  --record-cap 500
```

## Loading into BI tools

### Tableau

1. Open Tableau Desktop → **Connect → Text File**
2. Select `files.csv`
3. In the data source pane, drag `labels.csv`, `annotations.csv` etc. onto the canvas
4. Join on `file_id = file_id`
5. Build your viz

### Power BI

1. **Get Data → Text/CSV** → select `files.csv`
2. Repeat for the other tables
3. In **Model view**, create relationships on `file_id`
4. Build your report

### Excel / SQL (DuckDB)

```sql
-- DuckDB: instant analysis without loading everything into memory
SELECT
    f.datasource_name,
    l.label_name,
    COUNT(*) AS files
FROM read_csv_auto('output/files.csv') f
JOIN read_csv_auto('output/labels.csv') l ON l.file_id = f.file_id
GROUP BY f.datasource_name, l.label_name
ORDER BY files DESC;
```

## Project structure

```
bi-csv-export/
├── src/dxr_to_csv/
│   ├── client.py       Streaming /api/v1/files client
│   ├── splitter.py     Splits JSONL records into 5 table buffers
│   └── writer.py       Writes tables to CSV files
├── scripts/
│   └── export_to_csv.py  CLI entry point
├── tests/
│   ├── fixtures/
│   │   └── sample_files.jsonl  4 representative DXR file records
│   └── test_splitter.py
├── sample_output/          Example CSVs generated from fixtures
├── .env.example
└── pyproject.toml
```

## Running tests

```bash
pip install -e ".[dev]"
pytest -m unit
```

## Related playbooks

- **[file-records-management-in-databricks](../file-records-management-in-databricks/)** — Delta Lake snapshot for records management policies
- **[dxr-metadata-athena-pipeline](../dxr-metadata-athena-pipeline/)** — AWS Athena for ad-hoc governance queries
- **[glue-unstructured-dq-monitoring](../glue-unstructured-dq-monitoring/)** — AWS Glue DQ rulesets over DXR labels
