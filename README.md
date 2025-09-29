# ohalo-playbooks

Welcome to the **ohalo-playbooks** repository! This repo aggregates automation playbooks, reference applications, and infrastructure blueprints used to accelerate data governance and security workflows with Data X-Ray.

## Playbooks

This repository currently includes the following playbooks:

- **[`rag-app`](./rag-app/)**  
  Full-stack Retrieval Augmented Generation demo that helps teams surface institutional knowledge in seconds, pairing a FastAPI + Postgres backend with a Next.js frontend for rapid iteration. Check the [README](./rag-app/README.md) in the folder for environment setup and developer workflow details.

- **[`scan-email-attachments`](./scan-email-attachments/)**  
  SMTP proxy playbook that intercepts outbound attachments to prevent sensitive data leakage and orchestrate remediation workflows before messages leave the organization. Refer to the [README](./scan-email-attachments/README.md) in the folder for setup and execution steps.

- **[`purview-stream-endpoint`](./purview-stream-endpoint/)**  
  Azure Purview integration scripts and helpers that unlock continuous catalog exports so governance leads can monitor data health and lineage in near real time. Start with the [README](./purview-stream-endpoint/README.md) for usage instructions and integration guidance.

- **[`dxr-metadata-athena-pipeline`](./dxr-metadata-athena-pipeline/)**  
  Terraform-driven pipeline that lifts Data X-Ray's metadata plane into AWS Glue and exposes it through Athena so governance teams can run ad-hoc discovery and compliance queries. The [README](./dxr-metadata-athena-pipeline/README.md) breaks down deployment and query workflows.

- **[`glue-unstructured-dq-monitoring`](./glue-unstructured-dq-monitoring/)**
  Lambda, Glue, and Data Quality assets that score unstructured files using Data X-Ray labels and AWS Glue DQ rulesets, keeping data lakes clean before ETL jobs consume sensitive content. See the [README](./glue-unstructured-dq-monitoring/README.md) for setup and runbook commands.

- **[`s3-etl-quarantine`](./s3-etl-quarantine/)**
  Pulumi-managed guardrail that watches S3 staging zones, classifies files with Data X-Ray, and automatically quarantines risky payloads feeding unstructured ETL pipelines. The [README](./s3-etl-quarantine/README.md) covers prerequisites, deployment, and cleanup steps.

- **[`atlan-dxr-integration`](./atlan-dxr-integration/)**
  Containerised service that syncs Data X-Ray classifications into Atlan as assets using the official Python SDK. Review the [README](./atlan-dxr-integration/README.md) for configuration, runtime options, and deployment guidance.

## Getting Started

Each playbook is self-contained in its own directory and includes a dedicated `README.md` with all the information you need to configure and run it.

Clone the repository and navigate to the relevant folder to get started:

```bash
git clone https://github.com/your-org/ohalo-playbooks.git
cd ohalo-playbooks/<playbook-name>
```

## Legal Disclaimer
This repository and the contents within are provided "as is" and without any warranty, express or implied. Use of the playbooks is at your own risk. Ohalo Ltd. is not responsible for any loss, damage, or issues that may arise from the use, modification, or distribution of this code.

By using this repository, you agree to assume full responsibility for any consequences that result from its use.

---

For questions or contributions, please open an issue or submit a pull request.
**Happy coding!**
