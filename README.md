# ohalo-playbooks

Welcome to the **ohalo-playbooks** repository! This repo contains a collection of Ansible playbooks used to automate common workflows and integrations at Ohalo.

## Projects

This repository currently includes the following projects:

- **[`rag-app`](./rag-app/)**  
  Full-stack Retrieval Augmented Generation demo that helps teams surface institutional knowledge in seconds, pairing a FastAPI + Postgres backend with a Next.js frontend for rapid iteration. Check the [README](./rag-app/README.md) in the folder for environment setup and developer workflow details.

- **[`purview-stream-endpoint`](./purview-stream-endpoint/)**  
  Azure Purview integration scripts and helpers that unlock continuous catalog exports so governance leads can monitor data health and lineage in near real time. Start with the [README](./purview-stream-endpoint/README.md) for usage instructions and integration guidance.

- **[`export-stream-endpoint`](./export-stream-endpoint/)**  
  Terraform module and Lambda code that operationalize compliant data delivery pipelines, speeding up partner onboarding while keeping infrastructure reproducible. See the [README](./export-stream-endpoint/README.md) in the folder for detailed instructions and deployment information.

- **[`scan-email-attachments`](./scan-email-attachments/)**  
  SMTP proxy playbook that intercepts outbound attachments to prevent sensitive data leakage and orchestrate remediation workflows before messages leave the organization. Refer to the [README](./scan-email-attachments/README.md) in the folder for setup and execution steps.

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
