# ohalo-playbooks

Welcome to the **ohalo-playbooks** repository! This repo contains a collection of Ansible playbooks used to automate common workflows and integrations at Ohalo.

## Playbooks

Currently, this repository includes the following playbooks:

- **[`export-stream-endpoint`](./export-stream-endpoint/)**  
  Automates the setup and management of an export stream endpoint. See the [README](./export-stream-endpoint/README.md) in the folder for detailed instructions and usage information.

- **[`scan-email-attachments`](./scan-email-attachments/)**  
  Scans email attachments for sensitive data and integrates with external tools for processing. Refer to the [README](./scan-email-attachments/README.md) in the folder for setup and execution steps.

## Getting Started

Each playbook is self-contained in its own directory and includes a dedicated `README.md` with all the information you need to configure and run it.

Clone the repository and navigate to the relevant folder to get started:

```bash
git clone https://github.com/your-org/ohalo-playbooks.git
cd ohalo-playbooks/<playbook-name>
```

For questions or contributions, please open an issue or submit a pull request.
**Happy coding!**
