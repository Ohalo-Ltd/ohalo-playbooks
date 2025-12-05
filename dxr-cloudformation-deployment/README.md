# DXR single-server CloudFormation wrapper

This directory contains the infrastructure glue required to stand up a disposable
Ansible control host (Rocky Linux 8, `c5.4xlarge`) and run the
`deploy-dxr-single-server-playbook.yml` playbook that lives in
`../dxr/ohalo-ansible`.

## Contents
- `dxr-single-server-cloudformation.yaml` – full CloudFormation template that
  builds a fresh VPC, public subnet, security group, IAM role, and the DXR host.
- `package-and-deploy-dxr.sh` – helper script that bundles the local
  `../dxr/ohalo-ansible` tree, uploads it plus an inventory file to S3, and
  optionally deploys the stack.

## Prerequisites
1. AWS CLI v2 configured with credentials that can read/write to your artifact
   bucket and create CloudFormation/IAM/EC2 resources.
2. An existing EC2 key pair in the target Region (needed for SSH access).
3. An S3 bucket to temporarily host the packaged ohalo-ansible archive and your
   inventory JSON.
4. (Optional) Update the provided inventory example located at
   `../dxr/ohalo-ansible/inventory-example-dxr-single-server.json` with the DXR
   secrets, domain, and other environment-specific values.

## Preparing the inventory
The user-data on the instance automatically replaces the `ansible_host` and
`internal_ip` attributes with the instance's public and private IPs when the
value is set to the literal strings `AUTO_PUBLIC_IP` and `AUTO_PRIVATE_IP`.
All other variables must already be populated before packaging. A minimal
inventory file therefore looks like:

```json
{
  "ohalo": {
    "hosts": {
      "ohalo-single-server": {
        "ansible_host": "AUTO_PUBLIC_IP",
        "internal_ip": "AUTO_PRIVATE_IP",
        "server_url": "dxr.example.com",
        "dxr_version": "latest"
      }
    }
  }
}
```

Save the customized JSON locally and pass the path to the helper script via the
`--inventory` flag.

## Uploading artifacts and deploying the stack
Example invocation (adjust the placeholders):

```bash
./package-and-deploy-dxr.sh \
  --artifact-bucket my-dxr-artifacts \
  --artifact-prefix customer-a/dxr \
  --inventory ../dxr/ohalo-ansible/inventory-example-dxr-single-server.json \
  --stack-name customer-a-dxr \
  --key-pair my-ec2-key \
  --ssh-location 203.0.113.0/24 \
  --region us-east-1
```

The template parameter `LatestAmiId` now accepts a direct AMI ID and defaults to
`ami-02391db2758465a87` (Rocky Linux 8 in `us-east-2`). Override it to match the
Region you deploy into.

What the script does:
1. Creates a timestamped `dxr-ansible-<timestamp>.tar.gz` from
   `../dxr/ohalo-ansible` and stores it in this directory.
2. Uploads the tarball and the provided inventory JSON to the specified S3
   bucket/prefix.
3. Runs `aws cloudformation deploy` with the correct parameter overrides (unless
   `--skip-deploy` is provided).

## Stack behaviour
- The Rocky Linux instance installs Python 3, pip, Ansible, jq, git, tar, unzip, and the AWS CLI.
- The playbook runs directly on the EC2 host; logs are streamed into
  `/var/log/dxr-ansible.log` and also surfaced in the CloudFormation console via
  the resource signal.
- The stack exports the instance ID plus public/private IPs as outputs.
- If the Ansible run fails the stack enters the `CREATE_FAILED` state, making it
  easy to troubleshoot without leaving orphaned infrastructure.

## Cleanup
Once validation is complete you can remove all resources with:

```bash
aws cloudformation delete-stack --stack-name <stack-name> [--region <region>]
```

Remember to occasionally purge old `dxr-ansible-*.tar.gz` archives from this
folder and from the S3 bucket.
