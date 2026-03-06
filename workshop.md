# Exasol Workshop

## Setting Up Exasol

### Install the Exasol CLI

The `exasol` CLI is pre-installed in the Codespace. If you're not using Codespaces, install it manually:

```bash
mkdir -p ~/bin
curl https://downloads.exasol.com/exasol-personal/installer.sh | bash
mv exasol ~/bin/
```

Or download it from the [Exasol Personal Edition page](https://downloads.exasol.com/exasol-personal) and place it in `~/bin/`.

### Deploy Exasol Personal Edition

Create a deployment directory and run the installer:

```bash
export AWS_DEFAULT_REGION=eu-central-1
mkdir deployment
cd deployment
exasol install
```

Accept the EULA when prompted. The deployment takes 7-10 minutes.

All `exasol` commands must be run from within the deployment directory.

#### Default settings

This deploys a single-node cluster with sensible defaults:
- 1 node
- Instance type: `r6i.xlarge` (4 vCPUs, 32 GB RAM)

#### Custom install

To customize cluster size or instance type:

```bash
exasol install --cluster-size 3 --instance-type r6i.2xlarge
```

Available instance types:

| Instance Type | vCPUs | RAM    | Use Case              |
|---------------|-------|--------|-----------------------|
| r6i.xlarge    | 4     | 32 GB  | Default, getting started |
| r6i.2xlarge   | 8     | 64 GB  | Larger workloads      |
| r6i.4xlarge   | 16    | 128 GB | High performance      |

#### What happens during deployment

The `exasol install` command:

1. Generates Terraform files in the deployment directory
2. Provisions AWS infrastructure (VPC, EC2, security groups, etc.)
3. Starts up the infrastructure
4. Downloads and installs Exasol Personal on the EC2 instance

If the deployment process is interrupted, EC2 instances may continue to accrue costs. Check the AWS console and manually terminate any orphaned instances.

#### Completion

When the deployment finishes, you will see:

- Connection details for the database
- Exasol Admin URL
- SSH access information
- Where to find passwords (`secrets-exasol-<id>.json`)

#### .gitignore

If you're working in a git repo, add these to `.gitignore` to prevent committing sensitive files:

```gitignore
deployment/secrets-*.json
deployment/*.pem
deployment/terraform.tfstate
deployment/.terraform/
deployment/tofu
deployment/.workflowState.json
```

### Check the status

Once the deployment finishes, check that the database is running:

```bash
exasol status
```

You should see `database_ready` in the output.

### Get connection details

```bash
exasol info
```

This shows the host, port, and password for your Exasol instance. The connection details are also saved in:

- `deployment-exasol-<id>.json` - host, port, DNS name
- `secrets-exasol-<id>.json` - database and admin UI passwords
- `exasol-<id>.pem` - SSH private key for EC2 access
- `terraform.tfstate` - Terraform state (tracks AWS resources)

The scripts in `code/` read these files automatically to connect to the database.

### Connect to the database

Use the built-in SQL client:

```bash
exasol connect
```

Try a simple query:

```sql
SELECT 'Hello, Exasol!' AS greeting;
```

Type `quit` or press `Ctrl+D` to exit.

### Get connection details for VS Code

If you're not using the Codespace (where `code/` is already set up), download the script:

```bash
mkdir -p code && cd code
uv init
wget https://raw.githubusercontent.com/alexeygrigorev/exasol-workshop-starter/main/code/connection_info.py
```

Run it:

```bash
uv run python connection_info.py
```

This prints everything you need: host, port, username, password, and the TLS certificate fingerprint.

When connecting from VS Code, use "Fingerprint (pin certificate)" for TLS validation and paste the fingerprint from the output.

### Stopping and resuming

Stop the instance when you're not using it (to save costs):

```bash
exasol stop
```

Resume later:

```bash
exasol start
exasol info    # IPs change after restart
```

### Destroying the deployment

When you're completely done with the workshop:

```bash
exasol destroy
```

This terminates the EC2 instance and cleans up all AWS resources.

### Additional help

```bash
exasol help
exasol install --help
```

## Loading NHS Prescription Data

### Install dependencies

```bash
cd ~/code
uv add pyexasol requests
```

### Find available data URLs

```bash
uv run python find_urls.py
```

This queries the NHS BSA Open Data Portal API and saves `prescription_urls.json` with ~138 months of data (2014-present, ~840 GB total).

### Stage data

Load one month to test (each month is ~6 GB, ~18M rows):

```bash
uv run python ingest.py stage -t 1 -n 1
```

To load more months in parallel (e.g. 6 months with 2 threads):

```bash
uv run python ingest.py stage -t 2 -n 6
```

### Create the final table

```bash
uv run python ingest.py finalize
```

### Clean up staging tables

```bash
uv run python ingest.py cleanup
```

### Check what's loaded

```bash
uv run python ingest.py summary
```

### Run the challenge queries

```bash
uv run python ingest.py query
```

This runs two queries on East Central London (EC postcodes):
1. Top 3 most prescribed chemicals
2. The year with the most prescriptions of the top chemical

The scripts (`find_urls.py`, `ingest.py`) read connection details automatically from `~/deployment/deployment-exasol-*.json` and `~/deployment/secrets-exasol-*.json`.

## Troubleshooting

- Codespace created before setting the secret? Rebuild it: `Cmd/Ctrl+Shift+P` -> "Rebuild Container"
- "Wrong passphrase"? Double-check with your instructor
- Permission errors on AWS? Ask your instructor -- the role may need updated permissions
- `exasol install` fails? Make sure `aws sts get-caller-identity` works first
- Lock file error? Remove `~/deployment/.exasolLock.json` and retry
