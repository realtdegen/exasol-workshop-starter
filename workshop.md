# Exasol Workshop

## Setting Up Exasol

### Deploy Exasol Personal Edition

The `exasol` CLI is pre-installed in the Codespace. Create a deployment directory and run the installer:

```bash
export AWS_DEFAULT_REGION=eu-west-1
mkdir -p ~/deployment && cd ~/deployment
exasol install
```

Accept the EULA when prompted. The deployment takes 10-20 minutes. It provisions an EC2 instance with Exasol database installed.

### Check the status

Once the deployment finishes, check that the database is running:

```bash
cd ~/deployment
exasol status
```

You should see `database_ready` in the output.

### Get connection details

```bash
exasol info
```

This shows the host, port, and password for your Exasol instance. The connection details are also saved in:

- `~/deployment/deployment-exasol-<id>.json` - host, port, DNS name
- `~/deployment/secrets-exasol-<id>.json` - database username and password

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

### Stopping and resuming

Stop the instance when you're not using it (to save costs):

```bash
cd ~/deployment
exasol stop
```

Resume later:

```bash
cd ~/deployment
exasol start
exasol info    # IPs change after restart
```

### Destroying the deployment

When you're completely done with the workshop:

```bash
cd ~/deployment
exasol destroy
```

This terminates the EC2 instance and cleans up all AWS resources.

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
