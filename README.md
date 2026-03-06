# Exasol Workshop Starter

## Getting Started

### 1. Fork this repo

Click the **Fork** button at the top right of this page to create your own copy.

### 2. Set the passphrase

Your instructor will share the passphrase during the workshop.

Via the web UI:

1. Go to [github.com/settings/codespaces](https://github.com/settings/codespaces) → New secret
2. Name: `WORKSHOP_PASSPHRASE`
3. Value: the passphrase from your instructor
4. Repository access: select your fork (`<your-username>/exasol-workshop-starter`)

Or via the CLI:

```bash
gh secret set WORKSHOP_PASSPHRASE --user --repos <your-username>/exasol-workshop-starter --app codespaces
# paste the passphrase when prompted
```

### 3. Create a Codespace

Via the web UI: go to your fork → Code → Codespaces → Create codespace on main

Or via the CLI:

```bash
gh codespace create --repo <your-username>/exasol-workshop-starter --branch main --machine basicLinux32gb
gh codespace ssh  # or open in VS Code
```

AWS access is configured automatically. Verify:

```bash
aws sts get-caller-identity
```

Credentials refresh automatically in the background. No keys to manage.

### 4. Install the Exasol Launcher

```bash
curl https://downloads.exasol.com/exasol-personal/installer.sh | bash
```

This downloads the `exasol` binary to your home directory.

### 5. Deploy Exasol Personal Edition

```bash
export AWS_DEFAULT_REGION=eu-west-1
mkdir -p ~/deployment && cd ~/deployment
~/exasol install
```

Accept the EULA when prompted. The deployment takes 10-20 minutes. It provisions an EC2 instance with Exasol installed.

### 6. Check status and connect

```bash
cd ~/deployment
~/exasol status
~/exasol info       # shows connection details (host, port, password)
~/exasol connect    # opens built-in SQL client
```

### 7. When you're done

Stop the instance to save costs:

```bash
cd ~/deployment
~/exasol stop
```

To resume later:

```bash
cd ~/deployment
~/exasol start
~/exasol info    # IPs change after restart
```

To destroy everything:

```bash
cd ~/deployment
~/exasol destroy
```

## Troubleshooting

- Codespace created before setting the secret? Rebuild it: `Cmd/Ctrl+Shift+P` → "Rebuild Container"
- "Wrong passphrase"? Double-check with your instructor
- Permission errors on AWS? Ask your instructor — the role may need updated permissions
- `exasol install` fails? Make sure `aws sts get-caller-identity` works first
- Lock file error? Remove `~/deployment/.exasolLock.json` and retry
