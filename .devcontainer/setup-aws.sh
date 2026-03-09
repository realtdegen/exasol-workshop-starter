#!/bin/bash
# .devcontainer/setup-aws.sh
# Decrypts bearer token, calls Lambda vending machine, writes AWS credentials.
set -euo pipefail

echo "================================================"
echo "CODESPACE: ${CODESPACE_NAME:-local}"
echo "COMMIT:    $(git log --oneline -1 2>/dev/null || echo unknown)"
echo "================================================"

if [ -z "${WORKSHOP_PASSPHRASE:-}" ]; then
  read -rsp "Enter workshop passphrase: " WORKSHOP_PASSPHRASE
  echo ""
fi

BEARER_TOKEN=$(echo "${WORKSHOP_TOKEN_ENC}" \
  | openssl enc -aes-256-cbc -d -a -pbkdf2 \
    -pass "pass:${WORKSHOP_PASSPHRASE}" 2>/dev/null || true)

if [ -z "${BEARER_TOKEN:-}" ]; then
  echo "❌ Wrong passphrase or corrupt token."
  echo "   Re-run: bash .devcontainer/setup-aws.sh"
  exit 1
fi

CREDS_JSON=$(curl -sf \
  -H "Authorization: ${BEARER_TOKEN}" \
  "${WORKSHOP_CRED_URL}" || true)

if [ -z "${CREDS_JSON:-}" ]; then
  echo "❌ Lambda returned empty response. Check WORKSHOP_CRED_URL."
  exit 1
fi

mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id     = $(echo "${CREDS_JSON}" | jq -r .AccessKeyId)
aws_secret_access_key = $(echo "${CREDS_JSON}" | jq -r .SecretAccessKey)
aws_session_token     = $(echo "${CREDS_JSON}" | jq -r .Token)
EOF

cat > ~/.aws/config << EOF
[default]
region = ${AWS_DEFAULT_REGION:-eu-central-1}
output = json
EOF

chmod 600 ~/.aws/credentials ~/.aws/config

echo "✅ AWS credentials written"
echo "   Expiry: $(echo "${CREDS_JSON}" | jq -r '.Expiration // "n/a"')"

aws sts get-caller-identity \
  && echo "✅ AWS identity verified" \
  || echo "❌ aws sts failed"
