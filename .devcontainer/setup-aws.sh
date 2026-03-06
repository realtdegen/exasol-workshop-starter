#!/bin/bash
# Installs a .bashrc hook that decrypts the workshop token on every shell open.
# If WORKSHOP_PASSPHRASE is set (via Codespaces secret), it's fully automatic.
# If not, it prompts once and caches the result.

cat >> ~/.bashrc << 'BASHRC_HOOK'

# --- Workshop AWS credentials ---
if [ -z "$AWS_CONTAINER_CREDENTIALS_FULL_URI" ] && [ -n "$WORKSHOP_CRED_URL" ] && [ -n "$WORKSHOP_TOKEN_ENC" ]; then
    if [ -z "$WORKSHOP_PASSPHRASE" ]; then
        read -rsp "Enter workshop passphrase: " WORKSHOP_PASSPHRASE
        echo ""
    fi
    if [ -n "$WORKSHOP_PASSPHRASE" ]; then
        _TOKEN=$(echo "$WORKSHOP_TOKEN_ENC" | openssl enc -aes-256-cbc -d -a -pbkdf2 -pass "pass:$WORKSHOP_PASSPHRASE" 2>/dev/null)
        if [ -n "$_TOKEN" ]; then
            export AWS_CONTAINER_CREDENTIALS_FULL_URI="$WORKSHOP_CRED_URL"
            export AWS_CONTAINER_AUTHORIZATION_TOKEN="$_TOKEN"
        else
            echo "Wrong passphrase. Run: bash .devcontainer/setup-aws.sh"
        fi
        unset _TOKEN
    fi
fi
# --- End workshop AWS credentials ---
BASHRC_HOOK

echo "AWS credential hook installed in .bashrc"
