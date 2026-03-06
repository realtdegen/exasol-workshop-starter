#!/bin/bash
# Maps Codespaces secrets to the env vars AWS SDKs expect.
# Codespaces secrets WORKSHOP_CRED_URL and WORKSHOP_TOKEN are
# injected automatically — this script wires them up.

if [ -n "$WORKSHOP_CRED_URL" ] && [ -n "$WORKSHOP_TOKEN" ]; then
    echo "export AWS_CONTAINER_CREDENTIALS_FULL_URI=\"\$WORKSHOP_CRED_URL\"" >> ~/.bashrc
    echo "export AWS_CONTAINER_AUTHORIZATION_TOKEN=\"\$WORKSHOP_TOKEN\"" >> ~/.bashrc
    echo "AWS credentials configured. Run: aws sts get-caller-identity to verify."
else
    echo "WARNING: WORKSHOP_CRED_URL or WORKSHOP_TOKEN secrets not set."
    echo "Ask your instructor for the setup command, then run:"
    echo "  source setup.sh <url> <token>"
fi
