FROM mcr.microsoft.com/devcontainers/base:ubuntu

# ── System deps ────────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    ca-certificates \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ── AWS CLI v2 ─────────────────────────────────────────────────────────────────
RUN ARCH=$(uname -m) \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" \
       -o /tmp/awscli.zip \
    && unzip -q /tmp/awscli.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscli.zip \
    && aws --version

# ── uv — direct binary download, no pipe-to-sh ────────────────────────────────
# Avoids pipe-to-sh installer failures in restricted build environments
RUN ARCH=$(uname -m) \
    && UV_ARCH=$([ "$ARCH" = "aarch64" ] && echo "aarch64" || echo "x86_64") \
    && curl -fsSL \
       "https://github.com/astral-sh/uv/releases/latest/download/uv-${UV_ARCH}-unknown-linux-gnu.tar.gz" \
       | tar -xz --strip-components=1 \
             -C /usr/local/bin \
             "uv-${UV_ARCH}-unknown-linux-gnu/uv" \
             "uv-${UV_ARCH}-unknown-linux-gnu/uvx" \
    && chmod +x /usr/local/bin/uv /usr/local/bin/uvx \
    && uv --version

# ── vscode user dir — create if not yet present ────────────────────────────────
# Use root for the curl, then fix ownership after — avoids USER-not-found error
RUN mkdir -p /home/vscode/bin \
    && ARCH=$(uname -m) \
    && curl -fSL \
       "https://x-up.s3.eu-west-1.amazonaws.com/releases/exasol-personal/linux/${ARCH}/latest/exasol" \
       -o /home/vscode/bin/exasol \
    && chmod +x /home/vscode/bin/exasol \
    # Defer chown — vscode UID=1000 is guaranteed in devcontainers/base
    && chown -R 1000:1000 /home/vscode/bin

ENV PATH="/home/vscode/bin:${PATH}"
