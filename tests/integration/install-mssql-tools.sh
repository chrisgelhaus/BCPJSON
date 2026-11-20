#!/usr/bin/env bash
# Installs Microsoft's sqlcmd and bcp (mssql-tools18) on an Ubuntu GitHub runner and puts them on PATH.
# The ODBC-based sqlcmd is used on purpose: it supports the same switches as bcp (-C, -f, -v).
set -euo pipefail

TOOLS=/opt/mssql-tools18/bin

if [ ! -x "$TOOLS/bcp" ] || [ ! -x "$TOOLS/sqlcmd" ]; then
    # Add the Microsoft package feed unless the runner image already has it.
    if ! grep -rqs "packages.microsoft.com/ubuntu" /etc/apt/sources.list.d/; then
        curl -sSfL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/$(lsb_release -rs)/prod $(lsb_release -cs) main" \
            | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null
    fi
    sudo apt-get update -qq
    sudo ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y -qq mssql-tools18 unixodbc
fi

# Put the ODBC tools ahead of any go-sqlcmd already on the image.
if [ -n "${GITHUB_PATH:-}" ]; then
    echo "$TOOLS" >> "$GITHUB_PATH"
fi
"$TOOLS/sqlcmd" -? | head -n 2
"$TOOLS/bcp" -v
