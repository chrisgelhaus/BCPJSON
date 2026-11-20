# Security policy

## Supported versions

Security fixes are made for the latest release only. Please upgrade to the [newest release](https://github.com/chrisgelhaus/BCPJSON/releases/latest) before reporting.

## Reporting a vulnerability

Please **do not open a public issue** for a security problem.

Report it privately through GitHub instead: on the repository's **Security** tab choose **Report a vulnerability** ([direct link](https://github.com/chrisgelhaus/BCPJSON/security/advisories/new)). Include:

- the bcpJson version (`bcpjson --version`) and operating system;
- what an attacker could do, and the steps or command line to reproduce it;
- any suggested fix, if you have one.

You should get a reply within a week. Once a fix is released, the advisory is published with credit to the reporter unless you prefer otherwise.

## Things to know when using bcpJson

- **SQL passwords on the command line.** The `bcp` utility only accepts SQL passwords through `-P`, so they are visible in the process list while bcp runs (BCP export/import, and `copy` of tables with geography/geometry/hierarchyid columns). Prefer Windows Authentication, or use a login that has only the permissions the job needs.
- **`--trust-server-certificate`** turns off TLS certificate validation. Use it only for local or test instances.
- **Exported files contain your data in plain text.** Protect the export folder like a database backup.
- **`--snapshot`** needs `CREATE DATABASE` permission on the source server; grant it to the account that runs exports only if you use the option.

## Verifying downloads

Every release asset has a SHA-256 checksum in `SHA256SUMS` and a signed build-provenance attestation. See [Install](README.md#install) for how to verify them.
