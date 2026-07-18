# Security Policy

## Supported versions

Security fixes are released against the latest published version of `pbix-mcp`.
Please upgrade to the most recent release before reporting an issue.

| Version | Supported |
|---------|-----------|
| 0.9.7 and later | ✅ |
| < 0.9.7 | ❌ (upgrade) |

## Reporting a vulnerability

Please report security issues **privately** — do not open a public issue for a
suspected vulnerability.

- Preferred: use GitHub's **private vulnerability reporting** on this repository
  (the **Security** tab → *Report a vulnerability*). This keeps the report
  private and lets us coordinate a fix and a CVE.

When reporting, please include:

- The affected tool or component and the `pbix-mcp` version.
- A description of the issue and its impact.
- A minimal proof of concept, if you have one.

We aim to acknowledge reports promptly and to coordinate disclosure with the
reporter. We are happy to credit reporters in the release notes and in the
published advisory.

## Threat model notes

`pbix-mcp` is an MCP server that reads and writes Power BI files. It treats the
following inputs as **untrusted** and validates them:

- **`.pbix` / `.pbit` / `.pbiviz` archives** — extraction is guarded against
  decompression bombs and path traversal (Zip-Slip); members are size-capped and
  contained to the working directory.
- **Tool arguments that name files** (e.g. a theme `filename`) — all writes that
  incorporate caller-controlled names are contained to the per-file working
  directory and refuse paths that would escape it.

## Acknowledgments

We thank the following researchers for responsibly disclosing security issues:

- **Moshe Levi (Levinity Cyber)** — path traversal / arbitrary file write in
  `pbix_set_theme` (CWE-22 / CWE-73), fixed in 0.9.7.
