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
- **Binary format parsers** — the ABF container, VertiPaq column encoder, and
  XPRESS9 codec treat every length, offset, and count field in a file as
  untrusted: sizes are bound-checked before allocation and malformed input fails
  closed (a clear error) rather than over-allocating or reading out of bounds.
- **Working / temp directories** — each open file gets its own working directory
  created with a secure API (unpredictable name, not shared across files) and
  removed after use; nothing is written to a predictable shared temp path.

The server also holds to these handling rules for untrusted content:

- **No silent network egress from report content.** URLs embedded in a file
  (e.g. `ImageUrl` values, custom-visual resource references, Vega/Deneb spec
  references) are treated as opaque data. The server does not dereference or
  fetch them, and authoring/parsing a file makes no outbound network request on
  its own.
- **Credentials are never logged or persisted.** Passwords, tokens, and
  connection strings (including anything surfaced by `pbix_get_password`) are
  never written to logs — at any log level — never persisted to disk or temp
  files by the server, and are kept out of error messages.
- **Report text is data, not instructions.** Text read from an untrusted file
  (table/measure/column names, titles, descriptions, annotations, M / DAX query
  text) is untrusted data. The server does not interpret it as instructions and
  does not present it to a calling model as trusted directives — a defense
  against prompt injection carried inside a `.pbix`/`.pbit`.
- **No untrusted values in a shell.** Caller-controlled or file-derived strings
  are never interpolated into a shell command. Any external process is invoked
  with an explicit argument vector (no `shell=True`) and validated arguments.

## Acknowledgments

We thank the following researchers for responsibly disclosing security issues:

- **Moshe Levi (Levinity Cyber)** — path traversal / arbitrary file write in
  `pbix_set_theme` (CWE-22 / CWE-73), fixed in 0.9.7.
