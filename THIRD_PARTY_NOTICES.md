# Third-Party Notices

`pbix-mcp` is licensed under the MIT License (see [LICENSE](LICENSE)). It builds
on the third-party components listed below, each used under its own license. This
file consolidates the required attributions in one place.

`pbix-mcp` does **not** vendor (copy source into this tree) any of the runtime
dependencies below — they are ordinary Python package dependencies resolved by
`pip` at install time, and each ships its own license text in its own
distribution. This notice is provided for convenience and to satisfy attribution
obligations; it is not a substitute for each dependency's own license file.

## Runtime dependencies

### xpress9 — MIT License
XPRESS9 compression/decompression primitive.
- Project: <https://github.com/Hugoberry/xpress9-python>
- Used for: the low-level XPRESS9 codec beneath `pbix-mcp`'s DataModel container
  handling. The container framing itself (chunk framing, headers, multi-thread
  format, full read/write round-trip) is original work in
  `src/pbix_mcp/formats/datamodel_roundtrip.py`.

### xmhuffman — MIT License
Canonical-Huffman string-page codec (MS-XLDM §2.7.4).
- Project: <https://github.com/Hugoberry/xmhuffman-cython>
- Used for: encoding/decoding Huffman-compressed VertiPaq string dictionaries.

### mcp (Model Context Protocol Python SDK / FastMCP) — MIT License
- Project: <https://github.com/modelcontextprotocol/python-sdk>
- Used for: the MCP server runtime (FastMCP, `mcp.server.fastmcp`). Pinned to the
  1.x line (`mcp>=1.0.0,<2`).

### pydantic — MIT License
- Project: <https://github.com/pydantic/pydantic>
- Used for: tool request/response models and validation.

### apsw (Another Python SQLite Wrapper) — "any-OSI"
- Project: <https://github.com/rogerbinns/apsw>
- License: the author releases apsw under the "any-OSI" terms — you may use it
  under any license approved by the Open Source Initiative. `pbix-mcp` uses it
  under the MIT License.
- Used for: reading and writing the PBIX metadata SQLite layer.

## Test corpus (development only — not redistributed in the package)

The integration test suite downloads a public corpus of sample `.pbix` files at
test time (`scripts/download_test_corpus.py`). These files are **not** included
in the `pbix-mcp` distribution.
- Corpus: public sample PBIX collection, MIT License (Sajjad Ahmadi).

## Bundled asset provenance

- `src/pbix_mcp/assets/pbix_html_visual/` — the "PBIX HTML" Power BI custom
  visual is **original work of this project**. Its compiled `.pbiviz` bundle
  contains no third-party JavaScript runtime (no d3 / Vega / Deneb / lodash /
  tslib / core-js, etc.); it calls only the host-provided `window.powerbi` API.
  The embedded 20×20 `icon.png` is original artwork authored for this project
  (its PNG metadata carries an "Adobe ImageReady" authoring-tool tag only — that
  identifies the drawing tool, not any third-party ownership).

## What is NOT bundled

For the avoidance of doubt, `pbix-mcp` does not include or redistribute:

- any Microsoft source code, binary (DLL / assembly / `msmdsrv`), key material,
  or credentials;
- Microsoft fonts — "Segoe UI" appears only as a CSS `font-family` *name* request
  with a non-Microsoft fallback (`Segoe UI, Arial, sans-serif`); no font file
  ships;
- Microsoft theme assets — built-in theme names such as "CY24SU10" are resolved
  by Power BI at open time; no Microsoft theme JSON is bundled (a theme file is
  written only when the caller supplies the JSON);
- Deneb, Vega, or Vega-Lite source or runtime — these are referenced by
  marketplace GUID or user-supplied spec string only; nothing is redistributed.

See [`docs/reverse-engineering/methodology.md`](docs/reverse-engineering/methodology.md)
for how format knowledge in this project is obtained and recorded.
