# Format constants vs. author content — what a byte means

**Question.** The corpus is a mix of official Microsoft sample reports and
community-authored ones. Which bytes are the same across all of them, which
change per author, and do any bytes constitute a "Microsoft-generated"
fingerprint or a per-machine/user identity we should not reuse or claim?

**Method.** Compared one official Microsoft sample (`MS_AI_Sample`) with two
independently authored community reports (`GeoSales_Dashboard`,
`Agents_Performance`) — all lawfully obtained, all saved by Power BI Desktop
(the only mainstream authoring tool, so the *generator* is always Desktop
regardless of the human author).

**Observations.**

- **Format constants — byte-identical across every author:** the CryptKey
  bookend GUID (`98bc215d…`), the CryptKey format scaffold
  (`sha256[:12] = ed9bbd792509` for all three), the ABF signature, the XMLA
  schema, and the fixed system-column GUIDs. These are the same no matter who
  authored the file because Desktop's save routine writes them identically
  every time.
- **Author/content-variable — differs per file:** table names
  (Accounts/Industries vs fct_Orders/People vs FactSales/#Measures), the data,
  the report structure, the database catalog GUID, the CryptKey *variable*
  region (25 distinct values across 25 files), and the report `Version`
  string (1.28 / 1.33 / 1.31 — the save-format version, which tracks the
  Desktop build, not authorship).

**Conclusions.**

1. A *format constant* means "this is the Power BI file **format**" — a
   structural marker, like a PNG magic number or a ZIP local-file header. It
   is **not** a per-installation Microsoft fingerprint and carries **no**
   machine SID, user ID, email, or license: if it did, the scaffold would
   differ between independent authors, and it does not (identical across a
   Microsoft file and two community files).
2. Reusing these constants is **implementing the observable public file
   format for interoperability**, not copying a Microsoft-specific secret or
   an authorship signature. They are present, identical, in every lawfully
   generated file.
3. pbix-mcp's generated files carry these same format constants (so they are
   valid Power BI-format files another implementation can read) **but** with
   the author's own content and an independently generated CryptKey region —
   no Microsoft author's data and no extracted Microsoft key material. We do
   **not** claim, and no byte implies, that our files are authored by
   Microsoft.

**Sources.** Lawfully generated PBIX files; black-box byte comparison; our own
analysis. No Microsoft source, confidential material, or identity data used.
