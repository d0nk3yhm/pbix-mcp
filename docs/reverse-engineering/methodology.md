# Reverse-engineering methodology (clean-room)

pbix-mcp implements the PBIX/PBIT structures needed for interoperability using
a clean-room discipline. Implementation knowledge comes only from: public
Microsoft documentation and Open Specifications (e.g. MS-XLDM), public
standards, lawfully obtained/generated PBIX/PBIT files, controlled differential
experiments, black-box observation of documented behavior, and
permissively-licensed open source (licenses respected). We do not use Microsoft
source, decompiled/disassembled implementation code, confidential material,
proprietary binaries shipped as part of our implementation, or copied
graphical assets.

**Preferred method** — differential observation:

1. Generate two artifacts through ordinary authorized Power BI use, changing
   exactly one property.
2. Compare their representations.
3. Determine which bytes/fields control the observed behavior.
4. Document the functional observation.
5. Implement the behavior independently.
6. Verify by loading the result in Power BI Desktop.

Records of significant undocumented behaviors live under
`docs/reverse-engineering/experiments/`. Each records: what was investigated,
which lawful artifacts were examined, which public docs were consulted, the
experiment and what changed between inputs, what was observed, the
independently derived conclusion, and which code implements it.

Microsoft DLLs (e.g. the Desktop TOM/ADOMD assemblies) are used **only** in the
local research/verification harness for black-box observation — querying the
workspace engine a lawful Desktop install starts, or comparing our output to
Desktop's. They are **not** dependencies of, and are **not** distributed with,
the pbix-mcp package.

## Index

- [experiments/cryptkey.md](experiments/cryptkey.md) — CryptKey.bin is
  independently generated (format scaffold + self-authored key region), not
  lifted from a Microsoft file.
