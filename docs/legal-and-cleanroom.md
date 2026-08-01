# Legal posture & clean-room discipline

> This document explains how and why `pbix-mcp` is built the way it is. It is
> provided for context and transparency. **It is not legal advice.** How the laws
> and provisions summarized here apply depends on jurisdiction and circumstances;
> consult qualified counsel for your own situation.

## What this project is

`pbix-mcp` is an **independent, clean-room reimplementation** of the Power BI
`.pbix` / `.pbit` file format, developed to enable **interoperability** — letting
AI agents, automation, and non-Windows platforms create, read, write, and
evaluate Power BI files without Power BI Desktop.

It is **not** a Power BI clone and **not** affiliated with, endorsed by, or
associated with Microsoft Corporation. "Power BI" and "PBIX" are trademarks of
Microsoft Corporation, referenced here only nominatively to describe the file
formats this project interoperates with. The product brand is **pbix-mcp**.

## How format knowledge is obtained (permitted sources only)

All binary-format knowledge in this project is derived exclusively from lawful
sources:

- published **public documentation** and public specifications (e.g. the
  Microsoft Open Specifications, such as MS-XLDM);
- **differential / black-box observation** of PBIX files that were lawfully
  generated on machines we control;
- **permissively-licensed open-source** primitives, with their licenses and
  notices preserved (see [../THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md)).

It is **never** derived from Microsoft source code, decompiled or disassembled
Microsoft binaries, extracted Power BI JavaScript/CSS, or any leaked,
confidential, or NDA-covered material. No Microsoft binaries, key material, or
credentials ship in the package. The method and the observations behind specific
findings are recorded under
[reverse-engineering/methodology.md](reverse-engineering/methodology.md) so the
provenance trail stays auditable.

## Interoperability context (a foundation, not a guarantee)

Reverse engineering undertaken to achieve interoperability is recognized as a
permitted purpose under, among others, the EU Software Directive
([2009/24/EC, Article 6](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=celex%3A32009L0024))
and the US DMCA
([17 U.S.C. §1201(f)](https://www.law.cornell.edu/uscode/text/17/1201)). National
implementations vary (for example, in Norway the software-decompilation exception
appears at §42 of the Copyright Act / Åndsverkloven).

These provisions are the **foundation** the project is built on; they are
described here as context, not as a promise about any particular outcome. Each is
**subject to conditions** (for interoperability purposes, by a person entitled to
use the program, where the information is not otherwise readily available), and
none is a general override of contract terms. Whether and how they apply to a
given use is fact- and jurisdiction-specific. **This is not legal advice.**

## Contributions

Contributions are accepted under the **Developer Certificate of Origin (DCO)
1.1** together with a **clean-room provenance certification** — every contributor
certifies their work is original or drawn only from the permitted sources above,
and never from Microsoft proprietary/decompiled/leaked material. See
[../CONTRIBUTING.md](../CONTRIBUTING.md).

## If you believe something here is wrong

If you find a factual, attribution, or provenance problem — a source that should
not have been used, a missing notice, an overstated claim — please open an issue
or use the private security-reporting channel in
[../SECURITY.md](../SECURITY.md). The project's practice is to flag the problem,
explain it, propose a safe alternative, fix it, and record the correction.
