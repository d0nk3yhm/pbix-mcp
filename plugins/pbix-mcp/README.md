# Power BI (pbix-mcp) plugin for Claude Code

Lets Claude create, read, edit and evaluate Power BI `.pbix` / `.pbit` files and PBIP projects, without Power BI Desktop. It adds the [pbix-mcp](https://github.com/d0nk3yhm/pbix-mcp) MCP server to Claude Code: report pages and visuals, tables and relationships, measures and calculated columns, row-level security, themes, TMDL import and export, and a pure-Python DAX engine checked against Power BI Desktop's answers.

## Requirements

[uv](https://docs.astral.sh/uv/getting-started/installation/), which the plugin uses to run the server. `uv` fetches the server and a suitable Python itself, so nothing else is needed. The first start downloads them from PyPI, which takes a little longer.

## Install

In Claude Code:

```
/plugin marketplace add d0nk3yhm/pbix-mcp
/plugin install pbix-mcp@pbix-mcp
```

Or from a shell:

```bash
claude plugin marketplace add d0nk3yhm/pbix-mcp
```

```bash
claude plugin install pbix-mcp@pbix-mcp
```

Run `/mcp` in Claude Code to check that `plugin:pbix-mcp:powerbi-editor` is connected. If it failed to start, the usual cause is that `uvx` is not on your `PATH`.

## Try it

- "Open sales.pbix and list its pages, tables and measures."
- "What does [Total Sales] evaluate to for 2024 in sales.pbix?"
- "Build a new report from orders.csv with a revenue card and a bar chart by region, and save it as orders.pbix."

## Update

```bash
claude plugin marketplace update pbix-mcp
```

```bash
claude plugin update pbix-mcp@pbix-mcp
```

Each plugin version runs one exact pbix-mcp release from PyPI, so the server only changes when you update.

## Privacy

The server runs on your machine and works on the files you point it at. It has no telemetry and sends nothing to any service of its own. `uv` downloads the packages from PyPI when they are first needed. As with any MCP tool, what the tools return goes into your Claude conversation, including any data Claude reads from a report.

## Limits

DAX is evaluated by a Python engine, so very large models run slower than in Power BI Desktop. The plugin works where Claude Code runs a local server, and not on claude.ai. See the [full documentation](https://github.com/d0nk3yhm/pbix-mcp#readme), and report problems in the [issue tracker](https://github.com/d0nk3yhm/pbix-mcp/issues).

MIT licensed.
