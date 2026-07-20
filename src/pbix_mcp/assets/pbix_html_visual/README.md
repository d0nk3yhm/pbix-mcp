# pbix-mcp HTML renderer (our own Power BI custom visual)

`pbixHtml*.pbiviz` is a tiny (~3.8 KB) Power BI custom visual **built by pbix-mcp**
(not a third party). It injects the string bound to its `content` data role as
HTML / CSS / SVG and re-executes inline `<script>` so JS-driven graphs run.
External resources are blocked by Power BI's visual sandbox — inline everything
(base64 images, system fonts). It is intentionally uncertified (uses `innerHTML`).

It renders in its **own DOM** (not a sandboxed iframe), which lets it drive real
selection: bind a `category` role and tag clickable elements with
`data-pbix-select="<value>"` and clicks **cross-filter the report** via
`ISelectionManager` (Ctrl/Cmd-click multi-selects, background click clears,
right-click = context menu, unselected regions dim). Two data roles: `content`
(the HTML string) and `category` (optional, for cross-filtering).

Source in `visual_src/` (visual.ts + capabilities.json + pbiviz.json). Rebuild
(keep the GUID; bump `version`):

    cd visual_src && npm i powerbi-visuals-api ts-loader typescript \
        eslint-plugin-powerbi-visuals && npx powerbi-visuals-tools package

pbix-mcp reads the GUID from the .pbiviz manifest at embed time — never hardcoded.
Full guide: [`docs/html-visuals.md`](../../../../docs/html-visuals.md).
