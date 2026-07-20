"use strict";
import powerbi from "powerbi-visuals-api";
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;
import IVisualHost = powerbi.extensibility.visual.IVisualHost;
import ISelectionManager = powerbi.extensibility.ISelectionManager;
import ISelectionId = powerbi.visuals.ISelectionId;

/**
 * pbix-mcp HTML renderer with native cross-filtering.
 *
 * Renders the string bound to the "content" role as HTML/CSS/SVG (and re-runs
 * inline <script>). When a "category" field is also bound, any element the
 * author tags with `data-pbix-select="<category value>"` becomes clickable and
 * cross-filters / cross-highlights the rest of the report through Power BI's
 * selection manager — exactly like a native visual. Ctrl/Cmd-click multi-selects,
 * clicking the background clears, right-click opens the report context menu, and
 * unselected regions dim.
 *
 * External resources are blocked by the visual sandbox — inline all assets
 * (base64 images, system fonts).
 */
export class Visual implements IVisual {
    private root: HTMLElement;
    private host: IVisualHost;
    private selectionManager: ISelectionManager;
    private selectedKeys: { [k: string]: boolean } = {};

    constructor(options: VisualConstructorOptions) {
        this.host = options.host;
        this.selectionManager = this.host.createSelectionManager();

        this.root = document.createElement("div");
        this.root.className = "pbixHtmlRoot";
        this.root.style.cssText = "width:100%;height:100%;overflow:auto;box-sizing:border-box;";
        options.element.appendChild(this.root);

        // Background click clears the selection (native behaviour).
        this.root.addEventListener("click", (ev: MouseEvent) => {
            if (ev.target === this.root) {
                this.selectionManager.clear();
                this.selectedKeys = {};
                this.applyDim();
            }
        });

        // Reflect selection cleared/changed elsewhere in the report.
        const sm = this.selectionManager as unknown as {
            registerOnSelectCallback?: (cb: (ids: ISelectionId[]) => void) => void;
        };
        if (sm.registerOnSelectCallback) {
            sm.registerOnSelectCallback((ids: ISelectionId[]) => {
                if (!ids || ids.length === 0) {
                    this.selectedKeys = {};
                    this.applyDim();
                }
            });
        }
    }

    public update(options: VisualUpdateOptions): void {
        let html = "";
        const keyToId: { [k: string]: ISelectionId } = {};
        try {
            const dv = options.dataViews && options.dataViews[0];
            const table = dv && dv.table;
            if (table && table.rows && table.rows.length) {
                const cols = table.columns || [];
                let contentIdx = -1;
                let catIdx = -1;
                for (let i = 0; i < cols.length; i++) {
                    const roles = (cols[i] as { roles?: { [k: string]: boolean } }).roles || {};
                    if (roles["content"] && contentIdx < 0) { contentIdx = i; }
                    if (roles["category"] && catIdx < 0) { catIdx = i; }
                }
                if (contentIdx < 0) { contentIdx = 0; }

                const parts: string[] = [];
                for (let r = 0; r < table.rows.length; r++) {
                    const v = table.rows[r][contentIdx];
                    if (v !== null && v !== undefined && String(v).length > 0) {
                        parts.push(String(v));
                    }
                    if (catIdx >= 0) {
                        const key = String(table.rows[r][catIdx]);
                        if (!(key in keyToId)) {
                            try {
                                keyToId[key] = this.host.createSelectionIdBuilder()
                                    .withTable(table, r).createSelectionId();
                            } catch (idErr) { /* no identity for this row */ }
                        }
                    }
                }

                // If the content is identical for every category row (a single
                // full HTML/SVG whose regions carry data-pbix-select), render it
                // once. If it differs per row (per-category fragments), stack them.
                let allSame = parts.length > 0;
                for (let i = 1; i < parts.length; i++) {
                    if (parts[i] !== parts[0]) { allSame = false; break; }
                }
                html = allSame ? (parts[0] || "") : parts.join("");
            }
        } catch (e) {
            html = "<pre style='color:#b00020;white-space:pre-wrap;font:12px monospace;padding:8px;'>" +
                String((e && (e as Error).message) || e) + "</pre>";
        }

        this.root.innerHTML = html;
        this.rerunScripts();
        this.wireSelection(keyToId);
        this.applyDim();
    }

    /** Attach click / context-menu handlers to every [data-pbix-select] element. */
    private wireSelection(keyToId: { [k: string]: ISelectionId }): void {
        const els = this.root.querySelectorAll("[data-pbix-select]");
        for (let i = 0; i < els.length; i++) {
            const el = els[i] as HTMLElement;
            const key = el.getAttribute("data-pbix-select") || "";
            const id = keyToId[key];
            if (!id) { continue; }
            el.style.cursor = "pointer";

            el.addEventListener("click", (ev: MouseEvent) => {
                ev.stopPropagation();
                const multi = ev.ctrlKey || ev.metaKey;
                this.selectionManager.select(id, multi).then(() => {
                    if (multi) {
                        if (this.selectedKeys[key]) { delete this.selectedKeys[key]; }
                        else { this.selectedKeys[key] = true; }
                    } else {
                        const wasOnly = this.selectedKeys[key] &&
                            Object.keys(this.selectedKeys).length === 1;
                        this.selectedKeys = {};
                        if (!wasOnly) { this.selectedKeys[key] = true; }
                    }
                    this.applyDim();
                });
            });

            el.addEventListener("contextmenu", (ev: MouseEvent) => {
                ev.preventDefault();
                ev.stopPropagation();
                this.selectionManager.showContextMenu(id, { x: ev.clientX, y: ev.clientY });
            });
        }
    }

    /** Dim regions that are not part of the current selection. */
    private applyDim(): void {
        const els = this.root.querySelectorAll("[data-pbix-select]");
        const any = Object.keys(this.selectedKeys).length > 0;
        for (let i = 0; i < els.length; i++) {
            const el = els[i] as HTMLElement;
            const key = el.getAttribute("data-pbix-select") || "";
            el.style.opacity = (!any || this.selectedKeys[key]) ? "1" : "0.35";
        }
    }

    /** Re-execute inline <script> tags so JS-driven graphs run. */
    private rerunScripts(): void {
        try {
            const scripts = this.root.querySelectorAll("script");
            for (let s = 0; s < scripts.length; s++) {
                const oldS = scripts[s];
                const newS = document.createElement("script");
                if (oldS.type) { newS.type = oldS.type; }
                newS.appendChild(document.createTextNode(oldS.textContent || ""));
                oldS.parentNode!.replaceChild(newS, oldS);
            }
        } catch (e) { /* sandbox may block; ignore */ }
    }
}
