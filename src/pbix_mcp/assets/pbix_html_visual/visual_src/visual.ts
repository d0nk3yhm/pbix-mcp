"use strict";
import powerbi from "powerbi-visuals-api";
import IVisual = powerbi.extensibility.visual.IVisual;
import VisualConstructorOptions = powerbi.extensibility.visual.VisualConstructorOptions;
import VisualUpdateOptions = powerbi.extensibility.visual.VisualUpdateOptions;

/** pbix-mcp HTML renderer: injects the string bound to the "content" role as
 *  HTML/CSS/SVG, and re-executes inline <script> so JS-driven graphs run.
 *  External resources are blocked by Power BI's visual sandbox — inline all
 *  assets (base64 images, system fonts). */
export class Visual implements IVisual {
    private root: HTMLElement;

    constructor(options: VisualConstructorOptions) {
        this.root = document.createElement("div");
        this.root.className = "pbixHtmlRoot";
        this.root.style.cssText = "width:100%;height:100%;overflow:auto;box-sizing:border-box;";
        options.element.appendChild(this.root);
    }

    public update(options: VisualUpdateOptions): void {
        let html = "";
        try {
            const dv = options.dataViews && options.dataViews[0];
            if (dv && dv.table && dv.table.rows && dv.table.rows.length) {
                const cols = dv.table.columns || [];
                let idx = -1;
                for (let i = 0; i < cols.length; i++) {
                    const roles = (cols[i] as { roles?: { [k: string]: boolean } }).roles;
                    if (roles && roles["content"]) { idx = i; break; }
                }
                if (idx < 0) idx = 0;
                const parts: string[] = [];
                for (let r = 0; r < dv.table.rows.length; r++) {
                    const v = dv.table.rows[r][idx];
                    if (v !== null && v !== undefined) parts.push(String(v));
                }
                html = parts.join("");
            }
        } catch (e) {
            html = "<pre style='color:#b00020;white-space:pre-wrap;font:12px monospace;padding:8px;'>" +
                String((e && (e as Error).message) || e) + "</pre>";
        }
        this.root.innerHTML = html;
        try {
            const scripts = this.root.querySelectorAll("script");
            for (let s = 0; s < scripts.length; s++) {
                const oldS = scripts[s];
                const newS = document.createElement("script");
                if (oldS.type) newS.type = oldS.type;
                newS.appendChild(document.createTextNode(oldS.textContent || ""));
                oldS.parentNode!.replaceChild(newS, oldS);
            }
        } catch (e2) { /* sandbox may block; ignore */ }
    }
}
