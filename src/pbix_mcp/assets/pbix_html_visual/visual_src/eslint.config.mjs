import powerbiVisualsConfigs from "eslint-plugin-powerbi-visuals";
export default [
  powerbiVisualsConfigs.configs.recommended,
  { rules: { "powerbi-visuals/no-inner-outer-html": "off", "powerbi-visuals/no-http-string": "off" } },
];
