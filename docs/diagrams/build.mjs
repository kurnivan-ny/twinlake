// Transpile each diagram module with esbuild, render it to a static SVG string.
import { transformSync } from 'esbuild';
import { readFileSync, writeFileSync, mkdirSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

const [srcDir, outDir] = process.argv.slice(2);
mkdirSync(outDir, { recursive: true });

const SKIP = new Set(['key']);
const kebab = k => k.replace(/[A-Z]/g, m => '-' + m.toLowerCase());
const KEEP_CAMEL = new Set(['viewBox', 'refX', 'refY', 'markerWidth', 'markerHeight']);
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const escAttr = s => esc(s).replace(/"/g, '&quot;');

function h(tag, props, ...children) {
  const attrs = Object.entries(props || {})
    .filter(([k, v]) => !SKIP.has(k) && !k.startsWith('data-claude') && v !== undefined && v !== null && v !== false)
    .map(([k, v]) => ` ${KEEP_CAMEL.has(k) || k.includes('-') ? k : kebab(k)}="${escAttr(v)}"`)
    .join('');
  const body = children.flat(Infinity).filter(c => c !== null && c !== undefined && c !== false)
    .map(c => (c && c.__svg) ? c.__svg : esc(c)).join('');
  return { __svg: `<${tag}${attrs}>${body}</${tag}>` };
}

// Light + dark palette for the tokens the diagrams use.
const STYLE = `<style>
svg{--cds-text-primary:#1f1e1d;--cds-text-secondary:#5e5d59;--cds-chart-axis:#a8a59c;--cds-chart-grid:#e5e3dc;--cds-chart-reference-tint:#efede7;--cds-chart-categorical-1:#2c6fd6;--cds-chart-status-warning:#e0a100;--bg:#ffffff;font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
@media (prefers-color-scheme: dark){svg{--cds-text-primary:#f0eee8;--cds-text-secondary:#b4b1a8;--cds-chart-axis:#7d7a72;--cds-chart-grid:#3a3936;--cds-chart-reference-tint:#2f2e2b;--cds-chart-categorical-1:#6ea2f2;--cds-chart-status-warning:#f2b929;--bg:#1f1e1d}}
</style>`;

for (const f of readdirSync(srcDir).filter(f => f.endsWith('.jsx')).sort()) {
  const src = readFileSync(join(srcDir, f), 'utf8');
  const { code } = transformSync(src, { loader: 'jsx', jsxFactory: 'h', format: 'cjs' });
  const mod = { exports: {} };
  new Function('module', 'exports', 'h', code)(mod, mod.exports, h);
  let svg = mod.exports.default().__svg;
  const vb = svg.match(/viewBox="0 0 (\d+) (\d+)"/);
  svg = svg.replace('<svg ', `<svg xmlns="http://www.w3.org/2000/svg" width="${vb[1]}" height="${vb[2]}" `)
           .replace(/(<svg[^>]*>)/, `$1${STYLE}<rect width="100%" height="100%" fill="var(--bg)"/>`);
  const out = join(outDir, f.replace('.jsx', '.svg'));
  writeFileSync(out, `<?xml version="1.0" encoding="UTF-8"?>\n${svg}\n`);
  console.log('wrote', out, svg.length, 'bytes');
}
