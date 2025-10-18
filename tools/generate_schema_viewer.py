#!/usr/bin/env python3
"""Generate an interactive HTML viewer that renders per-season schemas using Mermaid.js (client-side).

Produces: data/schemas/visualize_schemas.html

This version embeds the two manifest JSON files into the HTML using
<script type="application/json"> tags so Python doesn't need to interpolate
JS template literals and we avoid f-string parsing issues.
"""
import json
from pathlib import Path
from datetime import datetime
import glob

ROOT = Path.cwd()
SCHEMAS_DIR = ROOT / "data" / "schemas"
RAW_MANIFEST = SCHEMAS_DIR / "raw_profiles_by_season.json"
STAGED_MANIFEST = SCHEMAS_DIR / "staged_schemas_by_season.json"
OUT_HTML = SCHEMAS_DIR / "visualize_schemas.html"


def load_json(p: Path):
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_raw_manifest_from_profiles(profiles_dir: Path) -> dict:
  """Scan per-season raw profile JSON files and build a combined raw manifest.

  Result format mirrors the staged manifest: { generated_at, seasons: {season: {tables: {name: {column_info: {...}}}}}}
  """
  out = {"generated_at": datetime.utcnow().isoformat() + 'Z', "seasons": {}}
  pattern = str(profiles_dir / 'per_season_*.initial_raw_data_profile.json')
  for path in glob.glob(pattern):
    try:
      j = json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
      continue
    season = j.get('season') or Path(path).stem
    tables = j.get('tables') or {}
    out['seasons'].setdefault(season, {'tables': {}})
    for tname, tinfo in tables.items():
      # keep the column_info as-is (used by mermaidForRaw)
      out['seasons'][season]['tables'][tname] = {'column_info': tinfo.get('column_info', {})}
  return out


def build_html(raw_manifest: dict, staged_manifest: dict) -> str:
  raw_json = json.dumps(raw_manifest, ensure_ascii=False)
  staged_json = json.dumps(staged_manifest, ensure_ascii=False)

  # choose selected manifest at generation time so the page works even if JS is disabled
  raw_has = bool(raw_manifest and raw_manifest.get('seasons'))
  staged_has = bool(staged_manifest and staged_manifest.get('seasons'))
  raw_selected_attr = ' selected' if raw_has else ''
  staged_selected_attr = ' selected' if (not raw_has and staged_has) else ''

  html = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Schema Visualizer</title>
  <style>
    body { font-family: system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; margin: 16px; }
    select, button { margin: 6px 8px 6px 0; }
    #diagram { border: 1px solid #ddd; padding: 12px; border-radius: 6px; min-height: 200px; background: #fff }
    #tables { max-height: 300px; overflow: auto; border: 1px solid #eee; padding: 8px; }
    .col-count { color: #666; font-size: 0.9em }
  </style>
  <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
  <script>mermaid.initialize({ startOnLoad: false });</script>
</head>
<body>
  <h2>Schema Visualizer</h2>
  <p>Choose manifest, season and table to view an ER diagram rendered by Mermaid (client-side).</p>

  <label for="manifest">Manifest</label>
  <select id="manifest">
    <option value="raw" __RAW_SELECTED__>Raw profiles (per-season)</option>
    <option value="staged" __STAGED_SELECTED__>Staged schemas (per-season)</option>
  </select>

  <label for="season">Season</label>
  <select id="season"></select>

  <label for="table">Table</label>
  <select id="table"></select>

  <label style="margin-left:8px"><input type="checkbox" id="all-tables" /> Render all tables for season</label>

  <button id="render">Render diagram</button>
  <button id="test">Test Mermaid</button>
  <button id="download">Download SVG</button>
  <button id="download-mmd">Download .mmd</button>

  <h3 id="title"></h3>
  <div id="diagram"></div>

  <!-- embedded manifests -->
  <script type="application/json" id="raw-manifest">__RAW_JSON__</script>
  <script type="application/json" id="staged-manifest">__STAGED_JSON__</script>

  <script>
  const RAW = JSON.parse(document.getElementById('raw-manifest').textContent || '{}');
  const STAGED = JSON.parse(document.getElementById('staged-manifest').textContent || '{}');

  function seasonsFor(manifest) {
    return Object.keys((manifest && manifest.seasons) || {}).sort();
  }

  function tablesFor(manifest, season) {
    const s = ((manifest && manifest.seasons) || {})[season] || {};
    return Object.keys(s.tables || {}).sort();
  }

  const manifestEl = document.getElementById('manifest');
  const seasonEl = document.getElementById('season');
  const tableEl = document.getElementById('table');
  const allTablesEl = document.getElementById('all-tables');
  const titleEl = document.getElementById('title');
  const diagramEl = document.getElementById('diagram');

  // (server-side) default selection is set when the HTML was generated
  // If an option has the 'selected' attribute, make sure the select reflects that
  (function honorServerSelected() {
    const opts = manifestEl.querySelectorAll('option[selected]');
    if (opts && opts.length) {
      manifestEl.value = opts[0].value;
    }
  })();

  function rebuildSeasonOptions() {
    const man = manifestEl.value === 'raw' ? RAW : STAGED;
    const seasons = seasonsFor(man);
    seasonEl.innerHTML = '';
    seasons.forEach(s => { const opt = document.createElement('option'); opt.value = s; opt.textContent = s; seasonEl.appendChild(opt); });
    seasonEl.dispatchEvent(new Event('change'));
  }

  function rebuildTableOptions() {
    const man = manifestEl.value === 'raw' ? RAW : STAGED;
    const season = seasonEl.value;
    const tables = tablesFor(man, season);
    tableEl.innerHTML = '';
    tables.forEach(t => { const opt = document.createElement('option'); opt.value = t; opt.textContent = t; tableEl.appendChild(opt); });
    // auto-select first table if present
    if (tableEl.options && tableEl.options.length) {
      tableEl.selectedIndex = 0;
    }
  }

  function mermaidForRaw(season, table) {
    let lines = ['erDiagram', '  %% Season: ' + season];
    lines = lines.concat(entityForRaw(season, table));
    return lines.join('\n');
  }

  function mermaidForStaged(season, table) {
    let lines = ['erDiagram', '  %% Season: ' + season];
    lines = lines.concat(entityForStaged(season, table));
    return lines.join('\n');
  }

  // return the entity block lines for a single raw table (reusable for bulk rendering)
  function entityForRaw(season, table) {
    const tbl = ((RAW && RAW.seasons) || {})[season] && ((RAW.seasons || {})[season].tables || {})[table];
    const cols = (tbl && tbl.column_info) || {};
    const ent = [];
    const safeTable = table.toUpperCase().replace(/[^A-Za-z0-9_]/g,'_').replace(/_+/g,'_').replace(/^_+|_+$/g,'');
    const tableName = safeTable || 'TBL';
    ent.push('  ' + tableName + ' {');
    const seen = new Set();
    for (const c of Object.keys(cols || {})) {
      let safeCol = c.replace(/[^A-Za-z0-9_]/g,'_');
      safeCol = safeCol.replace(/_+/g, '_').replace(/^_+|_+$/g, '');
      if (!safeCol) safeCol = 'col';
      if (/^[0-9]/.test(safeCol)) safeCol = 'f_' + safeCol;
      let base = safeCol;
      let idx = 1;
      while (seen.has(safeCol)) { safeCol = base + '_' + (idx++); }
      seen.add(safeCol);
      ent.push('    string ' + safeCol);
    }
    ent.push('  }');
    return ent;
  }

  // return the entity block lines for a single staged table
  function entityForStaged(season, table) {
    const tbl = ((STAGED && STAGED.seasons) || {})[season] && ((STAGED.seasons || {})[season].tables || {})[table];
    const cols = (tbl && tbl.columns) || {};
    const ent = [];
    const safeTable = table.toUpperCase().replace(/[^A-Za-z0-9_]/g,'_').replace(/_+/g,'_').replace(/^_+|_+$/g,'');
    const tableName = safeTable || 'TBL';
    ent.push('  ' + tableName + ' {');
    const seen = new Set();
    for (const c of Object.keys(cols || {})) {
      let safeCol = c.replace(/[^A-Za-z0-9_]/g,'_');
      safeCol = safeCol.replace(/_+/g, '_').replace(/^_+|_+$/g, '');
      if (!safeCol) safeCol = 'col';
      if (/^[0-9]/.test(safeCol)) safeCol = 'f_' + safeCol;
      let base = safeCol;
      let idx = 1;
      while (seen.has(safeCol)) { safeCol = base + '_' + (idx++); }
      seen.add(safeCol);
      ent.push('    string ' + safeCol);
    }
    ent.push('  }');
    return ent;
  }

  function render() {
    const man = manifestEl.value;
    const season = seasonEl.value;
    const table = tableEl.value;
    if (!season || !table) return;
    titleEl.textContent = (man && man.toUpperCase ? man.toUpperCase() : man) + ' — ' + season + ' / ' + table;
    let code;
    if (allTablesEl && allTablesEl.checked) {
      // bulk render: include all tables for the season
      const tables = tablesFor(man === 'raw' ? RAW : STAGED, season);
      let lines = ['erDiagram', '  %% Season: ' + season];
      for (const t of tables) {
        lines = lines.concat(man === 'raw' ? entityForRaw(season, t) : entityForStaged(season, t));
      }
      code = lines.join('\n');
    } else {
      code = man === 'raw' ? mermaidForRaw(season, table) : mermaidForStaged(season, table);
    }
    // clear
    diagramEl.innerHTML = '';
    // show the raw mermaid text for debugging
    let codePre = document.getElementById('mermaid-code');
    if (!codePre) {
      codePre = document.createElement('pre');
      codePre.id = 'mermaid-code';
      codePre.style = 'background:#f8f8f8;border:1px solid #eee;padding:8px;white-space:pre-wrap;max-height:200px;overflow:auto;font-family:monospace;font-size:13px';
      diagramEl.parentNode.insertBefore(codePre, diagramEl);
    }
    codePre.textContent = code;
    const graphDiv = document.createElement('div');
    graphDiv.className = 'mermaid';
    graphDiv.textContent = code;
    diagramEl.appendChild(graphDiv);
    // Try a parse pass to get better diagnostics; if parse fails, show fallback
    try {
      // mermaid.parse will throw with a useful message when invalid
      mermaid.parse(code);
      // If parse succeeds, render
      mermaid.init(undefined, graphDiv);
    } catch (err) {
      const msg = (err && (err.message || err.str)) || String(err);
      const stack = err && err.stack ? '\n' + err.stack : '';
      // Show diagnostic and raw mermaid text
      diagramEl.innerHTML = '';
      const errDiv = document.createElement('div');
      errDiv.style = 'color:#900;padding:12px;border:1px solid #fdd;background:#fff6f6;white-space:pre-wrap';
      errDiv.textContent = 'Mermaid parse/render error:\n' + msg + stack + '\n\nMermaid source below:';
      diagramEl.appendChild(errDiv);
      const srcPre = document.createElement('pre'); srcPre.style='background:#fff;border:1px solid #eee;padding:8px;margin-top:8px;white-space:pre-wrap;overflow:auto'; srcPre.textContent = code; diagramEl.appendChild(srcPre);
      // Provide an HTML fallback listing columns so user can inspect schema without Mermaid
      const fallback = document.createElement('div'); fallback.style='margin-top:12px;padding:8px;border:1px solid #eee;background:#fff';
      fallback.innerHTML = '<strong>Fallback: schema listing</strong>';
      const man = manifestEl.value === 'raw' ? RAW : STAGED;
      const tbls = allTablesEl && allTablesEl.checked ? tablesFor(man, season) : [table];
      tbls.forEach(tn => {
        const container = document.createElement('div'); container.style='margin-top:8px';
        const h = document.createElement('div'); h.textContent = tn; h.style='font-weight:600;margin-bottom:6px'; container.appendChild(h);
        const list = document.createElement('ul');
        const rawTbl = ((man && man.seasons) || {})[season] && ((man.seasons || {})[season].tables || {})[tn];
        const cols = (rawTbl && (rawTbl.column_info || rawTbl.columns)) || {};
        Object.keys(cols).forEach(cn => { const li=document.createElement('li'); li.textContent = cn; list.appendChild(li); });
        container.appendChild(list);
        fallback.appendChild(container);
      });
      diagramEl.appendChild(fallback);
    }
  }

  manifestEl.addEventListener('change', () => { rebuildSeasonOptions(); });
  seasonEl.addEventListener('change', () => { rebuildTableOptions(); });
  // toggle table select when 'all tables' is checked
  if (allTablesEl) {
    allTablesEl.addEventListener('change', () => { tableEl.disabled = allTablesEl.checked; });
  }
  document.getElementById('render').addEventListener('click', render);
  document.getElementById('test').addEventListener('click', () => {
    // minimal valid ER diagram for testing Mermaid
    const testCode = ['erDiagram','  CUSTOMER {','    string name','    string id','  }','  ORDER {','    string id','    string total','  }','  CUSTOMER ||--o{ ORDER : places'].join('\n');
    // show it
    let codePre = document.getElementById('mermaid-code');
    if (!codePre) {
      codePre = document.createElement('pre'); codePre.id='mermaid-code'; codePre.style='background:#f8f8f8;border:1px solid #eee;padding:8px;white-space:pre-wrap;max-height:200px;overflow:auto;font-family:monospace;font-size:13px'; diagramEl.parentNode.insertBefore(codePre, diagramEl);
    }
    codePre.textContent = testCode;
    diagramEl.innerHTML = '';
    const graphDiv = document.createElement('div'); graphDiv.className='mermaid'; graphDiv.textContent = testCode; diagramEl.appendChild(graphDiv);
    try { mermaid.init(undefined, graphDiv); } catch (err) { diagramEl.innerHTML = '<div style="color:#900;padding:12px;border:1px solid #fdd;background:#fff6f6">Test render error: ' + (err && err.message ? err.message : String(err)) + '</div>'; }
  });

  // initial build
  rebuildSeasonOptions();
  // auto-render the first selection if present
  setTimeout(() => {
    if (seasonEl.value && tableEl.value) {
      render();
    }
  }, 50);

  // Download SVG button
  document.getElementById('download').addEventListener('click', async () => {
    const svg = diagramEl.querySelector('svg');
    if (!svg) { alert('Render first to produce SVG'); return; }
    const serializer = new XMLSerializer();
    const svgStr = serializer.serializeToString(svg);
    const blob = new Blob([svgStr], {type: 'image/svg+xml;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = (manifestEl.value || '') + '_' + (seasonEl.value || '') + '_' + (tableEl.value || '') + '.svg';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  });

  // Download .mmd (Mermaid source) button
  document.getElementById('download-mmd').addEventListener('click', async () => {
    const code = document.getElementById('mermaid-code') ? document.getElementById('mermaid-code').textContent : '';
    if (!code) { alert('Render first to produce Mermaid source'); return; }
    const blob = new Blob([code], {type: 'text/plain;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = (manifestEl.value || '') + '_' + (seasonEl.value || '') + '.mmd'; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
  });

</script>
</body>
</html>"""

    # inject JSON safely
  # inject JSON safely and the selected attributes for the manifest options
  html = html.replace('__RAW_JSON__', raw_json)
  html = html.replace('__STAGED_JSON__', staged_json)
  html = html.replace('__RAW_SELECTED__', raw_selected_attr)
  html = html.replace('__STAGED_SELECTED__', staged_selected_attr)
  return html


def main():
  raw = load_json(RAW_MANIFEST)
  # If no raw manifest exists, attempt to build one from per-season raw profiles
  if not raw or not raw.get('seasons'):
    profiles_dir = SCHEMAS_DIR / 'profiles' / 'raw'
    if profiles_dir.exists():
      raw = build_raw_manifest_from_profiles(profiles_dir)
      try:
        RAW_MANIFEST.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f"Wrote derived raw manifest: {RAW_MANIFEST}")
      except Exception:
        print("Failed to write derived raw manifest; using in-memory manifest")
  staged = load_json(STAGED_MANIFEST)
  html = build_html(raw, staged)
  OUT_HTML.write_text(html, encoding='utf-8')
  print(f"Wrote HTML viewer: {OUT_HTML}")


if __name__ == '__main__':
  main()
