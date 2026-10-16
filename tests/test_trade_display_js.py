"""Trade table distinguishes paired cash differences from realised PnL."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_trade_table_preserves_small_losses_and_does_not_call_missing_edges_zero():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for frontend regression checks")
    source = Path(__file__).resolve().parents[1] / "web" / "app.js"
    result = subprocess.run([node, "-e", r'''
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8');
const utilities = source.slice(0, source.indexOf('async function api('));
const logs = source.slice(source.indexOf('function tradeNumber('), source.indexOf('let monitorRequest ='));
const nodes = new Map();
const context = vm.createContext({
  location: {pathname: '/monitor'},
  document: {
    querySelectorAll: () => [],
    getElementById(id) {
      if (!nodes.has(id)) nodes.set(id, {classList: {toggle() {}}, replaceChildren() {}});
      return nodes.get(id);
    }
  }
});
vm.runInContext(utilities + '\n' + logs, context);
const run = code => vm.runInContext(code, context);
const base = {ts: '1792134341', direction: 'bps_open:buy_primary', qty: '.0407',
  buy_fill: '.0407', sell_fill: '.0407', buy_avg_px: '2453.91', sell_avg_px: '2453.94',
  fees_usd: '.0002', fill_edge_usd: '-.0042', ok: '1'};
context.row = base;
assert.equal(run('tradeMatchedEdge(row)'), -.0042);
for (const patch of [{buy_fill: '0'}, {sell_fill: '0'}, {buy_avg_px: ''},
  {sell_avg_px: ''}, {fill_edge_usd: ''}, {fill_edge_usd: 'NaN'}, {buy_avg_px: undefined}]) {
  context.row = {...base, fill_edge_usd: '0', ...patch};
  assert.equal(run('tradeMatchedEdge(row)'), null);
}
context.row = {...base, fill_edge_usd: '0'};
assert.equal(run('tradeMatchedEdge(row)'), 0);
assert.equal(run('tradeNumber("")'), null);
assert.equal(run('tradeNumber("   ")'), null);
assert.equal(run('tradeNumber("Infinity")'), null);
assert.equal(run('tradeNumber("0")'), 0);
context.rows = [base, {...base, buy_fill: '0', fees_usd: '', fill_edge_usd: '0'},
  {...base, sell_fill: '0', fill_edge_usd: '0', ok: '0'}];
run(`state.logTab = 'trades'; state.detail = {trades: rows, trades_total: 3,
  trades_page: 1, trades_pages: 1}; renderLogs();`);
const html = nodes.get('trades-output').innerHTML;
assert.match(html, /非平仓盈亏/);
assert.match(html, /FIFO/);
assert.match(html, /\$-0\.0042/);
assert.match(html, /\$0\.0002/);
assert.match(html, /执行已确认/);
assert.match(html, /需核对/);
const tableRows = [...html.matchAll(/<tr>(.*?)<\/tr>/gs)].slice(1).map(match => match[1]);
assert.equal(tableRows.length, 3);
assert.doesNotMatch(tableRows[1], /\$0\.0000/);
assert.match(tableRows[1], /不能据此判断实际盈亏/);
assert.doesNotMatch(tableRows[2], /class="number positive"/);
''', str(source)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
