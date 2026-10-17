"""Execute the account UI renderers with public-data edge cases."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_account_rendering_unknown_bps_negative_cash_and_html_escaping():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([node, "-e", r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const app = fs.readFileSync(process.argv[1], 'utf8');
const accounts = fs.readFileSync(process.argv[2], 'utf8');
const nodes = new Map();
function element(id) {
  if (!nodes.has(id)) nodes.set(id, {innerHTML: '', textContent: '', value: '',
    addEventListener() {}, replaceChildren() {}, scrollIntoView() {}});
  return nodes.get(id);
}
const context = vm.createContext({document: {getElementById: element}, location: {pathname: '/accounts'},
  URLSearchParams, setTimeout, clearTimeout, stat: (...args) => args.join(' ')});
vm.runInContext(app.slice(0, app.indexOf('async function api(')) + accounts, context);
vm.runInContext(`renderAccountDetail({account: {venue: 'lighter-rh', id: 42,
  address: '0x123', fetched: 0, snapshot: null}, fills: [], analysis_fills: 0,
  truncated: false, peers: [], patterns: [{label: '<img src=x onerror=alert(1)>', evidence: 'evidence'}],
  matches: [], cycles: [{symbol: '<b>BTC</b>', started_at: 10, ended_at: 20, hold_sec: 10,
  count: 2, entry_bps: null, exit_bps: -1, cash: -.125}]});`, context);
const html = element('account-detail-body').innerHTML;
assert.match(html, /\$-0\.1250/);
assert.match(html, /未扣手续费、资金费/);
assert.match(html, /&lt;img/);
assert.doesNotMatch(html, /<img/);
assert.match(html, /— \/ -1\.000/);
assert.match(element('account-detail-source').textContent, /快照 —/);
vm.runInContext(`renderAccounts({status: {running: false, connections: {}, config: {symbols: 'BTC'},
  errors: {}, markets: {}, queue_size: 0}, summary: {accounts: 2, trades: 1, market_volume: 100,
  gaps: 0}, total: 2, page: 1, events: [], accounts: [{venue: 'lighter-rh', id: 0,
  watched: 1, volume: 100, trade_count: 1, maker_pct: 0, symbols: 1, last_trade: 100,
  address: '"<script>evil</script>'}]});`, context);
assert.match(element('accounts-rows').innerHTML, /0\.0%/);
assert.doesNotMatch(element('accounts-rows').innerHTML, /<script>/);
assert.match(element('accounts-rows').innerHTML, /data-account-open="0"/);
''', str(root / "web/app.js"), str(root / "web/accounts.js")], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
