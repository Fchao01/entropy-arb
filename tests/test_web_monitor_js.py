"""Render real monitor history text without requiring frontend dependencies."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_monitor_renders_history_timestamps_from_shared_scope():
    node = shutil.which('node')
    if not node:
        pytest.skip('Node.js is only needed to execute this frontend regression test')
    source = Path(__file__).resolve().parents[1] / 'web' / 'app.js'
    result = subprocess.run([node, '-e', r'''
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8');
const utilities = source.slice(0, source.indexOf('async function api('));
const monitor = source.slice(source.indexOf('function renderMonitor() {'), source.indexOf('\nfunction renderSignals('));
const nodes = new Map();
const context = vm.createContext({
  location: {pathname: '/monitor'},
  document: {getElementById(id) {
    if (!nodes.has(id)) nodes.set(id, {});
    return nodes.get(id);
  }},
  badge: () => '', stat: () => '', renderSignals() {}, drawChart() {}, renderLogs() {},
});
vm.runInContext(utilities + '\n' + monitor + `
state.selected = 'history-test';
state.detail = {minutes: [], task: {
  id: 'history-test', symbol: 'SPY', primary: 'arcus', hedge: 'lighter-rh',
  mode: 'record', state: 'running', status_stale: false,
  status: {updated_at: 1792076400, venues: [], bps_strategy: {
    levels: 'rolling', window_minutes: 120, entries: 0, reason: '等待开仓点位',
    coverage: {valid_minutes: 120, coverage_pct: 100, history_start_ts: 1792072800,
      history_end_ts: 1792076400, latest_data_age_sec: 60}
  }}
}};
renderMonitor();
`, context);
const html = nodes.get('monitor-summary').innerHTML;
assert.match(html, /最新参考历史/);
assert.match(html, /最后记录距今 1\.0 分钟/);
assert.ok(html.includes(vm.runInContext('windowTime(1792072800)', context)));
assert.equal(vm.runInContext('windowTime(null)', context), '—');
assert.equal(vm.runInContext('windowTime(NaN)', context), '—');
assert.match(vm.runInContext('windowTime(0)', context), /1970/);
''', str(source)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
