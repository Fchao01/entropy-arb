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
  status: {updated_at: 1792076400, venues: [], primary_order_type: 'maker', primary_maker_only: true,
    maker_cancel_policy: 'best_price', maker_reprice_delay_sec: .3,
    active_maker: {cancel_policy: 'best_price', state: 'OPEN', at_best_price: true, filled: 0, hedged: 0},
    bps_strategy: {
    levels: 'rolling', window_minutes: 120, entries: 0, reason: '等待开仓点位',
    position_residual: {accepted_dust: true, net_base: .0000694, notional_usd: .053597},
    coverage: {valid_minutes: 120, coverage_pct: 100, history_start_ts: 1792072800,
      history_end_ts: 1792076400, latest_data_age_sec: 60}
  }}
}};
renderMonitor();
`, context);
const html = nodes.get('monitor-summary').innerHTML;
assert.match(html, /最新参考历史/);
assert.match(html, /已确认精度尾差 \+0\.00006940/);
assert.match(html, /名义敞口 \$0\.053597/);
assert.match(html, /条件有效且在最优价持续保留/);
assert.match(html, /最优价排队中/);
assert.match(html, /Arcus 开仓、平仓、纠偏均只挂单，不转吃单/);
assert.doesNotMatch(html, /最长等待/);
assert.match(html, /最后记录距今 1\.0 分钟/);
assert.ok(html.includes(vm.runInContext('windowTime(1792072800)', context)));
assert.equal(vm.runInContext('windowTime(null)', context), '—');
assert.equal(vm.runInContext('windowTime(NaN)', context), '—');
assert.match(vm.runInContext('windowTime(0)', context), /1970/);
''', str(source)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


def test_monitor_does_not_retain_another_tasks_balances_after_errors():
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
const monitor = source.slice(source.indexOf('function clearMonitor('), source.indexOf('\nfunction renderSignals('));
const detail = source.slice(source.indexOf('let monitorRequest ='), source.indexOf('\nconst fieldMap ='));
const refresh = source.slice(source.indexOf('async function refresh()'), source.indexOf('\nfunction renderTriple('));
const nodes = new Map();
function node(id) {
  if (!nodes.has(id)) nodes.set(id, {
    html: '', get innerHTML() {return this.html;}, set innerHTML(v) {this.html = v;},
    get textContent() {return this.html;}, set textContent(v) {this.html = v;},
    replaceChildren() {this.html = '';}, removeAttribute(key) {delete this[key];},
    classList: {remove() {}, toggle() {}}, lastChild: {}, parentElement: {classList: {remove() {}, toggle() {}}},
  });
  return nodes.get(id);
}
const context = vm.createContext({
  location: {pathname: '/monitor'},
  document: {getElementById: node, querySelector: node},
  badge: () => '', stat: () => '', renderSignals() {}, drawChart() {}, renderLogs() {},
  refreshTriple() {}, renderTasks() {}, refreshTaskDetails() {},
});
vm.runInContext(utilities + '\n' + monitor + '\n' + detail + '\n' + refresh, context);
const run = code => vm.runInContext(code, context);
const snapshot = id => ({minutes: [], task: {id, symbol: 'SPY', primary: 'arcus',
  hedge: 'lighter-rh', mode: 'live', state: 'running', status_stale: false,
  status: {venues: [{name: 'ARCUS'}, {name: 'RH'}]}}});
(async () => {
  run("state.authenticated = true; state.selected = 'old'");
  node('venue-cards').innerHTML = 'RH $100 / LIGHTER $200';
  node('signal-rows').innerHTML = 'buy RH / sell LIGHTER';
  node('download-minutes').href = '/api/tasks/old/download/minutes';
  run("selectMonitorTask('new')");
  assert.doesNotMatch(node('venue-cards').textContent, /LIGHTER|\$200/);
  assert.doesNotMatch(node('signal-rows').innerHTML, /LIGHTER/);
  assert.equal(node('download-minutes').href, undefined);
  context.api = async path => path.endsWith('/logs') ? {} : snapshot('new');
  await run('refreshDetail()');
  assert.match(node('venue-cards').innerHTML, /ARCUS/);
  assert.doesNotMatch(node('venue-cards').innerHTML, /LIGHTER/);

  // A renderer throwing after a successful API call must clear the last balances.
  const broken = snapshot('new');
  broken.task.status.bps_strategy = {coverage: {get history_start_ts() {
    throw new ReferenceError('windowTime is not defined');
  }}};
  context.api = async path => path.endsWith('/logs') ? {} : broken;
  await assert.rejects(run('refreshDetail()'), /windowTime is not defined/);
  assert.match(node('monitor-summary').textContent, /监控刷新失败/);
  assert.doesNotMatch(node('venue-cards').textContent, /ARCUS/);
  assert.equal(run('state.detail'), null);

  // Reject data explicitly belonging to another task.
  context.api = async path => path.endsWith('/logs') ? {} : snapshot('wrong');
  await assert.rejects(run('refreshDetail()'), /返回的任务与所选任务不一致/);
  assert.equal(run('state.detail'), null);

  // A late response/failure for an older request must not overwrite the new page.
  const pending = [];
  context.api = path => new Promise((resolve, reject) => pending.push({path, resolve, reject}));
  const first = run('refreshDetail()');
  const second = run('refreshDetail()');
  pending[2].resolve(snapshot('new')); pending[3].resolve({});
  await second;
  pending[0].reject(new Error('obsolete request')); pending[1].resolve({});
  await first;
  assert.match(node('venue-cards').innerHTML, /ARCUS/);

  // A client rendering failure is not a disconnected server.
  context.api = async () => ({tasks: []});
  context.renderTasks = () => {throw new ReferenceError('render failed');};
  await run('refresh()');
  assert.equal(node('connection-label').textContent, '服务器已连接 · 页面刷新失败');
  assert.equal(node('connection-label').title, 'render failed');
})().catch(error => {console.error(error); process.exitCode = 1;});
''', str(source)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
