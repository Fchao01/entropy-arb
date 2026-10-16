"""Execute the real browser config migration and executable signal renderer."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_browser_migrates_once_and_renders_executable_targets():
    node = shutil.which('node')
    if not node:
        pytest.skip('Node.js unavailable')
    source = Path(__file__).resolve().parents[1] / 'web' / 'app.js'
    result = subprocess.run([node, '-e', r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8');
const nodes = new Map();
function get(id) {
  if (!nodes.has(id)) nodes.set(id, {value: '', innerHTML: '', textContent: '',
    closest() {return this;}, querySelectorAll() {return [];}});
  return nodes.get(id);
}
const context = vm.createContext({location: {pathname: '/monitor'}, document: {getElementById: get}});
const utilities = source.slice(0, source.indexOf('async function api('));
const config = source.slice(source.indexOf('const fieldMap ='), source.indexOf('async function openEditor('));
const render = source.slice(source.indexOf('function renderSignals('), source.indexOf('\nfunction drawChart('));
vm.runInContext(utilities + '\n' + config + '\n' + render + String.raw`
state.meta = {primary_venues: ['lighter-rh', 'arcus']};
element('config-yaml').value = 'strategy:\n  mode: bps\nbps:\n  levels: rolling\n  direction: long_primary\n  entry_percentile: 10\n  exit_percentile: 75\nexecution:\n  primary_order_type: maker\n';
syncFields();
`, context);
assert.equal(get('bps-price-basis').value, 'executable');
assert.equal(get('bps-entry-percentile').value, 90);
assert.equal(get('bps-exit-percentile').value, 75);
assert.equal(get('primary-order-type').value, 'maker');
assert.equal(get('primary-order-type').disabled, false);
assert.equal(get('maker-cancel-policy').hidden, false);
assert.equal(get('primary-maker-fee').disabled, false);
assert.match(get('config-yaml').value, /primary_order_type: maker/);
vm.runInContext('syncFields()', context);
assert.equal(get('bps-entry-percentile').value, 90);
vm.runInContext(`
element('bps-exit-mode').value = 'profit';
refreshStrategyFields();
`, context);
assert.equal(get('bps-price-basis').value, 'mid');
assert.equal(get('bps-entry-percentile').value, 10);
vm.runInContext(`
renderSignals({bps_strategy: {price_basis: 'executable', entry_bps: 5, exit_bps: -4},
  venues: [{fresh: true}], execution_signals: {mode: 'bps', rows: [
  {action: 'open', primary_buy: true, buy: 'ARCUS', sell: 'RH', order_type: 'taker',
   comparison: '>=', executable_premium_bps: 6, trigger_premium_bps: 6, hurdle_bps: 5, gap_bps: 1}
]}}, true);
`, context);
assert.match(get('signal-summary').textContent, /可成交价差/);
assert.match(get('signal-rows').innerHTML, /双腿吃单/);
assert.match(get('signal-rows').innerHTML, /≥/);
assert.match(get('signal-note').textContent, /卖方买一/);
assert.doesNotMatch(get('signal-note').textContent, /中间价溢价/);
vm.runInContext(`
renderSignals({bps_strategy: {price_basis: 'executable', entry_bps: 5, exit_bps: -4},
  venues: [{fresh: true}], execution_signals: {mode: 'bps', rows: [
  {action: 'open', primary_buy: true, buy: 'ARCUS', sell: 'RH', order_type: 'maker',
   comparison: '>=', executable_premium_bps: 7, trigger_premium_bps: 6, hurdle_bps: 5, gap_bps: 1}
]}}, true);
`, context);
assert.match(get('signal-summary').textContent, /开仓主腿挂单/);
assert.match(get('signal-rows').innerHTML, /主腿挂单 \/ 对冲腿吃单/);
assert.match(get('signal-note').textContent, /确认新增成交后才对冲/);
vm.runInContext(`
element('task-primary').value = 'arcus';
refreshStrategyFields();
renderSignals({primary_maker_only: true,
  bps_strategy: {price_basis: 'executable', entry_bps: 5, exit_bps: -4},
  venues: [{fresh: true}], execution_signals: {mode: 'bps', rows: [
  {action: 'open', primary_buy: true, buy: 'ARCUS', sell: 'RH', order_type: 'maker',
   comparison: '>=', executable_premium_bps: 7, trigger_premium_bps: 6, hurdle_bps: 5, gap_bps: 1},
  {action: 'close', primary_buy: false, buy: 'RH', sell: 'ARCUS', order_type: 'maker',
   comparison: '>=', executable_premium_bps: -3, trigger_premium_bps: -3, hurdle_bps: -4, gap_bps: 1}
]}}, true);
`, context);
assert.match(get('execution-note').textContent, /开仓、平仓、余仓修正均只使用 Post-only/);
assert.match(get('execution-note').textContent, /绝不自动转吃单/);
assert.match(get('signal-summary').textContent, /开仓、平仓主腿均挂单/);
assert.doesNotMatch(get('signal-summary').textContent, /平仓双腿吃单/);
assert.match(get('signal-note').textContent, /不自动转为 IOC/);
assert.match(get('signal-note').textContent, /不能保证立即完成/);
''', str(source)], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
