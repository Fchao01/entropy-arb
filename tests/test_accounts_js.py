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
vm.runInContext(`renderAccountDetail({account: {venue: 'lighter', id: 99, fetched: 0},
  fills: [], peers: [{venue:'lighter-rh', id:42}], patterns: [], cycles: [], analysis_fills: 0,
  matches: [{symbol:'BTC', ts:10.123, total_volume:203, delay_ms:-222, quote_bps:null, fill_bps:-196,
    legs:[{venue:'lighter', account:99, market:8, id:'9007199254740995', symbol:'BTC',
      ts:10.345, side:-1, maker:1, price:102, size:1, usd:102, position_action:'开空', kind:'trade'},
      {venue:'lighter-rh', account:42, market:0, id:'9007199254740993', symbol:'BTC',
      ts:10.123, side:1, maker:0, price:100, size:1.01, usd:101, position_action:'开多',kind:'trade'}]}],
  peer_fills:[{venue:'lighter-rh',account:42,market:0,id:'9007199254740993',symbol:'BTC',
    ts:10.123,side:1,maker:0,price:100,size:1.01,usd:101,position_action:'开多',kind:'trade'}]});`, context);
const paired = element('account-detail-body').innerHTML;
assert.match(paired, /关联对边最近成交（含未匹配）/);
assert.match(paired, /两边成交额 \$203\.0000/);
assert.match(paired, /\$102\.0000/);
assert.match(paired, /1\.01000000/);
assert.match(paired, /Maker · 挂单成交/);
assert.match(paired, /Taker · 吃单成交/);
assert.match(paired, /9007199254740993/);
assert.match(paired, /9007199254740995/);
assert.match(paired, /08:00:10\.345/);
assert.match(paired, /data-account-open="42" data-venue="lighter-rh"/);
assert.match(paired, /<strong>Lighter · 99<\/strong><small>本账户/);
vm.runInContext(`renderWalletAnalysis({address:'0x1111111111111111111111111111111111111111',
  collecting:true, checks:[{venue:'lighter-rh',complete:1},{venue:'lighter',complete:1}],
  accounts:[{venue:'lighter-rh',id:42},{venue:'lighter',id:99}],stats:{
  'lighter-rh':{trades:2,volume:200},lighter:{trades:2,volume:202}},matched_groups:2,complete_cycles:1,
  cycles:[{symbol:'BTC',direction:'RH 多 / 主网空',started_at:10,ended_at:20,hold_sec:10,
  sequence:[{phase:'开仓',symbol:'BTC',fill_bps:-100,legs:[
    {venue:'lighter-rh',account:42,ts:10,price:99,size:1,usd:99,side:1,maker:1,position_action:'开多',symbol:'BTC',id:'1'},
    {venue:'lighter',account:99,ts:10.1,price:100,size:1,usd:100,side:-1,maker:0,position_action:'开空',symbol:'BTC',id:'2'}]},
  {phase:'平仓',symbol:'BTC',fill_bps:0,legs:[
    {venue:'lighter-rh',account:42,ts:20,price:100,size:1,usd:100,side:-1,maker:1,position_action:'平多',symbol:'BTC',id:'3'},
    {venue:'lighter',account:99,ts:20.1,price:100,size:1,usd:100,side:1,maker:0,position_action:'平空',symbol:'BTC',id:'4'}]}]}],
  strategy:{findings:['仅根据观察样本判断，不是策略代码']},matches:[],incomplete:[],unmatched:[],unmatched_count:0});`, context);
const wallet = element('wallet-result').innerHTML;
assert.match(wallet, /开仓对比/);
assert.match(wallet, /平仓对比/);
assert.match(wallet, /RH · 42/);
assert.match(wallet, /Lighter 主网 · 99/);
assert.match(wallet, /开多/);
assert.match(wallet, /平空/);
assert.match(wallet, /99\.00000000/);
assert.match(wallet, /仅根据观察样本判断/);
assert.equal(element('wallet-result').hidden, false);
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
