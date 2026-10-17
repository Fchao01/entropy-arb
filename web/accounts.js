"use strict";

const accountState = {page: 1, result: null, selected: null, refreshing: false, configBusy: false,
  detailRequest: 0, detailAt: 0, detailBusy: false, actionError: ""};
const walletState = {address: null, period: "24h", request: 0, busy: false, updated: 0};
const accountVenue = (v) => v === "lighter-rh" ? "RH" : "Lighter";
const shortAddress = (v) => v ? `${v.slice(0, 8)}…${v.slice(-6)}` : "待查询";
const accountParams = () => new URLSearchParams({period: element("accounts-period").value,
  venue: element("accounts-venue").value, search: element("accounts-search").value.trim(), page: accountState.page});
const accountPath = (venue, id) => `/api/account-monitor/${encodeURIComponent(venue)}/${encodeURIComponent(id)}`;
const accountTable = (head, rows, empty) => `<div class="table-scroll"><table class="task-table account-table"><thead><tr>${head.map((h) => `<th>${escapeHtml(h)}</th>`).join("")}</tr></thead><tbody>${rows.length ? rows.join("") : `<tr><td colspan="${head.length}" class="table-empty">${escapeHtml(empty)}</td></tr>`}</tbody></table></div>`;
const accountTradeTime = (ts) => ts == null || !Number.isFinite(Number(ts)) ? "—" : new Date(Number(ts) * 1000).toLocaleString("zh-CN", {timeZone: "Asia/Shanghai", hour12: false, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3});

function hedgeLegRow(leg, selected) {
  const own = leg.venue === selected.venue && String(leg.account) === String(selected.id);
  const identity = `${accountVenue(leg.venue)} · ${escapeHtml(leg.account)}`;
  return `<tr class="account-hedge-leg"><td>${own ? `<strong>${identity}</strong><small>本账户</small>` : `<button class="text-button" data-account-open="${escapeHtml(leg.account)}" data-venue="${escapeHtml(leg.venue)}">${identity}</button><small>关联对边</small>`}</td>
    <td>${escapeHtml(leg.symbol)}<small>${escapeHtml(leg.position_action || "开平未知")}</small></td><td>${accountTradeTime(leg.ts)}</td>
    <td class="${leg.side === 1 ? "positive" : "negative"}">${leg.side === 1 ? "买入" : "卖出"}<small>${leg.maker ? "Maker · 挂单成交" : "Taker · 吃单成交"}</small></td>
    <td class="number">${number(leg.price, 8)}</td><td class="number">${number(leg.size, 8)}</td><td class="number">${money(leg.usd, 4)}</td>
    <td><span class="account-fill-id" title="${escapeHtml(leg.id)}">${escapeHtml(leg.id)}</span><small>市场 ${escapeHtml(leg.market)} · ${escapeHtml(leg.kind)}</small></td></tr>`;
}

function renderAccounts(result) {
  const s = result.status;
  const connections = Object.values(s.connections || {});
  element("accounts-status").textContent = s.running ? `采集中 · ${connections.filter((x) => x === "已订阅").length}/${connections.length} 组已订阅` : "已停止 · 历史保留";
  element("accounts-status").className = `badge ${s.running ? "running" : ""}`;
  element("accounts-stop").disabled = !s.running || accountState.configBusy;
  const errorValues = Object.entries(s.errors || {}).map(([key, error]) => `${key}：${error}`);
  element("accounts-error").textContent = [accountState.actionError, ...errorValues].filter(Boolean).join("；");
  const summary = result.summary;
  element("accounts-coverage").textContent = `当前采集币种：${s.config.symbols}。所选窗口实际成交范围：${windowTime(summary.first_trade)} 至 ${windowTime(summary.last_trade)}；RH ${s.markets["lighter-rh"] || 0} / Lighter ${s.markets.lighter || 0} 个市场。累计 ${summary.gaps} 次中断或缺口事件；排队 ${s.queue_size} 条。近期成交补充 ${s.backfill?.done || 0}/${s.backfill?.total || 0} 个市场，最多 100 笔/市场，不能视为完整历史。`;
  element("accounts-stats").innerHTML = stat("累计记录账户", number(summary.accounts, 0), "含主动关注和地址查询发现的账户", "◎")
    + stat("当前筛选活跃账户", number(result.total, 0), "按成交额排序，每页 50 个", "▥")
    + stat("窗口内撮合成交额", money(summary.market_volume), "平台筛选生效，双方账户合计会重复计数", "⇄")
    + stat("窗口内撮合笔数", number(summary.trades, 0), "重连重复成交已去重", "↗");
  element("accounts-rows").innerHTML = result.accounts.length ? result.accounts.map((r) => `<tr>
    <td><strong>${accountVenue(r.venue)} · ${escapeHtml(r.id)}</strong>${r.watched ? '<small>★ 已关注</small>' : ""}</td>
    <td title="${escapeHtml(r.hedge?.reason || "尚未核验")}"><span class="badge ${r.hedge?.code === "matched" ? "running" : ""}">${escapeHtml(r.hedge?.label || "待核验")}</span><small>${(r.hedge?.peers || []).slice(0, 2).map((p) => `${accountVenue(p.venue)} · ${escapeHtml(p.id)}`).join(" / ")}</small></td>
    <td title="${escapeHtml(r.address || r.error || "后台逐步查询地址；点击详情可主动查询")}">${escapeHtml(shortAddress(r.address))}</td>
    <td class="number">${money(r.volume)}</td><td class="number">${number(r.trade_count, 0)}</td><td class="number">${money(r.volume / r.trade_count)}</td><td class="number">${number(r.maker_pct, 1)}%</td><td class="number">${r.symbols}</td><td>${windowTime(r.last_trade)}</td>
    <td>${r.address ? `<button class="text-button" data-wallet-address="${escapeHtml(r.address)}">开平仓分析</button>` : ""}<button class="text-button" data-account-open="${r.id}" data-venue="${r.venue}">详情</button><button class="text-button" data-account-watch="${r.id}" data-watched="${r.watched ? "0" : "1"}" data-venue="${r.venue}">${r.watched ? "取消关注" : "关注"}</button></td></tr>`).join("") : '<tr><td colspan="10" class="table-empty">当前筛选没有已采集成交。检查订阅状态，或添加已知账户查看仓位。</td></tr>';
  const pages = Math.max(1, Math.ceil(result.total / 50));
  element("accounts-pagination").textContent = `第 ${result.page} / ${pages} 页 · ${result.total} 个账户`;
  element("accounts-prev").disabled = result.page <= 1;
  element("accounts-next").disabled = result.page >= pages;
  element("accounts-watches").innerHTML = (result.watches || []).map((r) => `<button class="button secondary small" data-account-open="${r.id}" data-venue="${r.venue}">★ ${accountVenue(r.venue)} · ${r.id}</button>`).join("");
  element("accounts-events").innerHTML = result.events.length ? result.events.map((e) => `<div class="account-event"><time>${windowTime(e.ts)}</time><b>${escapeHtml(e.venue ? accountVenue(e.venue) : "系统")}</b><span>${escapeHtml(e.message)}</span></div>`).join("") : "还没有采集事件";
}

async function refreshAccounts() {
  if (!state.authenticated || accountState.refreshing) return;
  accountState.refreshing = true;
  const query = String(accountParams());
  try {
    const result = await api(`/api/account-monitor?${query}`);
    if (query !== String(accountParams())) return;
    if (!accountState.result) {
      element("accounts-symbols").value = result.status.config.symbols;
      element("accounts-recent").checked = Boolean(result.status.config.recent);
    }
    accountState.result = result;
    renderAccounts(result);
    if (accountState.selected && !accountState.detailBusy && Date.now() - accountState.detailAt > 15000) await loadAccountDetail();
    if (walletState.address && !walletState.busy && Date.now() - walletState.updated > 15000) await refreshWalletAnalysis();
  } catch (error) {
    element("accounts-error").textContent = `账户监控刷新失败：${error.message}`;
    element("accounts-status").textContent = "连接异常 · 数据未更新";
    element("accounts-status").className = "badge failed";
  } finally {accountState.refreshing = false;}
}

function renderAccountDetail(result) {
  const a = result.account, snapshot = a.snapshot || {};
  element("account-detail-title").textContent = `${accountVenue(a.venue)} · ${a.id}${a.watched ? " · 已关注" : ""}`;
  element("account-detail-source").textContent = `地址 ${a.address || "待查询"} · 公开仓位快照 ${a.fetched ? windowTime(a.fetched) : "—"} · 成交分析使用所选窗口内最近 ${result.analysis_fills} 笔${result.truncated ? "（已截取，非完整窗口）" : ""}；明细显示最近 100 笔。`;
  const positions = Array.isArray(snapshot.positions) ? snapshot.positions : Object.values(snapshot.positions || {});
  const patterns = result.patterns.map((p) => `<p><b>${escapeHtml(p.label)}</b> · ${escapeHtml(p.evidence)}</p>`).join("");
  const peers = result.peers.map((p) => `<button class="button secondary small" data-account-open="${p.id}" data-venue="${p.venue}">${accountVenue(p.venue)} · ${p.id}</button>`).join("");
  const legHeaders = ["平台 / 账户", "币种 / 仓位变化", "成交时间（上海）", "方向 / 成交方式", "实际成交价", "实际成交数量", "实际成交额", "成交编号 / 市场"];
  const matches = result.matches.map((m, i) => `<tr class="account-hedge-group"><td colspan="8"><strong>第 ${i + 1} 组 · ${escapeHtml(m.symbol)}</strong><span>两边成交额 ${money(m.total_volume, 4)}</span><span>对边时间差 ${signed(m.delay_ms, 0)} ms</span><span>中间价 ${signed(m.quote_bps, 3)} bps</span><span>双边成交价 ${signed(m.fill_bps, 3)} bps</span></td></tr>${(m.legs || []).map((leg) => hedgeLegRow(leg, a)).join("")}`);
  const peerTrades = (result.peer_fills || []).map((leg) => hedgeLegRow(leg, a));
  const cycles = result.cycles.map((c) => `<tr><td>${escapeHtml(c.symbol)}</td><td>${windowTime(c.started_at)}</td><td>${windowTime(c.ended_at)}</td><td>${number(c.hold_sec, 1)}</td><td>${c.count}</td><td>${signed(c.entry_bps, 3)} / ${signed(c.exit_bps, 3)}</td><td>${money(c.cash, 4)}</td></tr>`);
  const fills = result.fills.map((f) => `<tr><td>${windowTime(f.ts)}</td><td>${escapeHtml(f.symbol)}</td><td>${f.side === 1 ? "买" : "卖"} · ${f.maker ? "Maker" : "Taker"}</td><td>${number(f.price, 8)}</td><td>${number(f.size, 8)}</td><td>${money(f.usd)}</td><td>${signed(f.bps, 3)}</td><td>${escapeHtml(f.kind)}</td></tr>`);
  element("account-detail-body").innerHTML = `<div class="account-note">${escapeHtml(a.error || "当前仓位是快照，成交筛选窗口不会改变仓位快照。余额、盈亏字段以交易所公开口径为准。")}</div>
    <div class="account-evidence"><strong>${escapeHtml(result.hedge?.label || "对冲证据尚未核验")}</strong><p>${escapeHtml(result.hedge?.reason || "点击查询最新仓位与关联以核验对边")}</p>${a.address ? `<button class="button primary small" data-wallet-address="${escapeHtml(a.address)}">对比此地址的 RH / 主网开平仓</button>` : ""}</div>
    <h3 class="account-subheading">当前仓位</h3>${accountTable(["币种", "方向", "数量", "均价", "未实现盈亏（平台口径）"], positions.filter((p) => Number(p.position) !== 0).map((p) => `<tr><td>${escapeHtml(p.symbol || p.market_id)}</td><td>${Number(p.sign) < 0 ? "空" : "多"}</td><td>${number(p.position, 8)}</td><td>${number(p.avg_entry_price, 8)}</td><td>${money(p.unrealized_pnl, 4)}</td></tr>`), a.fetched ? "快照无持仓；若刚查询了地址关联，需在该平台单独查询仓位" : "尚未查询公开仓位")}
    <div class="account-note"><b>成交行为观察</b>${patterns || "<p>样本不足，或未达到行为标签条件。</p>"}标签不能证明策略或盈利，未成交订单和撤单历史不在公开成交统计内。</div>
    <h3 class="account-subheading">同地址关联账户</h3><div class="account-note account-watches">${peers || "尚无已确认的同地址对边账户，点击上方“查询最新仓位与关联”。"}</div>
    <h3 class="account-subheading">疑似跨平台对冲成交</h3><p class="account-note">同地址、同币种、方向相反、时间相差 ≤ 3 秒且基础数量相差 ≤ 5%，一对一匹配。对边也只取所选窗口最近 2,000 笔${result.peer_truncated ? "（已截取）" : ""}，匹配显示最近 50 组。时间差为对边减本账户；成交溢价按两笔实际价格计算，非同时报价或净利润。拆单、不同钱包和缺口可能漏配，偶合也可能误配。</p>
    ${accountTable(legHeaders, matches, "当前样本没有符合条件的同地址反向成交")}
    <h3 class="account-subheading">关联对边最近成交（含未匹配）</h3><p class="account-note">显示同地址另一平台在所选窗口内已采集的最近 100 笔成交，未满足匹配条件的记录也保留。开多、平空等标记按公开成交前仓位推算，未知时标为开平未知。这里的成交额和价差不是净利润，未确认手续费和资金费不填为 0。</p>
    ${accountTable(legHeaders, peerTrades, result.peers.length ? "已关联对边账户，但所选窗口内尚无本地成交数据；确认对应币种正在采集" : "尚未关联对边账户，请先查询最新仓位与关联")}
    <h3 class="account-subheading">已观察到的完整单平台往返</h3><p class="account-note">仅计交易所成交前仓位能确认从 0 开始、回到 0 且数据未出现中断的往返。成交现金差未扣手续费、资金费，不等于跨平台套利净收益。开平仓 bps 是首末成交时的 RH / Lighter 中间价溢价。</p>
    ${accountTable(["币种", "开始", "结束", "持有秒数", "成交笔数", "首末 bps", "成交现金差（未扣费）"], cycles, "还没有可以确认起止仓位的完整往返")}
    <h3 class="account-subheading">最近成交</h3>${accountTable(["时间", "币种", "方向 / 成交方式", "价格", "数量", "成交额", "当时中间价 bps", "类型"], fills, "本地尚无该账户在所选窗口内的成交")}`;
}

async function openAccount(venue, id) {
  accountState.selected = {venue, id: String(id)};
  element("account-detail-panel").hidden = false;
  element("account-detail-title").textContent = `${accountVenue(venue)} · ${id}`;
  element("account-detail-source").textContent = "正在读取本地记录…";
  element("account-detail-body").replaceChildren();
  await loadAccountDetail();
  element("account-detail-panel").scrollIntoView({behavior: "smooth", block: "start"});
}

async function loadAccountDetail(fetchPublic = false) {
  if (!accountState.selected) return;
  const {venue, id} = accountState.selected, request = ++accountState.detailRequest;
  const period = element("accounts-period").value;
  accountState.detailBusy = true;
  element("account-fetch").disabled = true;
  try {
    const path = accountPath(venue, id) + (fetchPublic ? "/refresh" : "") + `?period=${encodeURIComponent(period)}`;
    const result = await api(path, fetchPublic ? "POST" : "GET", fetchPublic ? {} : undefined);
    if (request !== accountState.detailRequest || period !== element("accounts-period").value) return;
    renderAccountDetail(result);
    accountState.detailAt = Date.now();
  } catch (error) {
    if (request === accountState.detailRequest) element("account-detail-source").textContent = `详情未更新：${error.message}`;
  } finally {
    if (request === accountState.detailRequest) {accountState.detailBusy = false; element("account-fetch").disabled = false;}
  }
}

async function accountWatch(venue, id, watched) {
  await api(accountPath(venue, id) + "/watch", "POST", {watched});
  accountState.detailAt = 0;
  await refreshAccounts();
}

element("accounts-start-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  accountState.configBusy = true;
  accountState.actionError = "";
  element("accounts-start").disabled = true;
  element("accounts-stop").disabled = true;
  element("accounts-error").textContent = "正在发现所选公开市场…";
  try {
    await api("/api/account-monitor/start", "POST", {symbols: element("accounts-symbols").value, recent: element("accounts-recent").checked});
    await refreshAccounts();
    toast("账户监控已启动，正在逐步订阅成交和报价");
  } catch (error) {accountState.actionError = error.message; element("accounts-error").textContent = error.message;}
  finally {accountState.configBusy = false; element("accounts-start").disabled = false; element("accounts-stop").disabled = !accountState.result?.status.running;}
});
element("accounts-stop").addEventListener("click", async () => {
  element("accounts-stop").disabled = true;
  try {await api("/api/account-monitor/stop", "POST", {}); await refreshAccounts();}
  catch (error) {toast(error.message, true); element("accounts-stop").disabled = false;}
});
for (const id of ["accounts-period", "accounts-venue"]) element(id).addEventListener("change", () => {
  accountState.page = 1; accountState.detailAt = 0;
  if (id === "accounts-period" && accountState.selected) {
    accountState.detailRequest++; accountState.detailBusy = false;
    element("account-detail-body").replaceChildren();
    element("account-detail-source").textContent = "正在按新窗口读取…";
    loadAccountDetail();
  }
  refreshAccounts();
});
let accountSearchTimer;
element("accounts-search").addEventListener("input", () => {
  clearTimeout(accountSearchTimer);
  accountSearchTimer = setTimeout(() => {accountState.page = 1; refreshAccounts();}, 300);
});
element("accounts-prev").addEventListener("click", () => {accountState.page = Math.max(1, accountState.page - 1); refreshAccounts();});
element("accounts-next").addEventListener("click", () => {accountState.page++; refreshAccounts();});
element("accounts-page").addEventListener("click", (event) => {
  const button = event.target.closest("[data-account-open], [data-account-watch], [data-wallet-address]");
  if (!button) return;
  if (button.dataset.walletAddress) queryWallet(button.dataset.walletAddress);
  else if (button.dataset.accountOpen) openAccount(button.dataset.venue, button.dataset.accountOpen);
  else accountWatch(button.dataset.venue, button.dataset.accountWatch, button.dataset.watched === "1").catch((error) => toast(error.message, true));
});
element("accounts-watch-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const venue = element("accounts-watch-venue").value, id = element("accounts-watch-id").value.trim();
  try {await accountWatch(venue, id, true); await openAccount(venue, id);}
  catch (error) {toast(error.message, true);}
});
element("account-fetch").addEventListener("click", () => loadAccountDetail(true));
element("account-detail-close").addEventListener("click", () => {
  accountState.detailRequest++; accountState.detailBusy = false; accountState.selected = null;
  element("account-detail-panel").hidden = true;
});

function walletLeg(leg) {
  const label = leg.venue === "lighter-rh" ? "RH" : "Lighter 主网";
  return `<section class="wallet-leg"><header><b>${label} · ${escapeHtml(leg.account)}</b><span class="${leg.side === 1 ? "positive" : "negative"}">${leg.side === 1 ? "买入" : "卖出"} · ${escapeHtml(leg.position_action)}</span></header>
    <dl><dt>成交时间</dt><dd>${accountTradeTime(leg.ts)}</dd><dt>实际价格</dt><dd>${number(leg.price, 8)}</dd><dt>实际数量</dt><dd>${number(leg.size, 8)} ${escapeHtml(leg.symbol)}</dd><dt>成交额</dt><dd>${money(leg.usd, 4)}</dd><dt>成交方式</dt><dd>${leg.maker ? "Maker 挂单成交" : "Taker 吃单成交"}</dd><dt>成交编号</dt><dd>${escapeHtml(leg.id)}</dd></dl></section>`;
}

function walletPair(pair, title) {
  const legs = [...pair.legs].sort((a, b) => (a.venue === "lighter-rh" ? 0 : 1) - (b.venue === "lighter-rh" ? 0 : 1));
  const delta = (legs[1].ts - legs[0].ts) * 1000;
  return `<div class="wallet-pair"><h4>${escapeHtml(title || pair.phase || "疑似配对")} <small>${escapeHtml(pair.symbol)} · RH / 主网成交价溢价 ${signed(pair.fill_bps, 3)} bps · 主网晚于 RH ${signed(delta, 0)} ms</small></h4><div class="wallet-leg-grid">${legs.map(walletLeg).join("")}</div></div>`;
}

function renderWalletAnalysis(result) {
  const container = element("wallet-result"), viewKey = `${result.address}/${walletState.period}`;
  const fingerprint = JSON.stringify({...result, checked_at: undefined});
  if (walletState.renderedKey === viewKey && walletState.fingerprint === fingerprint) {container.hidden = false; return;}
  const scrollY = typeof window !== "undefined" ? window.scrollY : null;
  const saved = new Map(walletState.renderedKey === viewKey ? [...(container.querySelectorAll?.("details[data-wallet-section]") || [])].map((node) => [node.dataset.walletSection, node.open]) : []);
  const rh = result.stats["lighter-rh"], core = result.stats.lighter;
  const checks = new Map(result.checks.map((c) => [c.venue, c]));
  const notes = [];
  if (result.source_notice) notes.push(result.source_notice);
  for (const venue of ["lighter-rh", "lighter"]) {
    const check = checks.get(venue), count = result.accounts.filter((a) => a.venue === venue).length;
    if (check?.error) notes.push(`${accountVenue(venue)} 地址查询失败：${check.error}`);
    else if (!check) notes.push(`${accountVenue(venue)} 尚未核验地址目录`);
    else if (!count) notes.push(`${accountVenue(venue)} 未查到此地址的账户，不排除使用其他钱包`);
    else if (!check.complete) notes.push(`${accountVenue(venue)} 地址目录仍有分页，账户关联可能不完整`);
    if (!(venue === "lighter-rh" ? rh : core).trades) notes.push(`${accountVenue(venue)} 在此窗口内没有已采集成交，无法补造历史开平仓`);
  }
  if (result.truncated) notes.push("分析仅取两边各最近 2,000 笔，已截取，不能代表完整窗口");
  if (!result.collecting) notes.push("连续采集当前已停止；此结果为本地历史观察");
  const accounts = result.accounts.map((a) => `<button class="button secondary small" data-account-open="${a.id}" data-venue="${a.venue}">${accountVenue(a.venue)} · ${a.id}</button>`).join("");
  const cycles = result.cycles.map((cycle, i) => `<details class="wallet-cycle" data-wallet-section="cycle-${cycle.started_at}-${cycle.rh_account}-${cycle.core_account}" ${i === 0 ? "open" : ""}><summary>第 ${i + 1} 轮 · ${escapeHtml(cycle.symbol)} · ${escapeHtml(cycle.direction)} <span>${accountTradeTime(cycle.started_at)} → ${accountTradeTime(cycle.ended_at)} · ${number(cycle.hold_sec, 1)} 秒</span></summary>
    ${walletPair(cycle.sequence[0], "开仓对比")}${walletPair(cycle.sequence[cycle.sequence.length - 1], "平仓对比")}
    <details class="wallet-sequence" data-wallet-section="sequence-${cycle.started_at}-${cycle.rh_account}-${cycle.core_account}"><summary>展开本轮全部 ${cycle.sequence.length} 组成交（含加仓 / 减仓）</summary>${cycle.sequence.map((p) => walletPair(p)).join("")}</details></details>`).join("");
  const timeline = result.matches.map((p) => walletPair(p)).join("");
  const unmatched = result.unmatched.map((leg) => `<tr><td>${leg.venue === "lighter-rh" ? "RH" : "Lighter 主网"} · ${escapeHtml(leg.account)}</td><td>${escapeHtml(leg.symbol)}</td><td>${accountTradeTime(leg.ts)}</td><td>${leg.side === 1 ? "买入" : "卖出"} · ${escapeHtml(leg.position_action)}</td><td>${number(leg.price, 8)}</td><td>${number(leg.size, 8)}</td><td>${escapeHtml(leg.id)}</td></tr>`);
  element("wallet-result").innerHTML = `<div class="panel-heading"><div><h2>钱包双边开平仓分析</h2><p class="wallet-address-text">${escapeHtml(result.address)}</p></div><span class="badge ${result.matched_groups ? "running" : ""}">${result.matched_groups ? "已观察到疑似配对" : "尚无双边配对证据"}</span></div>
    <div class="wallet-summary"><div><span>RH 已采集成交</span><strong>${number(rh.trades, 0)} 笔</strong><small>${money(rh.volume)}</small></div><div><span>Lighter 主网已采集成交</span><strong>${number(core.trades, 0)} 笔</strong><small>${money(core.volume)}</small></div><div><span>样本疑似配对</span><strong>${result.matched_groups} 组</strong></div><div><span>确认完整双腿往返</span><strong>${result.complete_cycles} 轮</strong></div></div>
    ${notes.length ? `<div class="wallet-notes">${notes.map((n) => `<p>${escapeHtml(n)}</p>`).join("")}</div>` : ""}<div class="account-watches">${accounts}</div>
    <section class="wallet-strategy"><h3>从成交样本观察到的交易特征</h3>${result.strategy.findings.map((f) => `<p>${escapeHtml(f)}</p>`).join("")}<small>成交价溢价与同时盘口溢价不同；未扣手续费、资金费或换汇成本，不能当作净收益或确定的触发阈值。</small></section>
    <h3 class="account-subheading">完整开仓 → 平仓对比</h3><p class="account-note">两边均从已知零仓位开仓、最终归零，且期间无未匹配成交或已知缺口，才计为完整往返。最多展示最近 50 轮。</p>
    <div class="wallet-cycles">${cycles || '<p class="account-note">尚未确认完整双腿往返。下方仍可查看已匹配的开仓 / 平仓片段。</p>'}</div>
    <details class="wallet-sequence" data-wallet-section="timeline" ${cycles ? "" : "open"}><summary>双边配对成交片段 · 最近 ${result.matches.length} 组</summary>${timeline || '<p class="account-note">当前样本没有符合同地址、同币种反向、3 秒内、数量差不超过 5% 的成交。</p>'}</details>
    ${result.incomplete.length ? `<div class="wallet-notes"><b>尚未组成完整往返的原因</b>${result.incomplete.map((c) => `<p>${escapeHtml(c.symbol)}：${escapeHtml(c.reason)}</p>`).join("")}</div>` : ""}
    <details class="wallet-sequence" data-wallet-section="unmatched"><summary>未匹配成交 · 样本 ${result.unmatched_count} 笔，显示最近 ${result.unmatched.length} 笔</summary>${accountTable(["平台 / 账户", "币种", "时间", "方向 / 仓位变化", "价格", "数量", "成交编号"], unmatched, "样本内无未匹配成交")}</details>`;
  for (const node of container.querySelectorAll?.("details[data-wallet-section]") || []) if (saved.has(node.dataset.walletSection)) node.open = saved.get(node.dataset.walletSection);
  walletState.renderedKey = viewKey;
  walletState.fingerprint = fingerprint;
  element("wallet-result").hidden = false;
  if (saved.size && scrollY != null) window.scrollTo({top: scrollY, behavior: "instant"});
}

async function refreshWalletAnalysis(refresh = false) {
  if (!walletState.address || walletState.busy) return;
  const address = walletState.address, period = walletState.period, request = ++walletState.request;
  walletState.busy = true;
  element("wallet-query-submit").disabled = true;
  if (refresh) element("wallet-query-error").textContent = "正在查询两边的公开地址目录，并核对本地开平仓记录…";
  try {
    const path = `/api/account-address/${encodeURIComponent(address)}${refresh ? "/refresh" : ""}?period=${encodeURIComponent(period)}`;
    const result = await api(path, refresh ? "POST" : "GET", refresh ? {} : undefined);
    if (request !== walletState.request || address !== walletState.address || period !== walletState.period) return;
    renderWalletAnalysis(result);
    element("wallet-query-error").textContent = "";
    walletState.updated = Date.now();
  } catch (error) {if (request === walletState.request) element("wallet-query-error").textContent = error.message;}
  finally {if (request === walletState.request) {walletState.busy = false; element("wallet-query-submit").disabled = false;}}
}

async function queryWallet(address) {
  walletState.request++;
  walletState.busy = false;
  walletState.address = address.trim().toLowerCase();
  walletState.period = element("wallet-period").value;
  element("wallet-address").value = walletState.address;
  element("wallet-result").hidden = true;
  element("wallet-query-panel").scrollIntoView({behavior: "smooth", block: "start"});
  await refreshWalletAnalysis(true);
}
element("wallet-query-form").addEventListener("submit", (event) => {event.preventDefault(); queryWallet(element("wallet-address").value);});
element("wallet-period").addEventListener("change", () => {walletState.request++; walletState.busy = false; walletState.period = element("wallet-period").value; element("wallet-result").hidden = true; if (walletState.address) refreshWalletAnalysis();});
