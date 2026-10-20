"use strict";

const element = (id) => document.getElementById(id);
const state = {tasks: [], meta: null, selected: null, page: location.pathname === "/accounts" ? "accounts" : location.pathname === "/monitor" ? "monitor" : "dex", authenticated: false, editing: null, detail: null, logs: null, logTab: "logs", tradesPage: 1, tradesPageSize: 10, busy: new Set(), polling: false, detailTask: null, detailTab: "logs", detailLogs: null, libraryKind: "yaml", libraryOriginal: null, tri: {candidates: [], snapshot: null, selectedSymbol: null}};
const venues = {entropy: "Entropy", lighter: "Lighter", "lighter-rh": "RH", tradexyz: "Trade.xyz", arcus: "Arcus"};
const active = (task) => ["running", "stopping"].includes(task.state);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (character) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[character]));
const number = (value, digits = 2) => value == null || !Number.isFinite(Number(value)) ? "—" : Number(value).toLocaleString("en-US", {maximumFractionDigits: digits, minimumFractionDigits: digits});
const signed = (value, digits = 2) => value == null ? "—" : `${Number(value) >= 0 ? "+" : ""}${number(value, digits)}`;
const money = (value, digits = 2) => value == null ? "—" : `$${number(value, digits)}`;
const timeLabel = (timestamp) => timestamp ? new Date(timestamp * 1000).toLocaleString("zh-CN", {hour12: false}) : "—";
const windowTime = (timestamp) => timestamp == null || !Number.isFinite(Number(timestamp)) ? "—" : new Date(Number(timestamp) * 1000).toLocaleString("zh-CN", {timeZone: "Asia/Shanghai", hour12: false});
const duration = (task) => {
  if (!task.started_at) return "—";
  const seconds = Math.max(0, Math.floor((active(task) ? Date.now() / 1000 : task.ended_at || Date.now() / 1000) - task.started_at));
  return `${String(Math.floor(seconds / 3600)).padStart(2, "0")}:${String(Math.floor(seconds % 3600 / 60)).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`;
};

async function api(path, method = "GET", body) {
  const response = await fetch(path, {method, credentials: "same-origin", headers: {"X-Web-Request": "1", ...(body !== undefined ? {"Content-Type": "application/json"} : {})}, ...(body !== undefined ? {body: JSON.stringify(body)} : {})});
  const payload = await response.json().catch(() => ({error: "服务器返回异常响应"}));
  if (!response.ok) {
    if (response.status === 401 && path !== "/api/login") showLogin();
    throw new Error(payload.error || "请求失败");
  }
  return payload;
}

let toastTimer;
function toast(message, error = false) {
  element("toast").textContent = message;
  element("toast").classList.toggle("error", error);
  element("toast").hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {element("toast").hidden = true;}, error ? 7000 : 4000);
}

function showLogin() {
  state.authenticated = false;
  element("credential-fields").replaceChildren();
  document.querySelectorAll("dialog[open]").forEach((dialog) => dialog.close());
  element("app").hidden = true;
  element("login-view").hidden = false;
}

async function enter() {
  state.meta = await api("/api/meta");
  state.authenticated = true;
  element("login-view").hidden = true;
  element("app").hidden = false;
  navigate(state.page, false);
  await refresh();
}

function navigate(page, push = true) {
  state.page = page;
  element("dex-page").hidden = page !== "dex";
  element("monitor-page").hidden = page !== "monitor";
  element("accounts-page").hidden = page !== "accounts";
  document.querySelectorAll(".nav-link").forEach((link) => link.classList.toggle("active", link.dataset.page === page));
  const label = page === "dex" ? "DEX 套利" : page === "accounts" ? "账户监控" : "监控";
  element("breadcrumb").textContent = `工作空间 / ${label}`;
  document.title = `${label} · Entropy Console`;
  if (push) history.pushState({}, "", page === "dex" ? "/dex-arbitrage" : `/${page}`);
  if (page === "monitor" && state.authenticated) {refreshDetail().catch((error) => toast(error.message, true)); loadTripleCandidates(); refreshTriple();}
  if (page === "accounts" && state.authenticated) refreshAccounts();
}

document.addEventListener("click", (event) => {
  const link = event.target.closest("a[data-page], .brand");
  if (link && !event.metaKey && !event.ctrlKey) {
    event.preventDefault();
    navigate(link.dataset.page || "dex");
  }
});
window.addEventListener("popstate", () => navigate(location.pathname === "/accounts" ? "accounts" : location.pathname === "/monitor" ? "monitor" : "dex", false));

function badge(task) {
  let label = {running: "运行中", stopping: "停止中", stopped: "已停止", failed: "异常退出", interrupted: "服务中断"}[task.state] || task.state;
  let className = task.state === "interrupted" ? "failed" : task.state;
  if (task.state === "running") {
    if (task.status_stale) {label = task.status ? "状态过期" : "启动中"; className = "stale";}
    else if (task.status?.liquidation?.latched) {label = "紧急平仓"; className = "halted";}
    else if (task.risk_watch_only) {label = "仅风险监控"; className = "running";}
    else if (task.status?.liquidation?.blocked) {label = "账户风险暂停"; className = "halted";}
    else if (task.status.paused) {label = "已暂停"; className = "halted";}
    else if (task.status.halted) {label = "风险暂停"; className = "halted";}
    else if (!task.status.ready) {label = "加载市场"; className = "starting";}
    else if (task.status.venues.some((venue) => !venue.fresh || venue.down)) {label = "行情异常"; className = "stale";}
  }
  return `<span class="badge ${className}"><span class="mini-dot"></span>${escapeHtml(label)}</span>`;
}

function thresholdStatus(task, compact = false) {
  if (task.mode !== "live") return "";
  if (task.status?.liquidation?.latched) return '<small class="threshold-status failed">紧急减仓 · 不等待策略点位</small>';
  if (task.risk_watch_only) return '<small class="threshold-status ok">只监控账户风险，不运行开仓策略</small>';
  if ((task.strategy_mode || task.status?.strategy_mode) === "spread") return `<small class="threshold-status ok">实际可成交价差 · 不限批次 · 按持仓上限控制</small>`;
  if ((task.strategy_mode || task.status?.strategy_mode) === "bps") {
    const reason = task.status?.bps_strategy?.reason || "独立点位";
    const waiting = compact && reason.match(/^等待有效分钟数据 (\d+)\/\d+；至少 (\d+) 条/);
    const summary = waiting ? `等待有效数据 ${waiting[1]}/${waiting[2]}` : compact ? reason.split(/[；;\n]/)[0] : reason;
    return `<small class="threshold-status ok" title="${escapeHtml(reason)}">bps 开平仓 · ${escapeHtml(summary)}</small>`;
  }
  const status = task.auto_threshold || {};
  const labels = {applied: "今日阈值已更新", unchanged: "今日已检查，阈值未变", failed: "今日阈值更新失败", pending: "今日阈值待更新", skipped: "今日阈值已跳过"};
  const label = labels[status.status] || labels.pending;
  const extra = status.status === "failed" ? compact ? "" : `：${status.reason || "请查看事件日志"}` : (status.window ? ` · ${status.window}` : "");
  const cls = status.status === "failed" ? "failed" : status.status === "applied" || status.status === "unchanged" ? "ok" : "pending";
  return `<small class="threshold-status ${cls}" title="${escapeHtml(status.reason || label)}">${escapeHtml(label + extra)}</small>`;
}

function stat(label, value, foot, icon, unit = "") {
  return `<article class="stat-card"><div class="stat-label">${label}<span class="stat-icon">${icon}</span></div><div class="stat-value">${value}${unit ? `<small>${unit}</small>` : ""}</div><div class="stat-foot">${foot}</div></article>`;
}

function renderTasks() {
  const running = state.tasks.filter(active);
  const live = running.filter((task) => task.mode === "live");
  const record = running.filter((task) => task.mode === "record");
  const caps = live.reduce((sum, task) => sum + task.cap_usd, 0);
  element("nav-count").textContent = running.length;
  element("task-total").textContent = state.tasks.length;
  element("dex-stats").innerHTML = stat("运行中的任务", running.length, `<span class="mini-dot"></span>${state.meta.max_running > 0 ? `运行上限 ${state.meta.max_running} 个实例` : "任务数量不设上限"}`, "↗", "个") + stat("实盘交易", live.length, "独立实例 · 真实订单", "⇄", "个") + stat("行情采集", record.length, "仅记录行情，不发送订单", "▥", "个") + stat("配置持仓上限合计", `$${number(caps, 0)}`, "运行中实盘的双腿上限合计，非实际持仓", "⊞");
  const query = element("search").value.toLowerCase();
  const filter = element("state-filter").value;
  const tasks = state.tasks.filter((task) => `${task.name} ${task.symbol}`.toLowerCase().includes(query) && (filter === "all" || (filter === "active" && active(task)) || (filter === "stopped" && task.state === "stopped") || (filter === "failed" && ["failed", "interrupted"].includes(task.state))));
  element("task-rows").innerHTML = tasks.map((task) => {
    const premium = !task.status_stale && task.state === "running" ? task.status?.premium_bps : null;
    const stats = task.status || {};
    const realized = stats.execution_stats?.net_realized_usd;
    const taskStats = `<div class="task-stats"><span>成交额 <b>${money(stats.session_volume_usd)}</b></span><span>已实现 <b class="${realized == null ? "" : realized >= 0 ? "positive" : "negative"}">${money(realized, 4)}</b></span><span>手续费 <b>${money(stats.session_fees_usd, 4)}</b></span></div>`;
    const disabled = state.busy.has(task.id) ? "disabled" : "";
    const paused = Boolean(task.manual_paused || task.status?.paused);
    const controls = active(task)
      ? task.state === "stopping" ? `<button class="text-button stop" data-action="stop" ${disabled}>强制结束</button>`
        : `${task.mode === "live" ? `<button class="text-button ${paused ? "resume" : "pause"}" data-action="${paused ? "resume" : "pause"}" ${disabled}>${paused ? "恢复" : "暂停"}</button>` : ""}<button class="text-button stop" data-action="stop" ${disabled}>停止</button>`
      : `<button class="text-button" data-action="start" ${disabled}>启动</button><button class="text-button" data-action="edit" ${disabled}>配置</button>`;
    return `<tr data-id="${task.id}"><td><div class="coin-cell"><span class="coin-icon">${escapeHtml(task.symbol.slice(0, 1))}</span><div><button class="text-button task-title" data-action="detail" aria-label="查看 ${escapeHtml(task.symbol)} 任务详情">${escapeHtml(task.symbol)}</button><small title="${escapeHtml(task.name)}">${escapeHtml(task.name)}</small></div></div></td><td class="route-cell"><b>${escapeHtml(venues[task.primary])}</b><span>⇄</span><b>${escapeHtml(venues[task.hedge])}</b></td><td><span class="badge ${task.mode}">${task.mode === "live" ? "LIVE · 实盘" : "DATA · 采集"}</span></td><td>${badge(task)}${thresholdStatus(task, true)}${(task.startup_warnings || []).map((warning) => `<small class="threshold-status pending" title="${escapeHtml(warning)}">签名提示：${escapeHtml(warning)}</small>`).join("")}</td><td class="number ${premium == null ? "" : premium >= 0 ? "positive" : "negative"}">${signed(premium)}</td><td class="number">${duration(task)}</td><td>${taskStats}</td><td><div class="actions">${controls}${(task.strategy_mode || task.status?.strategy_mode) === "spread" ? "" : `<button class="text-button" data-action="threshold">阈值</button>`}<button class="text-button" data-action="detail">详情</button><button class="text-button" data-action="monitor">监控 ↗</button>${!active(task) ? `<button class="icon-button" data-action="delete" aria-label="删除 ${escapeHtml(task.name)}" title="删除任务">×</button>` : ""}</div></td></tr>`;
  }).join("");
  element("task-empty").hidden = tasks.length > 0;
  if (!tasks.length) {
    element("task-empty").querySelector("h3").textContent = state.tasks.length ? "没有匹配的任务" : "从第一个币种开始";
    element("task-empty").querySelector("p").textContent = state.tasks.length ? "试试其他搜索词或状态筛选。" : "创建任务并配置交易路径。建议先采集行情，再用实际数据设定实盘阈值。";
    element("empty-new").hidden = state.tasks.length > 0;
  }
  const options = state.tasks.map((task) => `<option value="${task.id}">${escapeHtml(task.symbol)} · ${escapeHtml(task.name)}</option>`).join("");
  if (element("monitor-select").innerHTML !== options) element("monitor-select").innerHTML = options;
  if (!state.tasks.some((task) => task.id === state.selected)) selectMonitorTask(state.tasks[0]?.id || null);
  if (state.selected) element("monitor-select").value = state.selected;
  element("monitor-empty").hidden = state.tasks.length > 0;
  element("monitor-content").hidden = !state.tasks.length;
}

async function refresh() {
  if (!state.authenticated || state.polling) return;
  state.polling = true;
  let serverConnected = false;
  try {
    const result = await api("/api/tasks");
    serverConnected = true;
    state.tasks = result.tasks;
    if (result.liquidation) renderLiquidation(result.liquidation);
    element("connection-label").textContent = "服务器已连接";
    element("connection-label").title = "";
    element("connection-dot").parentElement.classList.remove("bad-connection");
    document.querySelector(".server-pill").classList.remove("bad-connection");
    document.querySelector(".server-pill").lastChild.textContent = " SERVER CONNECTED";
    renderTasks();
    if (state.page === "monitor") {await refreshDetail(); await refreshTriple();}
    if (state.page === "accounts") await refreshAccounts();
    if (element("task-detail-dialog").open) await refreshTaskDetails();
  } catch (error) {
    element("connection-label").textContent = serverConnected ? "服务器已连接 · 页面刷新失败" : "连接中断";
    element("connection-label").title = error.message;
    element("connection-dot").parentElement.classList.toggle("bad-connection", !serverConnected);
    document.querySelector(".server-pill").classList.toggle("bad-connection", !serverConnected);
    document.querySelector(".server-pill").lastChild.textContent = serverConnected ? " SERVER CONNECTED" : " SERVER DISCONNECTED";
    if (state.page === "monitor") {
      state.detail = null;
      state.logs = null;
      clearMonitor(`监控刷新失败：${error.message}`);
    }
  } finally {state.polling = false;}
}

function renderTriple() {
  const candidates = state.tri.candidates || [];
  const select = element("tri-symbol");
  const current = state.tri.selectedSymbol || select.value;
  select.innerHTML = candidates.length
    ? candidates.map((row) => `<option value="${escapeHtml(row.symbol)}">${escapeHtml(row.symbol)} · ${escapeHtml(row.label)}</option>`).join("")
    : '<option value="">暂无双市场共同币种</option>';
  if (candidates.some((row) => row.symbol === current)) select.value = current;
  const snapshot = state.tri.snapshot;
  state.tri.selectedSymbol = select.value || snapshot?.selected_symbol || null;
  const details = snapshot?.symbols?.[state.tri.selectedSymbol] || null;
  const running = Boolean(snapshot?.running);
  element("tri-monitor-status").textContent = running ? `运行中 · ${snapshot.count || 1} 个币种` : "未启动";
  element("tri-monitor-status").className = `badge ${running ? "running" : ""}`;
  element("tri-start").disabled = !select.value;
  element("tri-stop").disabled = !running;
  const marketLabels = {lighter_rh: "RH", entropy: "Entropy", arcus: "Arcus"};
  element("tri-candidates").innerHTML = candidates.length ? `<div class="overview-heading"><b>可监控币种</b><small>${candidates.length} 个 · 至少两个市场支持，不按交易规格筛选</small></div><div class="tri-candidate-grid">${candidates.map((row) => `<article class="tri-candidate compatible"><b>${escapeHtml(row.symbol)}</b><span>${escapeHtml(Object.keys(row.venues || {}).map((key) => marketLabels[key] || key).join(" / "))}</span><small>仅展示价差，不代表可直接交易</small></article>`).join("")}</div>` : '<div class="empty-state"><h3>没有双市场共同币种</h3><p>刷新后重试；至少两个市场都有行情才能计算价差。</p></div>';
  const overview = snapshot?.symbols || {};
  const pairFor = (row, names) => row.pairs?.find((pair) => new Set([pair.left, pair.right]).size === 2 && names.every((name) => [pair.left, pair.right].includes(name)));
  const overviewCell = (pair) => {
    if (!pair) return "<span class=\"overview-muted\">—</span>";
    const sell = pair.sell_left_buy_right_bps == null ? "—" : `卖${pair.left}/买${pair.right} ${signed(pair.sell_left_buy_right_bps)}`;
    const buy = pair.buy_left_sell_right_bps == null ? "—" : `买${pair.left}/卖${pair.right} ${signed(pair.buy_left_sell_right_bps)}`;
    return `<b>${signed(pair.mid_spread_bps)} bps</b><small>${escapeHtml(`${sell} · ${buy}`)}</small>`;
  };
  element("tri-overview").innerHTML = Object.keys(overview).length ? `<div class="overview-heading"><b>全部币种实时价差</b><small>${Object.keys(overview).length} 个币种 · 点击查看可用盘口，缺失市场显示 —</small></div><div class="table-scroll"><table class="tri-overview-table"><thead><tr><th>币种</th><th>RH ↔ Entropy</th><th>RH ↔ Arcus</th><th>Arcus ↔ Entropy</th><th>行情</th></tr></thead><tbody>${Object.entries(overview).map(([symbol, row]) => `<tr data-tri-symbol="${escapeHtml(symbol)}"><td><button class="text-button tri-symbol-link" type="button">${escapeHtml(symbol)}</button></td><td>${overviewCell(pairFor(row, ["RH", "ENTROPY"]))}</td><td>${overviewCell(pairFor(row, ["RH", "ARCUS"]))}</td><td>${overviewCell(pairFor(row, ["ARCUS", "ENTROPY"]))}</td><td><span class="badge ${row.venues?.every((venue) => venue.fresh) ? "" : "stale"}">${row.venues?.every((venue) => venue.fresh) ? "正常" : "等待 / 过期"}</span></td></tr>`).join("")}</tbody></table></div>` : "";
  element("tri-venues").innerHTML = details?.venues?.length ? details.venues.map((venue) => `<article class="venue-card"><div class="venue-top"><div><b>${escapeHtml(venue.name)}</b><small>${escapeHtml(venue.symbol)}</small></div><span class="badge ${venue.fresh ? "" : "stale"}">${venue.fresh ? `正常 · ${number(venue.age_sec, 1)}s` : "等待行情"}</span></div><div class="venue-values"><div><span>买一 / 卖一</span><strong class="number">${number(venue.bid, 5)} / ${number(venue.ask, 5)}</strong></div><div><span>中间价</span><strong>${number(venue.mid, 5)}</strong></div></div></article>`).join("") : "";
  element("tri-pairs").innerHTML = details?.pairs?.length ? details.pairs.map((pair) => `<article class="tri-pair"><div class="tri-pair-head"><b>${escapeHtml(pair.left)} ⇄ ${escapeHtml(pair.right)}</b><span class="badge ${pair.script_compatible ? "" : "stale"}">${pair.script_compatible ? "脚本可对冲" : "仅价差监控"}</span></div><div class="tri-pair-grid"><div><small>中间价差</small><strong>${signed(pair.mid_spread_bps)} bps</strong></div><div><small>卖左 / 买右</small><strong>${signed(pair.sell_left_buy_right_bps)} <em>可成交差价</em></strong></div><div><small>买左 / 卖右</small><strong>${signed(pair.buy_left_sell_right_bps)} <em>可成交差价</em></strong></div></div></article>`).join("") : "";
}

async function loadTripleCandidates() {
  element("tri-monitor-error").textContent = "正在读取三边市场列表…";
  try {
    const result = await api("/api/market-monitor/candidates");
    state.tri.candidates = result.symbols || [];
    element("tri-monitor-error").textContent = result.errors && Object.keys(result.errors).length ? `部分市场列表读取失败：${escapeHtml(Object.values(result.errors).join("；"))}` : "";
    renderTriple();
  } catch (error) {element("tri-monitor-error").textContent = error.message;}
}

async function refreshTriple() {
  if (!state.authenticated) return;
  try {state.tri.snapshot = await api("/api/market-monitor"); renderTriple();} catch (error) {element("tri-monitor-error").textContent = error.message;}
}

element("tri-refresh").addEventListener("click", () => loadTripleCandidates());
element("tri-symbol").addEventListener("change", () => {
  state.tri.selectedSymbol = element("tri-symbol").value;
  renderTriple();
});
element("tri-start").addEventListener("click", async () => {
  const symbol = element("tri-symbol").value;
  if (!symbol) return;
  element("tri-start").disabled = true;
  element("tri-monitor-error").textContent = "正在连接可用的公开盘口…";
  try {state.tri.snapshot = await api("/api/market-monitor/start", "POST", {symbol, all: true}); const errors = Object.values(state.tri.snapshot.errors || {}); element("tri-monitor-error").textContent = errors.length ? `部分币种未连接：${errors.join("；")}` : ""; renderTriple();}
  catch (error) {element("tri-monitor-error").textContent = error.message; renderTriple();}
});
element("tri-stop").addEventListener("click", async () => {
  try {state.tri.snapshot = await api("/api/market-monitor/stop", "POST", {}); renderTriple();}
  catch (error) {element("tri-monitor-error").textContent = error.message;}
});
element("tri-overview").addEventListener("click", (event) => {
  const row = event.target.closest("tr[data-tri-symbol]");
  if (!row) return;
  state.tri.selectedSymbol = row.dataset.triSymbol;
  element("tri-symbol").value = state.tri.selectedSymbol;
  renderTriple();
});

function clearMonitor(message = "正在加载所选任务…") {
  element("monitor-summary").textContent = message;
  element("monitor-stats").replaceChildren();
  element("venue-cards").textContent = message;
  element("signal-summary").textContent = message;
  element("signal-rows").innerHTML = '<tr><td colspan="6" class="table-empty">暂无当前任务信号</td></tr>';
  element("signal-note").textContent = "等待当前任务快照，暂不展示余额与信号。";
  element("premium-chart").textContent = message;
  element("log-output").textContent = message;
  element("trades-output").textContent = message;
  element("trades-pagination").hidden = true;
  element("trades-pagination").replaceChildren();
  element("download-minutes").removeAttribute("href");
  element("download-logs").removeAttribute("href");
}

function selectMonitorTask(taskId) {
  if (state.selected === taskId) return;
  state.selected = taskId;
  state.tradesPage = 1;
  state.detail = null;
  state.logs = null;
  clearMonitor();
}

function renderMonitor() {
  if (!state.detail || state.detail.task.id !== state.selected) {clearMonitor(); return;}
  const task = state.detail.task;
  const metrics = task.status;
  const fresh = active(task) && !task.status_stale;
  const metricNote = fresh ? "当前运行会话" : metrics ? "最后快照 · 当前非实时" : "等待首次状态快照";
  const orderMode = (metrics?.primary_order_type === "maker" ? metrics.maker_cancel_policy === "best_price"
    ? `主腿挂单 · 条件有效且在最优价持续保留 · 离开最优价 ${number(metrics.maker_reprice_delay_sec, 2)}s 后撤单`
    : `主腿挂单 · 最长等待 ${number(metrics.maker_timeout_sec, 1)}s` : "主腿吃单")
    + (metrics?.primary_maker_only ? " · Arcus 开仓、平仓、纠偏均只挂单，不转吃单" : "");
  const maker = metrics?.active_maker;
  const makerLabel = maker ? ` · 挂单${maker.state === "unresolved" ? "状态未确认" : maker.state === "canceling" ? "撤单确认中" : maker.cancel_policy === "best_price" ? maker.at_best_price === false ? "离开最优价，确认中" : "最优价排队中" : "跟踪中"} · 已成交 ${number(maker.filled, 6)} / 已对冲 ${number(maker.hedged, 6)}` : "";
  const bps = metrics?.bps_strategy;
  const historyRange = bps?.coverage?.history_start_ts != null ? ` · 最新参考历史 ${windowTime(bps.coverage.history_start_ts)} → ${windowTime(bps.coverage.history_end_ts)} · 最后记录距今 ${number(bps.coverage.latest_data_age_sec / 60, 1)} 分钟` : "";
  const coverageLabel = bps?.coverage ? ` · 有效数据 ${bps.coverage.valid_minutes}/${bps.window_minutes}（${number(bps.coverage.coverage_pct, 1)}%）` : "";
  const dustLabel = bps?.position_residual?.accepted_dust ? ` · 已确认精度尾差 ${signed(bps.position_residual.net_base, 8)}（名义敞口 ${money(bps.position_residual.notional_usd, 6)}），继续监控` : "";
  const tailLabel = bps?.dust_levels_current ? " · 仅剩精度尾仓，交易点位跟随当前历史；真实仓位和成本继续保留" : "";
  const strategyLabel = bps ? ` · bps ${bps.levels === "rolling" ? "滚动" : "固定"}：开仓 ${signed(bps.entry_bps)} / 平仓 ${signed(bps.exit_bps)} · 本轮已成交 ${bps.entries} 批 · 不限批次 · ${bps.reason}${dustLabel}${tailLabel}${coverageLabel}${historyRange}` : "";
  const telegram = metrics?.telegram;
  const telegramState = !metrics ? "" : !metrics.telegram_enabled ? "未配置" : telegram?.status === "failed" ? "发送失败" : `已启用 · 已发 ${telegram?.sent ?? 0}`;
  const telegramProfile = telegram?.profile ? ` · ${escapeHtml(telegram.profile)}` : "";
  const telegramLabel = task.mode === "live" && metrics ? ` · Telegram${telegramProfile} ${telegramState}` : "";
  element("monitor-summary").innerHTML = `<div><strong>${escapeHtml(task.symbol)}</strong><span>${escapeHtml(venues[task.primary])} ⇄ ${escapeHtml(venues[task.hedge])}</span><span class="badge ${task.mode}">${task.mode === "live" ? "LIVE · 实盘" : "DATA · 采集"}</span>${badge(task)}</div><small>${fresh ? "实时快照" : "最后快照"} ${timeLabel(metrics?.updated_at)} · ${escapeHtml(orderMode)}${escapeHtml(makerLabel)}${escapeHtml(strategyLabel)}${telegramLabel} ${(task.startup_warnings || []).map((warning) => `<span class="threshold-status pending">${escapeHtml(warning)}</span>`).join("")} ${task.exit_code == null ? "" : ` · 退出码 ${task.exit_code}`}</small>`;
  element("monitor-stats").innerHTML = stat("双腿中间价溢价", signed(metrics?.premium_bps), metricNote, "⇄", "bps") + stat("净敞口", metrics?.net_delta == null ? "—" : signed(metrics.net_delta, 6), "双腿基础币数量合计", "⊞") + stat("会话盈亏 · MTM", metrics?.session_pnl == null ? "—" : `$${signed(metrics.session_pnl)}`, "按盘口标记的会话估值变化，非已实现收益", "↗") + stat("套利执行 / 对冲", `${metrics?.trades ?? 0} / ${metrics?.hedges ?? 0}`, `当前会话记录 ${metrics?.minute_rows ?? 0} 条分钟数据`, "▥") + stat("总成交额 · 双腿合计", money(metrics?.session_volume_usd), "实际成交名义金额，不是账户权益", "⇄") + stat("净损益 · 已实现", money(metrics?.execution_stats?.net_realized_usd, 4), "已平仓价差减全部成交手续费，未含资金费", "↗") + stat("总手续费", money(metrics?.session_fees_usd, 4), "两条腿及后续对冲手续费", "▥");
  element("venue-cards").innerHTML = metrics?.venues?.length ? metrics.venues.map((venue) => {
    const balance = venue.balance_status;
    const readable = task.mode === "live" && (!balance || balance.updated_at != null);
    const account = venue.account;
    const accountLabel = account ? `Arcus ${account.network === "mainnet" ? "主网" : account.network === "testnet" ? "测试网" : account.network} · 子账户 #${account.account_index ?? "—"}` : "";
    let balanceNote = task.mode !== "live" ? "采集模式不读取账户权益" : !balance ? "旧版快照未提供权益读取状态" : balance.state === "error" ? `权益读取失败（${balance.error || "未知错误"}）${readable ? " · 金额为上次成功值，非实时" : " · 尚无有效余额"}` : balance.state === "waiting" ? (balance.waiting_reason === "shared_budget" ? "等待本地共享请求预算（查询未发送）" : balance.waiting_reason === "exchange_cooldown" ? "等待交易所限流冷却" : "等待首次权益读取") + (readable ? " · 金额为上次成功值，非实时" : "") : balance.state === "stale" ? "权益数据过期 · 金额为上次成功值" : "交易所账户接口已读取";
    if (balance?.updated_at != null) balanceNote += ` · 更新于 ${timeLabel(balance.updated_at)}`;
    if (venue.margin_block) balanceNote += " · 保证金取消后暂缓新开仓，等待可用保证金改善；正常减仓保留";
    if (!fresh && readable) balanceNote += " · 任务快照非实时";
    if (balance?.state === "ready" && readable && Number(venue.equity) === 0 && Number(venue.free) === 0) balanceNote += " · 接口返回零，请核对所选网络、账户和子账户";
    return `<article class="venue-card"><div class="venue-top"><div><b>${escapeHtml(venue.name)}</b><small>${escapeHtml(venue.symbol)}</small></div><span class="badge ${fresh && venue.fresh && !venue.down ? "" : "stale"}">${!fresh ? "非实时" : venue.down ? "连接异常" : venue.fresh ? `正常 · ${number(venue.age_sec, 1)}s` : "行情过期"}</span></div><div class="venue-values"><div><span>买一 / 卖一</span><strong class="number">${number(venue.bid, 5)} / ${number(venue.ask, 5)}</strong></div><div><span>持仓数量</span><strong>${signed(venue.position, 6)}</strong></div><div><span>持仓金额 · USD</span><strong>${money(venue.position_usd)}</strong></div><div><span>账户权益 / 可用</span><strong>${money(readable ? venue.equity : null)} / ${money(readable ? venue.free : null)}</strong>${accountLabel ? `<p class="field-note">${escapeHtml(accountLabel)}</p>` : ""}<p class="field-note">${escapeHtml(balanceNote)}</p></div><div><span>会话成交额 · USD</span><strong>${money(venue.volume_usd)}</strong></div></div></article>`;
  }).join("") : `<div class="empty-state"><h3>等待市场连接</h3><p>启动任务后将展示真实双腿盘口。启动失败请查看下方日志。</p></div>`;
  renderSignals(metrics, fresh);
  drawChart(state.detail.minutes);
  renderLogs();
  const base = `/api/tasks/${task.id}/download/`;
  element("download-minutes").href = base + "minutes";
  element("download-logs").href = base + state.logTab;
}

function renderSignals(metrics, fresh) {
  const signal = metrics?.execution_signals;
  if (!signal?.rows?.length) {
    element("signal-summary").textContent = "等待市场连接或新版状态快照";
    element("signal-rows").innerHTML = `<tr><td colspan="6" class="table-empty">暂无信号数据</td></tr>`;
    element("signal-note").textContent = "展示信号，不改变交易规则。";
    return;
  }
  const bps = signal.mode === "bps", spread = signal.mode === "spread";
  const levels = metrics.bps_strategy;
  const thresholds = metrics.thresholds;
  const executableBps = bps && levels?.price_basis === "executable";
  const openingMode = signal.rows.some((row) => row.action === "close" && row.order_type === "maker")
    ? "开仓、平仓主腿均挂单 / 对冲腿吃单；Arcus 纠偏也只挂单"
    : signal.rows.some((row) => row.action === "open" && row.order_type === "maker") ? "开仓主腿挂单 / 对冲吃单；平仓双腿吃单" : "双腿吃单";
  const summary = executableBps ? `可成交价差 bps · 开仓 ≥ ${signed(levels.entry_bps)} / 平仓 ≥ ${signed(levels.exit_bps)} · ${openingMode}${levels.entries ? " · 本轮点位已锁定" : ""}` : spread ? `实际可成交价差 > ${number(metrics.spread_bps, 3)} bps · 双向、不限批次 · 不依赖历史数据` : bps ? `溢价 bps 策略 · 中间价溢价 ${signed(metrics.premium_bps)} bps · 开仓 ${signed(levels?.entry_bps)} / 平仓 ${signed(levels?.exit_bps)} bps${levels?.entries ? " · 本轮点位已锁定" : ""}` : `套利阈值策略 · 中间价溢价 ${signed(metrics.premium_bps)} bps · 中线 ${signed(thresholds?.midline_bps)} · 区间 [${signed(thresholds ? thresholds.midline_bps - thresholds.lower_bps : null)} … ${signed(thresholds ? thresholds.midline_bps + thresholds.upper_bps : null)}]`;
  element("signal-summary").textContent = `${fresh ? "当前快照" : "最后快照 · 非实时"} · ${summary}`;
  element("signal-rows").innerHTML = signal.rows.map((row) => {
    const prefix = row.action === "open" ? "开仓 · " : row.action === "close" ? "平仓 · " : "";
    const direction = prefix + (row.primary_buy ? `买 ${row.buy} → 卖 ${row.sell}` : `卖 ${row.sell} → 买 ${row.buy}`);
    const stale = !fresh || !metrics.venues?.length || metrics.venues.some((venue) => !venue.fresh || venue.down);
    const reached = row.gap_bps != null && (row.comparison === ">" ? row.gap_bps > 0 : row.gap_bps >= 0);
    const status = stale ? "行情非实时" : row.action === "close" && !row.has_position ? "未持仓 · 参考点位" : row.action === "close" && row.closing ? "平仓中" : row.gap_bps == null ? "等待点位" : reached ? "已达到点位" : "未达到点位";
    const tone = !stale && row.gap_bps != null ? reached ? "positive" : "negative" : "";
    return `<tr><td>${escapeHtml(direction)}<small>${row.order_type === "maker" ? "主腿挂单 / 对冲腿吃单" : "双腿吃单"}</small></td><td class="number">${signed(row.executable_premium_bps)}</td><td class="number">${signed(row.trigger_premium_bps)}</td><td class="number">${escapeHtml(row.comparison === "<=" ? "≤" : row.comparison === ">" ? ">" : "≥")} ${signed(row.hurdle_bps)}</td><td class="number ${tone}">${signed(row.gap_bps)}</td><td>${status}</td></tr>`;
  }).join("");
  element("signal-note").textContent = bps ? "触发指标与门槛均为主腿相对对冲腿的中间价溢价；≤ 条件的 gap = 门槛 − 指标，≥ 条件的 gap = 指标 − 门槛。可成交溢价另按卖腿价格 / 买腿价格计算，不参与上述差值。" : "触发指标为可成交溢价；门槛包含当前方向阈值、配置手续费和仓位附加值。gap = 可成交溢价 − 门槛。";
  if (executableBps) {
    element("signal-note").textContent = "开仓和平仓触发指标分别按对应方向的（卖方买一 / 买方卖一 − 1）× 10,000 计算，两个方向均为 ≥ 门槛。历史分位数使用各自方向的分钟收盘买卖报价重算。开仓按配置选择主腿挂单或吃单：挂单时可成交溢价列使用主腿挂单价，另核验其与对冲盘口的价差及深度，确认新增成交后才对冲；信号失效立即请求撤单。平仓与余仓修正使用吃单。gap ≥ 0 不保证成交或盈利，手续费另计。";
    if (metrics.primary_maker_only) element("signal-note").textContent = "触发指标仍按对应方向的卖方买一 / 买方卖一计算，开平仓门槛不变。Arcus 主腿开仓和平仓均使用 Post-only，另一腿只对已确认新增成交量吃单对冲；平仓两腿均只减仓。Arcus 余仓修正也只挂单，不自动转为 IOC。价差失效、离开最优价、暂停或停止会按规则撤单并核对最终成交。挂单可能不成交，平仓和纠偏不能保证立即完成。";
    return;
  }
  if (spread) {
    element("signal-note").textContent = "触发条件：买入方卖盘与卖出方买盘的可成交价差严格大于门槛，gap 必须大于 0；逐档深度和仓位上限限制数量。手续费另计，触发不保证盈利。中位数、分钟历史、开仓批次和整轮止盈目标不参与判断。";
    return;
  }
  element("signal-note").textContent += " gap ≥ 0 仅表示达到点位，不代表已下单或净利润；下单仍受盘口深度、信号持续时间、仓位上限及暂停状态等条件限制。可成交溢价按最优报价计算，不代表整笔实际成交价。";
}

function drawChart(rows) {
  const points = rows.map((row) => ({timestamp: Number(row.minute_ts), value: Number(row.premium_close_bps)})).filter((point) => Number.isFinite(point.value) && Number.isFinite(point.timestamp)).sort((first, second) => first.timestamp - second.timestamp);
  if (!points.length) {
    element("premium-chart").innerHTML = `<div class="empty-state"><h3>等待第一条分钟记录</h3><p>任务启动并收到双腿有效盘口后，每分钟汇总一条记录。这里将展示实际溢价走势。</p><span class="chart-caption">图表不会填充模拟行情</span></div>`;
    return;
  }
  const values = points.map((point) => point.value);
  const minimum = Math.min(...values);
  const maximum = Math.max(...values);
  const padding = Math.max((maximum - minimum) * 0.18, 0.5);
  const lower = minimum - padding;
  const upper = maximum + padding;
  const left = 52, right = 680, top = 16, bottom = 211;
  const start = points[0].timestamp;
  const end = points[points.length - 1].timestamp;
  const xValue = (point) => end <= start ? (left + right) / 2 : left + (point.timestamp - start) / (end - start) * (right - left);
  const yValue = (value) => bottom - (value - lower) / (upper - lower) * (bottom - top);
  const path = points.map((point, index) => `${index ? "L" : "M"}${xValue(point).toFixed(2)},${yValue(point.value).toFixed(2)}`).join(" ");
  const first = points[0], last = points[points.length - 1];
  const fill = `${path} L${xValue(last).toFixed(2)},${bottom} L${xValue(first).toFixed(2)},${bottom} Z`;
  let grid = "";
  for (let index = 0; index < 5; index++) {
    const value = lower + (upper - lower) * index / 4;
    const position = yValue(value).toFixed(2);
    grid += `<line x1="${left}" y1="${position}" x2="${right}" y2="${position}" stroke="#eaf0eb" stroke-dasharray="3 5"/><text x="40" y="${Number(position) + 3}" text-anchor="end" fill="#92a99f" font-size="9">${number(value, 1)}</text>`;
  }
  const label = (point) => new Date(point.timestamp * 1000).toLocaleTimeString("zh-CN", {hour: "2-digit", minute: "2-digit", hour12: false});
  element("premium-chart").innerHTML = `<svg viewBox="0 0 700 244" role="img" aria-label="最近分钟溢价走势"><defs><linearGradient id="chart-fill" x1="0" x2="0" y1="0" y2="1"><stop offset="0%" stop-color="#6eb89c" stop-opacity=".24"/><stop offset="100%" stop-color="#6eb89c" stop-opacity="0"/></linearGradient></defs>${grid}<path d="${fill}" fill="url(#chart-fill)"/><path d="${path}" fill="none" stroke="#348d72" stroke-width="2" stroke-linejoin="round"/><circle cx="${xValue(last)}" cy="${yValue(last.value)}" r="3.5" fill="#007d68"/><text x="${left}" y="236" fill="#92a99f" font-size="9">${label(first)}</text><text x="${right}" y="236" fill="#92a99f" font-size="9" text-anchor="end">${label(last)}</text></svg>`;
}

function tradeNumber(value) {
  if (value == null || String(value).trim() === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function tradeMatchedEdge(row) {
  const buyFill = tradeNumber(row.buy_fill), sellFill = tradeNumber(row.sell_fill);
  const buyPrice = tradeNumber(row.buy_avg_px), sellPrice = tradeNumber(row.sell_avg_px);
  if (!(buyFill > 0 && sellFill > 0 && buyPrice > 0 && sellPrice > 0)) return null;
  return tradeNumber(row.fill_edge_usd);
}

function renderLogs() {
  const trades = state.logTab === "trades";
  element("log-output").hidden = trades;
  element("trades-output").hidden = !trades;
  document.querySelectorAll("[data-log-tab]").forEach((button) => {
    const selected = button.dataset.logTab === state.logTab;
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-selected", String(selected));
  });
  const pagination = element("trades-pagination");
  if (trades) {
    const rows = state.detail?.trades || [];
    const total = Number(state.detail?.trades_total || 0);
    const page = Number(state.detail?.trades_page || state.tradesPage || 1);
    const pages = Number(state.detail?.trades_pages || 0);
    state.tradesPage = page;
    element("trades-output").innerHTML = rows.length ? `<p class="field-note">本次匹配成交净价差不是平仓盈亏；手续费按 YAML 配置费率估算。单腿成交、未成交或均价缺失时显示 —，不代表零亏损。已实现盈亏见任务会话摘要或监控页（本次运行、按交易所分别 FIFO 配对，未含资金费）。</p><table class="records"><thead><tr><th>时间</th><th>方向</th><th>数量</th><th>买 / 卖成交</th><th>估算手续费 USD</th><th>匹配成交净价差 USD（非平仓盈亏）</th><th>结果</th></tr></thead><tbody>${rows.map((row) => {
      const edge = tradeMatchedEdge(row);
      const edgeClass = edge == null ? "" : edge >= 0 ? "positive" : "negative";
      const edgeNote = edge == null ? "未形成可计算的双腿匹配成交；不能据此判断实际盈亏" : "只计算本次双腿匹配数量的卖出金额减买入金额及配置手续费，不是平仓损益";
      return `<tr><td>${timeLabel(Number(row.ts))}</td><td>${escapeHtml(row.direction)}</td><td>${escapeHtml(row.qty)}</td><td>${escapeHtml(row.buy_fill)} / ${escapeHtml(row.sell_fill)}</td><td class="number">${money(tradeNumber(row.fees_usd), 4)}</td><td class="number ${edgeClass}" title="${edgeNote}">${money(edge, 4)}</td><td><span class="badge ${row.ok === "1" ? "" : "failed"}">${row.ok === "1" ? "执行已确认" : "需核对"}</span></td></tr>`;
    }).join("")}</tbody></table>` : `<div class="empty-state"><h3>暂无成交记录</h3><p>采集模式不发送订单。实盘执行后，记录会显示在这里。</p></div>`;
    pagination.hidden = !total || pages <= 1;
    pagination.innerHTML = `<span>第 ${page} / ${pages} 页，共 ${total} 条</span><div><button class="button secondary small" data-trades-page="${page - 1}" ${page <= 1 ? "disabled" : ""}>上一页</button><button class="button secondary small" data-trades-page="${page + 1}" ${page >= pages ? "disabled" : ""}>下一页</button></div>`;
  } else {
    pagination.hidden = true;
    pagination.replaceChildren();
    const text = state.logTab === "events" ? state.logs?.events : state.logs?.text;
    const output = element("log-output");
    if (output.textContent !== (text || "等待运行事件。任务启动后的日志将在这里显示。")) {
      const previous = output.scrollTop;
      output.textContent = text || "等待运行事件。任务启动后的日志将在这里显示。";
      output.scrollTop = element("log-follow").checked ? output.scrollHeight : previous;
    }
  }
}

let monitorRequest = 0;
async function refreshDetail() {
  const request = ++monitorRequest;
  const selected = state.selected;
  if (!selected || !state.authenticated) return;
  const tradeQuery = `?trades_page=${state.tradesPage}&trades_page_size=${state.tradesPageSize}`;
  try {
    const [detail, logs] = await Promise.all([api(`/api/tasks/${selected}${tradeQuery}`), api(`/api/tasks/${selected}/logs`)]);
    if (state.selected !== selected || request !== monitorRequest) return;
    if (detail.task?.id !== selected) throw new Error("返回的任务与所选任务不一致");
    state.detail = detail;
    state.logs = logs;
    renderMonitor();
  } catch (error) {
    if (state.selected !== selected || request !== monitorRequest) return;
    state.detail = null;
    state.logs = null;
    clearMonitor(`监控刷新失败：${error.message}`);
    throw error;
  }
}

const fieldMap = {"bps-price-basis": ["bps", "price_basis", "executable"], "spread-bps": ["strategy", "spread_bps", 1], "strategy-mode": ["strategy", "mode", "threshold"], "bps-levels": ["bps", "levels", "rolling"], "bps-history-mode": ["bps", "history_mode", "records"], "bps-direction": ["bps", "direction", "long_primary"], "bps-window": ["bps", "window_minutes", 120], "bps-min-valid": ["bps", "min_valid_minutes", 30], "bps-entry-percentile": ["bps", "entry_percentile", 10], "bps-exit-percentile": ["bps", "exit_percentile", 75], "bps-entry": ["bps", "entry_bps", -9], "bps-exit": ["bps", "exit_bps", -6], "bps-add-interval": ["bps", "add_interval_sec", 60], "bps-position-cap": ["bps", "max_position_usd", 1000], "primary-order-type": ["execution", "primary_order_type", "taker"], "maker-cancel-policy": ["execution", "maker_cancel_policy", "best_price"], "maker-reprice-delay": ["execution", "maker_reprice_delay_sec", 0.3], "maker-requote-delay": ["execution", "maker_requote_delay_sec", 0.5], "maker-timeout": ["execution", "maker_timeout_sec", 5], "primary-maker-fee": ["primary", "maker_fee_bps", 0], "midline": ["thresholds", "midline_bps", 0], "upper": ["thresholds", "upper_bps", 4], "lower": ["thresholds", "lower_bps", 4], "order-cap": ["sizing", "max_order_notional_usd", 500], "primary-cap": ["primary", "max_position_usd", 1000], "hedge-cap": ["hedge", "max_position_usd", 1000], "primary-fee": ["primary", "taker_fee_bps", 0], "hedge-fee": ["hedge", "taker_fee_bps", 2.25], "primary-symbol": ["primary", "symbol", ""], "hedge-symbol": ["hedge", "symbol", ""]};

function sectionLines(text, section) {
  const lines = text.split("\n");
  const start = lines.findIndex((line) => new RegExp(`^${section}:\\s*(?:#.*)?$`).test(line));
  let end = lines.length;
  if (start >= 0) {
    for (let index = start + 1; index < lines.length; index++) {
      if (/^[A-Za-z_][\w-]*\s*:/.test(lines[index])) {end = index; break;}
    }
  }
  return {lines, start, end};
}

function yamlValue(text, section, key, fallback) {
  const {lines, start, end} = sectionLines(text, section);
  if (start < 0) return fallback;
  const line = lines.slice(start + 1, end).find((value) => new RegExp(`^  ${key}:`).test(value));
  if (!line) return fallback;
  const raw = line.replace(new RegExp(`^  ${key}:\\s*`), "").replace(/\s+#.*$/, "").trim();
  try {return JSON.parse(raw);} catch {return raw.replace(/^['"]|['"]$/g, "");}
}

function setYaml(section, key, value, remove = false) {
  const textarea = element("config-yaml");
  const {lines, start, end} = sectionLines(textarea.value, section);
  const entry = `  ${key}: ${JSON.stringify(value)}`;
  if (start < 0) {
    if (!remove) lines.push(`${section}:`, entry);
  } else {
    const offset = lines.slice(start + 1, end).findIndex((line) => new RegExp(`^  ${key}:`).test(line));
    if (offset >= 0) {
      if (remove) lines.splice(start + 1 + offset, 1);
      else lines[start + 1 + offset] = entry;
    } else if (!remove) lines.splice(end, 0, entry);
  }
  textarea.value = lines.join("\n");
}

function setBpsBasis(basis) {
  const previous = yamlValue(element("config-yaml").value, "bps", "price_basis", "mid");
  if (previous !== basis) {
    const long = element("bps-direction").value === "long_primary";
    const id = long ? "bps-entry-percentile" : "bps-exit-percentile";
    const converted = 100 - Number(element(id).value);
    element(id).value = converted;
    setYaml("bps", long ? "entry_percentile" : "exit_percentile", converted);
    if (element("bps-levels").value === "manual") {
      for (const [field, value] of [["bps-entry", basis === "executable" ? 1 : long ? -9 : 9], ["bps-exit", basis === "executable" ? 1 : long ? -6 : 6]]) {
        element(field).value = value;
        setYaml("bps", field === "bps-entry" ? "entry_bps" : "exit_bps", value);
      }
    }
  }
  element("bps-price-basis").value = basis;
  setYaml("bps", "price_basis", basis);
}

function refreshStrategyFields() {
  const mode = element("strategy-mode").value;
  const bps = mode === "bps", spread = mode === "spread";
  const rolling = element("bps-levels").value === "rolling";
  const records = element("bps-history-mode").value === "records";
  element("bps-window-label").textContent = records ? "参考记录数" : "滚动窗口";
  element("bps-window-unit").textContent = records ? "条有效分钟记录" : "分钟";
  element("bps-settings").hidden = !bps;
  element("spread-settings").hidden = !spread;
  element("spread-bps").disabled = !spread;
  for (const control of element("bps-settings").querySelectorAll("input, select")) control.disabled = !bps;
  element("bps-price-basis").disabled = !bps;
  for (const id of ["midline", "upper", "lower"]) {
    element(id).closest("label").hidden = bps || spread;
    element(id).disabled = bps || spread;
  }
  for (const id of ["bps-history-mode", "bps-window", "bps-min-valid", "bps-entry-percentile", "bps-exit-percentile"]) {
    element(id).closest("label").hidden = !rolling;
    element(id).disabled = !bps || !rolling;
  }
  for (const id of ["bps-entry", "bps-exit"]) {
    element(id).closest("label").hidden = rolling;
    element(id).disabled = !bps || rolling;
  }
  const executableBps = bps && element("bps-price-basis").value === "executable";
  if (spread) {
    element("primary-order-type").value = "taker";
    setYaml("execution", "primary_order_type", "taker");
  }
  element("primary-order-type").disabled = spread;
  const maker = !spread && element("primary-order-type").value === "maker";
  const keepBest = element("maker-cancel-policy").value === "best_price";
  for (const [id, visible] of [["maker-cancel-policy", maker], ["maker-requote-delay", maker], ["maker-reprice-delay", maker && keepBest], ["maker-timeout", maker && !keepBest], ["primary-maker-fee", maker]]) {
    element(id).closest("label").hidden = !visible;
    element(id).disabled = !visible;
  }
  element("execution-note").textContent = maker
    ? element("task-primary").value === "arcus"
      ? "Arcus 主腿开仓、平仓、余仓修正均只使用 Post-only 挂单，绝不自动转吃单。另一腿仅按确认新增成交量吃单对冲，平仓两腿只减仓。挂单费用使用主腿 maker 费率；可能长时间不成交，不保证立即退出。暂停或停止会撤单并核对最终成交，不自动平仓。"
      : "主腿按配置 Post-only 挂单；BPS / 阈值条件失效立即请求撤单，条件有效且在买一/卖一时按保留规则排队。仅对确认新增成交量吃单对冲。BPS 平仓与余仓修正仍为吃单，手续费分别按配置计。"
    : (spread || executableBps)
    ? "1 bps = 0.01%。手续费按实际账户配置并另计，不参与价格差门槛判断；单笔下单量、每腿持仓上限与滑点保护继续生效。"
    : "1 bps = 0.01%。费率以你的实际账户为准；原套利阈值策略的两个方向门槛另加双腿手续费。最优价保留模式先检查 BPS / 阈值条件：失效立即撤单；有效且在买一/卖一持续排队，离开最优价确认后撤单。部分成交按实际新增数量吃单对冲；确认旧单撤销后才重评新单。";
}

function syncFields() {
  const text = element("config-yaml").value;
  if (yamlValue(text, "bps", "price_basis", null) == null) {
    const convert = yamlValue(text, "strategy", "mode", "threshold") === "bps" && yamlValue(text, "bps", "levels", "rolling") === "rolling" && yamlValue(text, "bps", "exit_mode", "levels") === "levels";
    setYaml("bps", "price_basis", convert ? "executable" : "mid");
    if (convert) {
      const long = yamlValue(text, "bps", "direction", "long_primary") === "long_primary";
      const key = long ? "entry_percentile" : "exit_percentile";
      setYaml("bps", key, 100 - Number(yamlValue(text, "bps", key, long ? 10 : 75)));
    }
  }
  // Preserve an old profit task's implicit mid basis above before discarding its guards.
  setYaml("bps", "exit_mode", "levels");
  for (const key of ["take_profit_bps", "stop_loss_bps", "max_hold_sec", "min_gap_bps", "entry_confirm_sec", "cycle_cooldown_sec", "add_step_bps", "execution_slippage_bps", "cost_buffer_bps"]) setYaml("bps", key, null, true);
  for (const [id, [section, key, fallback]] of Object.entries(fieldMap)) {
    const defaultValue = id === "primary-maker-fee" ? yamlValue(element("config-yaml").value, "primary", "taker_fee_bps", 0) : fallback;
    element(id).value = yamlValue(element("config-yaml").value, section, key, defaultValue);
  }
  refreshStrategyFields();
  const primary = yamlValue(element("config-yaml").value, "primary", "venue", element("task-primary").value);
  if (state.meta.primary_venues.includes(primary)) element("task-primary").value = primary;
}

function venueChanged() {
  const primary = element("task-primary").value;
  const hedge = element("task-hedge").value;
  setYaml("primary", "venue", primary);
  setYaml("primary", "dex", primary === "entropy" ? "io" : "xyz", !["entropy", "tradexyz"].includes(primary));
  setYaml("hedge", "dex", hedge === "entropy" ? "io" : "xyz", !["entropy", "tradexyz"].includes(hedge));
  element("task-error").textContent = primary === hedge ? "主腿和对冲腿不能是同一个交易所。" : "";
}

async function openEditor(task = null) {
  try {
    const [meta, detail] = await Promise.all([api("/api/meta"), task ? api(`/api/tasks/${task.id}`) : Promise.resolve(null)]);
    state.meta = meta;
    if (detail) task = detail.task;
  } catch (error) {toast(error.message, true); return;}
  state.editing = task?.id || null;
  element("task-form").reset();
  element("task-error").textContent = "";
  element("advanced-config").open = false;
  element("editor-title").textContent = task ? `配置 ${task.symbol} 任务` : "新建套利任务";
  for (const kind of ["primary", "hedge"]) element(`task-${kind}`).innerHTML = state.meta[`${kind}_venues`].map((value) => `<option value="${value}">${escapeHtml(venues[value])}</option>`).join("");
  element("task-profile").innerHTML = state.meta.profiles.map((profile) => `<option value="${escapeHtml(profile)}">${escapeHtml(profile === "default" ? "default · 默认 .env" : profile)}</option>`).join("");
  element("task-strategy").innerHTML = '<option value="">独立配置 · 此任务专用</option>' + state.meta.strategies.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(strategyLabel(name))}</option>`).join("");
  element("task-strategy").parentElement.querySelector("small").textContent = "选择文件会加载模板；修改后点击保存任务，会直接更新这个 YAML 文件。";
  element("task-strategy").value = task ? task.strategy_file || "" : state.meta.strategies[0] || "";
  element("task-primary").value = task?.primary || "lighter-rh";
  element("task-hedge").value = task?.hedge || "arcus";
  element("task-profile").value = task?.profile || "default";
  element("task-name").value = task?.name || "";
  element("task-symbol").value = task?.symbol || "";
  element("task-form").querySelector(`input[name="mode"][value="${task?.mode || "record"}"]`).checked = true;
  element("config-yaml").value = task?.config || state.meta.templates["rh-arcus"];
  syncFields();
  try {await applyStrategy();} catch (error) {element("task-error").textContent = error.message;}
  element("task-dialog").showModal();
}

function strategyLabel(name) {
  return name === "@default" ? "config.yaml · 项目默认" : name;
}

async function applyStrategy() {
  const name = element("task-strategy").value;
  element("save-task").disabled = true;
  try {
    if (name) {
      const saved = await api(`/api/configs/yaml/${encodeURIComponent(name)}`);
      if (element("task-strategy").value !== name) return;
      element("config-yaml").value = saved.preview;
      syncFields();
    }
    for (const id of [...Object.keys(fieldMap), "task-primary", "task-hedge", "sync-fields"]) element(id).disabled = false;
    element("config-yaml").readOnly = false;
    refreshStrategyFields();
  } finally {element("save-task").disabled = false;}
}

element("task-strategy").addEventListener("change", () => applyStrategy().catch((error) => {element("task-error").textContent = error.message;}));

for (const [id, [section, key]] of Object.entries(fieldMap)) {
  element(id).addEventListener("input", () => {
    const value = element(id).value;
    if (id === "bps-price-basis") setBpsBasis(value);
    if (id === "strategy-mode" && value === "bps") setBpsBasis("executable");
    if (["primary-order-type", "maker-cancel-policy", "strategy-mode", "bps-price-basis", "bps-levels", "bps-history-mode", "bps-direction"].includes(id)) {
      setYaml(section, key, value);
      if (id === "bps-direction" && element("bps-price-basis").value === "mid") {
        for (const [entryId, exitId] of [["bps-entry", "bps-exit"], ["bps-entry-percentile", "bps-exit-percentile"]]) {
          const entry = Number(element(entryId).value), exit = Number(element(exitId).value);
          if ((value === "long_primary" && entry > exit) || (value === "short_primary" && entry < exit)) {
            for (const [field, amount] of [[entryId, exit], [exitId, entry]]) {
              element(field).value = amount;
              setYaml(fieldMap[field][0], fieldMap[field][1], amount);
            }
          }
        }
      }
      refreshStrategyFields();
    }
    else if (id.endsWith("symbol")) setYaml(section, key, value.trim(), !value.trim());
    else if (value !== "" && Number.isFinite(Number(value))) setYaml(section, key, Number(value));
  });
}
element("task-primary").addEventListener("change", venueChanged);
element("task-hedge").addEventListener("change", venueChanged);
element("sync-fields").addEventListener("click", syncFields);
element("config-yaml").addEventListener("change", syncFields);
element("task-symbol").addEventListener("input", () => {
  if (!state.editing && !element("task-name").dataset.manual) element("task-name").value = `${element("task-symbol").value.toUpperCase()} · ${venues[element("task-primary").value]} / ${venues[element("task-hedge").value]}`;
});
element("task-name").addEventListener("input", () => {element("task-name").dataset.manual = "1";});
element("task-dialog").addEventListener("close", () => {delete element("task-name").dataset.manual;});

element("task-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  element("save-task").disabled = true;
  element("task-error").textContent = "";
  try {
    const strategyFile = element("task-strategy").value || null;
    const payload = {name: element("task-name").value, symbol: element("task-symbol").value, primary: element("task-primary").value, hedge: element("task-hedge").value, profile: element("task-profile").value, mode: element("task-form").querySelector('input[name="mode"]:checked').value, config: element("config-yaml").value, strategy_file: strategyFile, save_strategy: Boolean(strategyFile)};
    const saved = await api(state.editing ? `/api/tasks/${state.editing}` : "/api/tasks", state.editing ? "PUT" : "POST", payload);
    element("task-dialog").close();
    toast(saved.route_changed
      ? `交易路径已保存，旧记录已归档，任务尚未启动${saved.strategy_detached ? "；已使用独立配置，共用 YAML 保留" : ""}。原交易所仓位未平仓。`
      : "配置已保存，任务尚未启动");
    await refresh();
  } catch (error) {element("task-error").textContent = error.message;} finally {element("save-task").disabled = false;}
});

let confirmAction = null;
let confirmSymbol = null;
function confirm(title, message, callback, symbol = null, danger = false) {
  confirmAction = callback;
  confirmSymbol = symbol;
  element("confirm-title").textContent = title;
  element("confirm-message").textContent = message;
  element("confirm-symbol-label").hidden = !symbol;
  element("confirm-symbol").value = "";
  element("confirm-symbol").placeholder = symbol || "";
  element("confirm-error").textContent = "";
  element("confirm-submit").disabled = false;
  element("confirm-submit").classList.toggle("danger", danger);
  element("confirm-dialog").showModal();
}

element("confirm-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (confirmSymbol && element("confirm-symbol").value.trim().toUpperCase() !== confirmSymbol) {
    element("confirm-error").textContent = `请输入 ${confirmSymbol} 确认操作`;
    return;
  }
  element("confirm-submit").disabled = true;
  try {await confirmAction(); element("confirm-dialog").close();} catch (error) {element("confirm-error").textContent = error.message;} finally {element("confirm-submit").disabled = false;}
});

let thresholdTask = null;
async function openThreshold(task) {
  thresholdTask = task;
  element("threshold-title").textContent = `${task.symbol} 阈值计算`;
  element("threshold-error").textContent = "正在读取最近完成日的分钟数据…";
  element("threshold-result").hidden = true;
  element("threshold-apply").disabled = true;
  element("threshold-dialog").showModal();
  try {
    const result = await api(`/api/tasks/${task.id}/threshold`);
    element("threshold-window").textContent = `${windowTime(result.window_start)} → ${windowTime(result.window_end)}（上海时间，左闭右开） · ${result.rows} 条有效分钟数据 · 首尾跨度 ${number(result.span_hours, 1)} 小时 · ${result.primary_order_type === "maker" ? "主腿挂单 / 对冲吃单" : "双腿吃单"} · 配置手续费合计 ${number(result.fees_bps, 2)} bps · 数据：${result.data_path || "—"} · 配置：${result.config_path || "—"}`;
    element("threshold-current").textContent = `当前：中线 ${number(result.current.midline_bps, 1)} / 上带 ${number(result.current.upper_bps, 1)} / 下带 ${number(result.current.lower_bps, 1)} bps`;
    element("threshold-midline").value = result.suggested.midline_bps;
    element("threshold-upper").value = result.suggested.upper_bps;
    element("threshold-lower").value = result.suggested.lower_bps;
    element("threshold-suggested").textContent = `建议：中线 ${signed(result.suggested.midline_bps, 1)} / 卖主腿带宽 ${number(result.suggested.upper_bps, 1)} / 买主腿带宽 ${number(result.suggested.lower_bps, 1)} bps。目标为两边各约 10% 的分钟达标，四舍五入和最小带宽会影响比例。`;
    const report = result.analysis;
    const stats = [["mean_bps", "平均值"], ["std_bps", "标准差"], ["median_bps", "中位数"], ["p5_bps", "5% 分位数"], ["p25_bps", "25% 分位数"], ["p75_bps", "75% 分位数"], ["p95_bps", "95% 分位数"]];
    element("threshold-distribution").innerHTML = stats.map(([key, label]) => `<div class="threshold-stat"><span>${label}</span><strong>${key === "std_bps" ? number(report.distribution[key], 2) : signed(report.distribution[key], 2)}</strong></div>`).join("");
    element("threshold-band-basis").textContent = `参考中线 ${signed(report.midline_bps, 1)} bps；已扣除配置手续费 ${number(report.fees_bps, 2)} bps。`;
    element("threshold-bands").innerHTML = report.bands.map((band) => `<tr><td>${number(band.band_bps, 1)}</td><td>${number(band.sell_minutes, 0)}</td><td>${number(band.sell_per_day, 1)}</td><td>${number(band.buy_minutes, 0)}</td><td>${number(band.buy_per_day, 1)}</td></tr>`).join("");
    element("threshold-result").hidden = false;
    element("threshold-apply").disabled = false;
    element("threshold-error").textContent = "";
  } catch (error) {element("threshold-error").textContent = error.message;}
}

element("threshold-cancel").addEventListener("click", () => element("threshold-dialog").close());
element("threshold-apply").addEventListener("click", () => {
  if (!thresholdTask) return;
  if (!element("threshold-dialog").querySelector("form").reportValidity()) return;
  const task = thresholdTask;
  element("threshold-dialog").close();
  const apply = () => operateThreshold(task);
  if (task.mode === "live") confirm(`应用 ${task.symbol} 新阈值？`, "应用会优雅停止并重新启动该实盘任务；当前持仓不会自动平仓，请确认交易所持仓后继续。", apply, task.symbol, true);
  else apply();
});

async function operateThreshold(task) {
  state.busy.add(task.id);
  renderTasks();
  try {
    const thresholds = {midline_bps: Number(element("threshold-midline").value), upper_bps: Number(element("threshold-upper").value), lower_bps: Number(element("threshold-lower").value)};
    await api(`/api/tasks/${task.id}/threshold`, "POST", {apply: true, confirm_live: task.mode === "live", thresholds});
    toast("阈值已应用；任务会在下一次启动时读取新配置");
    await refresh();
  } catch (error) {toast(error.message, true);} finally {state.busy.delete(task.id); renderTasks();}
}

async function operate(task, action, payload = {}) {
  state.busy.add(task.id);
  renderTasks();
  try {
    const result = await api(`/api/tasks/${task.id}${action === "delete" ? "" : `/${action}`}`, action === "delete" ? "DELETE" : "POST", payload);
    toast(action === "start" ? "启动请求已提交，请在监控页确认市场连接与运行状态" : action === "stop" ? "停止请求已发送，请核对持仓；程序不会自动平仓" : action === "pause" ? "已暂停新套利，当前成交会正常结算" : action === "resume" ? "已请求恢复，风控门仍然有效" : "任务已删除，历史文件保留在服务器");
    if (action === "start" && result?.startup_warnings?.length) toast(result.startup_warnings.join("；"));
    await refresh();
  } finally {state.busy.delete(task.id); renderTasks();}
}

element("task-rows").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button || button.disabled) return;
  const task = state.tasks.find((candidate) => candidate.id === button.closest("tr").dataset.id);
  const action = button.dataset.action;
  if (action === "edit") openEditor(task);
  else if (action === "detail") openTaskDetails(task);
  else if (action === "threshold") openThreshold(task);
  else if (action === "monitor") {selectMonitorTask(task.id); element("monitor-select").value = task.id; navigate("monitor");}
  else if (action === "start") {
    if (task.mode === "live") confirm(`启动 ${task.symbol} 实盘交易`, "程序会使用所选凭据发送真实订单。请核对手续费、阈值、仓位上限和账户整体风险；多进程实盘必须使用独立签名密钥。", () => operate(task, "start", {confirm_live: true}), task.symbol, true);
    else operate(task, "start").catch((error) => toast(error.message, true));
  } else if (action === "stop") {
    const force = task.state === "stopping";
    confirm(force ? `强制结束 ${task.symbol}` : `停止 ${task.symbol} 任务`, force ? "强制结束会中断成交确认与退出清理，可能留下未核实的敞口。结束后请立即到交易所核对订单与持仓。" : "将请求程序优雅退出，等待正在执行的交易结算。已有仓位会保留，请到交易所核对并按需要处理。", () => operate(task, "stop", force ? {force: true, confirm_force: true} : {}), force ? task.symbol : null, force);
  } else if (action === "pause") {
    confirm(`暂停 ${task.symbol}？`, "只停止新的套利扫描，不强行中断当前成交；已有持仓不会自动平仓。", () => operate(task, "pause"));
  } else if (action === "resume") {
    operate(task, "resume").catch((error) => toast(error.message, true));
  } else if (action === "delete") confirm("删除这个任务？", `将移除「${task.name}」的配置记录，服务器上的历史 CSV 和日志仍会保留。`, () => operate(task, "delete"));
});

element("new-task").addEventListener("click", () => openEditor());
element("empty-new").addEventListener("click", () => openEditor());
element("close-editor").addEventListener("click", () => element("task-dialog").close());
element("cancel-editor").addEventListener("click", () => element("task-dialog").close());
element("confirm-cancel").addEventListener("click", () => element("confirm-dialog").close());
element("search").addEventListener("input", renderTasks);
element("state-filter").addEventListener("change", renderTasks);
element("monitor-select").addEventListener("change", () => {
  selectMonitorTask(element("monitor-select").value);
  refreshDetail().catch((error) => toast(error.message, true));
});
document.querySelectorAll("[data-log-tab]").forEach((button) => button.addEventListener("click", () => {state.logTab = button.dataset.logTab; renderMonitor();}));
element("trades-pagination").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-trades-page]");
  if (!button || button.disabled) return;
  state.tradesPage = Number(button.dataset.tradesPage);
  refreshDetail().catch((error) => toast(error.message, true));
});
element("refresh-detail").addEventListener("click", () => refreshDetail().catch((error) => toast(error.message, true)));
element("log-follow").addEventListener("change", () => {if (element("log-follow").checked) element("log-output").scrollTop = element("log-output").scrollHeight;});
element("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = element("login-form").querySelector("button");
  button.disabled = true;
  element("login-error").textContent = "";
  try {
    await api("/api/login", "POST", {password: element("password").value});
    element("password").value = "";
    await enter();
  } catch (error) {element("login-error").textContent = error.message;} finally {button.disabled = false;}
});
element("logout").addEventListener("click", async () => {
  try {await api("/api/logout", "POST", {}); showLogin();} catch (error) {toast(error.message, true);}
});

let detailsRequest = 0;
async function openTaskDetails(task) {
  state.detailTask = task.id;
  state.detailLogs = null;
  state.detailTab = "logs";
  element("task-detail-title").textContent = `${task.symbol} · 任务详情`;
  element("task-detail-summary").textContent = task.name;
  element("task-detail-error").textContent = "";
  element("task-detail-log").textContent = "正在读取日志…";
  element("detail-follow").checked = true;
  renderDetailLogs();
  element("task-detail-dialog").showModal();
  await refreshTaskDetails();
}

function renderDetailLogs() {
  const output = element("task-detail-log");
  const previousScroll = output.scrollTop;
  const text = state.detailTab === "events" ? state.detailLogs?.events : state.detailLogs?.text;
  output.textContent = text || (state.detailLogs ? "暂无日志。启动任务后，这里会显示连接、执行与错误记录；已停止任务的日志会保留。" : "正在读取日志…");
  output.scrollTop = element("detail-follow").checked ? output.scrollHeight : previousScroll;
  document.querySelectorAll("[data-detail-tab]").forEach((button) => button.classList.toggle("selected", button.dataset.detailTab === state.detailTab));
  element("detail-download").href = `/api/tasks/${state.detailTask}/download/${state.detailTab}`;
}

async function refreshTaskDetails() {
  const taskId = state.detailTask;
  const request = ++detailsRequest;
  if (!taskId || !state.authenticated) return;
  try {
    const [detail, logs] = await Promise.all([api(`/api/tasks/${taskId}`), api(`/api/tasks/${taskId}/logs`)]);
    if (request !== detailsRequest || state.detailTask !== taskId || !element("task-detail-dialog").open) return;
    const task = detail.task;
    state.detailLogs = logs;
    element("task-detail-summary").innerHTML = `<span><b>${escapeHtml(task.name)}</b></span>${badge(task)}${thresholdStatus(task)}<span>${escapeHtml(venues[task.primary])} ⇄ ${escapeHtml(venues[task.hedge])} · ${task.mode === "live" ? "实盘" : "只采集"}</span><span>YAML：${escapeHtml(task.strategy_file ? strategyLabel(task.strategy_file) : "任务独立配置")} · ENV：${escapeHtml(task.profile)}</span><span>运行时长 ${duration(task)} · 退出码 ${escapeHtml(task.exit_code ?? "—")}</span>`;
    element("task-detail-error").textContent = "";
    element("detail-updated").textContent = `每 3 秒刷新 · 最近更新 ${new Date().toLocaleTimeString("zh-CN", {hour12: false})} · 日志末尾 192 KB`;
    renderDetailLogs();
  } catch (error) {
    if (state.detailTask === taskId && element("task-detail-dialog").open) element("task-detail-error").textContent = `日志刷新失败：${error.message}（保留上次内容）`;
  }
}

element("close-task-detail").addEventListener("click", () => element("task-detail-dialog").close());
element("task-detail-dialog").addEventListener("close", () => {state.detailTask = null; state.detailLogs = null; detailsRequest++;});
element("detail-refresh").addEventListener("click", refreshTaskDetails);
element("detail-follow").addEventListener("change", renderDetailLogs);
document.querySelectorAll("[data-detail-tab]").forEach((button) => button.addEventListener("click", () => {state.detailTab = button.dataset.detailTab; renderDetailLogs();}));
element("detail-monitor").addEventListener("click", () => {selectMonitorTask(state.detailTask); element("monitor-select").value = state.selected; element("task-detail-dialog").close(); navigate("monitor");});

let libraryRequest = 0;
function renderCredentials(fields) {
  const groups = [
    ["RH 专用凭据（不随主腿 / 对冲腿切换）", fields.filter((field) => /^RH_LIGHTER_/.test(field.name))],
    ["Lighter 主网", fields.filter((field) => /^LIGHTER_/.test(field.name))],
    ["Arcus", fields.filter((field) => /^ARCUS_/.test(field.name))],
    ["Entropy / Trade.xyz", fields.filter((field) => /^HL_/.test(field.name))],
    ["旧版 PRIMARY 凭据（已有两组时请在 YAML 绑定交易所）", fields.filter((field) => /^PRIMARY_/.test(field.name))],
    ["Telegram 通知", fields.filter((field) => /^TELEGRAM_/.test(field.name))],
  ];
  element("credential-fields").innerHTML = groups.map(([title, entries], index) => `<details class="credential-group" ${index === 0 ? "open" : ""}><summary>${title}</summary><div class="form-grid">${entries.map((field) => `<div class="credential-item"><label for="credential-${field.name}">${field.name}</label><input id="credential-${field.name}" type="password" data-credential="${field.name}" autocomplete="off" maxlength="2048" placeholder="${field.configured ? "已配置 · 留空保留" : "未配置 · 可留空"}"><small>${field.inherited ? "由服务环境变量覆盖，文件修改不会覆盖它" : field.configured ? "服务器已保存，不回传原值" : "未配置，按所用交易所填写"}</small><label class="clear-secret"><input type="checkbox" data-clear="${field.name}" ${field.inherited ? "disabled" : ""}> 清除此字段</label></div>`).join("")}</div></details>`).join("");
}

function updateLibraryPath() {
  const name = element("library-name").value;
  element("library-path").textContent = state.libraryKind === "env" ? name === "default" ? "保存到项目 .env" : `保存到 credentials/${name || "名称"}.env` : state.libraryOriginal === "@default" ? "保存到项目 config.yaml" : `保存到 configs/${name || "名称"}${/\.ya?ml$/.test(name) ? "" : ".yaml"}`;
}

async function loadLibraryFile() {
  const request = ++libraryRequest;
  const kind = state.libraryKind;
  const name = element("library-file").value;
  state.libraryOriginal = name || null;
  element("library-error").textContent = "";
  element("library-name").readOnly = name === "@default";
  element("library-name").value = name === "@default" ? "config.yaml" : name;
  element("library-template").disabled = Boolean(name);
  element("save-library").disabled = true;
  element("credential-fields").replaceChildren();
  try {
    const saved = name ? await api(`/api/configs/${kind}/${encodeURIComponent(name)}`) : null;
    if (request !== libraryRequest || !element("library-dialog").open) return;
    if (kind === "yaml") element("library-yaml-text").value = saved?.config || state.meta.templates[element("library-template").value];
    else renderCredentials(saved?.fields || state.meta.credential_fields.map((key) => ({name: key, configured: false})));
    updateLibraryPath();
  } catch (error) {if (request === libraryRequest) element("library-error").textContent = error.message;} finally {if (request === libraryRequest) element("save-library").disabled = false;}
}

function switchLibrary(kind) {
  state.libraryKind = kind;
  element("test-telegram").hidden = kind !== "env";
  element("library-yaml").hidden = kind !== "yaml";
  element("library-env").hidden = kind !== "env";
  document.querySelectorAll("[data-library-tab]").forEach((button) => button.classList.toggle("selected", button.dataset.libraryTab === kind));
  const names = kind === "yaml" ? state.meta.strategies : state.meta.profiles;
  element("library-file").innerHTML = `<option value="">＋ 从模板新增 ${kind.toUpperCase()}</option>` + names.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(kind === "yaml" ? strategyLabel(name) : name === "default" ? "default · 项目 .env" : `${name}.env`)}</option>`).join("");
  loadLibraryFile();
}

async function openLibrary() {
  try {
    state.meta = await api("/api/meta");
    element("library-template").innerHTML = Object.keys(state.meta.templates).map((name) => `<option value="${name}">${name === "rh-arcus" ? "RH / Arcus" : "Entropy / RH"} · 示例模板</option>`).join("");
    element("library-dialog").showModal();
    switchLibrary("yaml");
    return true;
  } catch (error) {toast(error.message, true); return false;}
}

element("open-library").addEventListener("click", openLibrary);
element("open-telegram").addEventListener("click", async () => {
  if (!await openLibrary()) return;
  switchLibrary("env");
  element("library-file").value = "default";
  await loadLibraryFile();
  const telegramGroup = [...document.querySelectorAll(".credential-group")].find((group) => group.querySelector("summary")?.textContent.includes("Telegram"));
  if (telegramGroup) telegramGroup.open = true;
});
element("test-telegram").addEventListener("click", async () => {
  if (state.libraryKind !== "env") {
    toast("请先切换到 ENV 凭据", true);
    return;
  }
  if ([...document.querySelectorAll("[data-credential]")].some((input) => input.value.trim())) {
    toast("请先保存凭据，再发送测试", true);
    return;
  }
  const profile = element("library-name").value.trim() || "default";
  const button = element("test-telegram");
  button.disabled = true;
  element("library-error").textContent = "";
  try {
    await api("/api/telegram/test", "POST", {profile});
    toast("Telegram 测试消息已发送");
  } catch (error) {
    element("library-error").textContent = error.message;
  } finally {
    button.disabled = false;
  }
});
element("close-library").addEventListener("click", () => element("library-dialog").close());
element("cancel-library").addEventListener("click", () => element("library-dialog").close());
element("library-dialog").addEventListener("close", () => {libraryRequest++; element("credential-fields").replaceChildren();});
document.querySelectorAll("[data-library-tab]").forEach((button) => button.addEventListener("click", () => switchLibrary(button.dataset.libraryTab)));
element("library-file").addEventListener("change", loadLibraryFile);
element("library-name").addEventListener("input", updateLibraryPath);
element("library-template").addEventListener("change", () => {element("library-yaml-text").value = state.meta.templates[element("library-template").value];});
element("credential-fields").addEventListener("change", (event) => {
  const key = event.target.dataset.clear;
  if (!key) return;
  const input = element("credential-fields").querySelector(`[data-credential="${key}"]`);
  input.disabled = event.target.checked;
  if (input.disabled) input.value = "";
});
element("library-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const kind = state.libraryKind;
  let name = element("library-name").value.trim();
  if (kind === "yaml") name = state.libraryOriginal === "@default" ? "@default" : /\.ya?ml$/.test(name) ? name : `${name}.yaml`;
  const payload = {overwrite: name === state.libraryOriginal};
  if (kind === "yaml") payload.config = element("library-yaml-text").value;
  else {
    payload.values = Object.fromEntries([...document.querySelectorAll("[data-credential]")].map((input) => [input.dataset.credential, input.value]));
    payload.clear = [...document.querySelectorAll("[data-clear]:checked")].map((input) => input.dataset.clear);
  }
  element("save-library").disabled = true;
  element("library-error").textContent = "";
  try {
    await api(`/api/configs/${kind}/${encodeURIComponent(name)}`, "PUT", payload);
    if (kind === "env") document.querySelectorAll("[data-credential]").forEach((input) => {input.value = "";});
    state.meta = await api("/api/meta");
    toast("文件已保存；任务下次启动读取，不会自动启动交易");
    if (element("library-dialog").open && state.libraryKind === kind) {
      switchLibrary(kind);
      element("library-file").value = name;
      await loadLibraryFile();
    }
  } catch (error) {element("library-error").textContent = error.message;} finally {element("save-library").disabled = false;}
});

setInterval(() => {element("clock").textContent = new Date().toLocaleString("zh-CN", {hour12: false});}, 1000);
setInterval(refresh, 3000);
api("/api/session").then((session) => session.authenticated ? enter() : showLogin()).catch((error) => {showLogin(); element("login-error").textContent = error.message;});

let liquidationInitialized = false;
const liquidationFields = {warn_pct: 'warn', block_pct: 'block', close_pct: 'close', poll_sec: 'poll', stale_sec: 'stale', slippage_bps: 'slip'};
function renderLiquidation(value, fill = false) {
  if (!value) return;
  state.liquidation = value;
  if (!liquidationInitialized || fill) {
    element('liquidation-enabled').checked = value.settings.enabled;
    for (const [key, id] of Object.entries(liquidationFields)) element(`liquidation-${id}`).value = value.settings[key];
    liquidationInitialized = true;
  }
  const rows = Object.entries(value.tasks);
  const warning = rows.some(([, row]) => row.warning);
  element('liquidation-label').textContent = value.latched ? '紧急平仓已锁定' : !value.settings.enabled ? '未开启' : value.blocked ? '禁止新增仓位' : warning ? '风险预警' : '保护中';
  element('liquidation-label').classList.toggle('liquidation-danger', Boolean(value.latched || value.settings.enabled && value.blocked));
  element('liquidation-summary').textContent = value.latched ? `${value.latched.reason} · ${timeLabel(value.latched.at)}；退出后保持锁定，不自动恢复交易。` : `覆盖 ${rows.length} 个已运行过的实盘任务；${value.settings.enabled ? '数据异常时禁止新增仓位，停止任务后继续仅风险监控。' : '开启前请确认各任务市场独占；当前未执行自动爆仓保护。'}`;
  element('liquidation-reset').disabled = !value.latched;
  element('liquidation-verify').disabled = !value.latched;
  element('liquidation-accounts').innerHTML = rows.map(([id, row]) => {
    const task = state.tasks.find((t) => t.id === id);
    const metrics = (row.metrics || []).map((m) => `${escapeHtml(m.venue)}：${number(m.risk_pct)}% · 权益 ${money(m.equity)} / 维持保证金 ${money(m.maintenance)} <small>${escapeHtml(m.source)}</small>`).join('');
    return `<div class="liquidation-account"><b>${escapeHtml(task?.name || id)}</b>${task?.risk_watch_only ? ' · 仅风险监控' : ''}<div>${metrics || '等待账户风险数据'}</div><small>更新 ${timeLabel(row.updated_at)}${row.stale ? ' · 数据已过期' : ''}${row.error ? ` · ${escapeHtml(row.error)}` : ''}</small>${row.message ? `<div class="${row.flat ? '' : 'liquidation-danger'}">${escapeHtml(row.message)}</div>` : ''}</div>`;
  }).join('') || '<p class="field-note">尚无已运行过的实盘任务。</p>';
}
element('liquidation-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  element('liquidation-error').textContent = '';
  const settings = {enabled: element('liquidation-enabled').checked};
  for (const [key, id] of Object.entries(liquidationFields)) settings[key] = Number(element(`liquidation-${id}`).value);
  try {
    renderLiquidation(await api('/api/liquidation', 'PUT', settings), true);
    toast(settings.enabled ? '风控已开启，等待各账户风险数据确认' : '风控已关闭');
  } catch (error) {element('liquidation-error').textContent = error.message;}
});
element('liquidation-reset').addEventListener('click', async () => {
  try {
    renderLiquidation(await api('/api/liquidation/reset', 'POST', {}), true);
    toast('锁定已解除，保护已关闭；请检查设置后重新开启，交易任务需手动重新启动。');
  } catch (error) {element('liquidation-error').textContent = error.message;}
});

element('liquidation-verify').addEventListener('click', async () => {
  if (!element('liquidation-orders-checked').checked) {
    element('liquidation-error').textContent = '请先在交易所核对未知订单均已终结，再勾选确认。';
    return;
  }
  try {
    renderLiquidation(await api('/api/liquidation/verify', 'POST', {confirmed_terminal_orders: true}));
    element('liquidation-orders-checked').checked = false;
    element('liquidation-error').textContent = '';
    toast('已请求重新核验；风控锁定保留，仅继续减仓。');
  } catch (error) {element('liquidation-error').textContent = error.message;}
});
