"use strict";

const element = (id) => document.getElementById(id);
const state = {tasks: [], meta: null, selected: null, page: location.pathname === "/monitor" ? "monitor" : "dex", authenticated: false, editing: null, detail: null, logs: null, logTab: "logs", tradesPage: 1, tradesPageSize: 10, busy: new Set(), polling: false, detailTask: null, detailTab: "logs", detailLogs: null, libraryKind: "yaml", libraryOriginal: null, tri: {candidates: [], snapshot: null, selectedSymbol: null}};
const venues = {entropy: "Entropy", lighter: "Lighter", "lighter-rh": "RH", tradexyz: "Trade.xyz", arcus: "Arcus"};
const active = (task) => ["running", "stopping"].includes(task.state);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (character) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[character]));
const number = (value, digits = 2) => value == null || !Number.isFinite(Number(value)) ? "—" : Number(value).toLocaleString("en-US", {maximumFractionDigits: digits, minimumFractionDigits: digits});
const signed = (value, digits = 2) => value == null ? "—" : `${Number(value) >= 0 ? "+" : ""}${number(value, digits)}`;
const money = (value) => value == null ? "—" : `$${number(value)}`;
const timeLabel = (timestamp) => timestamp ? new Date(timestamp * 1000).toLocaleString("zh-CN", {hour12: false}) : "—";
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
  document.querySelectorAll(".nav-link").forEach((link) => link.classList.toggle("active", link.dataset.page === page));
  element("breadcrumb").textContent = `工作空间 / ${page === "dex" ? "DEX 套利" : "监控"}`;
  document.title = `${page === "dex" ? "DEX 套利" : "监控"} · Entropy Console`;
  if (push) history.pushState({}, "", page === "dex" ? "/dex-arbitrage" : "/monitor");
  if (page === "monitor" && state.authenticated) {refreshDetail().catch((error) => toast(error.message, true)); loadTripleCandidates(); refreshTriple();}
}

document.addEventListener("click", (event) => {
  const link = event.target.closest("a[data-page], .brand");
  if (link && !event.metaKey && !event.ctrlKey) {
    event.preventDefault();
    navigate(link.dataset.page || "dex");
  }
});
window.addEventListener("popstate", () => navigate(location.pathname === "/monitor" ? "monitor" : "dex", false));

function badge(task) {
  let label = {running: "运行中", stopping: "停止中", stopped: "已停止", failed: "异常退出", interrupted: "服务中断"}[task.state] || task.state;
  let className = task.state === "interrupted" ? "failed" : task.state;
  if (task.state === "running") {
    if (task.status_stale) {label = task.status ? "状态过期" : "启动中"; className = "stale";}
    else if (task.status.paused) {label = "已暂停"; className = "halted";}
    else if (task.status.halted) {label = "风险暂停"; className = "halted";}
    else if (!task.status.ready) {label = "加载市场"; className = "starting";}
    else if (task.status.venues.some((venue) => !venue.fresh || venue.down)) {label = "行情异常"; className = "stale";}
  }
  return `<span class="badge ${className}"><span class="mini-dot"></span>${escapeHtml(label)}</span>`;
}

function thresholdStatus(task) {
  if (task.mode !== "live") return "";
  const status = task.auto_threshold || {};
  const labels = {applied: "今日阈值已更新", unchanged: "今日已检查，阈值未变", failed: "今日阈值更新失败", pending: "今日阈值待更新", skipped: "今日阈值已跳过"};
  const label = labels[status.status] || labels.pending;
  const extra = status.status === "failed" ? `：${status.reason || "请查看事件日志"}` : (status.window ? ` · ${status.window}` : "");
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
  element("dex-stats").innerHTML = stat("运行中的任务", running.length, `<span class="mini-dot"></span>运行上限 ${state.meta.max_running} 个实例`, "↗", "个") + stat("实盘交易", live.length, "独立实例 · 真实订单", "⇄", "个") + stat("行情采集", record.length, "仅记录行情，不发送订单", "▥", "个") + stat("配置持仓上限合计", `$${number(caps, 0)}`, "运行中实盘的双腿上限合计，非实际持仓", "⊞");
  const query = element("search").value.toLowerCase();
  const filter = element("state-filter").value;
  const tasks = state.tasks.filter((task) => `${task.name} ${task.symbol}`.toLowerCase().includes(query) && (filter === "all" || (filter === "active" && active(task)) || (filter === "stopped" && task.state === "stopped") || (filter === "failed" && ["failed", "interrupted"].includes(task.state))));
  element("task-rows").innerHTML = tasks.map((task) => {
    const premium = !task.status_stale && task.state === "running" ? task.status?.premium_bps : null;
    const disabled = state.busy.has(task.id) ? "disabled" : "";
    const paused = Boolean(task.manual_paused || task.status?.paused);
    const controls = active(task)
      ? task.state === "stopping" ? `<button class="text-button stop" data-action="stop" ${disabled}>强制结束</button>`
        : `${task.mode === "live" ? `<button class="text-button ${paused ? "resume" : "pause"}" data-action="${paused ? "resume" : "pause"}" ${disabled}>${paused ? "恢复" : "暂停"}</button>` : ""}<button class="text-button stop" data-action="stop" ${disabled}>停止</button>`
      : `<button class="text-button" data-action="start" ${disabled}>启动</button><button class="text-button" data-action="edit" ${disabled}>配置</button>`;
    return `<tr data-id="${task.id}"><td><div class="coin-cell"><span class="coin-icon">${escapeHtml(task.symbol.slice(0, 1))}</span><div><button class="text-button task-title" data-action="detail" aria-label="查看 ${escapeHtml(task.symbol)} 任务详情">${escapeHtml(task.symbol)}</button><small title="${escapeHtml(task.name)}">${escapeHtml(task.name)}</small></div></div></td><td class="route-cell"><b>${escapeHtml(venues[task.primary])}</b><span>⇄</span><b>${escapeHtml(venues[task.hedge])}</b></td><td><span class="badge ${task.mode}">${task.mode === "live" ? "LIVE · 实盘" : "DATA · 采集"}</span></td><td>${badge(task)}${thresholdStatus(task)}</td><td class="number ${premium == null ? "" : premium >= 0 ? "positive" : "negative"}">${signed(premium)}</td><td class="number">${duration(task)}</td><td><div class="actions">${controls}<button class="text-button" data-action="threshold">阈值</button><button class="text-button" data-action="detail">详情</button><button class="text-button" data-action="monitor">监控 ↗</button>${!active(task) ? `<button class="icon-button" data-action="delete" aria-label="删除 ${escapeHtml(task.name)}" title="删除任务">×</button>` : ""}</div></td></tr>`;
  }).join("");
  element("task-empty").hidden = tasks.length > 0;
  if (!tasks.length) {
    element("task-empty").querySelector("h3").textContent = state.tasks.length ? "没有匹配的任务" : "从第一个币种开始";
    element("task-empty").querySelector("p").textContent = state.tasks.length ? "试试其他搜索词或状态筛选。" : "创建任务并配置交易路径。建议先采集行情，再用实际数据设定实盘阈值。";
    element("empty-new").hidden = state.tasks.length > 0;
  }
  const options = state.tasks.map((task) => `<option value="${task.id}">${escapeHtml(task.symbol)} · ${escapeHtml(task.name)}</option>`).join("");
  if (element("monitor-select").innerHTML !== options) element("monitor-select").innerHTML = options;
  if (!state.tasks.some((task) => task.id === state.selected)) state.selected = state.tasks[0]?.id || null;
  if (state.selected) element("monitor-select").value = state.selected;
  element("monitor-empty").hidden = state.tasks.length > 0;
  element("monitor-content").hidden = !state.tasks.length;
}

async function refresh() {
  if (!state.authenticated || state.polling) return;
  state.polling = true;
  try {
    const result = await api("/api/tasks");
    state.tasks = result.tasks;
    element("connection-label").textContent = "服务器已连接";
    element("connection-dot").parentElement.classList.remove("bad-connection");
    document.querySelector(".server-pill").classList.remove("bad-connection");
    document.querySelector(".server-pill").lastChild.textContent = " SERVER CONNECTED";
    renderTasks();
    if (state.page === "monitor") {await refreshDetail(); await refreshTriple();}
    if (element("task-detail-dialog").open) await refreshTaskDetails();
  } catch (error) {
    element("connection-label").textContent = "连接中断";
    element("connection-dot").parentElement.classList.add("bad-connection");
    document.querySelector(".server-pill").classList.add("bad-connection");
    document.querySelector(".server-pill").lastChild.textContent = " SERVER DISCONNECTED";
  } finally {state.polling = false;}
}

function renderTriple() {
  const candidates = state.tri.candidates || [];
  const compatible = candidates.filter((row) => row.compatible);
  const select = element("tri-symbol");
  const current = select.value;
  select.innerHTML = compatible.length
    ? compatible.map((row) => `<option value="${escapeHtml(row.symbol)}">${escapeHtml(row.symbol)} · ${escapeHtml(row.label)}</option>`).join("")
    : '<option value="">暂无三市场共同币种</option>';
  if (candidates.some((row) => row.symbol === current)) select.value = current;
  const snapshot = state.tri.snapshot;
  if (!state.tri.selectedSymbol) state.tri.selectedSymbol = snapshot?.selected_symbol || compatible[0]?.symbol || null;
  const details = snapshot?.selected || (state.tri.selectedSymbol ? snapshot?.symbols?.[state.tri.selectedSymbol] : null);
  const running = Boolean(snapshot?.running);
  element("tri-monitor-status").textContent = running ? `运行中 · ${snapshot.count || 1} 个币种` : "未启动";
  element("tri-monitor-status").className = `badge ${running ? "running" : ""}`;
  element("tri-start").disabled = !select.value;
  element("tri-stop").disabled = !running;
  element("tri-candidates").innerHTML = candidates.length ? `<div class="tri-candidate-grid">${candidates.map((row) => `<article class="tri-candidate ${row.compatible ? "compatible" : "incompatible"}"><b>${escapeHtml(row.symbol)}</b><span>${escapeHtml(row.label)}</span><small>${row.compatible ? "RH / Arcus / Entropy 均有活跃永续" : escapeHtml((row.warnings || []).join("；"))}</small></article>`).join("")}</div>` : '<div class="empty-state"><h3>没有共同活跃市场</h3><p>刷新后重试；共同币种为空时不会启动监控。</p></div>';
  const overview = snapshot?.symbols || {};
  const pairFor = (row, names) => row.pairs?.find((pair) => new Set([pair.left, pair.right]).size === 2 && names.every((name) => [pair.left, pair.right].includes(name)));
  const overviewCell = (pair) => {
    if (!pair) return "<span class=\"overview-muted\">—</span>";
    const signal = pair.sell_signal ? `卖${pair.left}/买${pair.right} ${signed(pair.sell_left_buy_right_bps)}` : pair.buy_signal ? `买${pair.left}/卖${pair.right} ${signed(pair.buy_left_sell_right_bps)}` : `净价差 ${signed(Math.max(pair.sell_left_buy_right_bps ?? -Infinity, pair.buy_left_sell_right_bps ?? -Infinity))}`;
    return `<b>${signed(pair.mid_spread_bps)} bps</b><small>${escapeHtml(signal)}</small>`;
  };
  element("tri-overview").innerHTML = Object.keys(overview).length ? `<div class="overview-heading"><b>全部币种实时价差</b><small>${Object.keys(overview).length} 个共同活跃市场 · 点击币种查看三边盘口</small></div><div class="table-scroll"><table class="tri-overview-table"><thead><tr><th>币种</th><th>RH ↔ Entropy</th><th>RH ↔ Arcus</th><th>Arcus ↔ Entropy</th><th>行情</th></tr></thead><tbody>${Object.entries(overview).map(([symbol, row]) => `<tr data-tri-symbol="${escapeHtml(symbol)}"><td><button class="text-button tri-symbol-link" type="button">${escapeHtml(symbol)}</button></td><td>${overviewCell(pairFor(row, ["RH", "ENTROPY"]))}</td><td>${overviewCell(pairFor(row, ["RH", "ARCUS"]))}</td><td>${overviewCell(pairFor(row, ["ARCUS", "ENTROPY"]))}</td><td><span class="badge ${row.venues?.every((venue) => venue.fresh) ? "" : "stale"}">${row.venues?.every((venue) => venue.fresh) ? "正常" : "等待 / 过期"}</span></td></tr>`).join("")}</tbody></table></div>` : "";
  element("tri-venues").innerHTML = details?.venues?.length ? details.venues.map((venue) => `<article class="venue-card"><div class="venue-top"><div><b>${escapeHtml(venue.name)}</b><small>${escapeHtml(venue.symbol)}</small></div><span class="badge ${venue.fresh ? "" : "stale"}">${venue.fresh ? `正常 · ${number(venue.age_sec, 1)}s` : "等待行情"}</span></div><div class="venue-values"><div><span>买一 / 卖一</span><strong class="number">${number(venue.bid, 5)} / ${number(venue.ask, 5)}</strong></div><div><span>中间价</span><strong>${number(venue.mid, 5)}</strong></div></div></article>`).join("") : "";
  element("tri-pairs").innerHTML = details?.pairs?.length ? details.pairs.map((pair) => `<article class="tri-pair"><div class="tri-pair-head"><b>${escapeHtml(pair.left)} ⇄ ${escapeHtml(pair.right)}</b><span class="badge ${pair.script_compatible ? "" : "stale"}">${pair.script_compatible ? "脚本可对冲" : "仅价差监控"}</span></div><div class="tri-pair-grid"><div><small>中间价差</small><strong>${signed(pair.mid_spread_bps)} bps</strong></div><div class="${pair.sell_signal ? "signal" : ""}"><small>卖左 / 买右</small><strong>${signed(pair.sell_left_buy_right_bps)} <em>门槛 ${signed(pair.sell_hurdle_bps)}</em></strong></div><div class="${pair.buy_signal ? "signal" : ""}"><small>买左 / 卖右</small><strong>${signed(pair.buy_left_sell_right_bps)} <em>门槛 ${signed(pair.buy_hurdle_bps)}</em></strong></div></div></article>`).join("") : "";
}

async function loadTripleCandidates() {
  element("tri-monitor-error").textContent = "正在读取三边市场列表…";
  try {
    const result = await api("/api/market-monitor/candidates");
    state.tri.candidates = result.symbols || [];
    const configured = state.meta?.strategies || [];
    element("tri-strategy").innerHTML = configured.length ? configured.map((name) => `<option value="${escapeHtml(name)}">${escapeHtml(strategyLabel(name))}</option>`).join("") : '<option value="">请先在配置库保存 YAML</option>';
    element("tri-monitor-error").textContent = result.errors && Object.keys(result.errors).length ? `部分市场列表读取失败：${escapeHtml(Object.values(result.errors).join("；"))}` : "";
    renderTriple();
  } catch (error) {element("tri-monitor-error").textContent = error.message;}
}

async function refreshTriple() {
  if (!state.authenticated) return;
  try {state.tri.snapshot = await api("/api/market-monitor"); renderTriple();} catch (error) {element("tri-monitor-error").textContent = error.message;}
}

element("tri-refresh").addEventListener("click", () => loadTripleCandidates());
element("tri-start").addEventListener("click", async () => {
  const symbol = element("tri-symbol").value;
  const strategy_file = element("tri-strategy").value;
  const hedge_venue = element("tri-hedge-venue").value;
  if (!symbol || !strategy_file) return;
  element("tri-start").disabled = true;
  element("tri-monitor-error").textContent = "正在连接三个公开盘口…";
  try {state.tri.snapshot = await api("/api/market-monitor/start", "POST", {symbol, strategy_file, hedge_venue, all: true}); const errors = Object.values(state.tri.snapshot.errors || {}); element("tri-monitor-error").textContent = errors.length ? `部分币种未连接：${errors.join("；")}` : ""; renderTriple();}
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

function renderMonitor() {
  if (!state.detail || state.detail.task.id !== state.selected) return;
  const task = state.detail.task;
  const metrics = task.status;
  const fresh = active(task) && !task.status_stale;
  const metricNote = fresh ? "当前运行会话" : metrics ? "最后快照 · 当前非实时" : "等待首次状态快照";
  const telegramLabel = task.mode === "live" && metrics ? ` · Telegram ${metrics.telegram_enabled ? "已启用" : "未配置"}` : "";
  element("monitor-summary").innerHTML = `<div><strong>${escapeHtml(task.symbol)}</strong><span>${escapeHtml(venues[task.primary])} ⇄ ${escapeHtml(venues[task.hedge])}</span><span class="badge ${task.mode}">${task.mode === "live" ? "LIVE · 实盘" : "DATA · 采集"}</span>${badge(task)}</div><small>${fresh ? "实时快照" : "最后快照"} ${timeLabel(metrics?.updated_at)}${telegramLabel} ${task.exit_code == null ? "" : ` · 退出码 ${task.exit_code}`}</small>`;
  element("monitor-stats").innerHTML = stat("双腿中间价溢价", signed(metrics?.premium_bps), metricNote, "⇄", "bps") + stat("净敞口", metrics?.net_delta == null ? "—" : signed(metrics.net_delta, 6), "双腿基础币数量合计", "⊞") + stat("会话盈亏 · MTM", metrics?.session_pnl == null ? "—" : `$${signed(metrics.session_pnl)}`, "按盘口标记的会话估值变化，非已实现收益", "↗") + stat("套利执行 / 对冲", `${metrics?.trades ?? 0} / ${metrics?.hedges ?? 0}`, `当前会话记录 ${metrics?.minute_rows ?? 0} 条分钟数据`, "▥");
  element("venue-cards").innerHTML = metrics?.venues?.length ? metrics.venues.map((venue) => `<article class="venue-card"><div class="venue-top"><div><b>${escapeHtml(venue.name)}</b><small>${escapeHtml(venue.symbol)}</small></div><span class="badge ${fresh && venue.fresh && !venue.down ? "" : "stale"}">${!fresh ? "非实时" : venue.down ? "连接异常" : venue.fresh ? `正常 · ${number(venue.age_sec, 1)}s` : "行情过期"}</span></div><div class="venue-values"><div><span>买一 / 卖一</span><strong class="number">${number(venue.bid, 5)} / ${number(venue.ask, 5)}</strong></div><div><span>持仓数量</span><strong>${signed(venue.position, 6)}</strong></div><div><span>持仓金额 · USD</span><strong>${money(venue.position_usd)}</strong></div><div><span>账户权益 / 可用</span><strong>${money(venue.equity)} / ${money(venue.free)}</strong></div><div><span>会话成交额 · USD</span><strong>${money(venue.volume_usd)}</strong></div></div></article>`).join("") : `<div class="empty-state"><h3>等待市场连接</h3><p>启动任务后将展示真实双腿盘口。启动失败请查看下方日志。</p></div>`;
  drawChart(state.detail.minutes);
  renderLogs();
  const base = `/api/tasks/${task.id}/download/`;
  element("download-minutes").href = base + "minutes";
  element("download-logs").href = base + state.logTab;
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
    element("trades-output").innerHTML = rows.length ? `<table class="records"><thead><tr><th>时间</th><th>方向</th><th>数量</th><th>买 / 卖成交</th><th>手续费 USD</th><th>利润 USD</th><th>结果</th></tr></thead><tbody>${rows.map((row) => {
      const profit = Number(row.fill_edge_usd);
      const profitClass = Number.isFinite(profit) ? profit >= 0 ? "positive" : "negative" : "";
      return `<tr><td>${timeLabel(Number(row.ts))}</td><td>${escapeHtml(row.direction)}</td><td>${escapeHtml(row.qty)}</td><td>${escapeHtml(row.buy_fill)} / ${escapeHtml(row.sell_fill)}</td><td class="number">${money(row.fees_usd)}</td><td class="number ${profitClass}">${money(row.fill_edge_usd)}</td><td><span class="badge ${row.ok === "1" ? "" : "failed"}">${row.ok === "1" ? "完成" : "异常"}</span></td></tr>`;
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

async function refreshDetail() {
  const selected = state.selected;
  if (!selected || !state.authenticated) return;
  const tradeQuery = `?trades_page=${state.tradesPage}&trades_page_size=${state.tradesPageSize}`;
  const [detail, logs] = await Promise.all([api(`/api/tasks/${selected}${tradeQuery}`), api(`/api/tasks/${selected}/logs`)]);
  if (state.selected !== selected) return;
  state.detail = detail;
  state.logs = logs;
  renderMonitor();
}

const fieldMap = {"midline": ["thresholds", "midline_bps", 0], "upper": ["thresholds", "upper_bps", 4], "lower": ["thresholds", "lower_bps", 4], "order-cap": ["sizing", "max_order_notional_usd", 500], "primary-cap": ["primary", "max_position_usd", 1000], "hedge-cap": ["hedge", "max_position_usd", 1000], "primary-fee": ["primary", "taker_fee_bps", 0], "hedge-fee": ["hedge", "taker_fee_bps", 2.25], "primary-symbol": ["primary", "symbol", ""], "hedge-symbol": ["hedge", "symbol", ""]};

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

function syncFields() {
  for (const [id, [section, key, fallback]] of Object.entries(fieldMap)) element(id).value = yamlValue(element("config-yaml").value, section, key, fallback);
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
    for (const id of [...Object.keys(fieldMap), "task-primary", "sync-fields"]) element(id).disabled = Boolean(name);
    element("config-yaml").readOnly = Boolean(name);
  } finally {element("save-task").disabled = false;}
}

element("task-strategy").addEventListener("change", () => applyStrategy().catch((error) => {element("task-error").textContent = error.message;}));

for (const [id, [section, key]] of Object.entries(fieldMap)) {
  element(id).addEventListener("input", () => {
    const value = element(id).value;
    if (id.endsWith("symbol")) setYaml(section, key, value.trim(), !value.trim());
    else if (value !== "" && Number.isFinite(Number(value))) setYaml(section, key, Number(value));
  });
}
element("task-primary").addEventListener("change", venueChanged);
element("task-hedge").addEventListener("change", () => {if (!element("task-strategy").value) venueChanged();});
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
    const payload = {name: element("task-name").value, symbol: element("task-symbol").value, primary: element("task-primary").value, hedge: element("task-hedge").value, profile: element("task-profile").value, mode: element("task-form").querySelector('input[name="mode"]:checked').value, config: element("config-yaml").value, strategy_file: element("task-strategy").value || null};
    await api(state.editing ? `/api/tasks/${state.editing}` : "/api/tasks", state.editing ? "PUT" : "POST", payload);
    element("task-dialog").close();
    toast("配置已保存，任务尚未启动");
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
    element("threshold-window").textContent = `${result.window} · ${result.rows} 条有效分钟数据 · 覆盖 ${number(result.span_hours, 1)} 小时`;
    element("threshold-current").textContent = `当前：中线 ${number(result.current.midline_bps, 1)} / 上带 ${number(result.current.upper_bps, 1)} / 下带 ${number(result.current.lower_bps, 1)} bps`;
    element("threshold-midline").value = result.suggested.midline_bps;
    element("threshold-upper").value = result.suggested.upper_bps;
    element("threshold-lower").value = result.suggested.lower_bps;
    element("threshold-result").hidden = false;
    element("threshold-apply").disabled = false;
    element("threshold-error").textContent = "";
  } catch (error) {element("threshold-error").textContent = error.message;}
}

element("threshold-cancel").addEventListener("click", () => element("threshold-dialog").close());
element("threshold-apply").addEventListener("click", () => {
  if (!thresholdTask) return;
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
    await api(`/api/tasks/${task.id}${action === "delete" ? "" : `/${action}`}`, action === "delete" ? "DELETE" : "POST", payload);
    toast(action === "start" ? "启动请求已提交，请在监控页确认市场连接与运行状态" : action === "stop" ? "停止请求已发送，请核对持仓；程序不会自动平仓" : action === "pause" ? "已暂停新套利，当前成交会正常结算" : action === "resume" ? "已请求恢复，风控门仍然有效" : "任务已删除，历史文件保留在服务器");
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
  else if (action === "monitor") {state.selected = task.id; state.tradesPage = 1; element("monitor-select").value = task.id; navigate("monitor");}
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
  state.selected = element("monitor-select").value;
  state.tradesPage = 1;
  state.detail = null;
  state.logs = null;
  element("log-output").textContent = "加载中…";
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
element("detail-monitor").addEventListener("click", () => {state.selected = state.detailTask; state.tradesPage = 1; element("task-detail-dialog").close(); navigate("monitor");});

let libraryRequest = 0;
function renderCredentials(fields) {
  const groups = [
    ["Lighter / RH 与 Arcus", fields.filter((field) => /^(LIGHTER|ARCUS)_/.test(field.name))],
    ["Entropy / Trade.xyz", fields.filter((field) => /^HL_/.test(field.name))],
    ["主腿独立凭据（Lighter ↔ RH 时必填）", fields.filter((field) => /^PRIMARY_/.test(field.name))],
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
