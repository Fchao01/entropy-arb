"""Small, best-effort Telegram Bot API client for trade notifications."""
from __future__ import annotations

import asyncio
import logging
import os
import time

import aiohttp

log = logging.getLogger("telegram")


class TelegramNotifier:
    """Send one notification per settled execution without affecting trading."""

    def __init__(self, session: aiohttp.ClientSession | None = None,
                 token: str | None = None, chat_id: str | None = None) -> None:
        self.token = (token if token is not None else os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
        self.chat_id = (chat_id if chat_id is not None else os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
        self.session = session
        self._owned_session = False
        self._send_lock = None
        self._next_send_at = 0.0
        self.last_status = "disabled" if not self.enabled else "idle"
        self.last_error = "" if self.enabled else "missing bot token or chat id"
        self.sent_count = 0
        self.failed_count = 0
        self.last_sent_at = None

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def close(self) -> None:
        if self._owned_session and self.session is not None:
            await self.session.close()
            self.session = None

    async def send(self, text: str) -> bool:
        if not self.enabled:
            self.last_status = "disabled"
            self.last_error = "missing bot token or chat id"
            self.failed_count += 1
            return False
        self.last_status = "pending"
        if self._send_lock is None:
            self._send_lock = asyncio.Lock()
        async with self._send_lock:
            wait = self._next_send_at - asyncio.get_running_loop().time()
            if wait > 0:
                await asyncio.sleep(wait)
            result = await self._send_unlocked(text)
            # Telegram asks clients to avoid more than one message per second
            # in a chat; serializing here also reduces avoidable 429 responses.
            self._next_send_at = asyncio.get_running_loop().time() + 1.0
            return result

    async def _send_unlocked(self, text: str) -> bool:
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self._owned_session = True
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text[:4096], "disable_web_page_preview": True}
        for attempt in range(3):
            try:
                async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as response:
                    if response.status == 200:
                        body = await response.json(content_type=None)
                        if body.get("ok"):
                            self.last_status = "sent"
                            self.last_error = ""
                            self.sent_count += 1
                            self.last_sent_at = time.time()
                            return True
                    retry_after = 0
                    if response.status == 429:
                        body = await response.json(content_type=None)
                        retry_after = min(int(body.get("parameters", {}).get("retry_after", 1)), 8)
                    if response.status >= 500 or response.status == 429:
                        if attempt < 2:
                            await asyncio.sleep(max(retry_after, 0.5))
                            continue
                    detail = (await response.text())[:300]
                    self.last_status = "failed"
                    self.last_error = f"HTTP {response.status}: {detail}"
                    self.failed_count += 1
                    log.warning("Telegram notification failed (%s): %s", response.status, detail)
                    return False
            except Exception as error:
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                self.last_status = "failed"
                self.last_error = str(error)[:300]
                self.failed_count += 1
                log.warning("Telegram notification failed: %s", error)
        return False


def _money(value) -> str:
    return "—" if value is None else f"${float(value):+,.4f}"


def _balance(value) -> str:
    return "—" if value is None else f"${float(value):,.2f}"


def trade_message(*, symbol: str, direction: str, buy_venue: str, sell_venue: str,
                  qty: float, buy_status: str, sell_status: str, buy_fill: float,
                  sell_fill: float, fill_edge: float | None, ok: bool,
                  trade_volume: float = 0.0, total_volume: float = 0.0,
                  total_profit: float = 0.0, hedges: int = 0,
                  balances: list[dict] | None = None,
                  account_delta: float | None = None) -> str:
    """Build a plain text message; values are escaped for safe display."""
    edge = _money(fill_edge)
    status = "完成" if ok else "异常"
    lines = [
        f"📣 交易通知 · {symbol}",
        f"状态：{status} · 方向：{direction}",
        f"买入：{buy_venue} {buy_status}，成交 {buy_fill:.8g}",
        f"卖出：{sell_venue} {sell_status}，成交 {sell_fill:.8g}",
        f"计划数量：{qty:.8g}",
        f"本次成交额（两边合计）：${trade_volume:,.2f} · 本次盈利：{edge}",
        f"累计成交额（两边合计）：${total_volume:,.2f} · 累计盈利：{_money(total_profit)}",
        f"对冲笔数：{hedges}",
    ]
    for balance in balances or []:
        lines.append(f"{balance.get('name', '账户')}余额：可用 {_balance(balance.get('free'))} · 权益 {_balance(balance.get('equity'))}")
    if account_delta is not None:
        lines.append(f"账户权益变化：{_money(account_delta)}")
    return "\n".join(lines)


def hedge_message(*, symbol: str, venue: str, side: str, qty: float,
                  status: str, fill: float, ok: bool, net: float,
                  trade_volume: float = 0.0, total_volume: float = 0.0,
                  total_profit: float = 0.0, hedges: int = 0,
                  balances: list[dict] | None = None,
                  account_delta: float | None = None) -> str:
    """Build a notification for a reduce-only net-delta hedge."""
    lines = [
        f"🛡️ 对冲成交 · {symbol}",
        f"状态：{'完成' if ok else '异常'} · 方向：{side}",
        f"交易所：{venue} {status}，成交 {fill:.8g}/{qty:.8g}",
        f"对冲前净敞口：{net:+.8g}",
        f"本次成交额（两边合计）：${trade_volume:,.2f} · 累计成交额（两边合计）：${total_volume:,.2f}",
        f"累计盈利：{_money(total_profit)} · 对冲笔数：{hedges}",
    ]
    for balance in balances or []:
        lines.append(f"{balance.get('name', '账户')}余额：可用 {_balance(balance.get('free'))} · 权益 {_balance(balance.get('equity'))}")
    if account_delta is not None:
        lines.append(f"账户权益变化：{_money(account_delta)}")
    return "\n".join(lines)


def test_message() -> str:
    return "✅ Telegram 测试消息\n连接成功；后续成交通知会发送到这里。"
