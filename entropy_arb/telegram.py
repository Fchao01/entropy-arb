"""Small, best-effort Telegram Bot API client for trade notifications."""
from __future__ import annotations

import asyncio
import logging
import os

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

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def close(self) -> None:
        if self._owned_session and self.session is not None:
            await self.session.close()
            self.session = None

    async def send(self, text: str) -> bool:
        if not self.enabled:
            return False
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
                    log.warning("Telegram notification failed (%s): %s", response.status, detail)
                    return False
            except Exception as error:
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                log.warning("Telegram notification failed: %s", error)
        return False


def trade_message(*, symbol: str, direction: str, buy_venue: str, sell_venue: str,
                  qty: float, buy_status: str, sell_status: str, buy_fill: float,
                  sell_fill: float, fill_edge: float | None, ok: bool) -> str:
    """Build a plain text message; values are escaped for safe display."""
    edge = "—" if fill_edge is None else f"${fill_edge:+,.4f}"
    status = "完成" if ok else "异常"
    return "\n".join([
        f"📣 交易通知 · {symbol}",
        f"状态：{status} · 方向：{direction}",
        f"买入：{buy_venue} {buy_status}，成交 {buy_fill:.8g}",
        f"卖出：{sell_venue} {sell_status}，成交 {sell_fill:.8g}",
        f"计划数量：{qty:.8g}",
        f"实际价差收益：{edge}",
    ])
