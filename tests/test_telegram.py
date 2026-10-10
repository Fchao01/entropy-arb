import asyncio

from entropy_arb import web as web_module
from entropy_arb.telegram import trade_message
from entropy_arb.web import TaskManager


def test_trade_message_contains_requested_summary_fields():
    message = trade_message(
        symbol="ETH", direction="SELL", buy_venue="RH", sell_venue="Arcus",
        qty=1, buy_status="filled", sell_status="filled", buy_fill=1,
        sell_fill=1, fill_edge=0.12, ok=True, trade_volume=250,
        total_volume=1250, total_profit=3.45, hedges=4,
        balances=[{"name": "RH", "free": 100, "equity": 120},
                  {"name": "Arcus", "free": 80, "equity": 90}],
    )
    assert "累计成交额（两边合计）：$1,250.00" in message
    assert "累计盈利：$+3.4500" in message
    assert "对冲笔数：4" in message
    assert "RH余额：可用 $100.00 · 权益 $120.00" in message


def test_manager_telegram_test_uses_selected_profile(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    (root / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=test-token\nTELEGRAM_CHAT_ID=test-chat\n",
        encoding="utf-8",
    )
    sent = []

    class FakeNotifier:
        def __init__(self, *, token, chat_id):
            self.token, self.chat_id = token, chat_id

        @property
        def enabled(self):
            return bool(self.token and self.chat_id)

        async def send(self, text):
            sent.append((self.token, self.chat_id, text))
            return True

        async def close(self):
            return None

    monkeypatch.setattr(web_module, "TelegramNotifier", FakeNotifier)
    manager = TaskManager(root, tmp_path / "data")

    async def scenario():
        try:
            assert await manager.telegram_test() == {"ok": True}
        finally:
            await manager.close()

    asyncio.run(scenario())
    assert sent == [("test-token", "test-chat", "✅ Telegram 测试消息\n连接成功；后续成交通知会发送到这里。")]
