"""Signer initialization uses the selected RH account without sending orders."""
import sys
from types import SimpleNamespace

import pytest

from entropy_arb.config import load_config
from entropy_arb.venue_lighter import LighterVenue


@pytest.mark.parametrize("primary,hedge,rh_leg", [
    ("lighter-rh", "arcus", "primary"),
    ("arcus", "lighter-rh", "hedge"),
])
def test_swapped_rh_signer_receives_the_same_credentials(tmp_path, monkeypatch,
                                                       primary, hedge, rh_leg):
    path = tmp_path / "config.yaml"
    path.write_text("""thresholds:
  midline_bps: 0
  upper_bps: 4
  lower_bps: 4
primary:
  taker_fee_bps: 2.25
hedge:
  taker_fee_bps: 2.25
""")
    cfg = load_config(str(path), symbol="QQQ", primary_venue=primary,
                      hedge_venue=hedge, credential_env={
                          "PRIMARY_LIGHTER_ACCOUNT_INDEX": "123",
                          "PRIMARY_LIGHTER_API_KEY_INDEX": "3",
                          "PRIMARY_LIGHTER_API_PRIVATE_KEY": "fixture-secret",
                          "LIGHTER_ACCOUNT_INDEX": "456",
                          "LIGHTER_API_KEY_INDEX": "4",
                          "LIGHTER_API_PRIVATE_KEY": "other-secret",
                      })
    calls = []

    def fake_signer(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(check_client=lambda: "no api key returned")

    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(SignerClient=fake_signer))
    venue = LighterVenue(getattr(cfg, rh_leg), session=None, settle_timeout_sec=5)
    with pytest.raises(RuntimeError) as result:
        venue.init_signer()
    assert calls == [{"url": "https://api.rh.lighter.xyz", "account_index": 123,
                      "api_private_keys": {3: "fixture-secret"}, "chain_id": 466324}]
    error = str(result.value)
    assert "account=123" in error and "api_key_index=3" in error
    assert "credentials=PRIMARY_LIGHTER_*" in error
    assert "fixture-secret" not in error and "other-secret" not in error
