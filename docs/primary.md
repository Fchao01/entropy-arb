# 配置主腿

主腿可以选择 `entropy`、`lighter`、`lighter-rh`、`tradexyz`、`arcus`。
默认仍为 Entropy，现有 `entropy:` YAML 和启动命令保持兼容。
对冲腿也支持这五个选项，通过 `--hedge` 指定。两边不能解析到同一个市场。

## RH 主腿 + Arcus 对冲

完整配置见 [configs/rh-arcus.yaml](../configs/rh-arcus.yaml)，其中：

```yaml
primary:
  venue: lighter-rh
  taker_fee_bps: 0.0
  max_position_usd: 1000
  max_orders_per_min: 30
hedge:
  taker_fee_bps: 2.25  # 用户提供的 Arcus 吃单费 0.0225%
  max_position_usd: 1000
  max_orders_per_min: 30
arcus:
  network: mainnet
```

这是一份采集起点配置，阈值和仓位规模是示例，实盘前应按 RH / Arcus 数据
重新设定并核对两边账户费率。先采集 ETH：

```bash
python3 main.py --record-only --symbol ETH --hedge arcus \
  --config configs/rh-arcus.yaml --no-dashboard
```

不需要 HL 密钥。实盘需要 `.env` 中的：

- RH：`LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX`、`LIGHTER_API_PRIVATE_KEY`。
- Arcus：`ARCUS_ACCOUNT_ADDRESS`、`ARCUS_ACCOUNT_INDEX`、`ARCUS_API_SIGNING_KEY`。

两边名称不同时，在同一文件分别加 `primary.symbol` 和 `hedge.symbol`。
例如 RH 使用 `ETH`，Arcus 使用 `ETH-USD`；不填则默认使用 `--symbol`，
Arcus 适配器会尝试将裸名 ETH 解析为 ETH-USD。

## 从旧 YAML 切换

将原来的 `entropy:` 段替换为 `primary:`，添加 `venue`，并填写新主腿的费率、
持仓上限等参数。非 Hyperliquid 主腿删除 `dex`。同时出现 `entropy:` 和
`primary:` 会报错，避免两个段的参数产生歧义。

`--primary` 可以覆盖 `primary.venue`，例如 `--primary lighter-rh`。
它只覆盖交易所选择，不会替你修改同一 YAML 中的费率、市场名或风险参数。
建议每个交易所组合各用一份配置。

对于需要独立主腿凭据的组合，可用以下环境变量覆盖默认凭据：

- HL：`PRIMARY_HL_PRIVATE_KEY`、`PRIMARY_HL_ACCOUNT_ADDRESS`。
- Lighter：`PRIMARY_LIGHTER_ACCOUNT_INDEX`、`PRIMARY_LIGHTER_API_KEY_INDEX`、
  `PRIMARY_LIGHTER_API_PRIVATE_KEY`。
- Arcus：`PRIMARY_ARCUS_ACCOUNT_ADDRESS`、`PRIMARY_ARCUS_ACCOUNT_INDEX`、
  `PRIMARY_ARCUS_API_SIGNING_KEY`。

只有一条 Lighter 腿时可以沿用 `LIGHTER_*`。两条腿分别为 Lighter 主网和 RH 时，
主腿必须使用 `PRIMARY_LIGHTER_*`，对冲腿使用 `LIGHTER_*`，不能复用错误网络
的密钥。trade.xyz 默认沿用 `HL_*_XYZ`（未设置时沿用 `HL_*`），同账户的两条
HL 腿共用 nonce 分配器和去重后的账户权益统计。

## 阈值与日志

主腿只是价差基准，不是固定先下单的一边。两个场所仍然并行发单。

```text
premium_bps = (主腿价格 / 对冲腿价格 - 1) × 10000
```

`upper_bps` 控制卖主腿、买对冲腿；`lower_bps` 控制买主腿、卖对冲腿。
平仓带宽继续使用 `close_upper_bps / close_lower_bps`。手续费由引擎计入，
无须再手动叠加到带宽。更换主腿、对冲腿或交换两腿后，需要重新采集并分析；
原组合的中枢和阈值不能直接照搬。

示例将日志隔离到 `logs/lighter-rh-arcus/ETH/`。可以使用路径占位符
`{primary}`、`{hedge}`、`{symbol}`。分析时：

```bash
python3 tools/analyze.py --csv logs/lighter-rh-arcus/ETH/minutes.csv --fees-bps 2.25
```

这里的 `2.25` 假设 RH 吃单费为 0；如账户费率不同，要填两边费率之和。
未指定自定义路径时仍保留旧默认 `logs/{symbol}/`，并行运行多个组合时应隔离路径。

日志和仪表盘显示实际场所名称。为了兼容已有分析器，分钟 CSV 中的
`entropy_bid/ask` 列名暂时保留，含义是当前主腿的 bid/ask；其溢价同样以当前
主腿为基准。旧 Entropy 组合保留 `sell_entropy / buy_entropy` 方向，新主腿使用
`sell_primary / buy_primary`。因此不要把不同组合采集到同一个 CSV 中。

这项改动仍是双腿套利，不会自动增加第三条腿。真实成交验证范围也没有因此扩大；
Arcus 实盘仍需授权账户、正确签名密钥及实际订单验证。
