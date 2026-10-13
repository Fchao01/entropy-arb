# 配置主腿

主腿可以选择 `entropy`、`lighter`、`lighter-rh`、`tradexyz`、`arcus`。
默认主腿为 RH（`lighter-rh`）；现有明确写了 `entropy:` 的旧 YAML 仍保持兼容。
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

Arcus 在主腿和对冲腿都优先使用同一整组 `ARCUS_*`。只有这组完全未设置时，
才使用兼容的 `PRIMARY_ARCUS_ACCOUNT_ADDRESS`、`PRIMARY_ARCUS_ACCOUNT_INDEX`、
`PRIMARY_ARCUS_API_SIGNING_KEY` 整组；不会把另一组的签名密钥拼到所选账户。
只填了部分字段会报缺少凭据，而不会换账户。Arcus 实盘启动前通过只读
`GET /v1/apiKeys` 检查密钥、钱包地址、子账户及有效期，日志记录凭据组、
子账户和账户/公钥指纹，不记录私钥或完整钱包地址。

只有一条 Lighter/RH 腿时，无论它是主腿还是对冲腿，都使用同一套凭据：
配置了任意 `PRIMARY_LIGHTER_*` 字段时选用这一整组，否则使用 `LIGHTER_*`。
选中的账户编号、API Key 编号和私钥必须全部填写；不会混用两组字段。
因此 RH + Arcus 交换两条腿后，RH 的账户及 API Key 不会随角色改变。
启动日志和校验失败信息会显示实际读取的变量组及账户、API Key 编号。
两条腿分别为 Lighter 主网和 RH 时，
主腿必须使用 `PRIMARY_LIGHTER_*`，对冲腿使用 `LIGHTER_*`，不能复用错误网络
的密钥。trade.xyz 默认沿用 `HL_*_XYZ`（未设置时沿用 `HL_*`），同账户的两条
HL 腿共用 nonce 分配器和去重后的账户权益统计。

## 阈值与日志

实盘启动时先核对两边旧仓位。如果净数量超过 `execution.net_tolerance_base`，
进程拒绝启动，不下新单也不自动平掉已有单边仓位。先核对原账户和交易所持仓；
已匹配的多空旧仓可以恢复运行。这个保护适用于挂单和吃单模式。

吃单模式下，主腿是价差基准，两个场所并行发单。挂单模式下，主腿先挂单，对冲腿按实际新增成交量吃单。

```text
premium_bps = (主腿价格 / 对冲腿价格 - 1) × 10000
```

`upper_bps` 控制卖主腿、买对冲腿；`lower_bps` 控制买主腿、卖对冲腿。
手续费由引擎计入，无须再手动叠加到带宽。更换主腿、对冲腿或交换两腿后，需要重新采集并分析；
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

## 主腿吃单 / 挂单

网页任务配置提供「主腿成交方式」「挂单等待时间」和「主腿挂单手续费」。
也可以在任务实际使用的 YAML 中设置：

```yaml
primary:
  venue: arcus
  taker_fee_bps: 2.25  # 示例，按实际账户费率填写
  maker_fee_bps: 0.0   # 示例，必须核对实际挂单费率
execution:
  primary_order_type: maker  # taker / maker；默认 taker
  maker_timeout_sec: 5.0     # 大于 0 且不超过 60 秒
```

此片段只展示新增字段，其余阈值、仓位、对冲费率等保留原配置。
RH / Lighter、Arcus、Entropy / trade.xyz 均支持主腿 Post-only。
买入挂主腿买一，卖出挂主腿卖一；报价固定，撤单后下一次信号重新取盘口。
Post-only 因盘口变化而被拒绝时不会转为吃单。对冲腿始终吃单。

最长等待时间从提交主腿订单时开始计算。超时、价差失效、行情过期、人工暂停
或正常停止都会撤单，并继续核对最终累计成交量。每次新增成交只对冲一次；
不足交易所最小数量或金额的部分先累计，到达最低要求再对冲。最终仍有残余时
尝试减少敞口；超过 `net_tolerance_base` 的残余无法消除时停止继续开单。
撤单 ACK 不是最终撤单状态，撤单途中发生的成交也会对冲。订单或对冲结果
无法确认时进入 `HALTED`，核对交易所未成交订单与持仓后才能重启。

`primary.maker_fee_bps` 未填写时沿用主腿吃单费，避免默认假定挂单免费。
挂单信号、成交费用、网页每日阈值分析和 `tools/analyze.py --config ...`
使用主腿挂单费 + 对冲腿吃单费。分钟数据仍采集真实 bid/ask 和吃单可成交价差；
历史数据没有队列位置、成交概率或撤单延迟，阈值建议不能保证挂单成交或收益。
切换模式后需按实际成交情况校准阈值。

引擎日志使用 `[MAKER]` / `[MAKER-HEDGE]` 记录报价、累计成交和撤单状态，
撤单日志包含 `reason`、实际已等待 `elapsed` 和配置 `timeout`。5 秒是最长等待，
订单状态无法确认、报价失效、暂停等条件仍会提前撤单。Arcus 已收到并保持连接的
订单推送直接用于状态确认；缺少订单推送或订阅失效时再查询 HTTP，查询失败不会假定零成交。
网页监控展示成交方式及当前挂单的已成交 / 已对冲数量。当前挂单期间的会话
持仓、成交额和费用在该挂单结算后统一计入。

正常停止会保留订单订阅，等待撤单与最后成交确认。强杀进程、主机故障或网络
中断可能留下交易所挂单；此版本没有服务器端断线自动撤单。
Arcus 不会因 WebSocket 断开撤单，且订单到期时间必须至少约一个月后；恢复前
必须检查未成交订单。协议参见 [Arcus WebSocket 文档](https://docs.arcus.xyz/api-reference/websocket)、
[Lighter 官方 SDK](https://github.com/elliottech/lighter-python) 和
[Hyperliquid 订单状态](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)。
