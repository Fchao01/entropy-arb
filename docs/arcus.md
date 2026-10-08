# Arcus 永续合约接入

启动参数 `--hedge arcus` 将对冲腿切换到 Arcus。引擎仍然只交易两条腿：
Entropy / Hyperliquid + Arcus，不会同时在多个对冲交易所下单。

实现依据 Arcus 官方文档：

- [认证与 Ed25519 签名](https://docs.arcus.xyz/api-reference/authentication)
- [WebSocket 交易](https://docs.arcus.xyz/guides/websocket-trading)
- [市场定义及价格、数量网格](https://docs.arcus.xyz/api-reference/public/get-markets)
- [全量 L2 盘口](https://docs.arcus.xyz/api-reference/market-data/l2orderbook)
- [下单与异步确认](https://docs.arcus.xyz/api-reference/exchange/place-order)
- [订单推送](https://docs.arcus.xyz/api-reference/account/orders)
- [订单状态查询](https://docs.arcus.xyz/api-reference/public/get-order-status)
- [订单历史查询](https://docs.arcus.xyz/api-reference/public/get-order-history)
- [持仓查询](https://docs.arcus.xyz/api-reference/public/get-positions)
- [延迟说明](https://docs.arcus.xyz/guides/latency)

## 配置

复制 `config.example.yaml` 到 `configs/arcus.yaml`，保留阈值、规模和风控配置，
修改以下字段：

```yaml
entropy:
  dex: io
  symbol: SNDK
  # 保留其他原有参数
hedge:
  symbol: SNDK-USD
  taker_fee_bps: 你的账户已确认费率
  # 保留其他原有参数
arcus:
  network: mainnet
```

上面的费率占位文字必须替换为非负数字，才能通过配置校验。示例文件中的
`0.0` 适用于原来的零费交易所，不代表 Arcus 免费；不能用 API 文档的静态
示例数据作为账户实际费率。1 bps = 0.01%。分析 Arcus 数据时，也要向
`tools/analyze.py --symbol SNDK --fees-bps 数字` 传入两边吃单费之和。

市场名优先精确匹配 Arcus 的 `marketDisplayName`。裸名 `SNDK` 也会尝试
匹配 `SNDK-USD`，但不会自动把 ANTH 翻译成 ANTHROPIC。两条腿的名称可以
不同，需要你确认它们具有相同标的、数量单位和经济敞口。未上市或离线市场
会在启动时明确报错。

先只采集行情，不需要交易密钥，也不会提交订单：

```bash
mkdir -p configs
cp config.example.yaml configs/arcus.yaml
# 修改 configs/arcus.yaml 中的 hedge.symbol 和已确认手续费
python3 main.py --record-only --symbol SNDK --hedge arcus \
  --config configs/arcus.yaml --no-dashboard
```

`--symbol SNDK` 命名交易组和默认日志目录 `logs/SNDK/`；更换交易所但沿用同一个
`--symbol` 时，这个目录也相同。并行运行不同对冲交易所时，应在各自 YAML 中
给 `recorder.csv`、`logging.trades_csv`、`logging.file` 设置独立路径。

`arcus.network: testnet` 只切换 Arcus 到测试网；Entropy / Hyperliquid 第一条腿
仍连主网。这个设置不构成整套套利策略的模拟盘，不能据此认为双方都是测试资金。

## 实盘凭据

在 Arcus 网站的 API Keys 页面生成并授权密钥，在 `.env` 填写：

```dotenv
ARCUS_ACCOUNT_ADDRESS=0x你的主钱包地址
ARCUS_ACCOUNT_INDEX=0
ARCUS_API_SIGNING_KEY=你的64位十六进制API签名种子
```

签名种子是 32 字节 Ed25519 API Signing Key，**不是 EVM 钱包私钥**。
公钥由种子推导，无须另填。网络、主钱包地址、子账户编号和已授权密钥必须
对应；子账户编号范围为 0–9。Entropy 第一条腿仍需要原来的 HL 凭据。
账户也需要 Arcus 的访问权限及足够保证金。不要将密钥写入 YAML 或提交到 Git。

安装 `requirements-live.txt` 后，去掉 `--record-only` 就会运行真实交易。
当前接入没有做真实资金下单验证；先验证授权账户的订单推送、持仓和权益读取。

## 接口与订单行为

| 功能 | 使用的官方接口 |
|---|---|
| 主网 REST / WS | `https://api.arcus.xyz` / `wss://api.arcus.xyz/v1/ws` |
| 测试网 REST / WS | `https://api.testnet.arcus.xyz` / `wss://api.testnet.arcus.xyz/v1/ws` |
| 市场精度、上下限 | `GET /v1/markets` |
| 行情 | WS `l2Orderbook`，订阅 100 档，每个快照替换整个盘口 |
| 下单 | 同一个 WS 上的 `post` / `placeOrder`，LIMIT + IOC |
| 最终成交 | WS `orders` 或 `GET /v1/order/{orderId}` |
| 未收到订单 ID | `GET /v1/orders` 按时间和市场查询，匹配唯一 `clientId` |
| 持仓 / 权益 | `GET /v1/positions` / `GET /v1/account` |

单笔签名使用官方 ordersign 类型化规范 JSON，按键排序，价格和数量转换为
整数 tick / quantum；不能直接签原始请求体。时间戳为纳秒，`goodTilTime`
在请求体中为微秒，进入签名时转为纳秒。IOC 也按要求提供未来至少一个月的
有效期；本实现设为 40 天，IOC 本身不会留下挂单。

价格按 `tickTiers` 对齐，签名换算仍使用基础 `tickSize`。数量使用 `stepSize`；
当前共用引擎只支持不大于 1 的十进制幂步长，其他步长会拒绝启动。下单前校验
最小数量、最小名义和最大数量，并让两条腿共同遵守 Arcus 的最大单笔数量。
reduce-only 豁免最小名义，仍遵守数量上限。

`202/ACK` 或带有成交信息的 `200` 响应都不能单独作为最终成交依据。
部分 IOC 成交按实际累计数量和均价处理；随后被拒绝的剩余部分不会抹掉已成交
部分。发送超时、断线或未知响应不会触发重发。未找到最终订单时保持暂停，
后续对账必须先确认该订单终态，再成功读取持仓，才允许新下单。

断线和 `degraded` 会使盘口或订单通道不可用；重连后重新订阅。心跳不能刷新
盘口年龄，旧序号快照不能覆盖更新盘口。引擎原有盘口时间差、滑点预留、动态
限价保护和按品种记录日志的规则继续适用。
收到 WS `429` 时，暂停时间同时遵守引擎配置和官方 `error.retryAfterMs`，
不会因为重连就提前恢复下单。

## 延迟及验证范围

官方延迟说明描述了吃单路径的 taker speed bump，文档所述默认配置约为 50 ms，
实际由运营配置决定。即使用 WebSocket 和靠近服务器的机器，也不能把该等待
消除。盘口还会随网络和快照频率产生延迟。需要重新采集 Entropy / Arcus 的
价差，实测延迟与成交滑点，不能直接照搬 RH 阈值或假定收益有保证。

已验证：公开市场列表和主网 WS 盘口；本地规范签名、网格、异步订单确认、
断线及未知订单处理，以及两个场所的行情采集。测试使用合成密钥和本地模拟
服务端，不代表 Arcus 生产服务已接受签名。没有授权账户时，订单订阅可能返回
`address not on access whitelist`；此时停止交易就绪并重试连接，不能绕过授权。
实盘签名接受、账户订阅数据结构和真实成交仍需要在授权账户上完成验证。
