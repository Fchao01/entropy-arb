# entropy-arb

**[English documentation / 英文文档 → README.md](README.md)**

开源双交易所永续合约套利机器人。其中一条腿永远是 **light-rh**（Lighter Robinhood 链）；另一条腿（对冲腿）三选一：

| `--hedge` | 交易所 | 计价货币 | 吃单费 | 协议 |
|---|---|---|---|---|
| `lighter` | Lighter 主网 | USDC | 0 bps | zkLighter ws（增量订单簿，异步结算） |
| `entropy` | Hyperliquid Entropy/io dex | USDC | 配置值 | HL l2Book |
| `tradexyz` | Hyperliquid trade.xyz dex | USDC | ~1 bps | HL l2Book，IOC 同步结算 |

> **推荐链接** —— 通过以下链接注册即可支持本项目：
> - Entropy — Tier 4 推荐，100% 返佣：<https://entropy.io/?r=yourquantguy>
> - Lighter Robinhood 链：<https://robinhoodchain.lighter.xyz/?referral=QUANT>
> - trade.xyz（Hyperliquid）：<https://app.hyperliquid.xyz/join/QUANTGUY>

当同一品种在一边贵、另一边便宜时，机器人同时在贵的一边卖出、便宜的一边买入
（均为吃单），持有 delta 中性仓位，等溢价回归后反向平仓。所有交易决策使用的
价格都来自**将要实际成交的那个交易所的真实订单簿**——Hyperliquid 的盘口来自
官方 websocket（`wss://api.hyperliquid.xyz/ws`），Lighter 的盘口来自 Lighter
官方 websocket。

机器人运行期间（即使没有密钥、没有开策略）会自动把两边盘口记录成**分钟级
CSV 数据**，配套的分析工具可以直接把这些数据变成策略所需的三个核心参数。

## 信号逻辑

整个信号就是 `config.yaml` 里三个数字，由你根据采集的数据自己设定：

```
premium_bps =（light-rh 价格 / 对冲腿价格 − 1）× 10 000

                          ┌──────────────  卖出 light-rh + 买入对冲腿
midline + upper  ───────────────────────────────────────────────────
                                       ▲
midline          ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─   溢价的长期中枢
                                       ▼
midline − lower  ───────────────────────────────────────────────────
                          └──────────────  买入 light-rh + 卖出对冲腿
```

- `midline_bps` —— 溢价的常态水平。跨所溢价几乎从不以零为中心（预言机不同、
  计价货币不同、新上市溢价等），零中心的带只会朝一个方向开仓、打满仓位上限、
  永远无法平仓。请实际测量溢价所在的位置，然后填入。
- `upper_bps` / `lower_bps` —— 中枢上下两侧的入场带宽。

两个方向的门槛都作用于**可实际成交的价格**（light-rh 买一 对 对冲腿卖一，
反之亦然），并且是**扣除双边吃单手续费之后的净门槛**——引擎会在阈值之上
另行叠加手续费。因此一次完整往返扣费后**净赚 ≥ upper + lower bps**，这是
结构上保证的。

有一点必须理解：当 `midline_bps: 5` 时，买入 Entropy 的门槛是
`lower − midline`，可能为**负数**。这是有意为之——如果 Entropy 长期贵 5 bps，
那么在溢价为 0 时买入它，相对其自身均衡水平就是便宜了 5 bps，这笔交易正是
此前在 `midline + upper` 处卖出的获利平仓。这同时意味着**中枢填错就是亏钱
策略**：若真实溢价中枢是 0 而你填了 5，机器人会整天以公允价买入 Entropy。
先测量、再交易——数据采集器和分析工具就是为此而生。

## 快速开始

```bash
git clone https://github.com/your-quantguy/entropy-arb.git && cd entropy-arb
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # 数据采集只需要这些

cp config.example.yaml config.yaml       # 策略配置（阈值、规模、风控）
cp .env.example .env                     # 密钥——交易必填
```

交易哪个市场**不在**配置文件中——每次启动时用命令行参数显式指定：
`--symbol`（两个交易所共同交易的品种）和 `--hedge`（三选一：
`entropy`、`lighter`、`tradexyz`；light-rh 永远是主腿）。

本机器人**没有模拟盘**——要么采集数据（`--record-only`），要么实盘交易。
请用采集的数据和最小的仓位上限来验证策略，而不是模拟成交。

**第一步：先采集数据**（不需要任何密钥）：

```bash
python3 main.py --record-only --symbol SNDK --hedge entropy
```

至少运行几个小时（最好一整天——溢价存在日内规律），数据写入
`logs/minutes.csv`。

**第二步：分析数据、设定阈值：**

```bash
python3 tools/analyze.py
```

它会输出溢价分布、各档带宽的历史触发频率，以及可直接粘贴进
`config.yaml` 的 `thresholds:` 配置块。

**第三步：实盘** —— 填写 `.env`，安装签名 SDK，仓位上限从刚好满足
交易所最小名义的水平开始：

```bash
pip install -r requirements-live.txt
python3 main.py --symbol SNDK --hedge entropy
```

不带 `--record-only` 运行时，只要两边行情就绪且溢价越过带宽，就会立即
发送真实订单。

**仪表盘。** 在终端运行时会显示实时 Rich 仪表盘：两边盘口（含数据龄/点差）、
持仓与上限、账户权益与本次会话盈亏、两个方向的可成交溢价对比完整门槛
（已含手续费与库存加价，● 表示已武装）、数据采集进度、最近成交，以及日志
尾部（完整日志写入 `logging.file`，默认 `logs/engine.log`）。`--record-only`
模式同样可用。加 `--cn` 参数可使仪表盘全部以中文显示。`--no-dashboard`
可切换为纯日志输出（nohup/systemd 等非终端环境会自动退回纯日志），也可
设置 `logging.dashboard: false`。

## 数据采集与分析

采集器在所有模式下自动运行（`recorder.enabled: true`）：每秒采样一次两边
的真实盘口，每分钟写一行：

| 列 | 含义 |
|---|---|
| `minute_ts`, `time_utc` | 分钟起点（epoch 秒 / ISO UTC） |
| `primary_bid/ask`, `hedge_bid/ask` | 该分钟最后一次有效盘口 |
| `premium_open/high/low/close/mean/std_bps` | light-rh 相对对冲腿的中间价溢价 |
| `sell_edge_mean/max_bps` | 卖出 light-rh 方向的可成交溢价 |
| `buy_edge_mean/max_bps` | 买入 light-rh 方向的可成交溢价 |
| `samples` | 该分钟约 60 秒中两边盘口同时有效的秒数 |

采集的 edge 为费前口径；分析工具在统计触发频率前会先扣除 `--fees-bps`
（请传入**两边吃单费之和**——零费交易所默认 0.0，对冲腿为 `tradexyz` 时
约为 1.0），因此其表格与建议值可直接填入配置。`--hours 24`
可只分析最近数据；溢价中枢会漂移，请定期重新分析并更新 `config.yaml`。

## 配置说明

策略在 `config.yaml`（严格校验——未知键名直接报错），密钥在 `.env`。
交易市场由命令行指定（`--symbol`、`--hedge`）。完整的双语注释参考：
[config.example.yaml](config.example.yaml)。核心项：

| 键 | 含义 | 默认值 |
|---|---|---|
| `thresholds.midline_bps` | 溢价中枢（必须实测！） | — |
| `thresholds.upper_bps` / `lower_bps` | 入场带宽（> 0） | — |
| `primary.*` | 固定 light-rh 主腿限制 | — |
| `hedge.dex` | 选择 Entropy/tradexyz 时的 dex 名 | `io` |
| `*.taker_fee_bps` | 各所吃单费 | 0.0（tradexyz 对冲腿：1.0） |
| `*.max_position_usd` | 各所持仓上限 | 1000 |
| `*.max_orders_per_min` | 各所每分钟下单预算（滑动 60 秒） | 120；Lighter 对冲腿 30 |
| `sizing.take_fraction` | 吃掉可套利深度的比例 | 0.5 |
| `sizing.max_order_notional_usd` | 单笔名义上限 | 500 |
| `inventory.scale_bps` / `floor_frac` | 库存阶梯（仓位超过上限的 `floor_frac` 后额外加价） | 10 / 0.5 |
| `execution.premium_persist_sec` | 信号需持续多久才触发 | 0.3 |
| `execution.*` | 滑点保护、超时、对账周期等 | 见配置文件 |
| `recorder.*` | 分钟数据采集器 | 开启，`logs/minutes.csv` |
| `logging.dashboard` / `logging.file` | 终端仪表盘；开启时日志写入文件 | 开启，`logs/engine.log` |

## 密钥配置（`.env`，仅实盘需要）

- **Entropy / tradexyz（Hyperliquid 对冲腿）** —— 在
  <https://app.hyperliquid.xyz/API> 创建 API（agent）钱包。`HL_PRIVATE_KEY`
  填 **agent 钱包私钥**，`HL_ACCOUNT_ADDRESS` 填主账户地址。当
  `--hedge tradexyz` 时两条腿默认共用该账户（内部自动共享 nonce 序列）；
  如需分开，设置 `HL_PRIVATE_KEY_XYZ` / `HL_ACCOUNT_ADDRESS_XYZ`。注意给
  所交易的各 dex 分别充入保证金。
- **Aster 对冲腿** —— 使用 Aster Pro API V3：填写主钱包
  `ASTER_USER_ADDRESS`、Agent 钱包地址 `ASTER_SIGNER_ADDRESS` 和 Agent 私钥
  `ASTER_SIGNER_PRIVATE_KEY`。V3 使用 EIP-712 签名，不再使用旧的
  `ASTER_API_KEY/ASTER_API_SECRET`。Aster 合约必须是 USD1 保证金；程序会在启动时
  检查 `marginAsset/quoteAsset`。按 Aster V3 规则，关联主钱包还必须完成过一次
  deposit，否则账户和交易接口会返回 `-5050`。Aster 默认使用 V3 IOC 限价吃单，和另一条腿同时发出。
- **light-rh 主腿** —— 优先使用 `LIGHTER_RH_ACCOUNT_INDEX`、
  `LIGHTER_RH_API_KEY_INDEX`、`LIGHTER_RH_API_PRIVATE_KEY`（也兼容旧的
  `LIGHTER_*` 变量）；Lighter 主网对冲腿使用 `LIGHTER_*`，两套部署账户和密钥
  必须分开（参见
  [lighter-python](https://github.com/elliottech/lighter-python)）。

## 执行机制

- 两条腿**同时发出吃单**：Lighter 用带均价保护的市价单，在鉴权 websocket
  上异步确认成交；Hyperliquid 用 IOC 限价单同步结算（结果未知时轮询
  orderStatus 兜底）。
- **持续性闸门**（`premium_persist_sec`）：信号先"武装"，持续存在才触发，
  过滤单 tick 的假信号。
- **库存阶梯**：仓位超过上限的 `floor_frac` 后，同方向加仓需要线性递增的
  额外溢价，满仓时最高加 `scale_bps`。
- **净敞口对冲**：两腿成交不对等时立即用 reduce-only 单（带滑点保护）
  削减敞口，并每 `reconcile_sec` 与链上仓位对账。
- **故障隔离**：被限频的交易所短暂暂停；交易所不可达（如例行维护）时暂停
  交易并每 `venue_probe_sec` 探测直至恢复；连续 `max_consecutive_errors`
  次执行异常则整体停机。
- **仅实盘**：没有模拟成交模式。`--record-only` 是唯一无风险的运行方式，
  其余都是真金白银。

## 目录结构

```
main.py                  入口（--record-only，默认即实盘）
entropy_arb/config.py    YAML + .env 配置契约与校验
entropy_arb/book.py      订单簿 + 含手续费的套利规模计算
entropy_arb/feeds.py     官方 HL ws + zkLighter ws 行情
entropy_arb/venue_hl.py  Hyperliquid dex 适配器（Entropy、tradexyz）
entropy_arb/venue_lighter.py  zkLighter 适配器（主网、Robinhood 链）
entropy_arb/engine.py    双交易所策略主循环
entropy_arb/dashboard.py Rich 终端仪表盘
entropy_arb/recorder.py  分钟级盘口数据采集
tools/analyze.py         minutes.csv -> 阈值建议
tests/                   python3 -m pytest tests/
```

## 已知风险

- **中枢填错就是亏钱策略。** 溢价中枢会漂移，请定期重新测量并保持
  `config.yaml` 与市场同步。
- **USDG 基差**（`lighter-rh`）：对冲腿以 USDG 计价，持续溢价中有
  一部分是稳定币本身的基差；midline 吸收其水平，但 USDG 的*变动*是真实盈亏。
- **资金费**：两个交易所、两套独立的资金费率，持仓成本未建模——仓位上限
  请设小一些。
- **薄盘口**：Entropy 深度可能很小；`take_fraction` 与名义上限控制单笔规模，
  但部分成交后对冲腿的滑点是真实存在的。
- **交易时段**：股票类永续（如 SNDK）盘后各所预言机行为不同，建议加宽带宽
  或避开盘后。
- **单腿风险**：一条腿成交后另一条可能失败。机器人会自动对冲并对账，但
  仍需人工关注。

风险自负。本软件直接操作真实资金，本文档不构成任何投资建议。请从最小的
仓位上限开始。

## 开源协议

[MIT](LICENSE)
