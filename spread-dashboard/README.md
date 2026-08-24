# 跨交易所价差监控

这个版本把**行情链路完全放在你的 Linux 服务器上**：浏览器只访问服务器自己的 `/api/data`，服务器后台连接 Binance Alpha / Binance RWA bStocks / Bybit / Gate / OKX / Coinbase。GitHub 只用于程序版本与 OTA 更新，不参与行情请求。

## 当前架构

- Binance Alpha：服务器定期同步 Alpha token list，并用服务器本地缓存作为监控币种清单。
- Bybit / Gate / OKX / Coinbase：服务器后台 WebSocket 实时接收行情并缓存。
- Binance bStocks：服务器定期同步 Binance RWA 股票 token 信息与价格。
- 浏览器：每秒最多请求一次服务器 `/api/data`，不会直接访问交易所或 GitHub。
- OTA：GitHub 仅用于程序更新；网页的「程序更新」按钮可触发服务器更新。

Bybit Spot ticker 官方推送频率为 50ms；Gate 的 book ticker 可到 10ms；OKX 与 Coinbase 都提供公开 WebSocket ticker，因此服务器侧 WebSocket 比浏览器轮询交易所 REST 更适合低延迟监控。具体实际延迟仍取决于服务器到交易所的网络质量。 

## Linux 安装

第一次部署完成后，后续程序更新不需要重新连接服务器。安装脚本会创建运行环境、systemd 服务和 OTA 更新机制。

```bash
curl -fsSL https://raw.githubusercontent.com/521xiaozhou-hash/-/main/install.sh | bash
```

然后访问 `http://服务器IP:8080`。

## 配置

```env
PORT=8080
REQUEST_TIMEOUT=6
ALPHA_REFRESH_SECONDS=60
BSTOCK_REFRESH_SECONDS=5
ALPHA_SYMBOLS=
BSTOCK_TICKERS=AAPL,TSLA,NVDA,MSTR
UPDATE_CHECK_SECONDS=15
```

`ALPHA_SYMBOLS` 留空表示监控 Binance Alpha token list 中服务器能够识别的全部 Alpha 币种。

## OTA 更新

网页右上角的「⚙ 程序更新」只负责**程序更新**，不是行情更新。服务器会从 GitHub 拉取新代码、安装新依赖并重启程序。行情数据不会从 GitHub 获取。

不要把 Binance/Gate/Bybit/OKX/Coinbase 私钥放进仓库；当前行情功能只使用公开市场数据接口。

## 价差

当前页面显示：

`Alpha 相对外部交易所最新价格价差 = (Alpha价格 / 外部价格 - 1) × 100%`

下一步如果用于实际套利，建议改成基于外部交易所 best bid / best ask 的可成交价差，并扣除手续费、滑点和提现/充值成本。
