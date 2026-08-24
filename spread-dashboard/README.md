# 跨交易所价差监控

一个可直接部署到 Linux 的 FastAPI + 单页 Web Dashboard，用于监控：

- Binance bStocks（由 `BSTOCK_SYMBOLS` 指定的 Binance 现货交易对）
- Binance Alpha 与 Bybit / Gate / OKX / Coinbase 的价格及百分比价差

> 注意：不同平台的 Alpha、代币化股票产品和交易对命名可能变化。程序不会硬编码不存在的市场；如果 Binance Alpha 公共接口返回结构变化，页面会显示当前可获得的数据或 `—`。

## Linux 一键安装

```bash
git clone https://github.com/521xiaozhou-hash/-\ncd -/spread-dashboard
bash install.sh
source .venv/bin/activate
python app.py
```

然后打开 `http://服务器IP:8080`。

## 配置

首次安装会生成 `.env`。常用配置：

```env
PORT=8080
REFRESH_SECONDS=10
REQUEST_TIMEOUT=8
ALPHA_SYMBOLS=
BSTOCK_SYMBOLS=AAPLUSDT,TSLAUSDT,NVDAUSDT,MSTRUSDT
```

`ALPHA_SYMBOLS` 留空时尝试自动发现 Binance Alpha 返回的币种；也可以手工指定。

## 价差定义

页面显示：

`Alpha 相对外部交易所价差 = (Alpha价格 / 外部交易所价格 - 1) × 100%`

因此正数表示 Alpha 价格高于该交易所，负数表示低于该交易所。

bStocks 部分显示 Binance 指定股票代币交易对的实时中间价；若需要严格比较真实美股现货价格，应额外接入股票基准数据源，不能把普通股票价格和链上/交易所代币价格直接混为一谈。

## 生产运行

建议用 systemd / Docker / nginx 管理进程。当前项目本身不需要 API Key，因为行情读取使用公开市场接口；不要把任何交易 API 私钥写进 `.env` 或 Git。
