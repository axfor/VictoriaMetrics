# API Key 用量统计 · model-router 边车接入

SDK：`github.com/axfor/client_golang v1.24.1024`

照下面做即可。指标定义、`Inc()`、`Observe()` 都不用改，只改注册和端点。采集侧见 [`vm-side.md`](vm-side.md)，上线顺序见第 4 步。

---

## 1. go.mod

```
replace github.com/prometheus/client_golang => github.com/axfor/client_golang v1.24.1024
```

## 2. 代码

整段照抄，只把 `frameworkMetrics`、`aistatistics` 换成你们实际的包名：

```go
import (
    "log"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/prometheus/client_golang/prometheus/promhttp/delta"
    _ "github.com/prometheus/client_golang/prometheus/promhttp/zstd" // 必须有，否则不压缩
)

// 1) 必须是进程里第一句和指标有关的代码，在任何指标创建之前
trk := prometheus.EnableChangeTracking()

// 2) 两个注册表：框架指标一个，per-API-key 指标一个
frameworkReg := prometheus.NewRegistry()
usageReg := prometheus.NewRegistry()
frameworkMetrics.Register(frameworkReg)
aistatistics.RegisterMetrics(usageReg) // 原来注册到大注册表，改成 usageReg

// 3) 启动自检：per-Key 指标混进框架注册表就直接退出
if mfs, err := frameworkReg.Gather(); err != nil || len(mfs) > 50 {
    log.Fatalf("frameworkReg 自检失败: %d 个 family, err=%v", len(mfs), err)
}

// 4) 三个端点
zstdFirst := promhttp.HandlerOpts{
    OfferedCompressions: []promhttp.Compression{promhttp.Zstd, promhttp.Gzip, promhttp.Identity},
}
opts := delta.Increments()
opts.TypeLabel = "_metric_type"
opts.Scope = usageReg // 必须有：只管 per-API-key 指标，框架指标不受影响
usage := delta.NewTracked(trk, opts)

mux.Handle("/metrics", promhttp.HandlerFor(frameworkReg, zstdFirst))            // 框架指标
mux.Handle("/metrics/usage", usage.Handler())                                   // 用量增量，vmagent 抓它
mux.Handle("/metrics/cumulative", promhttp.HandlerFor(usageReg, zstdFirst))     // 只在回退时用
```

**不要做的：**

- 不要把 per-Key 指标注册到 `frameworkReg`，也不要再留一个“全都注册进去”的大注册表给 `/metrics` 用。
- 不要手写 `delta.Options`，也不要改 `opts.IdleScrapes`，就用 `delta.Increments()` 的默认值。代码里如果有 `opts.IdleScrapes = 90`，删掉。
- 不要漏掉 `opts.Scope = usageReg` 这一行。
- 写 per-API-key 指标（注册到 `usageReg` 的那些）时，每次都用 `WithLabelValues(...)`（或 `With(...)`）现取再 `Inc()` / `Add()` / `Observe()`，不要把取到的指标对象存进 map 或结构体里复用：Key 半小时没流量会被自动清掉，存下来的旧对象再写就丢了。框架指标不受这条限制。
- 不要让 vmagent 以外的任何程序抓 `/metrics/usage`：它每被访问一次就把增量交出去一次，别人拿走的那份 vmagent 就收不到了。要手工看内容，必须带上下面这个请求头，这样看不会拿走任何东西：

  ```sh
  curl -s --compressed -H 'X-Prometheus-Delta-Ack: peek' http://<pod>:9090/metrics/usage | head
  ```
- 平时不要访问 `/metrics/cumulative`：每访问一次都是一次全量收集，要几十秒。

## 2.1 数据一致性（不用改代码）

所有计数器（`Counter`、`CounterVec`，token、费用、请求数都是）都一样，不管是不是用量指标，SDK 保证：

- 一次请求里先写的指标，vmagent 一定不会比后写的更晚拿到。例如先写 token、再写费用，不会出现“费用到了、token 没到”。
- vmagent 来取的时候，写指标的请求不会被卡住、不用等。
- 不丢、不重复：一段时间的合计和边车实际写入的完全一致。

vmagent 来取的那一瞬间，如果恰好有请求写了 token 还没写费用，这个请求的费用会记到下一分钟（合计不变）。要让这种情况几乎不发生，只需要：

- **同一个请求的用量写在一处、连着写完**：请求结束后一次性 `Add` token、费用等，中间不要夹网络调用、查库等慢操作。

不需要任何新的 API 或配置。

**可选（一般不需要）**：如果某个业务要求“一个请求的指标绝对不拆到两分钟”，把记录这个请求指标的那一处包一层，对任何业务计数器都适用：

```go
prometheus.Atomically(func() {
    tokensTotal.WithLabelValues(key, model).Add(tokens)
    costTotal.WithLabelValues(key, model).Add(cost)
})
```

包在里面的只能是写指标的几行，不要放网络调用、查库、`sleep`，也不要在里面再调 `prometheus.Atomically`。

### 已经在用 v1.24.1023 的

`go.mod` 里的版本改成 v1.24.1024 重新发版即可，代码不用动。和 vmagent 换版本的先后无关，任一边先换都不会丢用量。

## 3. 内存

边车容器的资源（按 3 万 Key、单 pod 约 230 万实例实测：稳态 2.6GiB、峰值 2.9GiB）：

```yaml
resources:
  requests:
    memory: 3Gi
  limits:
    memory: 4Gi
```

规模不同时，`limits.memory` 按“单 pod 实例数 × 1.7 KiB”算。单 pod 当前实例数看 `usage.Tracked()`。

## 4. 上线顺序

1. 先做完 [`vm-side.md`](vm-side.md) 的第 1、2 步（vm-select / vm-storage、新 vmagent，vmagent 要是 `v1.126.1030-cluster`）。
2. 紧接着发这个版本的边车。两步之间 vmagent 从旧边车抓不到用量，这段时间的用量不统计，所以两步要连着做。
3. 发完后检查：

```sh
# 在任一边车 pod 上
curl -s -o /dev/null -w "%{time_total}s\n" http://<pod>:9090/metrics    # 毫秒级
curl -s --compressed http://<pod>:9090/metrics | grep -c '^acg_'        # 0
curl -s -o /dev/null -D - -H 'X-Prometheus-Delta-Ack: peek' http://<pod>:9090/metrics/usage | grep -i x-prometheus-delta-round
#   X-Prometheus-Delta-Round: ...          有这一行，说明边车是 v1.24.1023 或更新
```

再跑一遍 [`verify-deploy.sh`](verify-deploy.sh)（直接 `./verify-deploy.sh`，退出码 0 为通过）。它会读出每个 pod 实际链进去的 client_golang 版本，应为 v1.24.1024。

## 5. 回退

边车不用重新发版。让采集侧按 [`vm-side.md`](vm-side.md) 的“回退”把抓取路径改回 `/metrics/cumulative` 即可。

---

## 出问题时

| 现象 | 怎么办 |
|---|---|
| `/metrics` 要几秒到几十秒、`up=0` | per-Key 指标还在 `/metrics` 用的注册表里。按第 2 步拆开，自检那几行不要删 |
| 边车启动就 panic | 手写了 `delta.Options`。改回 `delta.Increments()` |
| 指标完全没被跟踪 | `EnableChangeTracking()` 调晚了，挪到任何指标创建之前 |
| VM 里的数字比预期少一截 | 有 vmagent 以外的程序在抓 `/metrics/usage`，找出来停掉；手工看要带 `X-Prometheus-Delta-Ack: peek`，见第 2 步 |
| 某个 Key 停了一阵后再来的用量没进 VM | 代码里把 per-API-key 指标对象存起来复用了，改成每次 `WithLabelValues(...)` 现取，见第 2 步 |
| 框架指标（`/metrics` 上的）停了半小时后消失、或从 0 重新开始；`/metrics/usage` 里出现框架指标 | 漏了 `opts.Scope = usageReg`，按第 2 步补上 |
| 边车内存比第 3 步高很多 | 代码里改了 `IdleScrapes`，删掉那一行 |
| VM 里突然没有新数据，但 `up` 是 1 | 采集侧 `promscrape.maxScrapeSize` 没按 vm-side 第 2 步设成 500MiB。设上后 reload vmagent |

原理和实测数据见 [`gather-cost.md`](gather-cost.md)，参考实现是 client_golang 仓库的 `examples/delta`（`go run ./examples/delta`）。

