# API Key 用量统计 · model-router 边车接入

SDK：`github.com/axfor/client_golang v1.24.1027`

指标定义、`Inc()`、`Observe()` 都不用改，只改注册和端点。采集侧见 [`vm-side.md`](vm-side.md)，上线顺序见第 4 步。

---

## 1. go.mod

```
replace github.com/prometheus/client_golang => github.com/axfor/client_golang v1.24.1027
```

已经在用 v1.24.1023 ~ v1.24.1026 的：只改这里的版本，重新发版，代码不用动。

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
opts.Scope = usageReg // 必须有
usage := delta.NewTracked(trk, opts)

mux.Handle("/metrics", promhttp.HandlerFor(frameworkReg, zstdFirst))            // 框架指标
mux.Handle("/metrics/usage", usage.Handler())                                   // 用量增量，vmagent 抓它
mux.Handle("/metrics/cumulative", promhttp.HandlerFor(usageReg, zstdFirst))     // 只在回退时用
```

同一个请求的用量（token、费用等）在请求结束后一处连着写完，中间不要夹网络调用、查库等慢操作。

**不要做的：**

- 不要把 per-Key 指标注册到 `frameworkReg`，也不要再留一个“全都注册进去”的大注册表给 `/metrics` 用。
- 不要手写 `delta.Options`，也不要改 `opts.IdleScrapes`。代码里如果有 `opts.IdleScrapes = 90`，删掉。
- 写 per-API-key 指标（注册到 `usageReg` 的那些）时，每次都用 `WithLabelValues(...)`（或 `With(...)`）现取再 `Inc()` / `Add()` / `Observe()`，不要把取到的指标对象存进 map 或结构体里复用。框架指标不受这条限制。
- 不要让 vmagent 以外的任何程序抓 `/metrics/usage`。要手工看内容，必须带上这个请求头：

  ```sh
  curl -s --compressed -H 'X-Prometheus-Delta-Ack: peek' http://<pod>:9090/metrics/usage | head
  ```
- 平时不要访问 `/metrics/cumulative`。

**可选**：某个业务要求一个请求的指标绝对不拆到两分钟时，把记录这个请求指标的那几行包起来：

```go
prometheus.Atomically(func() {
    tokensTotal.WithLabelValues(key, model).Add(tokens)
    costTotal.WithLabelValues(key, model).Add(cost)
})
```

包在里面的只能是写指标的几行，不要放网络调用、查库、`sleep`，也不要在里面再调 `prometheus.Atomically`。

## 3. 内存

边车容器的资源（单 pod 约 230 万实例）：

```yaml
resources:
  requests:
    memory: 3Gi
  limits:
    memory: 4Gi
```

规模不同时，`limits.memory` 按“单 pod 实例数 × 1.7 KiB”算。单 pod 当前实例数看 `usage.Tracked()`。

## 4. 上线顺序

1. 先做完 [`vm-side.md`](vm-side.md) 的第 1、2 步（vmagent 要是 `v1.126.1035-cluster`）。
2. 紧接着发这个版本的边车，两步连着做。
3. 发完后检查：

```sh
# 在任一边车 pod 上
curl -s -o /dev/null -w "%{time_total}s\n" http://<pod>:9090/metrics    # 毫秒级
curl -s --compressed http://<pod>:9090/metrics | grep -c '^acg_'        # 0
curl -s -o /dev/null -D - -H 'X-Prometheus-Delta-Ack: peek' http://<pod>:9090/metrics/usage | grep -i x-prometheus-delta
#   X-Prometheus-Delta-Features: final
#   X-Prometheus-Delta-Round: ...          两行都要有
```

再跑一遍 [`verify-deploy.sh`](verify-deploy.sh)（`./verify-deploy.sh`，退出码 0 为通过），它读出的 client_golang 版本应为 v1.24.1027。

## 5. 网络策略

只让 vmagent 访问边车的 9090 端口。在 `acg-system` 里 apply 下面这个 NetworkPolicy（集群的网络插件要支持 NetworkPolicy，如 Calico）：

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: model-router-metrics
  namespace: acg-system
spec:
  podSelector:
    matchLabels:
      app: model-router                  # 换成 model-router pod 的标签
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: victoria-metrics-agent   # 换成 vmagent pod 的标签
      ports:
        - port: 9090
    - ports:                             # model-router 的业务端口，全部列上，来源不限
        - port: <业务端口>
```

- vmagent 不在 `acg-system` 时，`podSelector` 外面再加一层 `namespaceSelector`。
- 还有别的程序要抓 `/metrics`（不是 `/metrics/usage`）时，把它的 pod 也加进第一条的 `from`。

**确认：**

```sh
kubectl -n acg-system get networkpolicy model-router-metrics
# 从任一非 vmagent 的 pod 上：
curl -s -m 5 -o /dev/null -w "%{http_code}\n" -H 'X-Prometheus-Delta-Ack: peek' http://<pod>:9090/metrics/usage   # 超时，输出 000
# 业务请求照常；vmagent 的 /targets 里边车都是 up
```

## 6. 回退

边车不用重新发版。采集侧按 [`vm-side.md`](vm-side.md) 的“回退”把抓取路径改回 `/metrics/cumulative`。

---

## 出问题时

| 现象 | 怎么办 |
|---|---|
| `/metrics` 要几秒到几十秒、`up=0` | per-Key 指标还在 `/metrics` 用的注册表里。按第 2 步拆开，自检那几行不要删 |
| 边车启动就 panic | 手写了 `delta.Options`，改回 `delta.Increments()` |
| 指标完全没被跟踪 | `EnableChangeTracking()` 挪到任何指标创建之前 |
| VM 里的数字比预期少一截 | 找出 vmagent 以外抓 `/metrics/usage` 的程序，停掉；手工看要带 `X-Prometheus-Delta-Ack: peek` |
| 框架指标停了半小时后消失、或从 0 重新开始；`/metrics/usage` 里出现框架指标 | 补上 `opts.Scope = usageReg` |
| 边车内存比第 3 步高很多 | 删掉改 `IdleScrapes` 的那一行 |
| 检查时没有 `X-Prometheus-Delta-Features: final` | `go.mod` 里的版本改成 v1.24.1027 重新发版 |
| 加了第 5 步的网络策略后，vmagent 抓边车 `up=0` 或业务请求失败 | 核对网络策略里两个 pod 标签和业务端口，改对后重新 apply |
| VM 里突然没有新数据，但 `up` 是 1 | 采集侧按 vm-side 第 2 步把 `promscrape.maxScrapeSize` 设成 500MiB，reload vmagent |

参考实现：client_golang 仓库的 `examples/delta`（`go run ./examples/delta`）。
