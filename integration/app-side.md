# API Key 用量统计 · model-router 边车接入

SDK：`github.com/axfor/client_golang v1.24.1030`

指标定义、`Inc()`、`Observe()` 都不用改，只改注册和端点。采集侧见 [`vm-side.md`](vm-side.md)，上线顺序见第 4 步。model-router 里另一个 exposer（标签维度用量，`SetTagUsageExposer`）见第 7 步。

---

## 1. go.mod

```
replace github.com/prometheus/client_golang => github.com/axfor/client_golang v1.24.1030
```

已经在用 v1.24.1023 ~ v1.24.1029 的：只改这里的版本，代码不用动。v1.24.1023 ~ v1.24.1026 要重新发版；v1.24.1027 ~ v1.24.1029 不必专门为此发版，下次发版时带上即可。v1.24.1030 改的是另一种接法（`delta.New`，即第 7 步的标签维度 exposer）和 summary / native histogram 的空闲判断，`usageReg` 里只有 counter、经典 histogram、gauge 时，边车行为不变。

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

1. 先做完 [`vm-side.md`](vm-side.md) 的第 1、2 步（vmagent 要是 `v1.126.1036-cluster`）。
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

再跑一遍 [`verify-deploy.sh`](verify-deploy.sh)（`./verify-deploy.sh`，退出码 0 为通过），它读出的 client_golang 版本应为 v1.24.1030（v1.24.1027 ~ v1.24.1029 也算通过）。

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

网络策略：`kubectl -n acg-system delete networkpolicy model-router-metrics`。

## 7. 标签维度用量的 exposer（`SetTagUsageExposer`）

model-router 给标签维度用量用的那个 exposer 是 `delta.New(标签 registry, opts)`，配 `opts.Delete = aistatistics.DeleteTaggedSeries`。和上面的边车（`delta.NewTracked`）是两种接法，v1.24.1030 的修复主要在这一种。

**只升版本（第 1 步）、代码不动**：可以，比 v1.24.1029 好两点：`Delete` 回调没删掉的子项不再重报整值；标签维度改回旧值（A → B → A）不再报负数。

**要完全修好，代码改两处，和第 1 步同一次发版：**

1. 用 `opts.Vecs` 代替 `opts.Delete`，其余 options 保持原样：

   ```go
   // 删掉这一行
   opts.Delete = aistatistics.DeleteTaggedSeries
   // 换成：注册到标签 registry 的每一个 Vec（*CounterVec、*HistogramVec 等，
   // 包括 combinationOverflow、seriesOverflow 这类零维度的）
   opts.Vecs = []delta.Vec{tagTokens, tagCost /* , ... */}
   opts.ErrorLog = logger // 建议加上：漏登记、传错 Vec 时各打一条日志
   ```

   删空闲序列时，库在该 Vec 的锁内确认这一轮抓取之后没有新写入才删，抓取和删除之间的写入不会再丢。

2. `Reregister` 换代时，在新 registry 和新 Vec 都建好、注册完之后调用 `Replace`：

   ```go
   exp.Replace(newTagRegistry, newTagTokens, newTagCost /* , ... 新代的全部 Vec */)
   ```

   换 registry、换 Vec、清状态在锁内一步完成：新代从 0 报，旧代状态立即释放。

**注意：**

- `Vecs` 和 `Delete` 只能设一个，两个都设启动时 panic。
- 传的必须是注册到标签 registry 的那个 Vec 本身：不要传 `MustCurryWith` 柯里化后的，换代后也不要再传旧代的。传错的删不掉，打一条日志。
- `Replace` 一定要带上新代的全部 Vec。只传 registry、不传 Vec，换代后空闲序列一个都不删（计数仍正确，但状态常驻）。
- `delta.New` 和 `Replace` 都直接传 registry。如果现在传给 `delta.New` 的是自己写的、内部切换 registry 的 Gatherer 包装，改成直接传 registry，换代只走 `Replace`：先切包装再调 `Replace`，中间插进来的一次抓取会按旧状态读新指标。
- `SetTagUsageExposer` 仍只在 `Serve` 之前调用一次，换代不要重建 exposer。
- 标签 registry 里的指标都要是 Vec，普通 `Counter` 改成零维度 `CounterVec`；没有对应 Vec 的族不会被删（计数仍正确，但状态常驻），打一条日志。也不要用 `prometheus.WrapRegistererWithPrefix` 包标签 registry，改了名的族对不上 Vec。
- 写这些指标时同第 2 步：每次 `WithLabelValues(...)` 现取，不要存起来复用。

**确认**（加了 `ErrorLog` 时）：

- 发版约半小时后（`IdleScrapes` 为 30、60s 抓取），model-router 日志里不应有 `delta: idle series of`、`delta: an idle series of`、`delta: Options.Delete left` 开头的行。
- 改一次标签维度配置触发换代：`exp.Tracked()`（这个 exposer 持有状态的序列数）先回到 0，再随新代序列回升，不随换代次数累积；之后的增量没有负数。

**回退**：代码改动可以单独回退（恢复 `opts.Delete` 那一行、去掉 `Replace` 调用），版本留在 v1.24.1030。

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
| 检查时没有 `X-Prometheus-Delta-Features: final` | `go.mod` 里的版本改成 v1.24.1030 重新发版 |
| `usageReg` 里的某个 summary 或 native histogram 一直有请求，却每隔约半小时从 0 重新开始 | `go.mod` 里的版本改成 v1.24.1030 重新发版 |
| 加了第 5 步的网络策略后，vmagent 抓边车 `up=0` 或业务请求失败 | 核对网络策略里两个 pod 标签和业务端口，改对后重新 apply |
| VM 里突然没有新数据，但 `up` 是 1 | 采集侧按 vm-side 第 2 步把 `promscrape.maxScrapeSize` 设成 500MiB，reload vmagent |
| 启动就 panic：`delta: Options.Vecs and Options.Delete are mutually exclusive` | 第 7 步：删掉 `opts.Delete` 那一行 |
| 启动或换代时 panic：`delta: two Vecs in Options.Vecs gather as …` | 同一个 Vec 传了两次，或两个 Vec 同名，去掉重复的 |
| 日志 `delta: idle series of X are kept, not deleted` | X 没有对应的 Vec：加进 `opts.Vecs` 和 `Replace` 的参数；普通 `Counter` 改成零维度 `CounterVec` |
| 日志 `delta: an idle series of X is kept, not deleted: the Vec in Options.Vecs for it does not hold it` | 传了柯里化的 Vec，或换代后还在传旧代的 Vec（偶尔一条也可能只是那个子项刚被别处删掉） |
| 日志 `delta: Options.Delete left idle series of X in place` | 还在用 `Delete` 回调，且它没删掉 X 的子项：按第 7 步改用 `Vecs` |
| 改了标签维度配置后，标签维度用量少报一截或出现负数 | 换代没调 `Replace`，按第 7 步加上 |

参考实现：client_golang 仓库的 `examples/delta`（`go run ./examples/delta`）。
