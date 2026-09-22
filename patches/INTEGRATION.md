# API Key 用量统计 · 内网集成

`client_golang v1.24.1011` · `VictoriaMetrics v1.126.1010-cluster`

链路：**app（model-router，client_golang）→ vmagent → vm-insert → vm-storage**。
只有 vmagent 侧要换二进制，vm-insert / vm-select / vm-storage 一行没改，20 个补丁
全在 `lib/promscrape`、`lib/promutil`、`lib/streamaggr`、`lib/encoding/zstd`。
写入地址沿用现有的 `http://vm-insert:8480/insert/0/prometheus`（vm-insert 对
`prometheus`、`prometheus/api/v1/write` 等后缀一视同仁，不用改）。

---

## 一、边车（model-router）

```
replace github.com/prometheus/client_golang => github.com/axfor/client_golang v1.24.1011
```

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/prometheus/client_golang/prometheus/promhttp/delta"
)

trk := prometheus.EnableChangeTracking()   // 必须在任何指标创建之前，否则那些指标不被跟踪

usageReg := prometheus.NewRegistry()
// aistatistics 里 per-API-key 指标的 MustRegister：promReg → usageReg

opts := delta.Increments()
opts.TypeLabel = "_metric_type"
usage := delta.NewTracked(trk, opts)

mux.Handle("/metrics",            promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
mux.Handle("/metrics/usage",      usage.Handler())
mux.Handle("/metrics/cumulative", promhttp.HandlerFor(usageReg, promhttp.HandlerOpts{}))
```

改注册目标是唯一要动的业务代码，集中在 aistatistics 一个包里。指标定义、`Inc()`、`Observe()` 一行不改。

三个端点的分工：

| 端点 | 内容 | 谁抓 | 间隔 |
|---|---|---|---|
| `/metrics` | 框架指标，累计值 | `acg-framework` job | 10s |
| `/metrics/usage` | per-Key 用量，**增量** + 类型标签 | `acg-usage` job | 60s |
| `/metrics/cumulative` | per-Key 用量，累计值 | 不配 job。排查和回退用 | — |

`/metrics/usage` **只能有一个消费者**：它报的是「自上次成功送达以来」，基线只有一份。第二个采集方会拿走第一个再也看不到的增量，不报错。

---

## 二、vmagent

二进制用 `github.com/axfor/VictoriaMetrics` 的 `v1.126.1010-cluster` 构建。

**最低 `v1.126.1004-cluster`，低于它会有两个问题，一个起不来、一个静默算错：**

- **起不来**：`-zstd.encoderConcurrency` 在补丁 021 里只声明在 `zstd_pure.go`，那文件是 `//go:build !cgo`，而发布版 vmagent 用 `CGO_ENABLED=1` 构建，传这个参数会以 `flag provided but not defined` 直接退出。补丁 **022** 才把它挪到无 build 约束的 `concurrency.go`。
- **静默算错**：同样是补丁 022，修了 dedup 下所有输入序列挤进同一个 map 条目的 bug。gauge 那条规则开着 `dedup_interval`，停在 021 会让**三个 pod 各报 1 合出来是 1**。

用原版上游镜像则是另一回事：`sum_samples_total` 上游没有，配置加载时直接 fatal 退出。

**部署前先自检**，在容器里跑：

```sh
/vmagent-prod -help 2>&1 | grep -c "streamAggr.config"        # >0:是我们的 fork
/vmagent-prod -help 2>&1 | grep "zstd.encoderConcurrency"     # 有输出:版本 ≥ 1004
```

第二条没输出就别急着改 `extraArgs`，先换镜像。

沿用你们现有的部署，改这几项：

**聚合配置已经有了**，在 `feature-apikey-redis-notification-axx` 分支上：`extraArgs` 里 `remoteWrite.streamAggr.config: [/etc/vmagent/streamaggr/usage.yaml]`，内容来自 `extraObjects` 的 `vmagent-streamaggr` ConfigMap。**要改的是它的内容，见 §四**——现有规则有个会毁掉框架指标的错。

**`extraArgs` 里加三项**：

```yaml
extraArgs:
  promscrape.zstdCompression: "true"
  zstd.encoderConcurrency: "1"
  remoteWrite.queues: "2"
  remoteWrite.maxDiskUsagePerURL: "10GB"
```

后两项是同一类问题：**按核数放大的常驻内存，与实际吞吐无关**。

- `zstd.encoderConcurrency` 不设时按核数开压缩槽，每槽常驻 8 MB 历史缓冲。单块压缩用不到并发（1 和 10 都是 4.2 GB/s），只有多块同时压才有用。设成 1 之后堆里 `zstd.(*fastBase).ensureHist` 正好 8 MB，就是一个槽。
- `remoteWrite.queues` 默认 `核数 × 2`，每队列一份发送缓冲（`maxRowsPerBlock=10000` 样本，实测约 3.3 MB）。

三万 Key 实测（单独一轮 A/B，两个进程同负载），两项都设上后 vmagent 存活堆 **346 → 204 MiB（−41%）**、物理占用 428 → 286 MiB，**样本吞吐不变**。绝对值以 §六 那轮完整配置的为准，这里看的是差值。

按核数放大的程度：

| 核数 | 不设（zstd + 队列缓冲） | 设上后 |
|---|---|---|
| 10 | 80 + 66 = 146 MB | 15 MB |
| 32 | 256 + 210 = 466 MB | 15 MB |
| 64 | 512 + 420 = **932 MB** | 15 MB |

实测吞吐需求约 233 KB/s，`queues=2` 绰绰有余。真要更高吞吐时再往上调，每加一个队列约 3.3 MB。

`remoteWrite.maxDiskUsagePerURL` 的值按 `vmagent_remotewrite_pending_data_bytes` 的峰值乘容灾窗口定，`10GB` 只是占位。

**`tmpDataPath` 保持 `emptyDir`，不要改成 PVC。** vmagent 是无状态的，挂了卷就不能漂移——单副本 + 硬反亲和下节点故障时卷解绑不掉、pod 重建不出来，可用性反而更差。实测队列目录 8 MB 且几乎全是预分配结构，常态下直接推给 vm-insert，队列是空的。

但要知道有这么个窗口：**边车判定「已交付」只看 HTTP 响应写没写成功**，写成功就把基线前移，之后 vmagent 那边发生什么它一概不知。所以「VM 不可用」**且**「vmagent 同时重启」两件事同时发生时，队列里那段增量永久丢失，边车不会重发。报累计值时这个场景能自愈（下一轮的完整累计值补上），报增量不行。

这是双重故障，概率低，代价是丢一段账。监控 `vmagent_remotewrite_pending_data_bytes`——它**持续非零**才是真问题（说明 VM 跟不上），那时要解决的是 VM，不是把队列持久化。

`-remoteWrite.disableOnDiskQueue` 不能开，那会让队列连内存都不留。

**CPU limit 就是抓取并发度。** vmagent 没有抓取并发参数，多目标并行抓、单响应体内部并行解析、解析后处理的上限，三层都锚定 `cgroup.AvailableCPUs()`，给少了就是串行。

这正是上面两个 flag 的意义：**不设它们时，每多给一个核就多背 8 MB（zstd）+ 6.6 MB（队列缓冲）**，想要并发就得付内存。设上之后两者解耦——CPU 按抓取需要给，内存不跟着涨。

**RBAC**：§三用 `kubernetes_sd_configs`，需要 pod 的 `get`/`list`/`watch`。已经在用 k8s 服务发现的话通常已经有了。

---

## 三、scrape 配置

内网现有两个 job 在 `repos/aigateway-victoriametrics-conf/helm/victoria-metrics-agent/values.yaml` 的 `extraScrapeConfigs` 下。**不用新增 job，改现有的两处。**

### `model-router-metrics`（60s，用量）

```yaml
  - job_name: model-router-metrics
    scrape_interval: 60s
    scrape_timeout: 50s              # 改：原 10s，106 MB 的响应体不够
    scrape_align_interval: 60s       # 加：对齐整分，聚合窗口才稳
    no_stale_markers: true           # 加：不加内存翻几倍，见下
    metrics_path: /metrics/usage     # 改：原 /metrics
    # 其余 kubernetes_sd_configs、relabel_configs、metric_relabel_configs 不动
```

### `acg-model-router-metrics`（10s，框架）

```yaml
    no_stale_markers: true           # 加，其余不动
```

### 全量端点在默认配置下根本抓不动

`-promscrape.maxScrapeSize` 默认 **16 MiB**，限的是**压缩后**的字节——`io.LimitReader` 套在解压之前。3 万 Key 的全量响应实测 **1466 MiB**（解压后），vmagent 直接拒收：

```
the response from ".../metrics/acg" exceeds -promscrape.maxScrapeSize (16777216 bytes)
```

一个样本都进不去，而且只是 warn 日志、`up` 仍然是 1。所以增量不是「省内存的优化」，是**让这条链路能跑起来的前提**。

增量端点实测，每个 pod 每轮的**压缩后**大小（这才是 `maxScrapeSize` 判的那个字节数）：

| 一批活跃 Key | 压缩后 | 解压后 |
|---|---|---|
| 2000（固定） | 2.29 MiB | 约 94 MiB |
| 5000（轮换） | 7.11 MiB | 约 223 MiB |

**按活跃 Key 数线性走**，约 **1.4 KiB/Key**——每轮发的就是有变化的那些。推下去 16 MiB 上限对应**一批活跃 Key 约 1.1 万**。超过这个数就要调 `-promscrape.maxScrapeSize`，否则 vmagent 静默拒收整个响应（只有 warn 日志、`up` 仍然是 1）。盯 `vm_promscrape_max_scrape_size_exceeded_errors_total`，必须恒为 0。

注册 Key 总数不影响这个大小，只有**一批同时活跃的数量**影响。

两个口径别混：`vm_promscrape_scrape_response_size_bytes` 记的是**解压后**的字节，而 `maxScrapeSize` 判的是**压缩后**的（约差 30~40 倍，后者才是真正的网络流量）。要盯上限就看后者，别拿前一个指标去比。

### 为什么必须加 `no_stale_markers`

它默认是 false。不加的话补丁 008 的内存优化整个不生效——未压缩的响应体会被完整缓进内存，3 万 Key 下实测 4.03 GiB 变 26.94 GiB。不报错、抓取照常成功、数字照常正确。

同理**不要加 `sample_limit` / `series_limit`**，加了会掉到更差的一档。

### 现有 relabel 产生的标签，哪些要在聚合层删掉

| 标签 | 来源 | 处理 |
|---|---|---|
| `pod` | pod 名 | **删**，per-pod |
| `instance` | 也设成了 pod 名 | **删**，per-pod |
| `namespace` | `acg-system` | 保留，所有 pod 相同 |
| `scraper_pod_namespace` | 同上 | 保留 |
| `step` | 固定 `1m` | **保留**，RetentionFilter 靠它做分级保留 |

前两个要在聚合层去掉，但两条规则的去法不同：计数器那条进 `drop_input_labels`，gauge 那条进 `without`（`drop_input_labels` 在 dedup 之前生效，gauge 那条开了 dedup，见 §四）。将来 relabel 再加 per-pod 标签，两条都要同步加。

### `namespaces` 写了不生效

`templates/configmap.yaml` 会强制覆写：

```gotemplate
{{- if .namespaces }}
  {{- $_ := set .namespaces "names" (list $ns) }}
{{- end }}
```

`model-router-metrics` 里写的 `names: [acg-system]` 会被替换成 release 的命名空间（`.Values.namespace | default .Release.Namespace`）。两者一致时无感，不一致时就发现不到目标。`acg-model-router-metrics` 没有 `namespaces` 块，走的是全集群发现。

要限定命名空间，改 `.Values.namespace`，不是改 job 里的 `names`。

### 已经是对的，不用动

- `metric_relabel_configs` 里 `keep acg_.+`：增量端点上也会出现框架指标（变更追踪是进程级的），这条正好滤掉
- 两个 job 都没有 `sample_limit` / `series_limit`
- RBAC 已有 pods 的 `get`/`list`/`watch`（`rbac.create: true` + `namespaced: false` 渲染出 ClusterRole）
- `replicaCount: 1` + 硬反亲和 + `maxSurge: 0`：单副本滚动更新有几十秒空窗，增量留在边车不会丢，恢复后补上
- `persistence.enabled: false`：vmagent 保持无状态，见 §二

---

## 四、聚合规则（改现有的 `usage.yaml`）

现有内容（`values.yaml` 的 `extraObjects` → `vmagent-streamaggr` ConfigMap），两条规则：

```yaml
- match: '{_metric_type!="gauge"}'        # ← 选择器有错，漏了 _metric_type!=""
  interval: 60s
  without: [pod, instance, node]          # ← 换成 drop_input_labels
  drop_input_labels: [_metric_type]
  outputs: [sum_samples_total]
  staleness_interval: 40m
  reset_marker_on_stale: true
  flush_on_shutdown: true

- match: '{_metric_type="gauge"}'
  interval: 60s
  dedup_interval: 60s
  without: [pod, instance, node]          # ← 这条保持 without，不要跟着改
  outputs: [sum_samples]                  #   只补一行 drop_input_labels: [_metric_type]
  keep_metric_names: true
```

改成：

```yaml
- match: '{_metric_type!="gauge",_metric_type!=""}'
  interval: 60s
  drop_input_labels: [_metric_type, pod, instance, node]
  outputs: [sum_samples_total]
  keep_metric_names: true
  staleness_interval: 40m
  reset_marker_on_stale: true
  flush_on_shutdown: true

- match: '{_metric_type="gauge"}'
  interval: 60s
  drop_input_labels: [_metric_type]
  without: [pod, instance, node]
  outputs: [sum_samples]
  keep_metric_names: true
  dedup_interval: 60s
```

**两条规则去掉 pod 的方式不一样，这是有意的，不要统一。** 原因见下面第三条。

### 三处改动的原因

**`_metric_type!=""` 不能少。** PromQL 把缺失的标签当空值，所以 `{_metric_type!="gauge"}` 会把**根本没有类型标签的样本一并选中**。现网 `acg-model-router-metrics` 那个 job 采的 `model_router_*` 就没有类型标签——它们会被 `sum_samples_total` 当成增量逐次累加，一个已经到 100 万的计数器每分钟再加 100 万，同时 `without` 把 pod/instance 抹掉。不报错、抓取照常成功。

**计数器那条：`without` 换成 `drop_input_labels`。** `without` 是减法：将来 relabel 新增一个 per-pod 标签会被保留，series 静默按 pod 裂开。`drop_input_labels` 里列的在分组前就删掉，根本不存在。而且不写 `by`/`without` 时聚合器走 `aggregateOnlyByTime`，样本的 input key 是空的，省一次标签压缩。

**gauge 那条：`pod` 必须留在 `without` 里，不能挪进 `drop_input_labels`。** 两者的时机不同——`drop_input_labels` 在 **dedup 之前**删，`without` 在**分组时**删，也就是 dedup 之后。gauge 这条开了 `dedup_interval`，pod 要是在 dedup 之前就没了，三个 pod 在去重器眼里是同一条 series，每个窗口只留一个样本，`sum_samples` 加的就只有一个 pod 的值。

实测：三个 pod 各报 1，pod 放 `drop_input_labels` 得 **1**，放 `without` 得 **3**。不报错、series 数也对，只是值少了三分之二。VM fork 里有 `TestGaugeRuleSumsAcrossPodsWithDedupOn` 钉住这个形态。

计数器那条没有 dedup，所以不存在这个问题——它报的是增量，同一个 pod 在一个窗口里被抓两次本来就该相加。gauge 报的是当前值，抓两次不能相加，这才是它需要 dedup 的原因。

现网 relabel 把 `instance` 也设成了 pod 名，所以它和 `pod` 都必须在列表里。`namespace`、`scraper_pod_namespace`、`step` 对所有 pod 相同，保留——**`step: 1m` 尤其不能删，RetentionFilter 靠它做分级保留**。

代价说清楚：gauge 这条用 `without` 就保留了上面那个「新增 per-pod 标签会让 series 裂开」的风险。但两种失败方式不对等——裂开是看得见的（series 数变成三倍），少算是看不见的。将来 relabel 加了 per-pod 标签，两条规则都要同步加。

实测三个边车、50 个 Key，relabel 里故意加了个 `node_name`：VM 里 `acg_requests_total` 是 50 条 series（每 Key 一条）而不是 150 条，`acg_*` 里带 `pod` 标签的 0 条。

---

## 五、上线顺序与回退

**顺序不能反**：先上聚合层，再切边车。

1. 部署 `v1.126.1010-cluster` 的 vmagent + `aggr.yml`
2. 边车发版，但 `scrape.yml` 里 `metrics_path` 仍指 `/metrics/cumulative`
3. 确认 VM 里数字正常，再把 `metrics_path` 改成 `/metrics/usage`

反过来做——边车先报增量而聚合层不在——VM 会把增量当累计值存，数字直接错。

**回退**：把 `metrics_path` 改回 `/metrics/cumulative`，reload vmagent。不用重新发版。

那是个普通 handler，给累计值、不碰增量基线；它没有类型标签，两条聚合规则都不匹配，原样透传。回退后行为等同改造前：全量上报、内存回到老水平、数字正确。

---

## 六、配额

按**活跃 Key 的形态**分两档配，差 8 倍，先确认自己属于哪一档。

### 第一档：活跃 Key 基本固定（2000 活跃 / 3 万注册）

| | 存活堆 | 物理占用 | 建议配额 |
|---|---|---|---|
| 边车（每个） | 206 MiB | 299 MiB | 512Mi |
| vmagent | 148 MiB | 225 MiB | 512Mi |

### 第二档：Key 轮流活跃（每 2 分钟换一批 5000，12 分钟轮完 3 万）

| | 存活堆 | 物理占用 | 建议配额 |
|---|---|---|---|
| 边车（每个） | 1215 MiB | 1435 MiB | **2Gi** |
| vmagent | 1224 MiB | 1517 MiB | **2Gi** |

vm-insert 412 MiB、vm-storage 每分片约 1.7 GiB、vm-select 11 MiB（后两者按可用内存自调缓存，按现网经验给即可）。

### 为什么差 8 倍

两端持有的都不是「此刻在忙的 Key」：聚合器为 **`staleness_interval`（40 分钟）窗口内出现过的**每个 Key 留状态，边车只删**连续 `-idle-scrapes`（30 次抓取）没被写过**的实例。轮换一圈短于这两个窗口时，两端持有的就是**全量注册 Key**。

**怎么判断自己属于哪一档**：看 vmagent 的 RSS 是不是远超「此刻活跃 Key 数 × 145 条序列」对应的量——是就按第二档配。

**要降第二档的内存，唯一的杠杆是 `staleness_interval`**。报增量时缩短它是语义安全的，但它必须大于「同一个 Key 两次活跃之间的最长间隔」，否则中间那段会被判 stale、补 0 重来。上线前按真实活跃模式测一轮定值。

### 增长口径

**边车随注册 Key 线性长**（每个被访问过的 Key 都要有常驻实例），**vmagent 跟活跃窗口内的 Key 数走**。所以 vmagent 按活跃形态配，边车按注册 Key 扛峰值。

存储另见 `docs/apikey-usage/vm-storage-cardinality.md`——结论是 VM 的磁盘和查询成本都由**序列数**决定，与写入频率无关，降存储只能降基数。


## 七、上线后

对一次账：取同一时间窗，VM 里的增量与边车侧的请求计数应当相等。

```promql
sum(increase(acg_requests_total[1h]))
```

和边车 `/metrics/cumulative` 上同一窗口的差值比。相等即通过。

---

# 参考

集成时不用读这部分——要做的动作全在一～五。这里是出问题之后回来查的。

## 出问题时

| 现象 | 原因 |
|---|---|
| 数字比预期少一截 | 有第二个消费者在抓 `/metrics/usage`。它报的是「自上次成功送达以来」，基线只有一份，第二个采集方会拿走第一个再也看不到的增量。不报错 |
| 数字翻倍 | `aggr.yml` 第一条的 `_metric_type!=""` 漏了。缺失标签在 PromQL 里等于空值，少了它会把没有类型标签的样本（框架 job、node_exporter、vmagent 自身）也当增量累加 |
| series 按 pod 裂开 | `relabel_configs` 加的标签没全进两条规则的标签列表 |
| gauge 的值只有一个 pod 的（约为实际的 1/3） | gauge 那条规则把 `pod` 写进了 `drop_input_labels`。它在 dedup 之前生效，三个 pod 变成同一条 series，每窗口只留一个样本。`pod`/`instance`/`node` 要放 `without` |
| 内存没降 | `no_stale_markers` 没写，或配了 `sample_limit` / `series_limit`。查 `vm_promscrape_scrapes_by_parse_mode_total{mode="one_shot"}`，应为 0 |
| 边车启动就 panic | 手工拼了 `delta.Options` 而不是用 `delta.Increments()`。报增量时不要配 `GenLabel`、`RebaseAfterGap` |
| 指标完全没被跟踪 | `EnableChangeTracking()` 调晚了，在建指标之后 |
| vmagent 启动即退出，日志 `flag provided but not defined: -zstd.encoderConcurrency` | 镜像版本低于 `v1.126.1004-cluster`（补丁只打到 021、没打 022）。**应急**：把 `zstd.encoderConcurrency` 从 `extraArgs` 去掉即可启动，它只是内存优化，不影响正确性。**正解**：换镜像，否则 022 修的 dedup 少算 bug 也还在 |
| 聚合配置启动报错 | vmagent 不是用 `v1.126.1010-cluster` 构建的。`sum_samples_total` 上游没有 |
| 升级后代码没变 | 复用了 tag。`proxy.golang.org` 永久缓存快照，同名强推静默无效，必须换新版本号 |
| vmagent 内存一路涨、远超活跃 Key 数对应的量 | Key 在轮流活跃，而聚合状态跟的是「`staleness_interval` 窗口内出现过的 Key」。见 §六 末尾 |
| vmagent 重启后少一段账 | VM 当时不可用、队列非空，而 vmagent 又重启了。边车按 HTTP 响应写成功就把基线前移，那段增量没人再持有。看 `vmagent_remotewrite_pending_data_bytes` 是否持续非零 |

## 看着像 bug 但正常的

- **用量端点里有框架指标**。变更追踪是进程级的，框架指标也被跟上了，几十条，采集侧 relabel 丢掉即可。
- **gauge 每轮都在，counter 时有时无**。gauge 是当前值必须每轮发，counter 只在变化时发。
- **`up`、`scrape_duration_seconds` 仍带 `pod`**。它们没有类型标签，两条聚合规则都不匹配，原样透传——否则看不出哪个 pod 抓取失败。

## 每轮抓取的情况

`usage.Serve(w, r)` 返回 `ScrapeStats`，需要自己观测时用：`Round` / `Samples` / `Heartbeat` / `Gauges` / `Deleted` / `Delivered` / `Err`。

## 可运行的参考实现

```
go run ./examples/delta
```

按「10s job 抓三次、60s job 抓一次」的真实交错跑一遍，演示两个端点互不抢增量。源码 `examples/delta/main.go`。

## 试过但不推荐的

**`staleness_interval` 缩短到 5m。** 报增量时它确实可以短——40 分钟那个下限是为报累计值定的（聚合端忘掉还活着的闲置实例会导致整个累计值被重复计一遍），而报增量时状态被忘掉后累加器归零、下一个样本从 0 开始，`reset_marker_on_stale` 两端都补 0、`increase()` 正确识别，不丢不重。

但实测只省 16 MiB（同一轮 A/B 里的 204 → 188），因为省下的只是 `sync.Map` 的条目节点，trie 结构和字符串驻留都不随之释放。**8% 的收益配不上动一个和正确性相关的参数**，保持 40m。

**这个结论只对「活跃 Key 基本固定」成立。** Key 轮流活跃时，40 分钟窗口决定的是「要记住多少个 Key」，缩短它省的就不是 8% 而是成倍——见 §六 末尾那一节。

## 补丁

vmagent 补丁清单与打法见 `vm/vmagent/README.md`（20 个，编号 004~023 接内网现有的 001~003）。
