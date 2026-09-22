# API Key 用量统计 · 内网集成

`client_golang v1.24.1011` · `VictoriaMetrics v1.126.1014-cluster`

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

opts := delta.Increments()          // ReportIncrements + DisableHeartbeat + IdleScrapes:30
opts.TypeLabel = "_metric_type"
usage := delta.NewTracked(trk, opts)

mux.Handle("/metrics",            promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
mux.Handle("/metrics/usage",      usage.Handler())
mux.Handle("/metrics/cumulative", promhttp.HandlerFor(usageReg, promhttp.HandlerOpts{}))
```

改注册目标是唯一要动的业务代码，集中在 aistatistics 一个包里。指标定义、`Inc()`、`Observe()` 一行不改。

`delta.Increments()` 是一组必须配套的预设，**别手工拼 `delta.Options`**（拼错会在启动时 panic）。里面三项：报增量、关心跳、以及 **`IdleScrapes: 30`**——连续 30 次抓取没被写过就删掉实例，60 秒抓取间隔下就是闲置 30 分钟。

**`IdleScrapes` 是用来回收长期不访问的 Key 的**：注册了 3 万个，实际常年有量的可能只有几千，剩下的休眠 Key 不该一直占着常驻实例。它不是内存旋钮——**不要为了省内存把它调小**，那会把只是「两次请求之间」的活跃 Key 也删掉，下次请求再建一遍，白白抖动。

取值只要**大于「一个还在用的 Key 两次访问之间的正常间隔」**即可。30 分钟对绝大多数场景足够；如果你们有正常间隔超过半小时的低频 Key（比如按小时跑的批处理），相应调大：

```go
opts.IdleScrapes = 90   // 闲置 90 分钟才删
```

调小没有收益：实测 3 万 Key 每 2 分钟换一批 5000（12 分钟全部轮一遍，等于没有休眠 Key），`IdleScrapes` 从 30 压到 8 也只降 17%，因为每个 Key 在每 12 分钟里仍有 10 分钟是常驻的。真正决定边车内存的是**有多少 Key 在活跃窗口内有量**，不是这个参数。

**这一项要和 vmagent 的 `staleness_interval` 配成一对**，两端都得忘掉长期不来的 Key，见 §三。

三个端点的分工：

| 端点 | 内容 | 谁抓 | 间隔 |
|---|---|---|---|
| `/metrics` | 框架指标，累计值 | `acg-framework` job | 10s |
| `/metrics/usage` | per-Key 用量，**增量** + 类型标签 | `acg-usage` job | 60s |
| `/metrics/cumulative` | per-Key 用量，累计值 | 不配 job。排查和回退用 | — |

`/metrics/usage` **只能有一个消费者**：它报的是「自上次成功送达以来」，基线只有一份。第二个采集方会拿走第一个再也看不到的增量，不报错。

---

## 二、vmagent 配置

二进制用 `github.com/axfor/VictoriaMetrics` 的 `v1.126.1019-cluster` 构建。用原版上游镜像会在加载聚合配置时 fatal 退出——`sum_samples_total` 上游没有。

下面是 `victoria-metrics-agent` 的 `values.yaml` 里**全部要改的地方**。

### `extraArgs`

```yaml
extraArgs:
  promscrape.zstdCompression: "true"
  promscrape.maxScrapeSize: "500MiB"
  remoteWrite.queues: "2"
  remoteWrite.maxDiskUsagePerURL: "10GB"
```

### `extraScrapeConfigs`

两个 job，不新增，改这两处：

```yaml
extraScrapeConfigs:
  - job_name: model-router-metrics          # 60s,用量
    scrape_interval: 60s
    scrape_timeout: 50s
    scrape_align_interval: 60s
    no_stale_markers: true
    metrics_path: /metrics/usage
    # kubernetes_sd_configs、relabel_configs、metric_relabel_configs 不动

  - job_name: acg-model-router-metrics      # 10s,框架;只加下面这一行
    no_stale_markers: true
```

### 聚合规则（`extraObjects` → `vmagent-streamaggr` ConfigMap）

```yaml
- match: '{_metric_type!="gauge",_metric_type!=""}'
  interval: 60s
  drop_input_labels: [_metric_type, pod, instance, node]
  outputs: [sum_samples_total]
  keep_metric_names: true
  staleness_interval: 40m
  reset_marker_on_stale: true
  flush_on_shutdown: true
  output_heartbeat_interval: 4m

- match: '{_metric_type="gauge"}'
  interval: 60s
  drop_input_labels: [_metric_type]
  without: [pod, instance, node]
  outputs: [sum_samples]
  keep_metric_names: true
  dedup_interval: 60s
```

### vm-select 上加一条

```
-search.minStalenessInterval=5m
```

配合上面的 `output_heartbeat_interval`，见 §三。

### 保持不动的

- **`tmpDataPath` 保持 `emptyDir`，不要改成 PVC。** vmagent 是无状态的，挂了卷就不能漂移——单副本 + 硬反亲和下节点故障时卷解绑不掉、pod 重建不出来，可用性反而更差。实测队列目录 8 MB 且几乎全是预分配结构，常态下直接推给 vm-insert，队列是空的。
- **`-remoteWrite.disableOnDiskQueue` 不能开**，那会让队列连内存都不留。
- **RBAC**：`kubernetes_sd_configs` 需要 pod 的 `get`/`list`/`watch`，已经在用 k8s 服务发现的话通常已经有了。

---

## 三、为什么这么配

### CPU limit 就是抓取并发度

vmagent 没有抓取并发参数，多目标并行抓、单响应体内部并行解析、解析后处理的上限，三层都锚定 `cgroup.AvailableCPUs()`，给少了就是串行。

麻烦在于两项默认值是**按核数放大的常驻内存，与实际吞吐无关**：

- zstd 编码槽：库自身按核数开，每槽常驻 8 MB 历史缓冲。我们把默认值改成 1 了，不用配。单块压缩用不到并发（1 和 10 都是 4.2 GB/s），只有多块同时压才有用；实测生效后堆里 `zstd.(*fastBase).ensureHist` 正好 8 MB，就是一个槽。
- `remoteWrite.queues` 默认 `核数 × 2`，每队列一份发送缓冲（`maxRowsPerBlock=10000` 样本，实测约 3.3 MB）。

| 核数 | 不设（zstd + 队列缓冲） | 设上后 |
|---|---|---|
| 10 | 80 + 66 = 146 MB | 15 MB |
| 32 | 256 + 210 = 466 MB | 15 MB |
| 64 | 512 + 420 = **932 MB** | 15 MB |

设上之后 CPU 和内存解耦——CPU 按抓取需要给，内存不跟着涨。实测吞吐需求约 233 KB/s，`queues=2` 绰绰有余，真要更高吞吐时每加一个队列约 3.3 MB。

`remoteWrite.maxDiskUsagePerURL` 的值按 `vmagent_remotewrite_pending_data_bytes` 的峰值乘容灾窗口定，`10GB` 只是占位。

### 队列丢数据的那个窗口

**边车判定「已交付」只看 HTTP 响应写没写成功**，写成功就把基线前移，之后 vmagent 那边发生什么它一概不知。所以「VM 不可用」**且**「vmagent 同时重启」两件事同时发生时，队列里那段增量永久丢失，边车不会重发。报累计值时这个场景能自愈（下一轮的完整累计值补上），报增量不行。

这是双重故障，概率低，代价是丢一段账。监控 `vmagent_remotewrite_pending_data_bytes`——它**持续非零**才是真问题（说明 VM 跟不上），那时要解决的是 VM，不是把队列持久化。

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

**按活跃 Key 数线性走**，约 **1.4 KiB/Key**——每轮发的就是有变化的那些。注册 Key 总数不影响，只有**一批同时活跃的数量**影响。

推下去 16 MiB 的默认值对应一批活跃约 1.1 万。**不要留在默认值上赌**：撞上去的表现是 vmagent 静默拒收整个响应，**只有 warn 日志、`up` 仍然是 1、一个样本都不进**，等发现时账已经缺了一段。

所以上面直接把 `promscrape.maxScrapeSize` 设成 **500MiB**（约 35 万活跃 Key 的余量，等于把这个上限彻底挪出视线）。**调大它不花内存**——它只是 `io.LimitReader` 的上界，读取缓冲按上一次响应的实际大小定容，响应没真变大就不会多占。

但要知道它同时是个刹车，而 500MiB 基本等于松掉了：**撞上限之前 vmagent 会把压缩后的响应体完整读进内存**，最坏是 `500MiB × 目标数`。三个 pod 就是 1.5 GiB，已经超过 2Gi 配额的一半——正常情况下摸不到（实测 7.11 MiB），但边车要是因为 bug 疯狂膨胀，vmagent 会陪着一起 OOM 而不是拒收那一次抓取。

不想要这个风险就设回 `64MiB`（约 4.5 万活跃 Key，最坏 192 MiB），照样远超实测需要。

上线后盯 `vm_promscrape_max_scrape_size_exceeded_errors_total`，必须恒为 0。

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

前两个要在聚合层去掉，但两条规则的去法不同：计数器那条进 `drop_input_labels`，gauge 那条进 `without`（`drop_input_labels` 在 dedup 之前生效，gauge 那条开了 dedup，见 §三）。将来 relabel 再加 per-pod 标签，两条都要同步加。

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

### 两端的老化窗口是一对，顺序不能反

长期不访问的 Key 要在**两边**都被忘掉，否则一边省了另一边照样占着：

| | 参数 | 值 | 含义 |
|---|---|---|---|
| 边车 | `IdleScrapes` | 30 | 连续 30 次抓取没被写过就删实例（60s 间隔 = 30 分钟） |
| vmagent | `staleness_interval` | 40m | 40 分钟没收到输入就丢弃聚合状态 |

**约束：`staleness_interval` 必须大于 `IdleScrapes × scrape_interval`**（现在 40m > 30m）。

顺序要这样：边车先忘、停止上报，vmagent 的状态随后到期，由 `reset_marker_on_stale` 补一个 0 把这条输出正常收尾。反过来的话，vmagent 在边车还持有实例、只是暂时没写的时候就把状态丢了，Key 一恢复又要重建一遍聚合状态并补 0，白白多一轮抖动。

改其中一个就要同步看另一个。边车那边调大了 `IdleScrapes`（比如低频批处理 Key 要 90 分钟），`staleness_interval` 也得跟着大过它。

### `output_heartbeat_interval`：少写四分之三（v1.126.1014-cluster 起）

聚合器每个 `interval` 把**全部**输出序列刷一遍，不管值有没有变。实测轮换形态下 26180 条输出序列，两分钟窗口内真正有增量的只有 **6723 条（25.7%）**——四分之三是把同一个总数又写一遍。打开后，值和上次写的相同就不写，除非心跳到期。`4m` 写入降到约 44%。

**为什么不丢账**：值变了就一定写，所以空缺期间值必然恒定，`increase_pure` 在空缺两端取到同一个值。心跳不承担正确性，只负责不让序列在查询侧显得 stale。

**为什么 `4m` 是安全的**：VM 的 lookback 默认**按每条序列自己的样本间隔推断**（该序列前 20 个间隔的 0.6 分位 + 1/8 余量），不是固定 5 分钟。更重要的是，**用量查询全部是显式区间**（`increase_pure(m[@range])`、`sum_over_time(m_1h[@range])`），走 `rc.Window`，**根本不经过 lookback**。

只有**裸选择器的瞬时查询**（如直接查 `acg_requests_total` 不带区间）才可能因为空缺取不到点。要覆盖这种用法，在 vm-select 上加一条兜底：

```
-search.minStalenessInterval=5m
```

它把每条序列的 staleness 下限抬到 5 分钟，大于心跳即可。**心跳值必须小于这个下限。**

**只减写入压力（vmstorage 的 CPU 与 IOPS），不成比例减磁盘**——磁盘大头是按序列数算的索引，见 §五。小于 `interval` 的值会直接报错；空值或 0 退回原来每轮全写的行为。

上线后盯 `vm_streamaggr_skipped_unchanged_outputs_total` 应持续增长，并用 `increase()` 对一次账（不能用当前累计值）。

### 三处容易写错的地方

**`_metric_type!=""` 不能少。** PromQL 把缺失的标签当空值，所以 `{_metric_type!="gauge"}` 会把**根本没有类型标签的样本一并选中**。`acg-model-router-metrics` 那个 job 采的 `model_router_*` 就没有类型标签——它们会被 `sum_samples_total` 当成增量逐次累加，一个已经到 100 万的计数器每分钟再加 100 万。不报错、抓取照常成功。

**两条规则去掉 pod 的方式不一样，这是有意的，不要统一。** 计数器那条用 `drop_input_labels`，gauge 那条用 `without`。

- 计数器那条没有 dedup，`drop_input_labels` 更稳：`without` 是减法，将来 relabel 新增一个 per-pod 标签会被保留、series 静默按 pod 裂开；而且不写 `by`/`without` 时聚合器走 `aggregateOnlyByTime`，样本的 input key 是空的，省一次标签压缩。
- gauge 那条开了 `dedup_interval`，而 **`drop_input_labels` 在 dedup 之前删标签，`without` 在分组时删（之后）**。pod 要是提前没了，三个 pod 在去重器眼里就是同一条 series，每窗口只留一个样本，`sum_samples` 只加到一个 pod 的值。

  实测：三个 pod 各报 1，pod 放 `drop_input_labels` 得 **1**，放 `without` 得 **3**。不报错、series 数也对，只是值少三分之二。VM fork 里有 `TestGaugeRuleSumsAcrossPodsWithDedupOn` 钉住这个形态。

**per-pod 标签要列全。** 现网 relabel 把 `instance` 也设成了 pod 名，所以它和 `pod` 都必须在列表里；将来再加 per-pod 标签，**两条规则都要同步加**。`namespace`、`scraper_pod_namespace`、`step` 对所有 pod 相同，保留——**`step: 1m` 尤其不能删，RetentionFilter 靠它做分级保留**。

实测三个边车、50 个 Key，relabel 里故意加了个 `node_name`：VM 里 `acg_requests_total` 是 50 条 series（每 Key 一条）而不是 150 条，`acg_*` 里带 `pod` 标签的 0 条。

---

---

## 四、上线顺序与回退

**顺序不能反**：先上聚合层，再切边车。

1. 部署 `v1.126.1014-cluster` 的 vmagent + `aggr.yml`
2. 边车发版，但 `scrape.yml` 里 `metrics_path` 仍指 `/metrics/cumulative`
3. 确认 VM 里数字正常，再把 `metrics_path` 改成 `/metrics/usage`

反过来做——边车先报增量而聚合层不在——VM 会把增量当累计值存，数字直接错。

**回退**：把 `metrics_path` 改回 `/metrics/cumulative`，reload vmagent。不用重新发版。

那是个普通 handler，给累计值、不碰增量基线；它没有类型标签，两条聚合规则都不匹配，原样透传。回退后行为等同改造前：全量上报、内存回到老水平、数字正确。

### 已经部署过的：怎么升级

**换镜像**：拉 fork 的 `v1.126.1019-cluster` 构建。内网拉不到 GitHub 就在现有源码上按编号补打缺的补丁，打完自检：

```sh
ls patches/*.patch | wc -l                                             # 22
grep -c buildInputKey lib/streamaggr/streamaggr.go                     # 3
grep -c outputHeartbeatInterval lib/streamaggr/streamaggr.go           # >0
```

**改 values**：按 §二 整段对一遍。注意 `extraArgs` 里**不要有 `zstd.encoderConcurrency`**——它现在默认就是 1，写了反而会在旧镜像上让 vmagent 起不来。

**发布**：单副本 + `maxSurge: 0` 滚动更新有几十秒空窗，**不丢数据**——边车按「成功送达」才前移基线，空窗期的增量留在边车，恢复后补上。

**回滚**：改回原镜像 + 原 values。聚合规则若一并改过也要改回，**新旧规则不能混用**，否则同一指标会同时存在带 pod 和不带 pod 的两套序列。

---

## 五、配额

按**活跃 Key 的形态**分两档配，差 8 倍，先确认自己属于哪一档。

### 第一档：活跃 Key 基本固定（2000 活跃 / 3 万注册）

| | 存活堆 | 物理占用 | 建议配额 |
|---|---|---|---|
| 边车（每个） | 206 MiB | 299 MiB | 512Mi |
| vmagent | 148 MiB | 225 MiB | 512Mi |

### 第二档：全部 Key 都在活跃窗口内有量（最坏情况）

| | 存活堆 | 物理占用 | 建议配额 |
|---|---|---|---|
| 边车（每个） | 1215 MiB | 1435 MiB | **2Gi** |
| vmagent | 1224 MiB | 1517 MiB | **2Gi** |

vm-insert 412 MiB、vm-storage 每分片约 1.7 GiB、vm-select 11 MiB（后两者按可用内存自调缓存，按现网经验给即可）。

### 为什么差 8 倍

两端持有的都不是「此刻在忙的 Key」，而是「**最近一段时间内出现过的** Key」：聚合器按 `staleness_interval`（40 分钟）记，边车按 `IdleScrapes`（30 次抓取 = 30 分钟）记。

第二档那组数是**最坏情况**——测试里 3 万 Key 每 12 分钟全部轮一遍，等于没有一个是休眠的，两端都持有全量。真实环境通常不是这样：注册 3 万、常年有量的只有几千时，两端持有的就只有那几千，落在第一档附近。

**先量再配**：看 vmagent 的 `vm_streamaggr_labels_compressor_items_count`，或边车 `/debug/stats` 的 `children`，那才是实际持有量。

**别指望靠 `staleness_interval` 降内存。** 实测 40m → 5m 只省 **15.4%**（1224 → 1036 MiB），不是按窗口比例缩。省下的只是 `sync.Map` 的条目节点，而标签压缩器和字符串驻留按「**见过的全量 Key**」持有，跟窗口长度无关。

而且缩短它有个容易踩的副作用：累加器会不断过期归零，**`sum(acg_requests_total)` 这种直接读累计值的查询会得到几乎无意义的数**（实测比真实值低 85%）。`increase()` 能正确识别重置，所以对账要用 `increase()`，不能用当前值。

第二档的内存要降，方向是降基数（见 `vm-storage-cardinality.md`）。§三 的 `output_heartbeat_interval` 降的是写入压力，不是 vmagent 的内存。

### 增长口径

**边车随注册 Key 线性长**（每个被访问过的 Key 都要有常驻实例），**vmagent 跟活跃窗口内的 Key 数走**。所以 vmagent 按活跃形态配，边车按注册 Key 扛峰值。

存储另见 `docs/apikey-usage/vm-storage-cardinality.md`——结论是 VM 的磁盘和查询成本都由**序列数**决定，与写入频率无关，降存储只能降基数。


## 六、上线后

五条验收，都要过：

```sh
# 1. 没有 flag 报错
kubectl logs -n acg-system deploy/victoria-metrics-agent --tail=50 | grep -i "not defined"   # 无输出

# 2. 抓取正常
curl -s http://<vmagent>:8429/metrics | grep -E "^vm_promscrape_scrapes_failed_total|^vm_promscrape_max_scrape_size_exceeded_errors_total"   # 都是 0

# 3. 走的是流式解析
curl -s http://<vmagent>:8429/metrics | grep 'parse_mode_total{mode="one_shot"}'            # 0

# 4. 跳过不变的输出确实在生效
curl -s http://<vmagent>:8429/metrics | grep skipped_unchanged_outputs_total                # 应持续增长
```

```promql
# 5.【关键】跨 pod 真的合并了 —— 在 vm-select 上查
count({__name__=~"acg_.+",pod!=""})          # 必须是 0
count(acg_requests_concurrent_total)         # 应等于活跃 Key 数,不是它的 3 倍或 1/3
```

**第 5 条是这次改造的核心**，前四条过了但第 5 条不过，等于没生效。

再对一次账：取同一时间窗，VM 里的增量与边车侧的请求计数应当相等。

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
| vmagent 启动即退出，日志 `flag provided but not defined` | `extraArgs` 里配了当前镜像不认识的参数。`zstd.encoderConcurrency` 不该出现在配置里（默认已是 1），删掉；其余参数对照 §二 |
| 聚合配置启动报错 | vmagent 不是用 `v1.126.1014-cluster` 构建的。`sum_samples_total` 上游没有 |
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

**`staleness_interval` 缩短到 5m。** 报增量时它语义上确实可以短——40 分钟那个下限是为报累计值定的（聚合端忘掉还活着的闲置实例会导致整个累计值被重复计一遍），而报增量时状态被忘掉后累加器归零、下一个样本从 0 开始，`reset_marker_on_stale` 两端都补 0、`increase()` 正确识别，不丢不重。

但两种形态下都不值得：固定活跃集只省 8%，轮换形态实测也只省 15.4%（1224 → 1036 MiB）。**省下的只是 `sync.Map` 的条目节点**，trie 结构和字符串驻留都不随之释放。代价却是实打实的：累加器不断归零，任何直接读累计值的查询都会错（实测低 85%）。保持 40m。

**这个结论只对「活跃 Key 基本固定」成立。** Key 轮流活跃时，40 分钟窗口决定的是「要记住多少个 Key」，缩短它省的就不是 8% 而是成倍——见 §六 末尾那一节。

## 补丁

vmagent 补丁清单与打法见 `vm/vmagent/README.md`（20 个，编号 004~023 接内网现有的 001~003）。
