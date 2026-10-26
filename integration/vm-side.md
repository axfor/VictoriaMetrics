# API Key 用量统计 · vmagent / VM / ruler 上线手册

版本：vmagent、vm-insert、vm-select、vm-storage 都是 `v1.126.1036-cluster`（fork `github.com/axfor/VictoriaMetrics`，同一个 tag 构建）· ruler `v0.1.11`（Helm chart 0.1.14，仓库 `github.com/axfor/ruler`）

按顺序做。每一步做完先跑“确认”，通过了再做下一步。边车那侧见 [`app-side.md`](app-side.md)。

链路：model-router 边车 → vmagent → vm-insert → vm-storage；ruler 从 vm-select 读、往 vm-insert 写 1h / 1d 结果。

---

## 第 1 步：vm-insert、vm-select、vm-storage

换镜像，加启动参数，调资源。

镜像用 fork 的 tag `v1.126.1036-cluster` 源码构建：在仓库根目录执行 `make vminsert-linux-amd64-prod vmselect-linux-amd64-prod vmstorage-linux-amd64-prod`（需要 docker），产物是 `bin/vminsert-linux-amd64-prod`、`bin/vmselect-linux-amd64-prod`、`bin/vmstorage-linux-amd64-prod`，按现在的方式打进镜像。用这几个 `-linux-amd64-prod` 目标，不要用 `-pure-prod`。

vm-storage 一台一台地换；vm-insert 不用加参数、不用调资源，换完镜像即可。

**vm-select** 启动参数加上这三行：

```
-search.minStalenessInterval=5m
-dedup.minScrapeInterval=1ms
-search.maxQueryDuration=120s
```

`-dedup.minScrapeInterval=1ms` 在 vm-select 和 vm-storage 上**都必须有，且只能是 `1ms`**。

vm-select 的资源（4C6G）：

```yaml
resources:
  requests:
    cpu: "2"
    memory: 3Gi
  limits:
    cpu: "4"
    memory: 6Gi
```

**vm-storage** 启动参数加上这四行，现有的 `-retentionFilter*`、`-retentionPeriod` 等参数不动：

```
-dedup.minScrapeInterval=1ms
-storage.cacheSizeStorageMetricName=2GB
-memory.allowedPercent=35
-search.maxMetricIDsForDirectTSDBStatus=200000
```

没换镜像就不要加最后一行，旧版会启动失败。

vm-storage 的资源（4C8G）：

```yaml
resources:
  requests:
    cpu: "2"
    memory: 4Gi
  limits:
    cpu: "4"
    memory: 8Gi
```

**确认：**

```sh
curl -s http://vm-select:8481/flags | grep -E 'dedup|maxQueryDuration|minStaleness'
for i in 0 1 2; do curl -s http://vm-storage-$i.vm-storage:8482/flags | grep -E 'dedup|cacheSizeStorageMetricName|allowedPercent|maxMetricIDsForDirectTSDBStatus'; done

# 三个组件都是 v1.126.1036-cluster
for u in vm-select:8481 vm-insert:8480 $(for i in 0 1 2; do echo vm-storage-$i.vm-storage:8482; done); do echo "$u $(curl -s http://$u/metrics | grep -o 'short_version="[^"]*"')"; done
#   每一行都是 short_version="v1.126.1036-cluster"

# 换了镜像的 vm-select 对 ruler 的请求头回 zstd
curl -s -o /dev/null -D - -H 'Accept-Encoding: zstd, gzip;q=0.5' \
  'http://vm-select:8481/select/0/prometheus/api/v1/export?match[]=up&start=-1h' | grep -i content-encoding
#   Content-Encoding: zstd
```

---

## 第 2 步：vmagent

### 镜像

这一步只换 vmagent。用第 1 步同一个 tag `v1.126.1036-cluster` 构建：在仓库根目录执行 `make vmagent-linux-amd64-prod`（需要 docker），产物是 `bin/vmagent-linux-amd64-prod`，按现在的方式打进镜像。

**已经在用这个 fork 的 vmagent 的：**

- `v1.126.1030-cluster` ~ `v1.126.1035-cluster`：换镜像，`extraArgs` 里的 `remoteWrite.streamAggr.sampleTimestamps` 改名为 `remoteWrite.streamAggr.scrapeTimeIntervals`（值不变），`model-router-metrics` 这个 job 加上 `scrape_offset: 55s`，其余 values 不用动。
- `v1.126.1029-cluster`：换镜像，`extraArgs` 里加上 `remoteWrite.streamAggr.scrapeTimeIntervals: "true"`，内存改成下面的 5Gi / 7Gi。

### values.yaml

`victoria-metrics-agent` 的 values 里，下面几段整段替换成这样。

**`extraArgs`**：

```yaml
extraArgs:
  # 现有的，保持原样
  envflag.enable: "true"
  envflag.prefix: VM_
  loggerFormat: json
  remoteWrite.streamAggr.config: /etc/vmagent/streamaggr/usage.yaml
  # 新增
  promscrape.zstdCompression: "true"
  promscrape.maxScrapeSize: "500MiB"
  remoteWrite.queues: "12"
  remoteWrite.maxDiskUsagePerURL: "10GB"
  remoteWrite.streamAggr.scrapeTimeIntervals: "true"
```

- `remoteWrite.queues` 取 12，不要调小。
- vmagent 只有一个 `remoteWrite.url`（vm-insert）时照上面写。有多个时，`remoteWrite.streamAggr.scrapeTimeIntervals` 按 `remoteWrite.url` 的顺序逐个写，如 `"true,false"`：只有按第 1 步配了 `-dedup.minScrapeInterval=1ms` 的存储写 `true`。
- `remoteWrite.streamAggr.config` 如果你们现在写的是列表形式，保持你们的写法。

**`extraScrapeConfigs`**：整段替换成下面这段。两个 job 都是你们现有的，只有标了 `# 改`、`# 新增` 的行不一样，其余照旧。

```yaml
extraScrapeConfigs:
  - job_name: acg-model-router-metrics
    scrape_interval: 60s            # 改
    scrape_timeout: 50s             # 改
    scrape_align_interval: 60s      # 新增
    no_stale_markers: true          # 新增
    metrics_path: /metrics
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_container_name]
        action: keep
        regex: "model-router"
      - source_labels: [__meta_kubernetes_pod_container_port_number]
        action: keep
        regex: "9090"
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: instance
      - target_label: step
        replacement: '1m'
    metric_relabel_configs:
      - source_labels: [__name__]
        action: keep
        regex: model_router_.+

  - job_name: model-router-metrics
    scrape_interval: 60s
    scrape_timeout: 50s             # 改
    scrape_align_interval: 60s      # 新增
    scrape_offset: 55s              # 新增
    no_stale_markers: true          # 新增
    metrics_path: /metrics/usage    # 改
    kubernetes_sd_configs:
      - namespaces:
          names:
            - acg-system
        role: pod
    relabel_configs:
      - action: keep
        regex: model-router
        source_labels: [__meta_kubernetes_pod_container_name]
      - action: keep
        regex: "9090"
        source_labels: [__meta_kubernetes_pod_container_port_number]
      - action: replace
        regex: ([^:]+)(?::\d+)?
        replacement: $1:9090
        source_labels: [__address__]
        target_label: __address__
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: instance
      - source_labels: [__meta_kubernetes_namespace]
        target_label: scraper_pod_namespace
      - replacement: 1m
        target_label: step
    metric_relabel_configs:
      - action: keep
        regex: acg_.+
        source_labels: [__name__]
```

**`vmagent-streamaggr` ConfigMap 里的 `usage.yaml`**：整个文件换成下面这样。

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

`usage.yaml` 里给看板原始档用的 100 条 `_1m_by_*` 规则以你们 values.yaml 里的为准，**每一条都加上 `flush_on_shutdown: true`**。

**vmagent 的资源**：

```yaml
resources:
  requests:
    cpu: "2"
    memory: 5Gi
  limits:
    cpu: "4"
    memory: 7Gi
```

- **CPU 请求必须给到 2 核**，不能是 `200m`。
- 内存先按上面给。跑满一周后可以下调：`limits.memory` 改成这一周 vmagent 内存峰值的 2 倍（不低于 4Gi），`requests.memory` 改成峰值的 1.2 倍。峰值看容器监控的 working set，或 vmagent `/metrics` 里的 `process_resident_memory_bytes`。

**不要改的：**

- `persistence.enabled: false`、`tmpDataPath` 用 emptyDir，保持现状。
- 不要加 `-remoteWrite.disableOnDiskQueue`、`zstd.encoderConcurrency`、`promscrape.deltaAck: "false"`。
- 任何 job 都不要加 `sample_limit`、`series_limit`、`label_limit`。
- `replicaCount: 1`、硬反亲和、`maxSurge: 0` 保持现状。要扩成多个 vmagent，先按 fork 仓库 `patches/README.md` 里「多个 vmagent 实例」一节改完配置和查询再扩。

发布：`helm upgrade` vmagent。

**确认**（`<vmagent>` 是 vmagent 的 pod IP）：

```sh
kubectl -n acg-system logs deploy/victoria-metrics-agent --tail=100 | grep -iE 'fatal|not defined|cannot parse'   # 无输出
kubectl -n acg-system get pod | grep victoria-metrics-agent                                                    # Running
curl -s http://<vmagent>:8429/metrics | grep -o 'short_version="[^"]*"'                                        # short_version="v1.126.1036-cluster"
curl -s http://<vmagent>:8429/flags | grep scrapeTimeIntervals                                                 # -remoteWrite.streamAggr.scrapeTimeIntervals="true"
```

跑满一小时后：

```sh
curl -s http://<vmagent>:8429/metrics | grep '^vm_streamaggr_flush_timeouts_total' | awk '{s+=$2} END {print s}'   # 隔几分钟看两次，不再增长
curl -s http://<vmagent>:8429/metrics | grep 'vm_streamaggr_samples_lag_seconds_\(sum\|count\){.*position="1"' # sum/count 应在 60 以内
```

sum/count 超过 600 时先加 CPU。

---

## 第 3 步：发边车

第 2 步做完**紧接着**按 [`app-side.md`](app-side.md) 发边车。

**确认**（边车全部换完 10 分钟后，在 vm-select 上查）：

```promql
count({__name__=~"acg_.+",pod!=""})     # 0
count(acg_requests_total)               # 有值，约等于有流量的 Key 数
```

```sh
curl -s http://<vmagent>:8429/metrics | grep -E '^vm_promscrape_max_scrape_size_exceeded_errors_total|parse_mode_total\{mode="one_shot"\}'   # 都是 0
./verify-deploy.sh                                                                                                                     # 退出码 0
```

对账用 `sum(increase(acg_requests_total[1h]))`，不要用 `sum(acg_requests_total)`。结果应和边车实际处理的请求数**完全一致**（最近一两分钟除外），有差额按“出问题时”排查。

---

## 第 4 步：ruler

第 1 步做完就可以做，和第 2、3 步互不依赖。以下命令里的 `deploy/...` 指 ruler 仓库 tag `v0.1.11`，`aigateway-victoriametrics-conf/...` 指你们的配置仓库。

### 4.1 停掉现有 ruler

把现有 ruler 的副本数缩到 0。

### 4.2 换 CRD 和规则

把配置仓库 `crd/aggregationrules/` 下的 4 个文件换成 ACGCluster 仓库 `docs/apikey-usage/repos/aigateway-victoriametrics-conf/crd/aggregationrules/` 下的新版本。

这 4 个文件**必须和 monitor-api 的拼接改动一起上线**，见 [`monitor-api-ruler-delay.md`](monitor-api-ruler-delay.md)。**顺序：先上 monitor-api，再 apply 这 4 个文件。**

```sh
# 留底，回退用
kubectl get crd aggregationrules.ruler.platform.axx.com -o yaml > old-crd.yaml
kubectl -n acg-system get aggregationrules -o yaml > old-rules.yaml

# 删旧 CRD（旧的 8 个规则对象会一起删掉），装新 CRD 和 4 个新规则
kubectl delete crd aggregationrules.ruler.platform.axx.com
kubectl apply -f deploy/crd/aggregationrules.yaml
kubectl wait --for condition=established crd/aggregationrules.ruler.platform.axx.com
kubectl apply -f aigateway-victoriametrics-conf/crd/aggregationrules/
```

**确认：** `kubectl -n acg-system get aggregationrules` 只有 4 个对象。

可选：`acg-model-router-concurrency.yaml` 里 `model_router_stats_concurrent_requests_max` 的两个表达式改成下面这样，1h、1d 都改。`[5m]` 要不小于这个指标最长的上报间隔。

```
max_over_time(sum by (route) (last_over_time(model_router_stats_concurrent_requests{route!=""}[5m]))[1h:30s])
max_over_time(sum by (route) (last_over_time(model_router_stats_concurrent_requests{route!=""}[5m]))[1d:30s])
```

### 4.3 镜像

在 ruler 仓库 tag `v0.1.11` 下打 x86 镜像：`deploy/lab/build.sh docker.io/acg/ruler:v0.1.11 amd64`（已打好的是 `ruler-v0.1.11-amd64.tar`）。有镜像仓库就推上去；没有就把 tar 在每个节点上 `ctr -n k8s.io images import`。

已经装了 v0.1.8 ~ v0.1.10 的：换镜像后执行 `helm upgrade ruler deploy/helm/ruler -n acg-system -f ruler-values.yaml`（4.4 的那个文件，不写 tag），规则不用动。不要用 `--reuse-values`。

### 4.4 Helm 安装

写一个 `ruler-values.yaml`：

```yaml
image:
  repository: docker.io/acg/ruler      # 换成你们的镜像地址
persistence:
  storageClass: <你们的存储类>          # 不写就用集群默认存储类
resources:                             # 2C8G
  requests:
    cpu: "2"
    memory: 3Gi
  limits:
    cpu: "2"
    memory: 8Gi
read:
  watermark: 'up{job="model-router-metrics"}'
```

`watermark` 里的 job 名要和第 2 步 vmagent 抓边车的 job 一致；以后改了那个 job 名，这里一起改。

```sh
helm install ruler deploy/helm/ruler -n acg-system -f ruler-values.yaml
```

其余都用默认值，不要改。内存 limit 不要低于 8Gi，上线第一天盯着主副本的内存。

**确认：**

```sh
kubectl -n acg-system logs ruler-0 | grep -E 'cron time zone|rules loaded|continuing|leading'
#   cron time zone ... now="... CST +08:00"
#   rules loaded groups=8 incremental=159 dependent=119 direct=6
#   continuing with the last window found written

kubectl -n acg-system port-forward pod/ruler-0 18080:8080 &
curl -s localhost:18080/raft/status          # "leader":true
curl -s localhost:18080/api/v1/progress      # 每条规则算到哪、哪些窗口没写出
```

**告警：** 把 ruler 仓库的 `deploy/alerts.yaml` 加进 vmalert 的规则；ruler 的指标按这个文件开头的 `job_name: ruler` 那段加到 vmagent 的抓取配置里。

---

## 回退

**用量链路**：把 `model-router-metrics` 这个 job 改成下面三行，`helm upgrade` vmagent。边车不用重新发版。

```yaml
    metrics_path: /metrics/cumulative
    scrape_interval: 120s
    scrape_timeout: 60s
```

**ruler**：

```sh
helm uninstall ruler -n acg-system
kubectl delete crd aggregationrules.ruler.platform.axx.com
kubectl apply -f old-crd.yaml
kubectl wait --for condition=established crd/aggregationrules.ruler.platform.axx.com
kubectl apply -f old-rules.yaml
```

然后把现有 ruler 扩回原来的副本数。

**vmagent**：换回原来的镜像，同时改 `extraArgs`：原来的镜像是 `v1.126.1028-cluster` ~ `v1.126.1035-cluster` 时，把 `remoteWrite.streamAggr.scrapeTimeIntervals` 改回 `remoteWrite.streamAggr.sampleTimestamps`（旧镜像不认新名，会启动失败）；早于 `v1.126.1028-cluster` 时，去掉这一行，内存改回 6Gi。

**vm-insert、vm-select、vm-storage**：换回原来的镜像，vm-storage 去掉 `-search.maxMetricIDsForDirectTSDBStatus` 那一行。vm-storage 一台一台地换。

**ruler 的 :10 / 00:30**：把 4 个文件改回 1h `schedule: 3 * * * *`、`queryTime: -3m`（算力池、并发两个文件 `7 * * * *`、`-7m`），1d `schedule: 5 0 * * *`、`queryTime: -5m`（`9 0 * * *`、`-9m`），`insertTime` 不动，`kubectl apply`；monitor-api 的拼接改动一起撤回。

---

## 日常操作

- **看边车交给 vmagent 的内容**：在 vmagent 的 `/targets` 页面点对应边车的 `response` 链接。不要直接 `curl` 边车的 `/metrics/usage`，要看就按 [`app-side.md`](app-side.md) 第 2 步带上 `peek` 请求头。
- **改规则、cron 或 RetentionFilter**：改配置仓库里的文件，`kubectl apply`，不用重启 ruler。日志里会出现 `the rules changed; going on with them`。
- **补没写出的窗口**：只补 `/api/v1/progress` 的 `failures` 里列出的窗口。在列出它的那个副本上调用，或在 `/api/v1/assignment?units=1` 里按记录名找到它所在的副本（下面以 `ruler-0` 为例）：

  ```sh
  kubectl -n acg-system port-forward pod/ruler-0 18080:8080 &
  curl -s localhost:18080/api/v1/recompute --data-urlencode record=<记录名> \
    --data-urlencode from=2026-09-26T14:00:00+08:00 --data-urlencode to=2026-09-26T14:00:00+08:00
  curl -s localhost:18080/api/v1/recompute        # 看进度
  ```

  - 超过 7 天的原始数据补不了。
  - 补了 1h 之后，返回里 `dependents` 列出的 1d 要等那天过完再补。
  - **原始数据晚到时不要补算**。
  - 确实要重写已写出的窗口时，加 `--data-urlencode force=1`。
- **看各副本的分工是否均衡**：对任一副本 `curl -s localhost:18080/api/v1/assignment`。`loadSecondsPerHour` 最高的副本不超过平均值的 1.2 倍就是均衡的；超出时主副本每天 02:00 之后自动调整，不用处理。
- **要主挂了秒级接管**（需要 3 个节点）：`helm upgrade ruler deploy/helm/ruler -n acg-system -f ruler-values.yaml --set replicas=3`。

---

## 出问题时

| 现象 | 怎么办 |
|---|---|
| vmagent 启动即退出，日志 `cannot parse stream aggregation config` 或 `sum_samples_total`、`output_heartbeat_interval` 不认识 | vmagent 镜像不是 `v1.126.1036-cluster` 构建的，按第 2 步换镜像 |
| vmagent 启动即退出，日志 `flag provided but not defined: -remoteWrite.streamAggr.scrapeTimeIntervals` | 镜像早于 `v1.126.1036-cluster`：按第 2 步换镜像，或把这一行改回 `remoteWrite.streamAggr.sampleTimestamps` |
| vmagent 启动即退出，日志 `flag provided but not defined` | `extraArgs` 里有多余参数（如 `zstd.encoderConcurrency`），按第 2 步整段替换 |
| vmagent 启动即退出，日志 `cannot be combined with ignore_old_samples` | 删掉那条规则里的 `ignore_old_samples` |
| 用量数字翻倍 | `usage.yaml` 第一条的 `_metric_type!=""` 漏了，按第 2 步整个文件替换 |
| 查到带 `pod` 标签的 `acg_*` 序列 | relabel 里新加了按 pod 区分的标签。把它同时加进 `usage.yaml` 第一条的 `drop_input_labels` 和第二条的 `without` |
| gauge 的值只有实际的约 1/3 | 第二条规则把 `pod` 写进了 `drop_input_labels`，按第 2 步改回 `without` |
| vmagent 内存远高于 7Gi | 补上 `no_stale_markers: true`，去掉 `sample_limit`、`series_limit`，按第 2 步改 |
| vmagent 被 OOMKilled | 内存按第 2 步给 7Gi，下调过的先改回 7Gi。7Gi 仍被杀：在 `extraArgs` 里去掉 `remoteWrite.streamAggr.scrapeTimeIntervals` 应急，并把被杀前的内存曲线发回来 |
| vmagent 日志 `exceeds sample_limit` 或 `exceeding label_limit` | 按第 2 步去掉 `sample_limit` / `label_limit` |
| VM 里突然没有新用量数据，但 `up` 是 1 | `vm_promscrape_max_scrape_size_exceeded_errors_total` 非零：`promscrape.maxScrapeSize` 设成 500MiB 并 `helm upgrade` vmagent |
| vmagent 抓边车偶尔超时（`up` 偶尔为 0） | 确认 vmagent 是 `v1.126.1036-cluster`、边车是 `v1.24.1029`，不是就换版本 |
| 每次 vmagent 重启或发版后，用量比边车实际少一分钟左右 | vmagent 换到 `v1.126.1036-cluster`、边车换到 `v1.24.1029`；`_1m_by_*` 规则按第 2 步补上 `flush_on_shutdown: true` |
| vmagent 重启那一刻，某个时间点上只有费用、没有 token（或反过来） | vmagent 换到 `v1.126.1036-cluster` |
| 看板分钟曲线时有空缺、尖刺，整点前后的小时数对不上；或某几分钟用量是 0、下一分钟翻倍 | vmagent 跟不上：按第 2 步的“确认”看落后和 `flush_timeouts`，CPU 请求给到 2 核，需要时再加 |
| vm-storage 被 OOMKilled | 核对第 1 步的 8Gi 和 `-memory.allowedPercent=35` |
| vm-storage 启动即退出，日志 `flag provided but not defined: -search.maxMetricIDsForDirectTSDBStatus` | 按第 1 步换镜像，或者先去掉这一行 |
| ruler 启动即退出，日志 `flag provided but not defined: -read.concurrency`（或 `-write.concurrency`、`-start`） | 按 4.4 用 Helm 装 |
| apply 规则被拒，`concurrency ... must be of type object` 或 `start ... should match` | 用配置仓库 `crd/aggregationrules/` 里的 4 个原文件 |
| `helm install` 报 `image.repository` 必填 | 在 `ruler-values.yaml` 里写上镜像地址 |
| ruler 被 OOMKilled | 内存 limit 按 4.4 给 8Gi。8Gi 仍被杀就先调大，并把被杀的时间和主副本的内存曲线发回来 |
| ruler 指标 `ruler_reads_before_watermark_total` 在涨，1h 结果晚出 | 先核对 4.4 里 `watermark` 的 job 名和 vmagent 的一致；一致就按第 2 步的“确认”查 vmagent 的落后和 VM 写入 |
| ruler 三个副本的数据盘都丢了 | 照 4.4 重新 `helm install`。要多补几天，在 `helm install` 之前给规则对象的那一组写上 `start: -2d` 这样的值并 apply；装好之后漏掉的窗口按“补没写出的窗口”加 `force=1` 补 |
| ruler 指标 `ruler_counter_series_from_zero_total` 在涨 | 查 vmagent 或 VM 有没有中断过，把记录名和时间发回来 |
