# API Key 用量统计 · vmagent / VM / ruler 上线手册

版本：vmagent `v1.126.1030-cluster`、vm-select / vm-storage `v1.126.1027-cluster`（fork `github.com/axfor/VictoriaMetrics`）· ruler `v0.1.11`（Helm chart 0.1.14，仓库 `github.com/axfor/ruler`）

按顺序做。每一步做完先跑“确认”，通过了再做下一步。边车那侧见 [`app-side.md`](app-side.md)。

链路：model-router 边车 → vmagent → vm-insert → vm-storage；ruler 从 vm-select 读、往 vm-insert 写 1h / 1d 结果。

---

## 第 1 步：vm-select、vm-storage

换镜像，加启动参数，调资源。

镜像用 fork 的 tag `v1.126.1027-cluster` 源码构建：在仓库根目录执行 `make vmselect-linux-amd64-prod vmstorage-linux-amd64-prod`（需要 docker），产物是 `bin/vmselect-linux-amd64-prod` 和 `bin/vmstorage-linux-amd64-prod`，按现在的方式打进镜像。这两处改动是给 ruler 用的，不换镜像也能用，只是 ruler 首次启动补数慢一些。

**vm-select** 启动参数加上这三行：

```
-search.minStalenessInterval=5m
-dedup.minScrapeInterval=1ms
-search.maxQueryDuration=120s
```

`-dedup.minScrapeInterval=1ms` 在 vm-select 和 vm-storage 上**都必须有、且只能是 `1ms`**：vmagent 补写同一分钟时，靠它留下数值最大（也就是完整）的那个点。去掉或改成别的值，晚到的用量会被丢掉。

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

没换镜像就不要加最后一行，旧版不认识这个参数，会启动失败。

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

# 换了镜像的 vm-select 对 ruler 的请求头回 zstd
curl -s -o /dev/null -D - -H 'Accept-Encoding: zstd, gzip;q=0.5' \
  'http://vm-select:8481/select/0/prometheus/api/v1/export?match[]=up&start=-1h' | grep -i content-encoding
#   Content-Encoding: zstd
```

---

## 第 2 步：vmagent

### 镜像

这一步只换 vmagent。用 fork `github.com/axfor/VictoriaMetrics` 的 tag `v1.126.1030-cluster` 源码构建：在仓库根目录执行 `make vmagent-linux-amd64-prod`（需要 docker），产物是 `bin/vmagent-linux-amd64-prod`，按现在的方式打进镜像。vm-insert 的镜像不换，vm-select、vm-storage 按第 1 步换。

这个版本要打开 `remoteWrite.streamAggr.sampleTimestamps`（见下面的 `extraArgs`）：用量按 vmagent 抓取的那一分钟统计，一次抓取的 token、费用等永远记在同一分钟、同一小时，不会出现相邻两分钟一正一负。它依赖第 1 步的 `-dedup.minScrapeInterval=1ms`，两步都要做。

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
  remoteWrite.streamAggr.sampleTimestamps: "true"
```

- `remoteWrite.queues` 取 12，不要调小，否则 `_1m_by_*` 的输出发不完、漏点。

`remoteWrite.streamAggr.config` 如果你们现在写的是列表形式，保持你们的写法。

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

上面是用量的两条基础规则。`usage.yaml` 里还有给看板原始档用的 100 条 `_1m_by_*` 规则，以你们 values.yaml 里的为准，**每一条都加上 `flush_on_shutdown: true`**，否则 vmagent 每次重启（含发版）`_1m_by_*` 会丢掉最后不到一分钟的用量。

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

内存比上个版本多给 1Gi，是 `sampleTimestamps` 要的。

**CPU 请求必须给到 2 核**，不能是 `200m`。vmagent 没有限流，抢不到 CPU 时只会越积越多，最后边车的用量送不出去。

**不要改的：**

- `persistence.enabled: false`、`tmpDataPath` 用 emptyDir，保持现状。
- 不要加 `-remoteWrite.disableOnDiskQueue`、`zstd.encoderConcurrency`。
- 任何 job 都不要加 `sample_limit`、`series_limit`、`label_limit`。
- `replicaCount: 1`、硬反亲和、`maxSurge: 0` 保持现状。vmagent 只能有一个；要扩成多个，先按 fork 仓库 `patches/README.md` 里「多个 vmagent 实例」一节把配置和查询改完再扩，否则用量会少算。
- 不要加 `promscrape.deltaAck: "false"`。新版本默认就会和边车确认每一轮用量，vmagent 抓取超时也不会丢用量，不需要任何配置。

发布：`helm upgrade` vmagent。

**确认：**

```sh
kubectl -n acg-system logs deploy/victoria-metrics-agent --tail=100 | grep -iE 'fatal|not defined|cannot parse'   # 无输出
kubectl -n acg-system get pod | grep victoria-metrics-agent                                                    # Running
```

跑满一小时后看 vmagent 是否跟得上（`<vmagent>` 是 pod IP）：

```sh
curl -s http://<vmagent>:8429/metrics | grep '^vm_streamaggr_flush_timeouts_total' | awk '{s+=$2} END {print s}'   # 隔几分钟看两次，不再增长
curl -s http://<vmagent>:8429/metrics | grep 'vm_streamaggr_samples_lag_seconds_\(sum\|count\){.*position="1"' # sum/count 是平均落后秒数，应在 60 以内
```

平均落后超过 10 分钟，ruler 已经算完的小时会漏掉晚到的用量，要先加 CPU。

---

## 第 3 步：发边车

第 2 步做完**紧接着**按 [`app-side.md`](app-side.md) 发边车。两步之间 vmagent 从旧边车抓不到用量，这段时间的用量不统计。

**确认**（边车全部换完 10 分钟后，在 vm-select 上查）：

```promql
count({__name__=~"acg_.+",pod!=""})     # 0
count(acg_requests_total)               # 有值，约等于有流量的 Key 数
```

```sh
curl -s http://<vmagent>:8429/metrics | grep -E '^vm_promscrape_max_scrape_size_exceeded_errors_total|parse_mode_total\{mode="one_shot"\}'   # 都是 0
./verify-deploy.sh                                                                                                                     # 退出码 0
```

对账用 `sum(increase(acg_requests_total[1h]))`，不要用 `sum(acg_requests_total)`。结果应和边车实际处理的请求数**完全一致**，只差最近一两分钟还没写出的部分；有差额就是有问题，按“出问题时”排查。

---

## 第 4 步：ruler

第 1 步做完就可以做，和第 2、3 步互不依赖。以下命令里的 `deploy/...` 指 ruler 仓库 tag `v0.1.11`，`aigateway-victoriametrics-conf/...` 指你们的配置仓库。

### 4.1 停掉现有 ruler

把现有 ruler 的副本数缩到 0。新 ruler 启动时会从现有 ruler 在 VM 里写到的位置接着算，指标名不变。

### 4.2 换 CRD 和规则

规则从 v0.1.7 起换了写法：每个任务写完整的记录名，不再用 `{cron}` 占位符；轮次和读取粒度在 cron 上显式配置。先把配置仓库 `crd/aggregationrules/` 下的 4 个文件换成 ACGCluster 仓库 `docs/apikey-usage/repos/aigateway-victoriametrics-conf/crd/aggregationrules/` 下的新版本，旧写法新 ruler 不认。

新版本的 4 个文件，1h 都在每小时 **:10**、1d 都在 **00:30** 算：1h 是 `schedule: 10 * * * *`、`offset: {queryTime: -10m, insertTime: -1h}`，1d 是 `schedule: 30 0 * * *`、`offset: {queryTime: -30m, insertTime: -1d}`，算力池、并发两个文件也一样。queryTime 偏移必须是 cron 分钟的负数（窗口才从整点、零点开始，对不上 ruler 会报错不启动），insertTime 不变（结果时间戳是 `h:10`、`d 00:30`，和 monitor-api 的拼法对应）。**这一项必须和 monitor-api 的拼接改动一起上线**，见 [`monitor-api-ruler-delay.md`](monitor-api-ruler-delay.md)：否则每小时 :00–:10 看板会少掉上一个小时，每天 00:00–00:30 会少掉前一天。**顺序：先上 monitor-api，再 apply 这 4 个文件。** 新拼法在旧的 :03/00:05 下也对（原始档只是多读几分钟），反过来先改 cron 会有缺口。ruler v0.1.10 起改 cron 不用挑时间。

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

建议（改不改由你们定）：`acg-model-router-concurrency.yaml` 里 `model_router_stats_concurrent_requests_max` 的两个表达式，子查询里给序列加上回看，1h、1d 都改：

```
max_over_time(sum by (route) (last_over_time(model_router_stats_concurrent_requests{route!=""}[5m]))[1h:30s])
max_over_time(sum by (route) (last_over_time(model_router_stats_concurrent_requests{route!=""}[5m]))[1d:30s])
```

不改的话，由 1h 合成的 1d 和 VM 直接算的 1d 可能不一样。`[5m]` 要不小于这个指标最长的上报间隔。

### 4.3 镜像

在 ruler 仓库 tag `v0.1.11` 下打 x86 镜像：`deploy/lab/build.sh docker.io/acg/ruler:v0.1.11 amd64`（已打好的是 `ruler-v0.1.11-amd64.tar`）。已经装了 v0.1.8 到 v0.1.10 的，换镜像后 `helm upgrade ruler deploy/helm/ruler -n acg-system -f ruler-values.yaml`（4.4 的那个文件，不写 tag，镜像版本跟 chart 走）即可，规则不用动；不要用 `--reuse-values`，它会沿用旧 chart 的默认值。升级后各副本的分工会重排一次。有镜像仓库就推上去；没有就把生成的 tar 在每个节点上 `ctr -n k8s.io images import`。

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
  watermark: 'up{job="model-router-metrics"}'   # vmagent 积压时等它过了整点再读，最多等 30 分钟
```

`watermark` 里的 job 名要和第 2 步 vmagent 抓边车的 job 一致；以后改了那个 job 名，这里一起改。

```sh
helm install ruler deploy/helm/ruler -n acg-system -f ruler-values.yaml
```

其余都用默认值，不要改：1 个副本、每次最多读 30 万条序列、原始数据保留期自动从 RetentionFilter 读。内存 limit 不要低于 8Gi，上线第一天盯着主副本的内存。

**确认：**

```sh
kubectl -n acg-system logs ruler-0 | grep -E 'cron time zone|rules loaded|continuing|leading'
#   cron time zone ... now="... CST +08:00"          时区对
#   rules loaded groups=8 incremental=159 dependent=119 direct=6
#   continuing with the last window found written    接上了现有 ruler 的进度（找到的最后一个窗口会再写一遍，值相同）

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

**vmagent**：换回原来的镜像。原来的镜像早于 `v1.126.1028-cluster` 时，先在 `extraArgs` 里去掉 `remoteWrite.streamAggr.sampleTimestamps`（旧版不认识它，起不来），内存可以改回 6Gi。

**vm-select、vm-storage**：换回原来的镜像，并去掉 `-search.maxMetricIDsForDirectTSDBStatus` 那一行（旧版不认识它）。vm-storage 一台一台地换。

**ruler 的 :10 / 00:30**：把 4 个文件改回 1h `schedule: 3 * * * *`、`queryTime: -3m`（算力池、并发两个文件 `7 * * * *`、`-7m`），1d `schedule: 5 0 * * *`、`queryTime: -5m`（`9 0 * * *`、`-9m`），`insertTime` 不动，`kubectl apply`；monitor-api 的拼接改动一起撤回。queryTime 偏移必须是 cron 分钟的负数，这样窗口才从整点、零点开始，对不上 ruler 会报错不启动。

---

## 日常操作

- **看边车交给 vmagent 的内容**：在 vmagent 的 `/targets` 页面点对应边车的 `response` 链接。用 `v1.126.1030-cluster` 的 vmagent 和 `v1.24.1024` 的边车时，这样看不影响用量。不要直接 `curl` 边车的 `/metrics/usage`，要看就按 [`app-side.md`](app-side.md) 第 2 步带上 `peek` 请求头。
- **改规则或 RetentionFilter**：改配置仓库里的文件，`kubectl apply`。几秒内生效，不用重启 ruler。日志里会出现 `the rules changed; going on with them`。改 cron 和偏移也一样，不用挑时间。
- **补没写出的窗口**：读写失败按规则里 cron 的 `retry` 重试（现在是 10 次、隔 3 分钟），用完就跳过，记进 `failures`，进度照常往前，重启 ruler 也不会再算它，只能按下面的办法补。只补 `failures` 里列出的窗口（跳过的、不完整的），补成后会从列表里消失。每个副本的 `/api/v1/progress` 只列自己负责的规则。在哪个副本的 `failures` 里看到，就对那个副本调用；也可以在 `/api/v1/assignment?units=1` 里按记录名找到它所在副本的地址（下面以 `ruler-0` 为例）：

  ```sh
  kubectl -n acg-system port-forward pod/ruler-0 18080:8080 &
  curl -s localhost:18080/api/v1/recompute --data-urlencode record=<记录名> \
    --data-urlencode from=2026-09-26T14:00:00+08:00 --data-urlencode to=2026-09-26T14:00:00+08:00
  curl -s localhost:18080/api/v1/recompute        # 看进度
  ```

  超过 7 天的原始数据补不了。补了 1h 之后，返回里 `dependents` 列出的 1d 要等那天过完再补。已经写出的窗口会被拒绝，**原始数据晚到时不要补算**：下一个小时已经把晚到的量算进去了，补算会重复计数。确实要重写（比如开始时间之前、从没写过的窗口）时加 `--data-urlencode force=1`。
- **看各副本的分工是否均衡**：副本之间按负载分工，不按规则数，规则数相差很多是正常的。对任一副本：

  ```sh
  curl -s localhost:18080/api/v1/assignment     # 每个副本的地址、单元数、规则数、loadSecondsPerHour
  ```

  `loadSecondsPerHour` 最高的副本不超过平均值的 1.2 倍，就是均衡的。超出时，主副本每天 02:00 之后自动调整，一天最多挪 2 个单元。指标 `ruler_assigned_load_seconds_per_hour` 也是这个数，每个副本报自己的。
- **要主挂了秒级接管**（需要 3 个节点）：`helm upgrade ruler deploy/helm/ruler -n acg-system -f ruler-values.yaml --set replicas=3`。副本数随时可改，不用做别的。

---

## 出问题时

| 现象 | 怎么办 |
|---|---|
| vmagent 启动即退出，日志 `cannot parse stream aggregation config` 或 `sum_samples_total`、`output_heartbeat_interval` 不认识 | vmagent 镜像不是 `v1.126.1030-cluster` 构建的，按第 2 步换镜像 |
| vmagent 启动即退出，日志 `flag provided but not defined` | `extraArgs` 里有多余参数（如 `zstd.encoderConcurrency`），按第 2 步整段替换 |
| 用量数字翻倍 | `usage.yaml` 第一条的 `_metric_type!=""` 漏了，按第 2 步整个文件替换 |
| 查到带 `pod` 标签的 `acg_*` 序列 | relabel 里新加了按 pod 区分的标签。把它同时加进 `usage.yaml` 第一条的 `drop_input_labels` 和第二条的 `without` |
| gauge 的值只有实际的约 1/3 | 第二条规则把 `pod` 写进了 `drop_input_labels`，按第 2 步改回 `without` |
| vmagent 内存远高于 7Gi | 少了 `no_stale_markers: true`，或加了 `sample_limit`、`series_limit`，按第 2 步改 |
| vmagent 抓边车偶尔超时（`up` 偶尔为 0），担心丢用量 | vmagent 是 `v1.126.1030-cluster`、边车是 `v1.24.1024` 时不会丢，超时那一轮下一分钟补上。任一边还是旧版本时会丢，按第 2 步和 [`app-side.md`](app-side.md) 换版本 |
| VM 里突然没有新用量数据，但 `up` 是 1 | `vm_promscrape_max_scrape_size_exceeded_errors_total` 非零。`promscrape.maxScrapeSize` 设成 500MiB 并 `helm upgrade` vmagent，它不会自己恢复 |
| vm-storage 被 OOMKilled | 第 1 步的 8Gi 和 `-memory.allowedPercent=35` 没生效，重新核对 |
| vm-storage 启动即退出，日志 `flag provided but not defined: -search.maxMetricIDsForDirectTSDBStatus` | 镜像不是 `v1.126.1027-cluster` 构建的。按第 1 步换镜像，或者先去掉这一行 |
| ruler 启动即退出，日志 `flag provided but not defined: -read.concurrency`（或 `-write.concurrency`、`-start`） | 用了旧的部署清单，按 4.4 用 Helm 装 |
| apply 规则被拒，`concurrency ... must be of type object` 或 `start ... should match` | 规则文件被改过，用配置仓库 `crd/aggregationrules/` 里的 4 个原文件 |
| `helm install` 报 `image.repository` 必填 | 在 `ruler-values.yaml` 里写上镜像地址 |
| ruler 被 OOMKilled | 内存 limit 按 4.4 给 8Gi，不要调低。8Gi 仍被杀就先调大 limit，并把被杀时是一天中的第几个小时、主副本的内存曲线发回来 |
| ruler 指标 `ruler_reads_before_watermark_total` 在涨，1h 结果晚出 | 先查 4.4 里 `watermark` 的 job 名和 vmagent 的是否一致（不一致时每小时都会白等 30 分钟）；一致就是 vmagent 写入积压超过 30 分钟：查 vmagent 的落后和 VM 写入；积压期间读过的那一小时缺晚到的数据，gauge 的最大值会偏小 |
| ruler 三个副本的数据盘都丢了（raft 状态全丢） | 照 4.4 重新 `helm install` 即可：ruler 会在 VM 里找到每组最后写到的位置，接着补算到现在（7 天以内）；VM 里找不到结果的组从当天零点开始。要让这些组多补几天，在 `helm install` 之前给规则对象的那一组写上 `start: -2d` 这样的值并 apply（组一旦有了进度，`start` 就不再起作用）；已经装好之后，以及中间漏掉的窗口，都按"补没写出的窗口"加 `force=1` 补 |
| ruler 指标 `ruler_counter_series_from_zero_total` 在涨 | 有计数器序列中断超过 25 小时（vmagent 或 VM 中断、数据积压）后又接着报，或 ruler 重启时它已超过 1 小时没有样本：那一小时的值偏大，补算改不小。查中断原因，把记录名和时间发回来 |
| 看板分钟曲线时有空缺、时有尖刺，整点前后的小时数对不上 | vmagent 跟不上：按第 2 步的"确认"看平均落后和 `flush_timeouts`，检查 CPU 请求是不是 2 核 |
| 每次 vmagent 重启后，看板原始档比原始累计数据略低 | `_1m_by_*` 规则缺 `flush_on_shutdown: true`，按第 2 步补上 |
| vmagent 日志 `exceeds sample_limit` 或 `exceeding label_limit` | 有 job 加了 `sample_limit` / `label_limit`，按第 2 步去掉。被拒的那一轮不会丢也不会重复，去掉后下一分钟补上 |
| vmagent 被 OOMKilled | 内存 limit 按第 2 步给 7Gi。7Gi 仍被杀：先在 `extraArgs` 里去掉 `remoteWrite.streamAggr.sampleTimestamps` 应急（合计不受影响，只是分钟归属回到按处理时刻），并把被杀前的内存曲线发回来 |
| vmagent 积压时，某几分钟某些 Key 的用量是 0、下一分钟翻倍 | vmagent 跟不上，看第 2 步的确认、按需加 CPU。用量没丢：按小时、按天的合计不受影响 |
| vmagent 启动即退出，日志 `cannot be combined with ignore_old_samples` | 某条规则配了 `ignore_old_samples`，它会丢掉晚到的用量，删掉这一行 |

原理和实测数据（不影响集成）见 [`ruler-redesign.md`](ruler-redesign.md)、[`vm-storage-cardinality.md`](vm-storage-cardinality.md)、[`vmagent-rule-index.md`](vmagent-rule-index.md)。

