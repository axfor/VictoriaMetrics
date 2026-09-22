# vmagent 启动失败 + gauge 少算：修复手册

**影响**：当前 vmagent 镜像存在两个问题。

1. 传 `-zstd.encoderConcurrency` 会直接启动失败（`flag provided but not defined`）。
2. **更严重**：开了 `dedup_interval` 的聚合规则会把一个输出组里的所有输入序列挤进同一个 map 条目。你们 gauge 那条规则正好开着 dedup，**三个 higress pod 各报 1，合出来是 1——数字少三分之二，不报错、不告警、图照常画**。

第 2 条是正确性问题，**只有换镜像能解决**，没有配置层面的办法。

---

## 前置检查：确认当前版本

在任意一个 vmagent pod 里执行：

```sh
kubectl exec -n acg-system deploy/victoria-metrics-agent -- \
  /vmagent-prod -help 2>&1 | grep -c "zstd.encoderConcurrency"
```

- 输出 `0` → 镜像早于 `v1.126.1003-cluster`，两个问题都有，必须升级
- 输出非 0 → 版本已够，只需确认是否 ≥ 1011（见下）

---

## 步骤一：构建镜像

### 方式 A：直接用 fork 的 tag（能访问 GitHub 时）

```sh
git clone https://github.com/axfor/VictoriaMetrics.git
cd VictoriaMetrics
git checkout v1.126.1011-cluster
# 按现有 CI/构建流程编 vmagent 镜像,tag 建议带上版本,例如 acg-vmagent:1.126.1011
```

### 方式 B：在现有源码上补打补丁（内网拉不到 GitHub 时）

当前内网应已打到 **021**。补打剩下三个：

```sh
# patches/ 目录来自 fork 的 v1.126.1011-cluster,共 21 个(004~024)
git am patches/022-acg_streamaggr-dedup-input-key.patch
git am patches/023-acg_streamaggr-gauge-dedup-guard.patch
git am patches/024-acg_zstd-default-one-slot.patch
```

打完自检：

```sh
grep -c buildInputKey lib/streamaggr/output.go                    # 应为 2
grep -c buildInputKey lib/streamaggr/streamaggr.go                # 应为 3
test -f lib/encoding/zstd/concurrency.go && echo ok               # 应输出 ok
head -1 lib/encoding/zstd/concurrency.go                          # 应为 "package zstd",不能有 //go:build
grep -o 'encoderConcurrency", [0-9]' lib/encoding/zstd/concurrency.go   # 应为 ", 1"
```

**最后一条是 `, 0` 就说明 024 没打上**，虽然不影响正确性，但这个参数就还是必配项。

### 构建后验证（重要）

发布版 vmagent 是 `CGO_ENABLED=1` 构建的，而这正是原来出问题的地方。编完先验：

```sh
docker run --rm <你的镜像> /vmagent-prod -help 2>&1 | grep "zstd.encoderConcurrency"
```

**有输出才能往下走。** 没输出说明 022 没生效，别发布。

---

## 步骤二：改 helm values

`repos/aigateway-victoriametrics-conf/helm/victoria-metrics-agent/values.yaml`：

```yaml
extraArgs:
  promscrape.zstdCompression: "true"
  remoteWrite.queues: "2"
  remoteWrite.maxDiskUsagePerURL: "10GB"
```

**把 `zstd.encoderConcurrency: "1"` 整行删掉**（不是注释掉）。从 1011 起它默认就是 1，配置里不需要出现；留着它会在任何旧镜像上把 vmagent 搞挂。

### 顺带确认聚合规则（`extraObjects` 的 `vmagent-streamaggr` ConfigMap）

gauge 那条**必须是 `without`，不能是 `drop_input_labels`**：

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
  without: [pod, instance, node]        # ← 必须是 without
  outputs: [sum_samples]
  keep_metric_names: true
  dedup_interval: 60s
```

`drop_input_labels` 在 **dedup 之前**删标签，`without` 在分组时删（之后）。把 pod 写进前者，三个 pod 在去重器眼里就是同一条序列——和上面那个 bug 同样的少算后果。

---

## 步骤三：发布

```sh
helm upgrade victoria-metrics-agent <chart> -n acg-system -f values.yaml
kubectl rollout status deploy/victoria-metrics-agent -n acg-system
```

单副本 + `maxSurge: 0`，滚动更新有几十秒空窗。**不会丢数据**：边车按「成功送达」才前移基线，空窗期的增量留在边车，vmagent 恢复后补上。

---

## 步骤四：验证（四条，都要过）

```sh
# 1. 进程起来了,没有 flag 报错
kubectl logs -n acg-system deploy/victoria-metrics-agent --tail=50 | grep -i "not defined"
#    → 无输出

# 2. 抓取正常
curl -s http://<vmagent>:8429/metrics | grep -E "^vm_promscrape_scrapes_failed_total|^vm_promscrape_max_scrape_size_exceeded_errors_total"
#    → 两个都是 0

# 3. 解析走的是流式(内存优化生效)
curl -s http://<vmagent>:8429/metrics | grep 'parse_mode_total{mode="one_shot"}'
#    → 应为 0

# 4. 【关键】gauge 真的跨 pod 合并了
#    在 vm-select 上查:
count({__name__=~"acg_.+",pod!=""})
#    → 必须是 0(有 pod 标签说明没合并)

#    再对一条 gauge 的量级:
count(acg_requests_concurrent_total)
#    → 应等于活跃 Key 数,不是活跃 Key 数 × 3,也不该是它的 1/3
```

**第 4 条是这次修复的核心**，前三条过了但第 4 条不过，等于 bug 还在。

---

## 回滚

改回原镜像 + 原 values（把 `zstd.encoderConcurrency` 加回去）即可，`helm rollback`。

聚合规则如果也一起改了，回滚时要一并改回——**新旧规则不能混用**，否则同一指标会同时存在带 pod 和不带 pod 的两套序列。

---

## 常见问题

| 现象 | 原因 |
|---|---|
| 启动仍报 `flag provided but not defined` | `extraArgs` 里还留着那行，或镜像没换成功（确认 pod 用的是新 image digest） |
| 起来了但 `count({__name__=~"acg_.+",pod!=""})` 非 0 | 聚合规则的 `drop_input_labels` / `without` 写反了，见步骤二 |
| gauge 数值只有实际的 1/3 | 镜像还是旧的（022 没生效），或 gauge 规则把 pod 写进了 `drop_input_labels` |
| 聚合配置加载时 fatal 退出 | 用的是上游原版镜像，`sum_samples_total` 是 fork 才有的输出 |

---

完整集成说明见同目录 `integration.md`。
