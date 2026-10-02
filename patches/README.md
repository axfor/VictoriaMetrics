# VictoriaMetrics 补丁:vmagent、vmselect、vmstorage(基于 **v1.126.0-cluster**)

> 本文件只讲补丁本身。集成步骤、最佳配置、配额、以及已部署环境怎么升级,见 ACGCluster 仓库的 `docs/apikey-usage/integration.md`。

二十八个补丁,**按编号顺序打**。004~027、030、031 是 vmagent 的,028 是 vmselect 的,029 是 vmstorage 的。源码是 fork `github.com/axfor/VictoriaMetrics`
(本地 `/Users/axx/code/VictoriaMetrics`)分支 `v1.126.1000-cluster`,基线为上游 tag `v1.126.0-cluster`,
一个补丁一个提交。

| 补丁 | 解决什么 | 必要性 |
|---|---|---|
| `004-acg_promscrape-zstd` | 抓取端支持 zstd 响应(下文第一部分) | 可选,省带宽 |
| `005-acg_streamaggr-staleness-0` | 聚合输出因 staleness 过期时补发 0,修低频 key 少算(第二部分) | **必须** |
| `006-acg_streamaggr-flush-timestamp` | 聚合刷新时间戳不再重复(第三部分;测试依赖 005) | **必须** |
| `007-acg_streamaggr-leading-zero` | 新建的聚合输出先补一个 0,修 vmagent 重启或崩溃后低频 key 少算(第四部分) | **必须** |
| `008-acg_promscrape-stream-without-body` | 流式解析时不再整块解压响应(第五部分) | 强烈建议,物理占用 26.94 → 4.03 GiB |
| `009-acg_streamaggr-sum-samples-total` | 新增 `sum_samples_total` 输出:边车报增量时按输出累加,不存各实例明细(第六部分) | **必须**,缺了聚合配置直接报错起不来 |
| `010-acg_streamaggr-shutdown-flush-timestamp` | 停机刷出不再用未来的时间戳(第七部分) | **必须** |
| `011-acg_docs-sum-samples-total` | 补文档:`sum_samples_total` 与 `reset_marker_on_stale` 的说明,并把新输出加进上游基准 | 可选 |
| `012-acg_promutil-sort-labels` | 标签排序不走 sort.Interface | 可选,性能 |
| `013-acg_promutil-labels-cache` | 标签查找加每 goroutine 缓存;012+013 合计让聚合 push 路径快 27% | 可选,性能 |
| `014-acg_promutil-labels-sort-bench` | 给上面两条补一个按暴露端真实标签顺序的基准 | 可选,只加基准 |
| `015-acg_storage-test-timezone` | `TestStorageDeletePendingSeries` 不再依赖本机时区 | 可选但几乎零成本,只改 9 行;不打的话 UTC 以西的机器上这个测试必挂 |
| `016-acg_streamaggr-type-label-selector-test` | 钉住类型标签选择器的匹配语义,防止聚合配置里的 `_metric_type!=""` 被当成冗余删掉 | 可选,只加测试 |
| `017-acg_promscrape-parse-mode-metric` | 导出每次抓取实际走的解析模式,让"未压缩响应有没有进内存"这件事可观测 | **强烈建议**,见下文 |
| `018-acg_streamaggr-input-key-only-when-read` | 没有输出读 input key 时不再压缩它;`sum_samples_total` 耗时 −42%、分配 −51% | 建议,纯性能 |
| `019-acg_promutil-label-sort-order-test` | 钉住标签排序的结果顺序,防止 012/013 的改动悄悄改变语义 | 可选,只加测试 |
| `020-acg_streamaggr-acg-shape-bench` | 按 ACG 实际配置形态(`drop_input_labels` + 不写 by/without)补聚合 push 基准 | 可选,只加基准 |
| `021-acg_zstd-encoder-concurrency` | 新增 `-zstd.encoderConcurrency`,限制 zstd 编码器并发槽位;每槽一个 8 MB 历史窗口,默认按核数开,64 核就是 512 MB 永不释放 | **强烈建议**,vmagent 存活堆 346 → 271 MB |
| `022-acg_streamaggr-dedup-input-key` | 修 018 的一处严重错误:它复用了 `useInputKey`,而这个字段在 `dedup_interval > 0` 时本来就是 false,导致开 dedup 时一个输出组的所有输入序列挤进同一个 map 条目——三个 pod 各报 1 合出来是 1。顺带修 017 的 `SizeBytes()`/`Len()` 不一致,以及 021 的 flag 只在 !cgo 下声明(发布版是 CGO_ENABLED=1,传参直接起不来) | **必须**,打了 018 就必须打这个 |
| `023-acg_streamaggr-gauge-dedup-guard` | 钉住「开了 dedup 的 gauge 规则仍然跨 pod 求和」:`drop_input_labels` 在 dedup 之前删标签,`without` 在分组时删(之后),把 pod 写进前者会让三个 pod 在去重器眼里是同一条序列,只算一个 | 可选,只加测试 |
| `024-acg_zstd-default-one-slot` | `-zstd.encoderConcurrency` 默认值改成 1,让它不必出现在部署配置里。一个纯内存优化不该有能力让进程起不来:旧镜像上传这个参数会 `flag provided but not defined` 直接退出 | **建议**,少一个必配项 |
| `025-acg_streamaggr-output-heartbeat` | 新增 `output_heartbeat_interval`:`sum_samples_total` 跳过值没变的刷出,只在心跳到期时补写。实测轮换形态下四分之三的输出序列每轮都在重写同一个值 | 可选,减少写入压力 |
| `026-acg_streamaggr-sharded-outputs` | 聚合输出条目从 `sync.Map` 改为 256 个分片的普通 map:3 万 key 时近千万条输出序列,堆剖面里三分之一是 `sync.Map` 的 trie 节点而不是数据。每条输出序列 434 → 360 字节 | 建议,纯内存 |
| `027-acg_streamaggr-inline-single-output` | 单个输出直接放在条目里,`sum_samples_total` 的值与状态一次分配。每条输出序列 360 → 313 字节(两条合计 −28%) | 建议,纯内存;依赖 026 |
| `028-acg_httpserver-zstd-response` | 客户端在 `Accept-Encoding` 里给 zstd 的权重高于 gzip(或相同且排在前面)时,响应改用 zstd 压缩(第八部分);其余客户端照旧拿 gzip | 可选,ruler 读导出时省 vmselect 的 CPU;没有它 ruler 退回 gzip |
| `029-acg_storage-tsdb-status-from-names` | 带 `match[]` 的 `/api/v1/status/tsdb` 在匹配数不超过新参数时从匹配序列的指标名统计,不再扫当天全部倒排索引(第九部分)。实验集群 2.4 亿序列的一天:29–41 秒 → 0.06–0.38 秒,结果逐项相同 | **建议**(ruler 切分片要问它),需在 vmstorage 上加参数才生效 |
| `030-acg_streamaggr-sample-timestamps` | 新增 `sample_timestamps`:`sum_samples`、`sum_samples_total` 按样本的抓取时刻打时间戳,vmagent 处理落后时用量仍记在抓取的那一分钟;抓取 tick 晚于间隔十分之一时改用实际开始时刻(第十部分) | **建议**,配合 ruler 推迟到 :10 / 00:30;要加参数才生效 |
| `031-acg_streamaggr-name-index` | 流式聚合按 `match` 里写死的指标名索引规则,每条样本只交给可能命中它的规则,输出逐条不变(第十一部分)。ACG 的 102 条规则全规模本机复现:vmagent CPU 1.30 → 0.82 核 | **建议**,纯性能,不用改配置 |

只动源码的是 28 个文件:028 改了 `lib/httpserver/httpserver.go` 并新增 `zstd.go`,029 改了 `lib/storage/index_db.go` 和 `app/vmstorage/main.go`;
030 加的参数在 `app/vmagent/remotewrite/streamaggr.go`,031 加的判定在 `lib/promrelabel/if_expression.go`;其余 22 个是`lib/encoding/zstd/{concurrency,zstd_pure}.go`、`lib/promscrape/{client,scrapework}.go`、
`lib/promutil/{labels,labelscompressor}.go`、`lib/streamaggr/` 下的 `deduplicator.go`、`output.go`、`streamaggr.go`
以及每个输出类型各自的文件(`avg.go`、`sum_samples.go`、`total.go` 等,补丁 018 给它们各加了一个
`needsInputKey()`)。其中 `concurrency.go` 和 `sum_samples.go` 是新文件。
冲突热点是 `streamaggr.go`(11 个补丁碰它)和 `output.go`(8 个)。

## 基线:为什么是集群版

内网跑的是 vm-insert / vm-select / vm-storage,即 VictoriaMetrics 集群模式,
所以基线是 `v1.126.0-cluster`,不是单机的 `v1.126.0`。两者不是一回事:
201 个文件、+13207 / −6772,集群版新增 `lib/vmselectapi`、`lib/handshake`、`lib/netutil`,
重写 `app/vmselect` / `app/vminsert` / `app/vmstorage` / `lib/storage`,
并且没有单机的全合一二进制 `app/victoria-metrics`。

**但我们改的部分两版完全一样。** 25 个文件里 24 个逐字节相同,包括全部 8 个源码文件
(`promscrape/{client,scrapework}.go`、`promutil/{labels,labelscompressor}.go`、
`streamaggr/{deduplicator,output,streamaggr,sum_samples}.go`)——上游集群化动的是
`app/vm*` 和存储层,和我们不重叠。唯一文件内容不同的 `lib/storage/storage_test.go`
是上游自己就差 516 行,我们的补丁在两边加的是同样两处 `.UTC()`,而且它是测试不进二进制。

集群版里这些共用代码的调用面反而**更窄**:`lib/streamaggr` 单机版由 vmagent 和 vminsert
共用,集群版只有 vmagent——**集群模式下流式聚合只能放在 vmagent**,vm-insert 没有这个能力。

## 部署检查:内存优化默认是关的

补丁 `008` 让未压缩的响应永不进内存(实测 26.94 → 4.03 GiB),但它有**三个静默失效条件**:

- `no_stale_markers` 必须为 true —— **它默认是 false**
- `sample_limit` 设了就失效
- `series_limit` 设了就失效

三者都不报错、不告警,抓取照常成功、数字照常正确,只是进程把整份未压缩响应扛在内存里。
很多生产配置会加 `sample_limit` 防基数爆炸,一加就把这个补丁关掉了。

补丁 `017` 把它变成可观测的。**上线后第一件事**是确认:

```
vm_promscrape_scrapes_by_parse_mode_total{mode="stream_without_body"}   # 应持续涨
vm_promscrape_scrapes_by_parse_mode_total{mode="one_shot"}              # 应停在 0
```

2026-09-20 在真 vmagent 上三档实测,同一个边车、209 MiB 的响应、各跑 18 秒:

| 抓取配置 | one_shot | stream | stream_without_body |
|---|---|---|---|
| `no_stale_markers: true` | 0 | 1 | **2** |
| 不设 `no_stale_markers`(默认) | 0 | 4 | **0** |
| `no_stale_markers` + `sample_limit` | **4** | 0 | **0** |

两点比预想的更要紧:

- **`sample_limit` 不只是丢掉 `stream_without_body`,是一路掉到 `one_shot`** ——
  连普通流式解析都关了,209 MiB 的 body 当一整块解析。`canSwitchToStreamParseMode()`
  同时挡住了两条路。
- **进程启动后第一次抓取必然走较贵的那条**(表里那个 `stream` 1)。判定用
  `prevBodyLen` 估未压缩大小,首次为 0 估不出来,第二次起才进最省的路。
  所以 `stream` 停在 1 是正常的,`one_shot` 不为 0 才要回头查上面三个条件。

## 编号:为什么从 004 起,且是三位

内网那套 VM 已经打了自己的补丁 `001-add_feature_retentionfilter`、
`002-feature_downsampling_and_tlshandshake`、`003-add_vmagent_artifacts`,我们接着往下排。

**位数必须和它们一致。** 四位的 `0001` 和三位的 `001` 放同一目录时,shell glob 会排成
`0001…0009, 001, 0010…0013, 002, 003`(实测),`git am *.patch` 的顺序就全错了 ——
我们前 9 个会在 retentionfilter 之前打。

要相对上游的序号(不考虑内网现状)就 `./make-patches.sh -s 1 -p ''`。

## 重新生成

改了 fork 之后跑 `./make-patches.sh` 重出这批文件,**不要手工改补丁**:

```bash
./make-patches.sh                 # 默认:004..016,acg_ 前缀,写到本目录
./make-patches.sh -s 1 -p ''      # 相对上游的序号,不带前缀
./make-patches.sh -o /tmp/x       # 写到别处
```

短名在 `names.txt`,一行一个按提交顺序;行数和提交数对不上会直接报错,不会静默退回长名。

脚本生成完**自带验证,不过就非零退出**:把补丁依次 `git am` 到纯净的 `v1.126.0-cluster` 上,
逐文件与 fork 分支比对字节,再 `go build ./lib/...`;`all-changes.diff` 单独再验一遍。
两条都做过变异测试(故意漏掉补丁、故意让 names.txt 行数不符),确认会失败并点名原因。

## 打到内网

源码在仓库根:`git am 0*.patch`。
源码在子目录(例如 `vms/`)——**此时 cherry-pick 用不了,它不支持路径改写**:

```bash
git am --directory=vms 0*.patch
```

先干跑看会不会撞内网自己的改动:

```bash
for p in 0*.patch; do git apply --check --directory=vms "$p" || echo "^^ 会冲突:$p"; done
```

**注意基线不同**:这批补丁是对着纯净上游 `v1.126.0-cluster` 生成的,内网树上已经有 `001`~`003`。
同文件同区域会冲突,最可能撞的是 `002-feature_downsampling_and_tlshandshake` ——
如果它动了 `lib/promscrape/client.go`,就和 `004-acg_promscrape-zstd` 正面撞
(后者改了 `ReadData` 的签名,返回响应编码而不是 `isGzipped bool`)。

只要改动不要提交历史:`git apply --directory=vms all-changes.diff`。

## 下面的详解覆盖到哪

一~七节对应 `004`~`010`,即全部"必须"和"强烈建议"的那几条;八、九节是 `028`、`029`。
`011`~`016` 是文档、性能和测试,上表的一句话就够了,没有单独章节。

---


## 一、抓取端 zstd

官方 vmagent 抓取时只发 `Accept-Encoding: gzip`、也只会解 gzip(`lib/promscrape/client.go`),边车即使支持 zstd 也用不上。本补丁让抓取端可以请求并解压 zstd。

### 改了什么

| 文件 | 改动 |
|---|---|
| `lib/promscrape/client.go` | 新增 `-promscrape.zstdCompression`(**默认关闭**);开启后请求头为 `Accept-Encoding: zstd, gzip`;`ReadData` 返回响应的编码(`"zstd"` / `"gzip"` / `""`),不再是 `isGzipped bool` |
| `lib/promscrape/scrapework.go` | `readFromBuffer` 按编码选解压器,复用 VM 已有的 `protoparserutil.GetUncompressedReader`(本来就支持 zstd) |
| `lib/promscrape/client_zstd_test.go` | 新增:用边车同款**流式** zstd 编码器造响应,覆盖默认关闭、zstd、目标只支持 gzip、不压缩四种情形 |
| 3 个既有测试 | 跟随 `ReadData` 签名 |

没有引入新依赖:zstd 解压用的是 VM 自带的 `lib/encoding/zstd`(cgo 构建走 gozstd,纯 Go 构建走 klauspost)。

**行为兼容**:不加参数时请求头、解压逻辑与上游完全一致;加了参数但目标只回 gzip 或不压缩时,照常处理。

### 应用与构建

```bash
cd VictoriaMetrics-1.126.0
git apply /path/to/0001-promscrape-zstd.patch
git apply /path/to/0002-streamaggr-staleness-0.patch
git apply /path/to/0003-streamaggr-flush-timestamp.patch
git apply /path/to/0004-streamaggr-leading-zero.patch
git apply /path/to/0005-promscrape-stream-without-body.patch
git apply /path/to/0006-streamaggr-sum-samples-total.patch
git apply /path/to/0007-streamaggr-shutdown-flush-timestamp.patch

# 本地验证(两套 zstd 实现都要过)
CGO_ENABLED=0 go test -run TestClientReadDataContentEncoding ./lib/promscrape/
CGO_ENABLED=1 go test -run TestClientReadDataContentEncoding ./lib/promscrape/

# 生产二进制:linux/amd64 官方用 cgo 构建,需要 docker builder
make vmagent-linux-amd64-prod
```

启动时加 `-promscrape.zstdCompression`。

### 已做的验证

- 既有 `lib/promscrape` 测试全部通过。
- 新测试分别在 `CGO_ENABLED=0`(klauspost)和 `CGO_ENABLED=1`(gozstd)下通过,已用 `go list` 确认两次编进的是不同实现。
- **变异测试**:故意改坏 4 处(请求头仍只发 gzip / 不识别 zstd 响应 / 解压固定用 gzip / 开关恒开),新测试全部拦住。
- 补丁可干净地打在原始 v1.126.0-cluster 源码包上。
- **端到端正确性**(`demo/acg-usage-demo/storage/zstd-e2e.sh`):3,000 key、数值静止,官方 vmagent(gzip)与打补丁 vmagent(zstd)各写一个 VM,**411,000 条 series 逐条比原始值,逐位相同**。
- **端到端性能**(3 万 key 单实例,两种 vmagent 从同一源码同一编译器构建;为公平比较停了流量,未压缩 1,466 MiB,绝对值偏乐观;有流量时 1,573 MiB → zstd 约 67 MiB):线上字节 72.3 → **47.6 MiB(−34%)**,边车进程 CPU 2.68 → **2.14 s(−20%)**,vmagent 抓取耗时 2.38 → 1.86 s,vmagent CPU 6.65 → 5.69 s(大头是解析文本)。

### 注意

- `-promscrape.maxScrapeSize` 在 v1.126 限的是**线上(压缩后)字节**。3 万 key 单实例 zstd 后约 67 MiB、gzip(BestSpeed)约 95 MiB,**都超过默认 16 MiB**,必须一起调大。
- 边车侧 zstd 编码器必须 `WithEncoderConcurrency(1)`:默认并发按 GOMAXPROCS 起协程,墙钟变快但总 CPU 更多(`demo/acg-usage-demo/storage/comprbench`)。
- **边车若用 client_golang**(v1.23.2 起原生支持 zstd):默认协商平局先到先得、默认顺序 gzip 在前,**打了补丁也拿不到 zstd**。须 `import _ ".../promhttp/zstd"` 并设 `OfferedCompressions: []promhttp.Compression{promhttp.Zstd, promhttp.Gzip}`(见 `../promhttp_zstd_test.go`)。

---

## 二、聚合输出过期时补发 0

### 解决什么

stream aggregation 的 `total` 输出,某个输出 series 的输入全部停报超过 `staleness_interval` 后,状态被删除(`lib/streamaggr/output.go:100`),之后新输入从小值 Y 重新累计。VM 靠值下降识别计数器重置(`app/vmselect/promql/rollup.go:915`),**Y ≥ 过期前的 X 时认不出,少算 X**。边车开闲置老化后,低频 key(一波请求 → 闲置 → 下一波量级相近)正好落在这里。

回放实测(`demo/acg-usage-demo/storage/aggr-expiry-replay.py`):不补发时 320 次查询少算 144 次;月度场景(月初一波、闲置 30 天、月底一波)180 次少算 72 次。补发 0 后两组都是 **0 次算错**。

### 改了什么

| 文件 | 改动 |
|---|---|
| `lib/streamaggr/streamaggr.go` | 新增聚合配置 `reset_marker_on_stale`(**默认关闭**);配了却没有 `total` / `total_prometheus` 输出时**加载报错** |
| `lib/streamaggr/output.go` | 输出状态因过期被删除前,为每个 `total` / `total_prometheus` 输出追加一个值为 0 的点;`increase` 等不受影响 |
| `lib/streamaggr/reset_marker_test.go` | 新增 4 个测试,用 `testing/synctest` 虚拟时间推进 20 分钟 |

### 已做的验证

- 测试断言的是输出**形态**而非点数:每段存活期刷新几次取决于协程调度(实测第一段 3 次、第二段 4 次,开不开补发都一样),按点数断言会时过时不过。修正后连跑 30 次稳定。
- **变异测试**:不补发 / `increase` 也补发 / 活着的 series 停机时也补发 / 不校验输出类型 / 开关恒开,5 处全部拦住。其中"停机时"一项起初没拦住——默认 `flush_on_shutdown: false` 会丢弃停机刷新的输出,测试看不到;加上 `flush_on_shutdown: true` 后才真正生效。
- 既有 `lib/streamaggr`、`app/vmagent/remotewrite`、`lib/promscrape` 测试通过。
- 两个补丁可依次干净地打在原始 v1.126.0-cluster 源码包上。
- 端到端:`demo/acg-usage-demo/storage/sessiondrv.sh`(结果见设计文档)。

> 上游 `lib/streamaggr/streamaggr_synctest_test.go` 用的是 `goexperiment.synctest` 标签与已移除的 `synctest.Run`,在 Go 1.26+ 下编译不过,与本补丁无关;新测试用的是 Go 1.25 起正式提供的 `synctest.Test`。

### 用法与约束

```yaml
- match: '{__name__=~"acg_.+"}'
  interval: 60s
  without: [pod, instance, node, gen]
  outputs: [total]
  keep_metric_names: true
  staleness_interval: 40m            # > (边车 IdleScrapes + HeartbeatScrapes + 1) × 抓取间隔
  ignore_first_sample_interval: 40m  # 同上
  reset_marker_on_stale: true
```

**补发 0 只在"输出过期 = 边车已删除全部实例"时正确。** 采集中断超过 `staleness_interval` 时会多算,单测 `lib/streamaggr/outage_test.go` 实测(中断前 60,恢复后 100,真实 100):

- 全部 pod 中断:不补发算出 100,补发算出 160,**补发 0 会重复计数**。
- 单个 pod 中断、另一个 pod 照常上报 12 次(真实 112):补不补发都算出 172,**重复计数与补发 0 无关**:聚合器忘了这个 pod 的输入,恢复后把它的整个累计值当新值全额计入。
- 中断短于 `staleness_interval`:都算对。

重复计入的是该 pod 上这些 key 从创建起的全部累计值,不只是中断期间的量。根因是聚合器按真实时间遗忘、边车按抓取次数老化,不在补发 0,修在边车:`RebaseAfterGap` 在长中断后换 gen、只报没送达过的增量(`../vmsdkexp/rebase_test.go`)。同一个测试里另有三个"边车已换 gen"的场景,全部 pod 中断(配补发 0)、单个 pod 中断、短中断都算对。设计文档 §3.2 末尾有完整说明。

---

## 三、聚合刷新时间戳不再重复

### 解决什么

`runFlusher` 刷新后只在 `time.Now()` 严格晚于 `flushTime` 时才前移它。ticker 恰好落在对齐边界上(真实时钟里是略早于边界,端到端里 vmagent 重启约 10 分钟后开始每 2 分钟出现一次)时 `flushTime` 不动,下一次刷新复用同一时间戳。

- VM 不去重:同一时刻两个值顺序颠倒,被当成计数器重置而**多算**。
- VM 开去重:同一时间戳保留最大值,补发的 0 若与下一次刷新同戳会被丢掉,**少算又回来**。

原先推测是整分钟抓取与整分钟刷新撞车、可用 `scrape_offset` 错开,**不对**:与抓取相位无关,单测里抓取偏移 0s 和 30s 结果完全一样。

### 改了什么

`lib/streamaggr/streamaggr.go`:刷新后先无条件前移一个 `interval`,再按原逻辑追赶。3 行。

### 已做的验证

- `TestFlushTimestampOnBoundary`:虚拟时钟下聚合器在整分钟边界启动,抓取偏移 0s / 30s 各推 10 分钟。修复前两种偏移都在 1m 处重复,修复后无重复。撤掉修复测试即失败。
- `lib/streamaggr` 全部测试通过。
- 修复后 VM 仍建议开 `-dedup.minScrapeInterval=1m`(多副本 vmagent 需要),但不再依赖它兜底时间戳重复。

---

## 四、新建的聚合输出先补一个 0

### 解决什么

补丁 0002 只在输出状态因 staleness 过期时补发 0。vmagent 重启或崩溃直接丢掉聚合状态,不发 0:重启前某个低频 key 的输出停在 X,重启后它来一个请求,新输出从小值 Y 起,Y ≥ X 时跨越这两段的 `increase()` 认不出重置,少算 X。按天、按月的长窗口查询都会碰到。

端到端第二轮(时间压缩后 VM 往前找前值的 5 分钟相当于 50 分钟,10 分钟窗口也能看到)暴露:burst-017 重启前 1 个请求、重启后 1 个,算出 1。

### 改了什么

| 文件 | 改动 |
|---|---|
| `lib/streamaggr/output.go` | `reset_marker_on_stale` 开启时,新建的输出状态在第一次可见的刷新之前,早一个 interval 输出一个 0;`appendResetMarkers` 带时间戳参数 |
| `lib/streamaggr/streamaggr.go` | 新增 `appendSeriesAt`;配置说明补上前补 0 |
| `lib/streamaggr/reset_marker_test.go` | 输出形态从 `[5 0 5 0]` 改为 `[0 5 0 5 0]`,每个 0 要么紧贴一段的第一个点之前、要么紧贴最后一个点之后;新增 `TestResetMarkerOnRestart` |

### 已做的验证

- `TestResetMarkerOnRestart`:两个聚合器先后启动,模拟重启。修复前算出 1、修复后 2。
- 变异测试:去掉前补 0、把前补 0 与第一个值放同一时刻,都被拦住。
- `lib/streamaggr`、`app/vmagent/remotewrite`、`lib/promscrape` 测试通过。
- 端到端第三轮:"正常"类 474 个窗口全部相等(修复前少算 2 个)。前补的 0 与上一段补发的 0 同一时刻出现 1 次,两个都是 0,不影响计数。

> 虚拟时钟测试里 `ignore_first_sample_interval` 设为 0s:非 `goexperiment.synctest` 构建下 `lib/fasttime` 读真实时钟,这个间隔在假时钟上走不完。它与这个问题无关。

---

## 五、流式解析时不再整块解压响应

### 解决什么

`scrapeInternal` 先把整个响应解压进内存,再决定是否流式解析。流式解析只省了解析出的行,解压后的完整响应仍然常驻:3 万 key 的 higress 每个 67 MiB 压缩、1.5 GiB 解压,同时抓 N 个就是 N × 1.5 GiB。

完整的解压响应只有陈旧标记与 `series_limit` 要用(和上一次响应比对)。ACG 配置是 `no_stale_markers: true`、不配 `series_limit`,解压出的 1.5 GiB 实际只用来算 `scrape_response_size_bytes`。

### 改了什么

`lib/promscrape/scrapework.go`:

- 关闭了陈旧标记且没配 `series_limit`、并且要走流式解析时,把读到的压缩响应直接交给流式解析:边解压边分块解析,每块解析完就推给聚合器。
- 是否走流式仍按 `-promscrape.minResponseSizeForStreamParse`(默认 1 MB)。解压前不知道解压后大小,用上一次响应大小与读到的字节数判断。
- 响应大小改为边读边计数,`scrape_response_size_bytes` 仍是解压后大小。
- 流式读取把 `io.ErrUnexpectedEOF` 当成正常结束(适合网络流)。这里响应已经完整读进内存,出现它说明压缩数据损坏,转成普通错误,与整块解压一样判为抓取失败;失败前可能已推出部分样本,与现有流式解析一致。

### 已做的验证

- 生成数据(10 万行,不压缩 / gzip / zstd,按大小自动切换与强制流式):新路径推出的样本哈希、条数、`up`、`scrape_samples_scraped`、`scrape_response_size_bytes` 与整块解压路径一致。
- 截断的 gzip 响应判为失败,`up=0`。截断的 zstd 帧在两条路径上都被当成正常结束,是上游 zstd 读取器原有行为,实际抓取中传输层截断会先被 HTTP 层发现。
- **真实响应**(`ACG_SCRAPE_BODY=acg.zst go test -run RealResponse ./lib/promscrape`,3 万 key 全量,zstd 67 MiB):441.5 万个样本哈希一致;单次解析 15.5 s → 1.2 s,堆峰值 6.9 GiB → 113 MiB(整块路径为了走到旧逻辑开了陈旧标记,多了保存与比对上次响应的开销,只作参考)。
- **进程实测**(`demo/acg-usage-demo/storage/vmagent-scrape-mem.sh`,3 个 higress 同时抓同一份真实响应,生产聚合配置,remote write 丢弃;物理占用用 macOS `footprint`,存活堆用 pprof `heap?gc=1`):
  - 不限核:物理占用峰值 **26.94 GiB → 4.03 GiB**,每轮存活堆 11.0~17.9 GiB → 3.44~3.54 GiB;不开聚合 0.71 GiB。
  - 限 2 核、60 秒一轮:整块解压约 9 分钟只完成 3 次抓取、物理占用 17.92 GiB,跟不上;边解压边解析每轮 CPU 35~49 秒、刷新 7.5~10.6 秒、物理占用 4.04 GiB。
  - 抓到的样本数各组一致。
- 变异测试:开了陈旧标记也走新路径、响应大小不计数、截断不报错、不按响应编码解压、只看上次大小,5 处全部拦住。
- `lib/promscrape` 全部测试通过。

---

## 六、新增 `sum_samples_total` 输出

### 解决什么

`total` 输出为了把各实例的累计值变成增量,要给**每个实例的每条线**记上一个值。这部分内存随 higress 个数增长,vmagent 重启时也全部丢失,重启后只能把首个样本当基线丢掉。

边车改为报增量(SDK `ReportIncrements`)后,vmagent 只需要把样本值按输出线累加。`sum_samples` 每次刷新清零;`sum_samples_total` 一直累加,输出累计计数器,400 条 `increase_pure` 查询不用改。`reset_marker_on_stale` 的补发 0 与前补 0 同样作用于它。

### 改了什么

| 文件 | 改动 |
|---|---|
| `lib/streamaggr/sum_samples.go` | 新增 `sumSamplesTotalAggrValue`,刷新不清零,超过 float64 精度时归零(与 `total` 一致) |
| `lib/streamaggr/streamaggr.go` | 注册 `sum_samples_total`;`reset_marker_on_stale` 允许与它搭配 |
| `lib/streamaggr/output.go` | 补发 0 / 前补 0 按"累计型输出"判断,覆盖 `total`、`total_prometheus`、`sum_samples_total` |
| `lib/streamaggr/sum_samples_total_test.go` | 累加、重启、停机刷出三个测试 |

### 已做的验证

- 单测:多个实例的增量跨刷新累加;重启后不需要基线,开补发 0 时 `increase` 精确(12),不开少算。
- 变异测试:刷新时清零、补发 0 不覆盖它,都被拦住。
- **内存实测**(`demo/acg-usage-demo/storage/vmagent-scrape-mem.sh`,每个 higress 返回 3 万 key 全量,60 秒一轮):

| | 3 个 higress | 10 个 higress |
|---|---|---|
| `total`:合并状态 / 物理占用峰值 | 3.0~3.2 GB / 4.03 GiB | 4.65 GB / 6.58 GiB |
| `sum_samples_total`:合并状态 / 物理占用峰值 | **1.41 GB / 2.27 GiB** | **1.48 GB / 2.98 GiB** |

合并状态与 higress 个数基本无关;物理占用随同时解析的响应个数增长。

---

## 七、停机刷出不再用未来的时间戳

### 解决什么

`flush_on_shutdown: true` 时,停机那次刷出的时间戳是下一个刷新边界,在未来。开了 `reset_marker_on_stale` 时,重启后的聚合器在自己启动时刻前补 0,旧进程停机刷出的点落在它后面,`increase()` 看到 X → 0 → X,把 X 重复计入。端到端(边车报增量 + `sum_samples_total`)里 vmagent 重启窗口**多算 12%**。

不开 `flush_on_shutdown` 则停机时还没刷出的那一段丢掉,端到端里 vmagent 重启窗口少算 1.8%(两轮一致)。

### 改了什么

`lib/streamaggr/streamaggr.go` 的 `runFlusher`:开了 `reset_marker_on_stale` 时,停机刷出的时间戳不晚于当前时刻。

### 已做的验证

- `TestSumSamplesTotalFlushOnShutdownRestart`:停机刷出后,在旧进程下一个刷新边界之前重启。修复前算出 18(应为 12),修复后 12。
- `lib/streamaggr`、`app/vmagent/remotewrite` 测试通过。
- **端到端**(`demo/acg-usage-demo/storage/sessiondrv.sh`,时间压缩 10 倍,两条链路都是边车报增量 + `sum_samples_total` + 补发 0,VM 不去重):

| vmagent 重启类别(2799 个窗口,真实 89062) | 算出 | 相等 / 少算 / 多算 |
|---|---|---|
| 不开 `flush_on_shutdown` | 87525(少 1.7%) | 2346 / 453 / 0 |
| 开 `flush_on_shutdown`,没打 0007 | 99890(多 12%) | 2511 / 0 / 288 |
| **开 `flush_on_shutdown`,打 0007** | **89062(全等)** | **2799 / 0 / 0** |

其余类别(正常、单 pod 短中断、单 pod 长中断、全部 pod 长中断)三组都全部相等;打 0007 那组重复时间戳为 0。

---

## 八、vmselect 按客户端要求用 zstd 压缩响应(028)

上游的响应压缩走 gzhttp,客户端同时列出 gzip 和 zstd 时它总是选 gzip,读大导出的客户端怎么要都拿不到 zstd。
补丁在 `Accept-Encoding` 里 zstd 的权重高于 gzip、或权重相同且排在前面时用 zstd 压缩响应;否则原样交给上游的 gzip,
浏览器和各种代理的行为不变。ACG ruler 读导出时发的是 `zstd, gzip;q=0.5`。

- 压不压照搬 gzhttp 对 gzip 的规则:先攒 1 KB 再决定,只压至少 1 KB 的 2xx 完整响应,所以错误信息和小响应原样发出;
  HEAD、204、206,以及 handler 自己设了 `Content-Encoding` 或 `Content-Range` 的都不压;1xx 立即转发;
  正文写出之前的 Flush 不提前发响应头;压缩时去掉 `Accept-Ranges`、加上 `Vary: Accept-Encoding`。
- 请求原样交给 handler,不删 `Accept-Encoding`;VM 出错中止响应(接管并断开连接)之后,不再往连接上写。
- 压缩窗口 1 MiB:80 MB 的真实导出压缩 15.2 倍,默认的 4 MiB 窗口是 16.1 倍,每个在途响应占的内存是后者的四分之一。
- 改的是 `lib/httpserver`,所有组件的 HTTP 接口都带上了,但只对明确偏好 zstd 的客户端生效。
  不需要参数;`-http.disableResponseCompression` 关掉压缩时它也一起关。
- 验证:10 个测试覆盖上面每条规则和对抗审查提出的全部情形;把 16 条规则逐一改回去,每次都有测试失败。
  实验集群 vmselect 换上后,ruler 用的两种导出(POST 的 JSON 导出和 native 导出)都返回 zstd,解压后和不压缩时逐字节一致;
  400 错误和小响应是明文。
- ruler 用 v0.1.8 及以后:它会先解压非 2xx 响应再读错误信息。上游 gzip 同样会压 1 KB 以上的错误,与 028 无关。

## 九、带选择器的 TSDB status 从匹配序列的指标名统计(029)

`/api/v1/status/tsdb` 带 `match[]` 时,vmstorage 先找出匹配的 metricID(很快),然后**不管匹配多少**
都把当天 per-day 倒排索引的每一行扫一遍、逐行和匹配集合求交,所以代价和当天的序列总数成正比。
实验集群一天 2.4 亿序列:只匹配 3 万条的选择器也要 29–41 秒,trace 里找 metricID 只占 0.1 秒。

补丁照 vmstorage 自己在查标签名、标签值时的做法(`searchLabelNamesWithFiltersOnDate` 匹配不超过 10 万时
走 `getLabelNamesForMetricIDs`):匹配数不超过 `-search.maxMetricIDsForDirectTSDBStatus` 时,
从这些序列的指标名统计各项;按倒排索引行的顺序推入 top heap,前 N 名边界上的并列和扫描时一样。

- **默认 0,即上游行为(总是扫描)**,要在 vmstorage 上加参数才生效。建议 `-search.maxMetricIDsForDirectTSDBStatus=200000`
  (按单个 vmstorage 上的匹配数算):ruler 切好的每一片三台合计不超过 30 万;实验集群四分之一规模时,切片前问的整个选择器每台约 17 万。
  超过阈值的照旧扫描,和上游一样。
- 撤回原先建议的 100 万:修复前的版本实测每台匹配 68 万条要 15.9 秒、vmstorage 内存涨约 300 MB,已接近扫描的 30 秒。
  本机 30 万序列的小索引上,匹配 1% 时新路径快于扫描(65 对 100 毫秒),10% 时持平,全部匹配时慢 5 倍。
- 统计在内存里做,每个不同的 `标签=值` 约分配 500 字节(本机实测),请求结束即释放。
- 对抗审查后的修复:按查询截止时间检查,超时即返回错误,和扫描一致;跳过索引自用的内部标签键;
  遇到找不到指标名的序列(新序列正在注册、索引快照不完整时)退回扫描。
- 和扫描的差异只在扫描本身算错的地方:名字里带 `=` 的标签,扫描会算成以它开头的另一个标签的值;
  per-day 索引里重复出现的序列,扫描会算两次。新路径这两处都算对。
- 验证:随机 600 条序列(含删除的序列、全局索引日期、需要转义的字符)在 400 组过滤、focusLabel、topN 下,新旧路径逐字段相同;
  另有多租户隔离、退回扫描、截止时间三个测试;把新路径设为默认时 `lib/storage` 原有测试全过;
  实验集群 5 个真实选择器的返回和打补丁前逐字段相同(修复后的版本复核过)。
- 实测(修复后的版本):29–41 秒 → 0.06–0.38 秒,重启后第一次 0.4–4.4 秒。ruler 三个副本首次补算一整天 2133 → 1271 秒,
  其中导出也快了 45%,因为扫描不再和导出抢 vmstorage 的 CPU(这一组是修复前的版本测的)。

## 十、聚合输出按样本的抓取时刻打时间戳(030)

### 解决什么

聚合输出的时间戳原来是刷出的时刻。vmagent 处理不过来时(全规模下抓取处理排队),10:59 抓的一轮可能 11:02 才聚合写出,
用量记进了后面的分钟;ruler 按整点切小时,还会落进下一个小时。

### 改了什么

- 新配置项 `sample_timestamps`(单条规则),以及参数 `-remoteWrite.streamAggr.sampleTimestamps`、`-streamAggr.sampleTimestamps`
  (对能用的规则全部打开,不能用的跳过)。只支持 `sum_samples` 和 `sum_samples_total`,
  不能和 `enable_windows`、`dedup_interval` 一起用,它们自己决定时间戳;单条规则这样配会报错,全局参数则跳过这些规则。
- `sum_samples`:按样本时间戳分组,每组写在自己的时间戳上。某一分钟已经写出后又到的部分,写成提前几毫秒的另一个样本,
  去重时两条都保留,`sum_over_time` 把它们相加。
- `sum_samples_total`:按抓取轮次依次写出累计值。写出的时间戳不回退(回退会被当成计数器重置),
  所以比后一轮还晚到的轮次记到后一轮上,误差以各目标之间处理的先后为界,与落后多久无关。
  心跳写在已见过的最新样本时刻,而不是墙钟;新输出前补的 0 在它第一个样本之前。
- 不开时行为不变。新状态放在单独的类型里,默认路径每条输出序列的内存不变。
- 抓取循环:上一轮处理太久时,下一个 tick 等到的时候已经晚了,原来仍用 tick 的时间打戳,把中间那段算到过去;
  现在 tick 晚于间隔的十分之一,就用实际开始抓取的时刻。

### 已做的验证

- 6 个测试在假时钟下覆盖:落后 4 分钟的各轮记回各自的分钟,晚到的部分累加而不覆盖,累计值不回退,
  补 0 在第一个样本之前,单条规则配错时拒绝,全局参数跳过带 `dedup_interval` 的 gauge 规则;另有 tick 的测试。
  7 处变异全部被抓到。
- 本机端到端(vmagent 1 核、处理落后):`_1m_by_global` 的点落在抓取时刻前 1–7 毫秒;
  `_1m` 的合计与原始增量一致(905,580 对 905,580,input tokens 1,060,993,780 两边相同),只差最后一轮,
  那是 `_1m` 规则没配 `flush_on_shutdown` 造成的。

### 用法与约束

- 内网在 vmagent 的 `extraArgs` 加 `remoteWrite.streamAggr.sampleTimestamps: "true"`:两条合并规则和 `_1m_by_*` 都生效,
  gauge 那条(带 `dedup_interval`)自动跳过。
- 换回不认识这个参数的旧镜像之前,先去掉它。

## 十一、流式聚合按指标名索引规则(031)

### 解决什么

每批样本原来要交给每条规则,逐条比对 `match`。ACG 的配置有 102 条规则,其中 100 条各写死一个指标名,
每条样本却要和全部 102 条比。全规模本机复现(3 个实例、3 核)的 profile 里,遍历和比对占 vmagent CPU 的 24%,
几乎都是不可能命中的比对。

### 改了什么

- `promrelabel.IfExpression.MetricNames()`:每个过滤组都含 `__name__="非空字面量"` 时返回这些名字,否则返回 nil。
  与 `Match` 的语义一致:MetricsQL 会把 `or` 前面的名字补进每一组。
- 加载配置时按名字建表。`Push` 把整批交给不能索引的规则,其余规则只拿名字对得上的那几段:
  只有一段就原地切片,多段则拷进子批,再把结果"或"回整批的 `matchIdxs`。同名连续的样本只查一次表,并记住最近 4 个名字。
  规则内部照旧执行完整的 `match`。
- 能索引的规则少于 2 条时不建表。推送开头只读一次规则列表,已停止的规则不再收样本(热加载时旧规则可能正在被停止)。
  被跳过的规则照旧给 `vm_streamaggr_samples_lag_seconds` 记 0,自监控指标不变。
- 通用机制:只看 `match` 的结构,不认任何指标名或规则名;配置不用改,不加参数。

### 已做的验证

- 等价:通用配置下,有索引和无索引两条路径的输出、`matchIdxs`、`samples_lag` 逐条相同;
  8 个协程并发推送与顺序推送相同,`-race` 通过;14 处变异全部被抓到。
- 三路对抗审核(语义、并发与内存、性能),发现并已修复:热加载时推送越界崩溃、最近名字缓存漏记没有规则的名字、
  `samples_lag` 观测次数变化。
- 回放:全规模 145 分钟长场景(19.8 亿条输入、9.6 亿个输出样本),两条路径的输出逐字节一致;
  按 apikey 抽样的长场景写进 VM 再读回,序列和时间戳完全一致,值相同或差 1 ULP
  (同一份数据写两次 VM 也是这样,来自 VM 存浮点时按块共用十进制指数)。
  与 030 合并后,在抽样长场景上把 `sample_timestamps` 关闭、打开各回放一遍,两条路径都逐字节一致
  (关闭时 1.61 亿、打开时 3.39 亿个输出样本);关闭时的输出与只打 031 时逐字节相同,即 030 不开参数时不改变任何输出。
- 性能:全规模本机复现 vmagent CPU 1.30 → 0.82 核(−37%),`Aggregators.Push` −63%;
  基准(每批 1000 条)ACG 形态 −49.7%,两条规则 −42% ~ −44%。只有 2–3 条可索引规则、而一批几乎全是这几个名字时,
  索引跳不过任何比对,多出读名字的开销,约每条 5 ns、每次推送 50 ns(这类窄形态多 1–16%)。

### 用法与约束

不用改配置,不加参数。设计与全部实测见 ACGCluster `docs/apikey-usage/vmagent-rule-index.md`,
回放与比对工具在 `vm/vmagent/verify/`。

