# VictoriaMetrics 补丁:vmagent、vmselect、vmstorage(基于 **v1.126.0-cluster**)

> 本文件只讲补丁本身。集成步骤、最佳配置、配额、以及已部署环境怎么升级,见 ACGCluster 仓库的 `docs/apikey-usage/vm-side.md`（边车那侧是 `app-side.md`）。

三十七个补丁,**按编号顺序打**。004~027、030~033、035~040 是 vmagent 的,028、034 是 vmselect 的(改的是各组件共用的 HTTP 服务端),029 是 vmstorage 的。源码是 fork `github.com/axfor/VictoriaMetrics`
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
| `030-acg_streamaggr-sample-timestamps` | 新增 `sample_timestamps`:`sum_samples`、`sum_samples_total` 按样本的抓取时刻打时间戳,vmagent 处理落后时用量仍记在抓取的那一分钟;抓取 tick 晚于间隔十分之一时改用实际开始时刻(第十部分) | 补丁要打(不开参数时行为不变);参数随 036 一起打开,见第十六部分 |
| `031-acg_streamaggr-name-index` | 流式聚合按 `match` 里写死的指标名索引规则,每条样本只交给可能命中它的规则,输出逐条不变(第十一部分)。ACG 的 102 条规则全规模本机复现:vmagent CPU 1.30 → 0.82 核 | **建议**,纯性能,不用改配置 |
| `032-acg_streamaggr-sample-timestamps-per-interval` | `sample_timestamps` 按规则的间隔累加,不再按每个抓取时刻:20 个目标时每条输出每分钟写 20 个样本 → 1 个,落后时内存不再随目标数涨;并修掉 030 打开参数时的两处数据错误(晚到部分撞同一毫秒被去重丢掉、刚过边界的样本退回上一分钟)(第十二部分) | 打开 `sample_timestamps` **之前必须打**;不开参数时输出不变 |
| `033-acg_promscrape-delta-ack` | 对用 client_golang `promhttp/delta` 报增量的目标做确认投递:每次抓取在请求头里带上上一轮完整处理过的轮次,边车只提交被确认的轮次,写出了却没处理(抓取超时)的那轮下次重报而不是丢掉;目标状态页看一眼不再吃掉一轮(第十三部分) | **必须**,配合含确认协议的边车;对其它目标无影响 |
| `034-acg_httpserver-zstd-unwrap` | 028 的 zstd 响应包装补上 `Unwrap()`,`http.NewResponseController` 能像穿过 gzip 包装一样穿过它(第十四部分) | 建议,目前没有调用方依赖 |
| `035-acg_promscrape-delta-ack-reload-whole-round` | 033 的两处漏洞:改了抓取配置(如 `sample_limit`)后新的抓取任务从 `new` 重新确认,边车把没处理的那轮提交掉;流式解析推了几块后才失败的增量轮次(超 `sample_limit`/`label_limit`、压缩数据中途损坏)被重报,已推的那几块算两次。现在确认状态按抓取地址保存,增量轮次先整份读一遍、能整份推才推(第十五部分) | **必须**,配合 033 |
| `036-acg_streamaggr-sample-timestamps-rounds` | 重做 `sample_timestamps` 的归属:一轮的去向只由这一轮决定,每个输出、每条规则都一样;刚过边界的尾巴把那一分钟按同一时间戳重写完整,更晚的归到第一个还没写的分钟、每轮只定一次;每个区间只写一次且完整。修掉 032 里一轮跨刷出时 token 和费用分到两分钟的 ±、提前毫秒撞车丢数、边界抖动、与 `ignore_old_samples` 同用时静默丢数(第十六部分) | **必须**,打开 `sample_timestamps` 前打;打开后要求 vmselect、vmstorage 的 `-dedup.minScrapeInterval=1ms` |
| `037-acg_streamaggr-sample-timestamps-across-rules` | 036 之后仍有的 ±:token 和费用分在不同规则(`_1m_by_*` 就是这样)时,vmagent 积压下一轮在两条规则里可能进不同分钟。一轮的去向改为在它第一块被任一规则处理时定下,本配置的所有规则共用;每次推送各规则用同一个时刻;刷出先等进行中的推送;刷出不会提前写一个还没结束的区间(第十七部分) | **必须**,036 之后打 |
| `038-acg_vmagent-sample-timestamps-default-on` | `-remoteWrite.streamAggr.sampleTimestamps`、`-streamAggr.sampleTimestamps` 默认改为打开(第十八部分) | **必须**,要求 `-dedup.minScrapeInterval=1ms` |
| `039-acg_promscrape-delta-final-ack` | vmagent 停止时给报增量的目标发最后一次确认:正在路上的那轮(边车已写出、vmagent 还没处理)回滚重报,而不是被下一个 vmagent 的 `new` 提交掉。此前每次重启、滚动发布赶上抓取都丢一轮(第十九部分) | **必须**,配合 client_golang v1.24.1025;对旧边车无影响 |
| `040-acg_streamaggr-memory` | 流式聚合省内存:刷出每批 1000 条而不是 1 万条(102 条规则同时刷出,每条规则的缓冲在两次刷出之间一直留着);输出键不再进全局 intern 表;`sum_samples_total` 每条输出 80 → 64 字节。ACG 102 条规则下堆 1251 → 752 MiB(−40%),每条规则刷出 80 → 36 毫秒,输出逐字节不变(第二十部分) | 建议,纯内存与性能 |

只动源码的是 29 个文件(033 新碰了 `lib/promscrape/scraper.go`):028 改了 `lib/httpserver/httpserver.go` 并新增 `zstd.go`,029 改了 `lib/storage/index_db.go` 和 `app/vmstorage/main.go`;
030 加的参数在 `app/vmagent/remotewrite/streamaggr.go`,031 加的判定在 `lib/promrelabel/if_expression.go`;其余 22 个是`lib/encoding/zstd/{concurrency,zstd_pure}.go`、`lib/promscrape/{client,scrapework}.go`、
`lib/promutil/{labels,labelscompressor}.go`、`lib/streamaggr/` 下的 `deduplicator.go`、`output.go`、`streamaggr.go`
以及每个输出类型各自的文件(`avg.go`、`sum_samples.go`、`total.go` 等,补丁 018 给它们各加了一个
`needsInputKey()`)。其中 `concurrency.go` 和 `sum_samples.go` 是新文件。
冲突热点是 `streamaggr.go`(15 个补丁碰它)和 `output.go`(11 个)。

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

## 部署检查:用量规则要配 `flush_on_shutdown`

`_1m_by_*` 和两条合并规则若不配 `flush_on_shutdown: true`,vmagent 停机或热加载配置时,当前区间还没刷出的用量直接丢掉
(第七部分实测重启窗口少算 1.7%)。用量规则都要配上;第七部分(`010`)保证停机刷出的时间戳不会造成重复计数。
`033` 的确认投递只管「边车 → vmagent」这一段:vmagent 收下并处理完一轮就确认,之后在内存里丢掉的,边车不会重报。

另:`021`/`024` 的 `-zstd.encoderConcurrency` 默认 1,只在纯 Go(`CGO_ENABLED=0`)构建里生效,意味着所有压缩排队用一个编码器。
发布版是 cgo 构建、走 gozstd,不受影响;若改用纯 Go 构建且远程写吞吐大,把它调到远程写的并发数(`-remoteWrite.queues`)。

## 多个 vmagent 实例:现在是单实例,扩成多个之前先看这里

ACG 现在只有一个 vmagent,下面的问题都不存在。将来要扩成多个实例分担抓取,**先改完下面几点再扩**,
否则用量会静默少算:抓取照常成功,数字只是变小,没有报错。

**不能用多副本重复抓同一个边车**(`-promscrape.cluster.replicationFactor` 大于 1,或两套 vmagent 抓同一批目标)。
边车的增量导出(client_golang `promhttp/delta`)只允许一个抓取方:每次抓取交付的增量就算交出去了,
两个抓取方会各拿走一部分,每一方都少算。所以只能**按目标分片**(`membersCount` = N、`replicationFactor` = 1),
每个边车 pod 只归一个实例。

**按目标分片后,各实例写的是同名同标签的部分和。** 每个实例只聚合自己那部分 pod,`_1m_by_*` 和两条合并规则的输出
在各实例上标签完全一样,只是值各是一部分。vmstorage、vmselect 都开了 `-dedup.minScrapeInterval=1ms`,
同一序列同一毫秒只留最大的那个值,于是:

- 不开 `sample_timestamps`:各实例都写在同一个刷出边界上,**必撞**,只剩最大的那一份,其余实例的用量丢掉。
- 开了 `sample_timestamps`:晚到部分提前的毫秒数取自各实例自己的刷出序号,同时启动的实例序号相同,**一样会撞**。

**做法:给每个实例加一个区分标签,读的地方对它求和。**

- 用 `-remoteWrite.label=vmagent_shard=<n>`,每个实例 `<n>` 不同。vmagent 在流式聚合**之后**才加这个标签
  (`app/vmagent/remotewrite/remotewrite.go` 的 `tryPushTimeSeriesInternal`),全局规则和按 URL 的规则都一样,
  所以它能留在聚合输出上,各实例的部分和成了不同的序列,不再互相去重。
- **不要用 `-promscrape.cluster.memberLabel`**:它在抓取时加到目标标签上(`lib/promscrape/config.go`),
  在聚合之前,规则的 `by:` 会把它丢掉,等于没加。
- 读这些序列的地方都要对 `vmagent_shard` 求和:ruler 的规则、看板、对账脚本、告警。`sum by (provider_id, …)`
  这类写法不列出它,自动就合上了;**直接读裸序列、不带 `sum` 的,要改**。ruler 的查询要逐条核对。
- `sum_samples_total` 的合并规则也一样:每个实例各有一条自己的累计计数器,`increase()` 按序列各算再相加,结果对;
  没有区分标签时,两个计数器挤在一条序列上,会被当成计数器重置。

**其余影响:**

- 输出序列数最多变成 N 倍:一个 API key 的流量分散在各个 pod 上,每个实例都会写一份它的 `_1m_by_apikey*`。
  vmstorage 的 `storage.cacheSizeStorageMetricName`、内存配额要按此重估。
- 改分片数或实例重启时,目标会在实例之间迁移,迁移那一刻可能两个实例都抓了同一个 pod 一两轮。
  增量按每次抓取交付,不会重复也不会丢,只是分在两个实例名下;有区分标签就没问题,没有就会撞。
- 分钟级的 ± 对账问题不受影响:同一个 pod 的 cost 和 tokens 总由同一个实例抓取、聚合,
  开了 `sample_timestamps` 照样落在同一分钟。
- 每个实例跟不上、刷出超时的风险各自独立,`vm_streamaggr_flush_timeouts_total` 要按实例看。

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

一~七节对应 `004`~`010`,即全部"必须"和"强烈建议"的那几条;八、九节是 `028`、`029`,十~二十节是 `030`~`040`。
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

- (以下是 2026-10-02 的记录,已由 `032`、`036` 解决;现在的用法见第十六部分。)vmagent 跟不上时,打开它反而更慢、内存更高。全规模回放下,
  不开时每分钟聚合 1787 万条(跟上 89%)、没有重启;打开后只有 1053 万条,比 1027 的 1227 万还少,刷出超时 2.6 次/分钟,
  两次因内存到 5 GiB 被 OOM。原因是落后时每条输出要按每个待写的抓取轮次分开累加、分开刷出。优化这部分并重新验证之前不开;
  不开时 030 不改变任何输出(回放逐字节一致)。
- 打开的方法:在 vmagent 的 `extraArgs` 加 `remoteWrite.streamAggr.sampleTimestamps: "true"`,两条合并规则和 `_1m_by_*` 都生效,
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

## 十二、`sample_timestamps` 按间隔累加,并修掉两处数据错误(032)

### 解决什么

- **性能**:030 按每个不同的样本时间戳各存一份和。vmagent 给每个目标一个自己的抓取偏移,
  所以一条由 N 个边车 pod 供数的输出,每分钟要存 N 份、写 N 个样本;落后几分钟还要再乘几倍。
  实验集群上打开参数后反而更跟不上、两次 OOM(第十部分),原因就在这里。
- **数据(030 打开参数时才有)**:
  - 晚到部分写在「时间戳减去刷出序号毫秒」上。两个目标的时间戳差几毫秒、又恰好隔几次刷出写出,
    就落在同一毫秒,去重只留一个。随机测试里 030 每个种子都丢数,例如 40 分钟合计 961,应为 978。
  - 刷出比边界晚几毫秒时,已经收到的、边界之后抓的样本也被写出,减去提前的毫秒数后落回**上一分钟**。

### 改了什么

- 样本按所在区间的终点归组:区间是规则自己的 `interval`,`(终点 − interval, 终点]`,网格与刷出对齐。
  注意 `time.Truncate` 从公元 1 年起算,不是 Unix 纪元;不能整除一天的间隔(如 7m)两种网格不同,这里用刷出的那种。
- 一次刷出只写在它的边界之前**已经结束**的区间,之后的区间都留给结束时的那次刷出,写在各自的终点、按先后写。
  一次刷出走得比间隔还长时(全规模下实测过 91 秒),走到后面的条目会同时攒着边界之后的两个区间;
  若先写后一个,`sum_samples_total` 写出的时间戳不能回退,前一个就被挪进后一个,又成了相邻分钟的 ±。
- 条目超过 `staleness_interval` 没收到样本本应删掉、且删时不刷出;它还攒着没写的区间时推迟删除,写完再删,停机时也一样。
  停机那次刷出全部写出。没开 `flush_on_shutdown` 时停机或热加载照旧丢掉还没写的部分,和不开参数时丢掉当前区间是一回事。
- 每个写出的时间戳都落在自己的区间内,且不晚于墙钟。区间终点相隔至少 1 秒,提前的毫秒数小于 1 秒,不同区间不会撞。
- 一条输出待写的几个区间合成一条序列、多个样本写出;多于一个区间时用的切片从池里取。
- `sum_samples_total` 的心跳不越过本次刷出的边界;`vm_streamaggr_output_samples_total` 改为数样本而不是序列。
- 写出的时间戳从抓取时刻变成区间终点(如 12:01:00)。按分钟、按小时的合计不变。

### 已做的验证

- 基准(20 个目标、2000 条输出、`interval: 1m`,每次刷出):每条输出写的样本 20 → 2,耗时 11.8 → 7.8 ms
  (不开参数 7.5 ms),分配次数 14264 → 547;落后 4 分钟时样本 60 → 3.75,耗时 45.0 → 30.5 ms(不开 29.5 ms)。
  每条输出在刷出前占的状态 655 → 119 字节,落后 4 分钟时 2190 → 207 字节(不开参数约 103)。
- 随机测试:20 个种子 × 两种输出 × 1m、7m 两种间隔;目标各有偏移(含边界后 1 毫秒),
  处理最多晚 150 秒,刷出最多超时 100 秒,外加停机刷出。`sum_samples` 每个区间都与按样本时间戳算出的真值相等,
  `sum_samples_total` 终值正确、从不回退。它在 030 上每个种子都失败;去掉提前毫秒、用错网格也都会失败。
- 推迟写出、区间内钳制、推迟删除各有专门的测试,单独去掉任何一个都会失败;
  刷出比间隔还长的场景另有一个测试(只留一个区间的做法在它上面失败)。`-race` 通过。
- 已知前提:正确性建立在 `-dedup.minScrapeInterval=1ms`(同一毫秒才去重)和**每条序列只有一个 vmagent 在写**之上,
  ACG 现在正是如此。若改成 `1m` 去重,同一分钟的晚到部分会被丢掉;多实例见上文「多个 vmagent 实例」。

### 用法与约束

- 打开前必须打 032 和 036。不开参数时输出不变。
- 032 的归属规则已被 036 取代(区间改为 `[起点, 终点)`,晚到的轮次不再写回自己的分钟),见第十六部分。

## 十三、对报增量的目标做确认投递(033)

### 解决什么

边车(client_golang `promhttp/delta`)报的是「上次交付以来的增量」,原来在**整份响应写出去**时就算交付、提交。
写出去不等于 vmagent 收到并处理了:响应还在 socket 发送缓冲里时 vmagent 的抓取超时、断开,
边车已经提交,vmagent 却把这轮整个丢掉,这个 pod 这一分钟的用量就没了。
另外 vmagent 的目标状态页(`/target_response`)会真去抓一次目标:对边车来说这是第二个抓取方,
看一眼就提交掉一轮、却不进聚合,同样丢一分钟。

### 改了什么

- 边车每个响应带 `X-Prometheus-Delta-Round: <session>:<round>`。vmagent 每次抓取带 `X-Prometheus-Delta-Ack`:
  - `new`:还没从这个目标拿到过轮次(vmagent 刚启动),边车按原来的方式提交挂着的那轮,本轮挂起等确认;
  - `<session>:<round>`:上一轮完整处理过的轮次,与挂着的一致就提交,不一致(没处理成)就回滚、下一轮重报;
    拿到过该 session 的轮次但一轮都没处理成时发 `<session>:0`;
  - `peek`:目标状态页,只看不处理:挂着的那轮不动,本轮写完立即回滚。
- 不带这个头的请求(旧 vmagent、curl)照旧「交付即提交」,与原来完全一样;
  vmagent 对不报轮次的目标也带这个头(值为 `new`),对方忽略。
- 抓取成功(`up=1`)才算处理完;解析出错被跳过的行照旧算在内(这一轮仍是成功的抓取)。
- 参数 `-promscrape.deltaAck`(默认 true)可关掉,关掉时与 032 及以前完全一样。

### 已做的验证

- 单测:请求头与返回的轮次;成功推进确认、失败保留上一轮或 `<session>:0`、目标重启换 session;状态页发 `peek` 且不改状态;
  关掉参数时走原来的 `ReadData`、不带头。
- 边车侧(client_golang)单测与随机测试见其提交说明:随机交错写入、成功抓取、失败抓取、状态页查看,
  并开着空闲清理,40 个种子每个 key 都恰好处理一次;把回滚改成提交或换回旧的空闲删除都会大量失败。
- 端到端(本 fork 的 client_golang 边车 + 本分支构建的 vmagent,远程写到一个只做求和的接收端):30 个 key 随机写 40 秒,
  vmagent 每 2 秒抓、超时 1 秒,边车每第三次抓取写完整份响应后卡 3 秒(写出了、vmagent 却超时丢掉),
  期间打开 5 次目标状态页。

  | | 写入 | vmagent 收到 | 对不上的 key |
  |---|---|---|---|
  | `-promscrape.deltaAck=true` | 3892 | **3892** | **0/30**(9 次卡住、8 次回滚重报,最后一次卡住时已无新写入) |
  | `-promscrape.deltaAck=false` | 3892 | 1266(**−67.5%**) | 30/30(卡住的轮次和状态页各吃掉一轮) |

### 用法与约束

- 需要边车用含确认协议的 client_golang(2026-10-23 之后的版本);旧边车不带轮次头,033 对它没有任何影响。
- 剩下的两种情况协议表达不了:vmagent 在一轮和下一次请求之间重启(发 `new`,按原来的方式提交,可能丢一轮——正常停止时已由 `039` 修掉,见第十九部分;崩溃、被强杀时仍会);
  vmagent 处理一半失败、已处理的部分再算一次——这一条已由 `035` 修掉,见第十五部分。
- 只确认「边车 → vmagent」这一段;vmagent 之后在内存里丢的(没配 `flush_on_shutdown` 时停机)不会重报,见「部署检查」。

## 十四、zstd 响应包装补上 `Unwrap`(034)

028 的 zstd 包装没有 `Unwrap()`,gzip 包装(gzhttp)有。处理函数用 `http.NewResponseController(w)` 设写超时之类,
对选 zstd 的客户端会报 `feature not supported`,对 gzip 客户端却正常。VM 目前没有这样的调用方,补上以免以后踩到。
测试:经包装设写超时,zstd 与 gzip 两种客户端都成功;去掉 `Unwrap` 后 zstd 那一路失败。

## 十五、增量轮次的确认跨配置变更保留,且整份推或不推(035)

### 解决什么

033 留下两个口子,都会让报增量的边车丢一轮或多算一轮:

- **改抓取配置后丢一轮**:vmagent 的抓取任务在抓取配置或目标标签变化时整个换掉(改 `sample_limit` 就会),
  确认状态挂在任务上,新任务从 `new` 开始。边车收到 `new` 会按老办法提交挂着的那轮——那轮旧任务并没有处理完,
  之后滚进去的各轮一起没了。
- **处理一半失败时多算**:流式解析每解析完一块就推出去。后面某一块超了 `sample_limit`/`label_limit`,
  或压缩数据在前几块之后损坏,整次抓取判失败,这一轮不确认,边车下一轮重报整份,已经推出去的那几块就算了两次。

### 改了什么

- 确认状态按抓取地址保存在一个登记表里,换上来的新任务接着用;一小时没用过的状态由后台清掉。
- 解析之前整份响应已经在内存里。对增量轮次先把整份读一遍、什么都不推:能整份推完才推,否则这一轮整个失败、不推一块。
  没配 `sample_limit`/`label_limit` 时只可能是解压失败,这时只做解压(约 4 GB/s,80 MB 的响应约 20 ms)。
  不报轮次的普通目标不受影响。

### 已做的验证

- 单测:任务换掉后接着确认旧任务处理过的轮次,不发 `new`;一小时没用的状态被清掉、用过的留着。
- 20 万样本的增量轮次:超 `sample_limit`(流式、保留正文两种)和 zstd 中途损坏,035 之前分别已推出 199864、197816、156621 个样本,
  035 之后一个都不推、抓取判失败;放得下的轮次整份推出。去掉先读一遍的检查,三种都失败。

### 用法与约束

- 不用改配置。需要 033。

## 十六、`sample_timestamps` 一轮只定一次去向,每个区间只写一次且完整(036)

### 解决什么

032 按聚合器「已经写到哪」决定样本的去向,这个状态每个输出不同、而且在一轮处理到一半时会变:
晚到的一轮正好赶上刷出在逐条写输出,有的输出把它记进这一分钟、有的记进下一分钟,
同一轮的 token 和费用就一分钟少、下一分钟多,正是最初报告的 ±。另外:

- 同一区间晚到的部分写在「终点减刷出序号毫秒」上,隔 1000 次刷出会撞同一毫秒,去重只留一个,丢数;
- 区间是 `(起点, 终点]`,正好落在边界上的抓取(`scrape_align_interval` 下就是这样)归哪一分钟取决于毫秒级抖动;
- 和 `ignore_old_samples` 一起用时,晚到的轮次被静默丢掉;时间戳在未来的样本一直扣着不写。

### 改了什么

一轮的去向只由这一轮决定,每个输出、每条规则都一样:

- 样本归它抓取时刻所在的区间 `[终点 − interval, 终点)`,写在终点。边界上的抓取归从边界开始的那个区间。
- 在这个区间写出之前处理到:随区间写出。
- 区间写出后才处理到、而它仍是最后写出的区间:是一轮跨过边界的尾巴。下一次刷出把这个区间**按同一时间戳重写一遍,值是完整的**;
  VM 的去重(`-dedup.minScrapeInterval=1ms`,同一毫秒留最大值)留下完整的那个。会让值变小的尾巴(负数、或 `sum_samples_total` 的累计值刚从头开始)不重写,记到下一个区间。
- 更晚才处理到:归到这一轮**第一个样本**被处理时还没写出的第一个区间,按抓取时刻每轮只定一次,这一轮后面的样本也去那里。
  只有一轮自己的处理就超过一整个区间,后面的部分才会再往后。
- 聚合器启动之前抓的轮次归它写的第一个区间(之前那个区间可能是上一个进程写的);时间戳在未来的样本按现在算。
- 无论落后多少都不丢样本,每条输出只存固定几份和;去掉了提前毫秒、水位、跨输出的「最后写到哪」。
- `sample_timestamps` 不能与 `ignore_old_samples` 同用,配了会报错。

行为上的变化:vmagent 积压(一轮处理时它那一分钟早已写出)时,这一轮记在处理时的那一分钟,而不是抓取的那一分钟;
合计不变,token 和费用始终在一起。

### 已做的验证

- 每条规则一个测试;一轮在刷出逐条写输出的过程中到达(准时的和晚到的各一种,刷出中间用钩子推入),在每个输出里都落在同一个区间。
- 随机测试:20 个种子 × 两种输出 × 1m、7m × 有无刷出超时;每次检查每个区间费用都是 token 的三倍、合计准确、没有提前写出;
  无超时的那一半另按规则的参照模型逐区间核对。11 条规则逐条故意破坏,每条都有测试失败。`-race` 通过。
- 内存(ACG 生产规则形态,每条输出):036 在一个、两个区间待写时都是 163 字节;032 为 147、188;不开约 131。
  刷出(20 个目标 × 2000 个 key):7.7 ms,不开 7.5 ms。
- 端到端(本机,本分支的 vmagent + 本 fork 的 client_golang 边车):6 个合成边车,2.3 亿 token,一半抓取故意晚答最多 40 秒;
  每个 key 合计准确,两种输出 4.6 万个 key·分钟里费用 ≠ 3 × token 的为 0。
- 实验集群回放(真实边车响应的 37%,3 个目标,102 条生产规则,每分钟聚合 950 万样本):开与不开都没有刷出超时、落后 0.2 秒;
  费用与 token 分在两条不同的规则里,两种情况下 400 个业务组·分钟里每一个都恰好是 3 个完整轮次。开参数堆多 250 MB,按序列数折算到全量约 700 MB。
  全量响应在这套实验集群上跑不下(节点 3 核 8 GB,vmstorage 被 OOM),全量验证要在生产规格的机器上做。

### 用法与约束

- vmagent 的 `extraArgs` 加 `remoteWrite.streamAggr.sampleTimestamps: "true"`;内存 limit 比不开时多给 1Gi。
- vmselect、vmstorage 必须是 `-dedup.minScrapeInterval=1ms`:尾巴的重写靠它留下最大值。
- 规则里不能有 `ignore_old_samples`。
- **更正**:036 只保证一轮在**同一条规则**的各个输出里落在同一个区间。token 和费用分在不同规则时(`_1m_by_*` 就是这样),
  vmagent 积压下仍可能一条规则一分钟、另一条规则另一分钟——实验集群限 300m CPU 的回放里两条规则 400 个业务组·分钟有 200 个对不上。
  由 `037` 修掉,必须一起打。

## 十七、一轮在所有规则里落在同一个区间(037)

### 解决什么

036 之后,token 和费用分在不同规则(`_1m_by_*` 就是这样)时仍会出 ±。实验集群回放、vmagent 限 300m CPU(落后 60–180 秒)时,
`acg_bg_cost_total_1m_by_bg` 和 `acg_cache_creation_tokens_1m_by_bg` 两条规则 400 个业务组·分钟里有 200 个轮数对不上:
同一轮在一条规则里进了 05:58,在另一条里进了 06:00。原因有三:

- 晚到的轮次去哪,看的是**这条规则**的刷出写到了哪。各规则按自己的节奏刷出,一轮正好赶上一条写完、另一条还没写,就分两路。
- 一轮分块到达,后面的块各自重新判断:第一块被当成本分钟的尾巴,过了下一个边界才到的第二块被当成晚到,差了两分钟。
- 一次推送从一条规则到下一条时各自读时钟;刷出也可能把刷出前就定好去向、刷出后才落进输出的块当成刷出后的。

### 改了什么

- 一轮去哪只看它自己:它抓取的那个区间在处理它之前的那个边界还没结束,就进那个区间;否则进处理它时所在的区间(这个区间还没有任何规则写过)。
- 在这一轮**第一块被本配置任一规则处理时**定下,按抓取时间戳记在本配置所有规则共用的表里,这一轮后面的块、在每条规则里都去同一个区间。
  表最多存 13 万轮,超出时按同样的规则放、但不再记(推送时间戳各不相同的数据才会撑满)。
- 一次推送,各规则用同一个时刻。
- 刷出先等正在进行的推送做完(推送从不等待),再写;并且不会把时钟还没走到终点的区间当成已写。
- 只有一轮处理到它被放进的区间之后还超过一整个区间,后面的块才会再往后。

### 已做的验证

- 每个机制一个确定性测试:一条规则的刷出被卡住时到来的晚到轮次、一轮分两块跨边界落在两条规则里、一次推送跨刷出、
  `no_align_flush_to_interval` 下的提前刷出;每个都在 037 之前的代码上失败。四个机制逐个故意破坏,各有测试失败。
- 随机测试:token 和费用各一条规则,一轮常分两块、相隔数秒,各规则的刷出随机被卡住;20 个种子 × 两种输出 × 1m、7m,
  037 之后 0 个分裂区间,之前 48 个。`-race` 通过。
- 实验集群:同上的回放、限 300m CPU,落后 70–120 秒,两条规则 500 个业务组·分钟全部一致(037 之前 200 处不一致)。
- 本机端到端:token、费用各一条规则,前两分钟每轮都被 `sample_limit` 拒掉、热加载解除、中途重启 vmagent、四成抓取晚答最多 40 秒;
  2.45 亿 token 每个 key 合计准确,3.8 万个 key·分钟里费用 ≠ 3 × token 的为 0。
- 一轮跨刷出拆开推送(测试 5):开时每分钟准确,不开时 ± 照旧出现。

### 用法与约束

- 和 036 一起打,不用改配置。
- 积压时,一轮第一块被处理时它的分钟已经结束,就记到处理时的分钟;合计不变。

## 十八、`sample_timestamps` 默认打开(038)

- `-remoteWrite.streamAggr.sampleTimestamps`、`-streamAggr.sampleTimestamps` 不设时为真,照旧只作用于能用的规则
  (`sum_samples`、`sum_samples_total`,且没有 `dedup_interval`、`enable_windows`),其余跳过。
- 关掉:单条规则写 `sample_timestamps: false`,或把参数设为 `false`(只写一次即对所有 `-remoteWrite.url` 生效)。
- 存储侧必须是 `-dedup.minScrapeInterval=1ms`。换回 036 之前的镜像时它默认是关的,行为随之退回。

## 十九、vmagent 停止时发最后一次确认(039)

### 解决什么

vmagent 停止时,正在进行的抓取被取消。如果边车已经把响应写出(对它来说就是交付了)、vmagent 还没处理,边车就一直挂着这一轮;
下一个 vmagent 启动后第一次请求发 `new`,边车按协议把挂着的那轮提交掉——这一轮没人处理,丢了。每次重启、每次滚动发布,
只要赶上抓取,每个边车丢一轮。端到端实测:四次重启都卡在「边车已交付、vmagent 还没读完」的时刻,
`v1.126.1031-cluster` 收到 9167 万,实际写入 1.010 亿 token,少 9.2%,每个 key 都少。

### 改了什么

- 抓取停止之后,vmagent 给每个报过轮次、且声明支持的目标发最后一次确认:最后一轮完整处理过的轮次,单独一个请求,带 `X-Prometheus-Delta-Final`。
  边车用它结清挂着的那轮(是这一轮就提交,不是就回滚),不再取新的一轮,回 204。下一个 vmagent 的 `new` 时已没有挂着的轮次,没处理的那轮会重报给它。
- 停止时正在处理的抓取会先处理完,所以确认的就是它。
- 只发给响应里带 `X-Prometheus-Delta-Features: final` 的目标(client_golang v1.24.1025 起);旧边车会把这个请求当成一次抓取、挂起一轮没人处理的数据,所以不发,行为与之前相同。
- 并发 64 个,每个 5 秒超时。

### 已做的验证

- vmagent 单测:没声明支持的目标不发;声明后带轮次和头发出、204 视为完成;停止时只给报过轮次且有客户端的目标发最后处理的轮次;抓取会登记它的客户端。
- client_golang 单测:没处理的那轮被最后一次确认回滚、下一个抓取方拿到,每个增量恰好一次;不发时这一轮丢失(对照);处理过的那轮提交、不重报;这个请求不取数据;Gather 路径相同。忽略这个头,测试失败。
- 端到端:边车与 vmagent 之间放一个慢代理(一次把边车的响应读完,再慢慢交给 vmagent),在慢交付进行中重启 vmagent 四次。

  | | 写入 | 收到 | 对不上的 key |
  |---|---|---|---|
  | 本补丁 + client_golang v1.24.1025 | 102,451,114 | **102,451,114** | **0 / 20000** |
  | `v1.126.1031-cluster`(对照) | 100,964,977 | 91,665,911(**−9.2%**) | 20000 / 20000 |

### 用法与约束

- 不用改配置;边车升级到 client_golang v1.24.1025 才生效。
- 只覆盖正常停止(SIGTERM,含滚动发布)。崩溃、OOM、`kill -9` 时发不出最后一次确认,且 vmagent 内存里还没刷出的那一分钟也会丢,见「部署检查」。

## 二十、流式聚合省内存(040)

### 解决什么

在实验集群真实边车响应 37% 的回放上(102 条生产规则,约 230 万条输出序列)取 vmagent 的堆,1128 MiB 里:
刷出的缓冲约 290 MiB,全局 intern 表 205 MiB,`sum_samples_total` 每条输出的状态 186 MiB。

### 改了什么

- 刷出每 1000 条输出序列交给 push 一次,原来是 1 万条。102 条规则在同一时刻刷出,每条一个刷出上下文,放在池里留到下一分钟,
  里面是一整批的标签和样本:原来合计约 290 MiB,常驻且每分钟再满一次。远程写自己再打成块,批小只是调用多。
- 新输出条目的键改为复制一份,不再进全局 intern 表。键只在本规则的表里用,intern 表又为每个键多存一份引用,表本身约是字符串的四倍。
- `sample_timestamps` 下 `sum_samples_total` 的状态内联、不再经指针,"写过"用上次写出的时间戳是否为 0 表示:每条输出 80 → 64 字节。

### 已做的验证

- 本机回放(真实响应 37%,3 个目标,102 条规则,各跑 6 分钟):

  | | 堆 | 每条规则平均刷出 |
  |---|---|---|
  | 039 | 1251 MiB | 80 毫秒 |
  | + 批 1000 条 | 978 MiB | 97 毫秒 |
  | + 键不进 intern 表 | 759 MiB | 35 毫秒 |
  | + 状态 64 字节 | **752 MiB** | 36 毫秒 |

- 同样的轮次、同样的假时钟时刻,经 102 条规则,040 前后输出都是 2,887,061 个样本,逐字节相同(SHA-256 一致)。
- 全部单测与随机测试通过,`-race` 通过。

### 用法与约束

- 不用改配置。vmagent 的内存 limit 暂按 `vm-side.md` 不变,生产灰度确认后再下调。
