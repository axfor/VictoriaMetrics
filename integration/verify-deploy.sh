#!/usr/bin/env bash
# API Key 用量统计 · 内网部署探测
#
# 只读。全程不写任何东西,也不访问 /metrics/usage——那个端点「送达即推进基线」,
# 手工访问一次就会把那批增量吃掉,vmagent 下一次抓取看不到,那部分量不进 VM。
#
# 用法(默认值就是 acg-system 那套拓扑,裸跑即可):
#   ./verify-deploy.sh
#
# 换集群时按需覆盖:
#   NS=<ns> VMAGENT_PREFIX=<前缀> SELECT_PREFIX=<前缀> STORAGE_PREFIX=<前缀> \
#   ROUTER_NS=<ns> ROUTER_PREFIX=<前缀> ROUTER_CONTAINER=<容器名> ./verify-deploy.sh
#
# 退出码 = 失败项数。
set -uo pipefail

# 按 pod 名字前缀找,不依赖 label——各家 label 不一样,名字前缀是稳定的。
NS=${NS:-acg-system}
VMAGENT_PREFIX=${VMAGENT_PREFIX:-victoria-metrics-agent}
SELECT_PREFIX=${SELECT_PREFIX:-vm-select}
STORAGE_PREFIX=${STORAGE_PREFIX:-vm-storage}
# model-router 是 higress-gateway pod 里的边车,所以这里找的是 gateway 的 pod,
# 再在里面挑边车那个容器。容器名不给就按 model|router 自动认。
ROUTER_PREFIX=${ROUTER_PREFIX:-higress-gateway}
ROUTER_NS=${ROUTER_NS:-$NS}
ROUTER_CONTAINER=${ROUTER_CONTAINER:-}
USAGE_PATH=${USAGE_PATH:-/metrics/usage}            # 用量端点
CUMULATIVE_PATH=${CUMULATIVE_PATH:-/metrics/cumulative}
VMAGENT_PORT=${VMAGENT_PORT:-8429}
SELECT_PORT=${SELECT_PORT:-8481}
STORAGE_PORT=${STORAGE_PORT:-8482}
TENANT=${TENANT:-0}
AUTHKEY=${AUTHKEY:-}          # vmagent 配了 -metricsAuthKey / -flagsAuthKey 时填

FAIL=0; WARN=0
ok()   { printf '  \033[32m[OK]\033[0m   %s\n' "$*"; }
bad()  { printf '  \033[31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
warn() { printf '  \033[33m[WARN]\033[0m %s\n' "$*"; WARN=$((WARN+1)); }
info() { printf '  \033[36m[--]\033[0m   %s\n' "$*"; }
head2() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# 经 apiserver 的 pod proxy 取。VictoriaMetrics 的镜像是 scratch 的,kubectl exec
# 进不去,所以不能靠容器里的 curl/wget。需要 pods/proxy 的 RBAC。
raw() { kubectl get --raw "/api/v1/namespaces/$NS/pods/$1:$2/proxy$3" 2>/dev/null; }

urlencode() {
  local s=$1 i c out=
  for ((i = 0; i < ${#s}; i++)); do
    c=${s:i:1}
    case $c in
      [a-zA-Z0-9.~_-]) out+=$c ;;
      *) out+=$(printf '%%%02X' "'$c") ;;
    esac
  done
  printf '%s' "$out"
}

promq() {
  raw "$SELECT_POD" "$SELECT_PORT" "/select/$TENANT/prometheus/api/v1/query?query=$(urlencode "$1")" \
    | sed -n 's/.*"value":\[[^,]*,"\([^"]*\)".*/\1/p' | head -1
}

# 只取 Running 的,免得撞上正在滚动的旧 pod。
pods() { # pods <namespace> <名字前缀>
  kubectl -n "$1" get pod --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep "^$2" || true
}

printf '\033[1mAPI Key 用量统计 · 部署探测\033[0m  namespace=%s\n' "$NS"

# --- 零、找 pod -----------------------------------------------------------
AGENT_POD=$(pods "$NS" "$VMAGENT_PREFIX" | head -1)
SELECT_POD=$(pods "$NS" "$SELECT_PREFIX" | head -1)
STORAGE_PODS=$(pods "$NS" "$STORAGE_PREFIX")
ROUTER_PODS=$(pods "$ROUTER_NS" "$ROUTER_PREFIX")
head2 "零、发现"
[ -n "$AGENT_POD" ]  && ok "vmagent   $AGENT_POD"   || bad "$NS 里没有 $VMAGENT_PREFIX* 的 Running pod,用 VMAGENT_PREFIX= 指定"
[ -n "$SELECT_POD" ] && ok "vm-select $SELECT_POD"  || warn "没有 $SELECT_PREFIX*,§四跳过"
[ -n "$STORAGE_PODS" ] && ok "vm-storage $(echo "$STORAGE_PODS" | tr '\n' ' ')" || warn "没有 $STORAGE_PREFIX*,§六跳过"
if [ -n "$ROUTER_PODS" ]; then
  ok "model-router $(echo "$ROUTER_PODS" | wc -l | tr -d ' ') 个 pod"
else
  warn "$ROUTER_NS 里没有 $ROUTER_PREFIX* 的 Running pod,§五的二进制核对跳过"
  echo "        kubectl get pod -A | grep -iE \"router|gateway\"  然后 ROUTER_PREFIX= 指定"
fi
[ -n "$AGENT_POD" ] || { echo; echo "vmagent 找不到,退出。"; exit 1; }

Q=${AUTHKEY:+?authKey=$AUTHKEY}
M=$(raw "$AGENT_POD" "$VMAGENT_PORT" "/metrics$Q")
F=$(raw "$AGENT_POD" "$VMAGENT_PORT" "/flags$Q")
if [ -z "$M" ]; then
  bad "取不到 vmagent 的 /metrics。要么没有 pods/proxy 权限,要么配了 authKey(用 AUTHKEY= 传)"
  echo "      退路: kubectl -n $NS port-forward pod/$AGENT_POD $VMAGENT_PORT:$VMAGENT_PORT"
  exit 1
fi
has()  { grep -qE "$1" <<<"$M"; }
val()  { grep -E "$1" <<<"$M" | awk '{s+=$NF} END {printf "%.0f", s+0}'; }
# /flags 用 flag.Visit 输出,格式是 -name="value",而且**只列显式设过的**,
# 默认值根本不出现。所以「查不到」只说明没显式设,不说明版本不对。
fval() { grep -m1 "^-$1=" <<<"$2" | sed 's/^[^=]*="//; s/"$//'; }
# Go 的 Duration.String() 会把 5m 打成 5m0s,不能按字符串比。
dur2s() {
  awk -v d="$1" 'BEGIN{ s=0; n="";
    for (i = 1; i <= length(d); i++) { c = substr(d, i, 1);
      if (c ~ /[0-9.]/) { n = n c }
      else { if (c == "h") s += n * 3600; else if (c == "m") s += n * 60; else if (c == "s") s += n; n = "" } }
    printf "%d", s }'
}

# --- 一、vmagent 版本与补丁 ------------------------------------------------
head2 "一、vmagent 版本与补丁"
VER=$(grep -m1 '^vm_app_version' <<<"$M" | sed -n 's/.*short_version="\([^"]*\)".*/\1/p')
if [ -n "$VER" ]; then ok "vm_app_version = $VER"
else warn "vm_app_version 为空——镜像没走 Makefile 的 -X buildinfo.Version 注入。看下面的功能探针"; fi
PN=$(sed -n 's/.*v1\.126\.10\([0-9][0-9]\)-cluster.*/\1/p' <<<"$VER")
if [ -z "$PN" ]; then warn "认不出版本号,是不是 v1.126.1036-cluster 只能问镜像来源"
elif [ "$PN" -ge 36 ]; then ok "版本 ≥ v1.126.1036-cluster"
else bad "版本 $VER 低于 v1.126.1036-cluster,按 vm-side.md 第 2 步换镜像"; fi

# 这三个名字上游一个都没有,出现即证明补丁在。
has 'vm_streamaggr_skipped_unchanged_outputs_total' \
  && ok "补丁 025 跳过不变的输出:在 ⇒ 镜像 ≥ v1.126.1025-cluster" \
  || bad "补丁 025 不在:没有 vm_streamaggr_skipped_unchanged_outputs_total,写入量不会降"
has 'vm_promscrape_scrapes_by_parse_mode_total' \
  && ok "补丁 017 解析模式统计:在" \
  || bad "补丁 017 不在:上游没有这个指标,说明不是我们这套镜像"
has 'vm_streamaggr_output_samples_total\{.*sum_samples_total' \
  && ok "补丁 006 sum_samples_total:在用" \
  || bad "补丁 006 的 sum_samples_total 没在用,聚合状态会随实例数增长"
# 顺带:pod 能带着 output_heartbeat_interval 跑起来,本身就是 ≥1025 的证明——
# 低于这一版解析这个字段会直接失败、CrashLoopBackOff。

# --- 二、vmagent 关键参数 --------------------------------------------------
head2 "二、vmagent 关键参数(/flags 只列显式设过的)"
need() { # need <flag名> <期望值> <ok时的补充说明>
  local got; got=$(fval "$1" "$F")
  if [ -z "$got" ]; then bad "$1 没显式设,文档要求 $2"
  elif [ "$got" = "$2" ]; then ok "-$1=$got ${3:-}"
  else warn "-$1=$got,文档建议 $2"; fi
}
need promscrape.zstdCompression true "(能带着这个 flag 启动就证明补丁 004 在——上游不认这个 flag,会启动失败)"
need promscrape.maxScrapeSize   500MiB
need remoteWrite.queues         12
need remoteWrite.streamAggr.scrapeTimeIntervals true
ZC=$(fval zstd.encoderConcurrency "$F")
[ -n "$ZC" ] && info "-zstd.encoderConcurrency=$ZC(显式设了)" \
             || info "zstd.encoderConcurrency 没显式设,走默认 1(补丁 024 把默认从 0 改成 1);默认值不会出现在 /flags"

# --- 三、vmagent 运行时 ----------------------------------------------------
head2 "三、vmagent 运行时"
FAILED=$(val '^vm_promscrape_scrapes_failed_total')
OVER=$(val '^vm_promscrape_max_scrape_size_exceeded_errors_total')
[ "${FAILED:-0}" -eq 0 ] && ok "抓取失败 0" || bad "抓取失败 $FAILED 次"
[ "${OVER:-0}" -eq 0 ] && ok "超 maxScrapeSize 0" \
  || bad "超 maxScrapeSize $OVER 次——不会自愈(送达失败→增量放回→响应更大),必须调大后重启"

SWB=$(val 'parse_mode_total\{mode="stream_without_body"\}')
STR=$(val 'parse_mode_total\{mode="stream"\}')
ONE=$(val 'parse_mode_total\{mode="one_shot"\}')
RS=$(val '^vm_promscrape_scrape_response_size_bytes_sum')
RC=$(val '^vm_promscrape_scrape_response_size_bytes_count')
AVG=0; [ "${RC:-0}" -gt 0 ] && AVG=$(awk -v s="$RS" -v c="$RC" 'BEGIN{printf "%d", s / c}')
info "解析模式:stream_without_body $SWB、stream $STR、one_shot $ONE"

# one_shot 不等于配错了。vmagent 是**按响应体积自动切**的:
# needStreamParseMode() 在 响应 >= -promscrape.minResponseSizeForStreamParse
# (默认 1MB)时才走流式,小响应走 one_shot 是正常的,也更省。
# 所以判据是「响应已经很大了却还在 one_shot」。
# 计数和体积都是全局的,分不出是哪个 job:框架指标那个 job 小而频繁(10s),走 one_shot
# 是对的;用量 job 大而稀(60s)。所以「有 one_shot」不能判错,只有「响应已经很大、却一次
# 流式都没有」才说明大响应也在整块解压。
if [ "${ONE:-0}" -eq 0 ] && [ "$((SWB + STR))" -gt 0 ]; then
  ok "解析模式全部流式"
elif [ "$((SWB + STR))" -gt 0 ]; then
  ok "大响应在走流式($((SWB + STR)) 次);另有 $ONE 次 one_shot,通常是框架指标那个小 job,正常"
elif [ "$AVG" -lt 1000000 ]; then
  info "平均响应 $(awk -v a="$AVG" 'BEGIN{ if (a>=1048576) printf "%.1f MiB", a/1048576; else if (a>=1024) printf "%.1f KiB", a/1024; else printf "%d B", a }') < 1MB,走 one_shot 是对的——vmagent 按体积自动切,到 1MB 才转流式。量上来会自己切过去"
else
  bad "平均响应 $((AVG / 1048576)) MiB 已经过 1MB,却一次流式解析都没有——通常是那个 job 设了 sample_limit 或 series_limit,这两个选项会禁掉流式解析"
fi


# --- 四、vm-select 与数据 --------------------------------------------------
head2 "四、vm-select 与数据"
if [ -z "$SELECT_POD" ]; then
  warn "没有 vmselect pod,跳过"
else
  SF=$(raw "$SELECT_POD" "$SELECT_PORT" "/flags")
  DD=$(fval dedup.minScrapeInterval "$SF")
  [ "$DD" = "1ms" ] && ok "vm-select -dedup.minScrapeInterval=1ms" \
    || bad "vm-select -dedup.minScrapeInterval=${DD:-没设},必须是 1ms(vm-side.md 第 1 步)"
  for sp in $STORAGE_PODS; do
    DS=$(fval dedup.minScrapeInterval "$(raw "$sp" "$STORAGE_PORT" "/flags")")
    [ "$DS" = "1ms" ] && ok "$sp -dedup.minScrapeInterval=1ms" \
      || bad "$sp -dedup.minScrapeInterval=${DS:-没设},必须是 1ms(vm-side.md 第 1 步)"
  done
  MS=$(fval search.minStalenessInterval "$SF")
  if [ -z "$MS" ]; then bad "-search.minStalenessInterval 没设,文档要求 5m(必须大于 output_heartbeat_interval 的 4m)"
  elif [ "$(dur2s "$MS")" -ge 300 ]; then ok "-search.minStalenessInterval=$MS"
  else bad "-search.minStalenessInterval=$MS,小于 5m,盖不住 4m 的心跳间隔"; fi

  NPOD=$(promq 'count({__name__=~"acg_.+",pod!=""})')
  [ "${NPOD:-0}" = "0" ] && ok "带 pod 标签的 acg_* 0 条(跨 pod 合并生效)" \
    || bad "有 $NPOD 条 acg_* 还带着 pod——counter 规则要把 pod 放 drop_input_labels,gauge 规则要放 without"

  NTYPE=$(promq 'count({__name__=~"acg_.+",_metric_type!=""})')
  [ "${NTYPE:-0}" = "0" ] && ok "带 _metric_type 的 0 条(类型标签没泄漏进存储)" \
    || bad "有 $NTYPE 条带着 _metric_type——两条聚合规则都要在 drop_input_labels 里去掉它"

  NALL=$(promq 'count({__name__=~"acg_.+"})')
  if [ -n "$NALL" ] && [ "$NALL" != "0" ]; then ok "acg_* 序列总数 $NALL"
  else bad "查不到任何 acg_* 序列——链路没通,或者租户 $TENANT 不对"; fi
fi

# --- 五、model-router 侧 ---------------------------------------------------
head2 "五、model-router(higress-gateway 的边车)"

# 首选探法:不碰边车。vmagent 已经在抓它了,看聚合规则匹配到没有就行。
# 两条规则都靠 _metric_type 选,而 _metric_type 只有 fork 的 TypeLabel 能产出——
# 上游 promhttp 不可能给每个样本打上这个标签。匹配数 > 0 即证明 SDK 是 fork 的。
MC=$(val 'vm_streamaggr_matched_samples_total\{.*position="1"')
MG=$(val 'vm_streamaggr_matched_samples_total\{.*position="2"')
if [ "${MC:-0}" -gt 0 ] && [ "${MG:-0}" -gt 0 ]; then
  ok "两条聚合规则都匹配到样本(counter/histogram $MC、gauge $MG)⇒ _metric_type 在产出 ⇒ SDK 是 fork 的"
elif [ "${MC:-0}" -gt 0 ] || [ "${MG:-0}" -gt 0 ]; then
  bad "只有一条规则匹配到(counter/histogram $MC、gauge $MG)——另一条的 match 或 TypeLabel 配错了"
else
  bad "两条聚合规则都没匹配到样本:要么边车没打 _metric_type(TypeLabel 没配/不是 fork),要么 match 写错"
fi

# 逐个目标看:在抓哪个 URL、健不健康、单次多久、采到多少样本。
# 端口/路径配错、以及「Gather 比抓取间隔还慢」的堆积,都在这张表里现形。
TG=$(raw "$AGENT_POD" "$VMAGENT_PORT" "/api/v1/targets$Q")
# 响应里同时有 activeTargets 和 droppedTargets。服务发现带进来、又被 relabel 丢掉的
# pod 全在后者里,没有 scrapeUrl 和 health,一起切会冒出一堆空行。只取前者。
TG=$(tr -d '\n' <<<"$TG" | sed -n 's/.*"activeTargets":\[\(.*\)\],"droppedTargets".*/\1/p')
if [ -z "$TG" ]; then
  warn "vmagent 当前没有任何活跃抓取目标——服务发现没找到边车,或 relabel 把它们全丢了"
else
  UP=$(grep -o '"health":"up"' <<<"$TG" | wc -l | tr -d ' ')
  DOWN=$(grep -o '"health":"down"' <<<"$TG" | wc -l | tr -d ' ')
  [ "${DOWN:-0}" -eq 0 ] && ok "抓取目标 $UP 个全 up" || bad "有 $DOWN 个目标 down(up $UP 个)"
  SLOW=0
  while IFS= read -r rec; do
    [ -z "$rec" ] && continue
    u=$(sed -n 's/.*"scrapeUrl":"\([^"]*\)".*/\1/p' <<<"$rec")
    h=$(sed -n 's/.*"health":"\([^"]*\)".*/\1/p' <<<"$rec")
    d=$(sed -n 's/.*"lastScrapeDuration":\([0-9.e+-]*\).*/\1/p' <<<"$rec")
    n=$(sed -n 's/.*"lastSamplesScraped":\([0-9]*\).*/\1/p' <<<"$rec")
    e=$(sed -n 's/.*"lastError":"\([^"]*\)".*/\1/p' <<<"$rec")
    printf '  \033[36m[--]\033[0m   %-4s %8.3fs %8s 样本  %s\n' "$h" "${d:-0}" "${n:-0}" "$u"
    [ -n "$e" ] && echo "               └─ $e"
    awk -v x="${d:-0}" 'BEGIN{exit !(x > 5)}' && SLOW=$((SLOW + 1))
  done < <(sed 's/},{/}\n{/g' <<<"$TG")
  if [ "$SLOW" -gt 0 ]; then
    bad "有 $SLOW 个目标单次抓取超过 5 秒"
    echo "        典型原因:那个端点走 client_golang 的 Registry.Gather(),代价是 O(注册表里的实例数),"
    echo "        跟输出多少无关(name[] 过滤发生在 Gather 之后)。3 万 Key × 37 指标 ≈ 110 万实例时要几十秒。"
    echo "        而 Gather 不响应请求取消——vmagent 超时断开,它照跑完。抓取间隔小于这个耗时就会堆积,"
    echo "        越堆越多、永远追不上。两件事都要做:"
    echo "          1) 立刻止血:把那个 job 的 scrape_interval 改到大于最坏耗时,scrape_timeout 同步放大"
    echo "          2) 根治:per-Key 指标只注册到 usageReg,promReg 只留框架指标(是「换」不是「加」)"
    echo "        /metrics/usage 不受影响,它走变更跟踪,一次 Gather 都不调。"
  else
    ok "每个目标的单次抓取都在 5 秒内"
  fi
  grep -q "\"scrapeUrl\":\"[^\"]*$USAGE_PATH\"" <<<"$TG" \
    && ok "用量端点 $USAGE_PATH 在被抓" \
    || bad "没有一个目标指向 $USAGE_PATH——用量根本没在采(端点名不同就用 USAGE_PATH= 指定)"
  grep -q "\"scrapeUrl\":\"[^\"]*$CUMULATIVE_PATH\"" <<<"$TG" \
    && bad "有 job 在抓 $CUMULATIVE_PATH——那个端点走 Gather,3 万 Key 下是几十秒级,只能手工访问" \
    || ok "没有 job 在抓 $CUMULATIVE_PATH"
fi

# 次选:读二进制里的 buildinfo 段核对版本号。需要 exec 权限,且边车镜像里要有 sh。
for p in $ROUTER_PODS; do
  c=$ROUTER_CONTAINER
  if [ -z "$c" ]; then
    c=$(kubectl -n "$ROUTER_NS" get pod "$p" -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null \
        | grep -iE 'model|router' | head -1)
  fi
  if [ -z "$c" ]; then
    warn "$p: 认不出哪个容器是 model-router,容器有:$(kubectl -n "$ROUTER_NS" get pod "$p" -o jsonpath='{.spec.containers[*].name}' 2>/dev/null)"
    echo "        用 ROUTER_CONTAINER=<容器名> 指定"
    continue
  fi
  # 容器默认不共享 PID namespace,所以边车容器里的 PID 1 就是 model-router 自己。
  # Go 把依赖图以明文写进二进制的 buildinfo 段,不需要镜像里有 go 工具链。
  line=$(kubectl -n "$ROUTER_NS" exec "$p" -c "$c" -- sh -c \
          'grep -aoE "axfor/client_golang[[:space:]]+v[0-9][^[:space:]]*" /proc/1/exe 2>/dev/null | head -1' 2>/dev/null | tr -d '\0')
  v=$(awk '{print $2}' <<<"$line")
  if [ -n "$v" ]; then
    # v1.24.1027 是 v1.24.1028 改发前的版本号,代码相同
    { [ "$v" = "v1.24.1028" ] || [ "$v" = "v1.24.1027" ]; } && ok "$p/$c: client_golang $v" || bad "$p/$c: client_golang $v,要 v1.24.1028(app-side.md 第 1 步)"
  else
    warn "$p/$c: 读不到(镜像里没有 sh/grep、没有 exec 权限,或 pod 开了 shareProcessNamespace 导致 PID 1 是 pause)"
    echo "        退路: kubectl -n $ROUTER_NS exec $p -c $c -- cat /proc/1/exe > /tmp/mr"
    echo "              go version -m /tmp/mr | grep client_golang"
    echo "        要看到 '=> github.com/axfor/client_golang v1.24.1028';只有 dep 行 = replace 没生效"
  fi
done

# --- 六、vm-storage 分片均衡 -----------------------------------------------
head2 "六、vm-storage 分片均衡"
if [ -z "$STORAGE_PODS" ]; then
  warn "没有 vm-storage pod,跳过"
else
  counts=""
  for sp in $STORAGE_PODS; do
    n=$(raw "$sp" "$STORAGE_PORT" "/metrics" \
        | awk '/^vm_cache_entries\{type="storage\/hour_metric_ids"\}/ {print $NF}')
    [ -z "$n" ] && { warn "$sp: 取不到 hour_metric_ids"; continue; }
    info "$sp: hour_metric_ids=$n"
    counts="$counts $n"
  done
  if [ "$(echo $counts | wc -w | tr -d ' ')" -ge 2 ]; then
    # 不要用三元:awk 里 `/` 紧跟变量会被当成正则开头,写成 if 才不会踩。
    spread=$(awk -v l="$counts" 'BEGIN{ n = split(l, a, " "); mn = a[1]; mx = a[1];
      for (i = 2; i <= n; i++) { if (a[i] < mn) mn = a[i]; if (a[i] > mx) mx = a[i] }
      r = 0; if (mx > 0) r = 100 * (mx - mn) / mx;
      printf "%.2f", r }')
    mx=$(awk -v l="$counts" 'BEGIN{n=split(l,a," ");m=0;for(i=1;i<=n;i++)if(a[i]>m)m=a[i];print m}')
    if [ -z "$spread" ]; then warn "分片偏差算不出来(取到的数:$counts)"
    elif [ "$mx" -lt 10000 ]; then info "序列数才 $mx 条,偏差 ${spread}% 是噪声,等有量了再看"
    elif awk -v s="$spread" 'BEGIN{exit !(s < 5)}'; then ok "分片最大偏差 ${spread}%(实测参考 0.05~0.3%)"
    else warn "分片最大偏差 ${spread}%,偏大——检查 vm-insert 的 -storageNode 是否三个都配了"; fi
  fi
fi

# --- 七、写入链路的延迟与积压 ----------------------------------------------
head2 "七、写入延迟与积压"
# 这条链路本来就不实时,延迟预算是固定的:
#   抓取间隔 60s + 聚合 interval 60s + vm-select 的 -search.latencyOffset 30s
#   = 典型 90~150 秒。下面查的是「有没有超出这个预算」。
avg() { # avg <histogram 前缀> <单位换算> —— VM 的 metrics 库导出 _sum/_count
  local sm cn; sm=$(val "^$1_sum"); cn=$(val "^$1_count")
  [ "${cn:-0}" -eq 0 ] && { printf ''; return; }
  awk -v s="$sm" -v c="$cn" -v k="$2" 'BEGIN{printf "%.3f", s / c * k}'
}
PEND=$(val '^vmagent_remotewrite_pending_data_bytes')
BLKQ=$(val '^vmagent_remotewrite_queue_blocked')
RETRY=$(val '^vmagent_remotewrite_retries_count_total')
ERRS=$(val '^vmagent_remotewrite_errors_total')
DROP=$(val '^vmagent_remotewrite_packets_dropped_total')

if [ "${PEND:-0}" -lt 1048576 ]; then ok "远程写没有积压(待发 ${PEND} 字节)"
else bad "远程写积压 $((PEND / 1048576)) MiB——vm-insert 追不上,或者网络/磁盘队列打满了"; fi
[ "${BLKQ:-0}" -eq 0 ] && ok "发送队列没有被阻塞" || bad "有 $BLKQ 个发送队列处于阻塞状态,下游顶不住"
[ "${ERRS:-0}" -eq 0 ] && ok "远程写错误 0" || warn "远程写错误 $ERRS 次、重试 $RETRY 次"
[ "${DROP:-0}" -eq 0 ] && ok "没有丢包" || bad "丢了 $DROP 个数据包——超过 -remoteWrite.maxDiskUsagePerURL 时会直接丢"

SD=$(avg vm_promscrape_scrape_duration_seconds 1)
[ -n "$SD" ] && {
  awk -v d="$SD" 'BEGIN{exit !(d < 50)}' \
    && info "单次抓取耗时 ${SD}s(scrape_timeout 是 50s,超了会整次失败)" \
    || bad "单次抓取平均 ${SD}s,逼近 scrape_timeout 50s——响应太大或边车太慢"
}
FD=$(avg vm_streamaggr_flush_duration_seconds 1)
[ -n "$FD" ] && {
  awk -v d="$FD" 'BEGIN{exit !(d < 30)}' \
    && info "聚合刷出耗时 ${FD}s(interval 是 60s,超过它就会挤掉下一次)" \
    || bad "聚合刷出平均 ${FD}s,接近 interval 60s——聚合状态太大,考虑降基数"
}
LAG=$(avg vm_streamaggr_samples_lag_seconds 1)
[ -n "$LAG" ] && {
  awk -v d="$LAG" 'BEGIN{exit !(d < 120)}' \
    && info "样本进聚合器时的滞后 ${LAG}s(= 样本时间戳到刷出的间隔,正常一个 interval 上下)" \
    || warn "样本滞后 ${LAG}s,超过两个 interval——抓取在堆积"
}
RWD=$(avg vmagent_remotewrite_duration_seconds 1)
[ -n "$RWD" ] && info "单次远程写耗时 ${RWD}s"
info "延迟预算:抓取 60s + 聚合 60s + vm-select 的 latencyOffset 30s = 典型 90~150 秒,这是设计值不是故障"

# --- 汇总 ------------------------------------------------------------------
head2 "汇总"
if [ "$FAIL" -eq 0 ] && [ "$WARN" -eq 0 ]; then echo "  全部通过。"
else echo "  失败 $FAIL 项,告警 $WARN 项。对照 vm-side.md 第 1~3 步与 app-side.md 第 1~5 步。"; fi
cat <<'TXT'

  这个脚本不做的两件事,要做得自己来:
    1. 增量语义(连抓两次、第二次不变的实例整行消失)——必须访问 /metrics/usage,
       而那一次访问会吃掉一批增量。只在灰度 pod 上做。
    2. 对账——拿业务侧真实请求数比 sum(increase(acg_requests_total[窗口]))。
       必须用 increase();直接 sum() 累计值在聚合状态过期后本来就不成立。
TXT
exit "$FAIL"
