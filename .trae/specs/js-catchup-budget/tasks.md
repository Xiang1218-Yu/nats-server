# JetStream Source/Mirror 分层追赶预算 - 实现计划

## Task 1: 公共配置类型、规范化与校验
- **Status**: `pending`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `server/stream.go` 的 `StreamSource` 增加 `CatchUpBudget *CatchUpBudget`（JSON `catchup_budget,omitempty`），新增 `CatchUpBudget` 导出结构（Window/MaxMsgs/MaxBytes/MaxConcurrent/MaxDuration/PauseDuration/LagThreshold，snake_case JSON）。
  - `(*StreamConfig).clone()` 深拷贝 Mirror/Sources 上的预算指针（避免 update/内部 clone 共享）。
  - 新增 `(cb *CatchUpBudget) normalize() (*CatchUpBudget, *ApiError)`：nil/全零 → nil（视为未启用）；负数拒绝；Window 缺省 1s；PauseDuration 缺省 = Window；LagThreshold 缺省 = `dgetCaughtUpThresh`(10)；MaxDuration>0 但 Window 等边界合法化。
  - 在 `checkStreamCfgLocked` 对 `cfg.Mirror` 与每个 `cfg.Sources` 条目调用规范化（回写 cfg）；拒绝预算 + `StreamSource.Consumer` 组合（返回 NewJSStreamInvalidConfigError 明确文案）。
  - 保证旧 JSON（无字段）零值解析、零预算不启用。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-15（配置面部分）, AC-16
- **Test Requirements**:
  - `rule` TR-1.1: 合法预算配置经 Stream Create API 成功且 Stream Info config 回显规范化结果；命令证据 `go test ./server -run 'TestJetStreamCatchUpBudgetConfig' -v`
  - `rule` TR-1.2: 负数、零窗口显式非法值、budget+consumer 组合返回 400 ApiError；同上测试
  - `rule` TR-1.3: nil/全零预算经 normalize 为 nil；clone 后修改副本不影响原对象；表驱动单元测试
- **Notes**: 不新增 errors.json 错误码，复用 `NewJSStreamInvalidConfigError`。

## Task 2: 追赶预算核心状态机 catchupGate（独立文件）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新文件 `server/stream_catchup_budget.go`：
    - `catchupMode`（realtime/catchingUp/paused）与暂停原因常量（`max_msgs`/`max_bytes`/`max_duration`）。
    - `catchupGate`：持有规范化 `CatchUpBudget`、独立 `sync.Mutex`、模式、窗口起点、窗口 msgs/bytes 计数、inflight、连续追赶起点、pausedUntil/reason、累计暂停次数、wake 通道（replace-on-signal 模式）、单个 `*time.Timer`、stopped 标记。
    - 可注入时钟（包内 `nowFunc`，默认 `time.Now`），便于单元测试。
    - 方法：`lagFromPending(pending) uint64` 判定；`delivered(lag)`（入站分类：维护模式迁移、inflight 计数）；`admit(payloadBytes) (bool, reason)`；`applied()`（inflight-- 并返回是否可尝试放行 FC）；`holdFlowControl() bool`（FC 到达时是否暂缓并登记）；`parkFCReply`/`takeParkedFC`（仅存 reply 字符串）；`stalledWhileHeld() bool`；`maybeResume(timer 触发)`；`wakeCh() <-chan struct{}`；`snapshot() CatchUpStatus`；`stop()`。
    - 锁纪律：gate 锁内绝不获取 mset/server/client 锁、不做 outq/Propose；计时器回调只改 gate 状态与发 wake 信号。
    - 语义：固定窗口；admit 超额 → paused(pausedUntil=窗口末)；max_duration → paused(+PauseDuration，恢复后窗口与 burstStart 重置)；lag 回落 → realtime（立即清暂停/复位 burst，放行已登记 FC 并 wake）；MaxConcurrent 在 FC 放行判定中检查（inflight ≤ cap）。
  - 导出 `CatchUpStatus` 观测结构（字段对应 FR-9）。
- **Acceptance Criteria Addressed**: AC-14, AC-3~AC-7（机制层）, AC-15, NFR-1, NFR-2
- **Test Requirements**:
  - `rule` TR-2.1: 表驱动单元测试覆盖 FR-3/4/5/6/7 全部状态转移（虚拟时钟）：窗口滚动、msgs/bytes 先后触发、max_duration 暂停与恢复、lag 双向阈值切换、inflight 与 FC hold/release、paused 期间 stalled 忽略、realtime 立即放行、stop 幂等且计时器回收；`go test ./server -run 'TestCatchUpGate' -v -race`
  - `rule` TR-2.2: 并发 admit/stop 不触发 race（-race 证据）
  - `rubric` TR-2.3: 核心内聚度；scale 1-5；anchors 1=与 stream.go 耦合严重/锁序风险，3=可用但职责混杂，5=纯状态机零外部锁依赖；threshold >= 4；证据为评审与代码走读

## Task 3: gate 接入 sourceInfo 生命周期（创建/更新/回收）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - `sourceInfo` 增加 `gate *catchupGate` 与 `heldFC string`（暂缓中的 FC reply，空表示无）字段。
  - Mirror：`subscribeToStream` 创建 `mset.mirror` 时用 `cfg.Mirror.CatchUpBudget` 构建 gate。
  - Sources：`resetSourceInfo` 重建 si 时为每个条目构建 gate；`updateWithAdvisory` 新增 source 分支同步构建。
  - gate 工厂 `newCatchUpGate(cfg *CatchUpBudget, onEvent func())`：事件回调仅做"唤醒处理循环"用的非阻塞信号（由 mset 注入，禁止回调中取 gate 锁）。
  - 回收：`cancelSourceInfo`、`cancelMirrorConsumer`、`stopSourceConsumers`、cleanUp（丢失领导者）、流 stop 路径调用 `gate.stop()`；确保单个 timer 在消费者重建时被旧 gate 停止、不向新 si 发信号。
  - 锁序审查：gate 锁为叶子锁，只允许 stream -> gate；更新 `locksordering.txt` 说明。
- **Acceptance Criteria Addressed**: AC-12, AC-15, AC-16, NFR-1, NFR-2
- **Test Requirements**:
  - `rule` TR-3.1: 配置预算的流创建后内部 mset.mirror/sources 的 gate 非 nil 且参数等于规范化配置；未配置为 nil（内部白盒测试）
  - `rule` TR-3.2: 消费者反复取消/重建、stepdown、流删除后旧 gate stopped（timer 字段为 nil/不再触发 wake）；`go test ./server -run 'TestCatchUpGateLifecycle' -v -race`
  - `rule` TR-3.3: `-race` 下无竞争；证据 `-race` 输出

## Task 4: Source 路径接线（入站分类、处理循环限速、FC 暂缓/放行）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - Source 投递回调（`trySetupSourceConsumer` 内 subscribeInternal）：queueInbound 后，对非控制消息用 `ackReplyInfo(reply)` 的 pending 调 `gate.delivered(lag)`（inflight++/模式迁移）；gate==nil 走原路径。
  - 重构 `processAllSourceMsgs` 的 msgs.ch 分支：支持"弹出批次后在某个 im 处等待后继续"：
    - 控制消息且 gate 激活：FC（带 reply）→ `gate.holdFlowControl()` 为真则登记 reply 到 `si.heldFC`、跳过本次处理（不 recycle，im 即弃处理）；stalled 心跳（JSConsumerStalled）在 heldFC 非空时丢弃；其余控制消息走原逻辑。
    - 数据消息且 catchup：`admit(len(m.msg))`；拒绝时保留批次剩余切片与索引，select 等待该 gate 的 wakeCh/quit/qch/新消息信号，唤醒后优先继续保留切片。
    - 每条数据消息处理返回后（含重复/跳过）调 `applied()`；当 inflight 满足且存在 heldFC 且 gate 允许时，通过新 helper 放行 FC。
  - 新 helper `releaseSourceFC(si)`：取 mset.mu，按 `handleFlowControl` 同样方式（集群 Propose / 单机 outq）以 heldFC 为 reply、si.dseq/si.sseq 为序号发送，清空 heldFC；非领导者时取消（交由清理路径）。
  - wake 后与批次耗尽时也尝试 releaseSourceFC，确保无数据消息驱动时 FC 不滞留。
  - gate==nil 时与现有代码逐行等价。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5, AC-6, AC-7, AC-13, AC-14, AC-16
- **Test Requirements**:
  - `rule` TR-4.1: 集成测试（单服务器，Source）观测 paused/pause_reason/pause_until、窗口计数上限与恢复后最终一致；`go test ./server -run 'TestJetStreamCatchUpBudgetSource' -v`
  - `rule` TR-4.2: 并发拉取 FC 时序断言（inflight>cap 时无 FC 回复，降落后恢复；stalled 不提前放行）；`TestJetStreamCatchUpBudgetConcurrent`
  - `rule` TR-4.3: gate==nil 路径既有 TestJetStreamSource* 不回归
  - `rubric` TR-4.4: 接线薄度与顺序保持；scale 1-5；anchors 1=重排/大改处理循环，3=功能正确但侵入大，5=最小分支接线且顺序语义不变；threshold >= 4；评审证据

## Task 5: Mirror 路径接线（同等机制）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 4（复用 helper 与模式）
- **Description**:
  - Mirror 投递回调（`setupMirrorConsumer` 内）：同 Source 的入站分类/inflight 维护。
  - `processMirrorMsgs`：与 Task 4 相同的批次保留、admit 等待、FC 暂缓登记到 `mset.mirror.heldFC`、stalled 忽略、放行（复用/镜像 releaseSourceFC 的 mirror 版本 `releaseMirrorFC`，走同一 handleFlowControl 路径）。
  - `processInboundMirrorMsg` 控制分支在 gate 激活时与新循环协作（避免双重 FC 回复：循环层先拦截 held FC，函数内只处理未被 gate 拦截的情况）。
  - 健康检查 `retryMirrorConsumer` 路径在 gate 暂停期间不得误判（si.last 由心跳刷新；如需要在 stalled 判定上豁免 paused）。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5, AC-6, AC-7, AC-12, AC-13
- **Test Requirements**:
  - `rule` TR-5.1: Mirror 集成测试覆盖 msgs/bytes/max_duration 暂停→恢复→最终一致；`go test ./server -run 'TestJetStreamCatchUpBudgetMirror' -v`
  - `rule` TR-5.2: 暂停期间不发生 mirror consumer 误重建（无 setup 重试日志/消费者名不变）
  - `rule` TR-5.3: 既有 TestJetStreamMirror* 全量不回归

## Task 6: 可观察性与日志
- **Status**: `pending`
- **Priority**: medium
- **Depends On**: Task 4, Task 5
- **Description**:
  - 扩展 `StreamSourceInfo`：`CatchUp *CatchUpStatus`（JSON `catchup,omitempty`），`CatchUpStatus` 字段与 gate snapshot 对齐（enabled/mode/paused/pause_reason/pause_until/lag_threshold/window_start/window_end/msgs_in_window/bytes_in_window/inflight/catching_up_since/pauses_total）。
  - `sourceInfo(si)`（持锁）中填充 `si.gate.snapshot()`（gate==nil 不填）；Mirror/Sources 经 `sourcesInfo()/mirrorInfo()` 自动覆盖 API、monitor.go、jetstream_cluster.go 各 info 链路。
  - gate 进入暂停与恢复时由 mset 层输出日志（Noticef/Warnf 级，包含 account/stream/source/mirror、reason、until）；通过 onEvent 回调在处理循环侧记录，避免 gate 锁内日志。
- **Acceptance Criteria Addressed**: AC-7, AC-8（状态可见部分）, AC-16
- **Test Requirements**:
  - `rule` TR-6.1: Stream Info（API 与 monitor）在暂停/恢复两时点字段断言正确；未配置时无 catchup 字段；`go test ./server -run 'TestJetStreamCatchUpBudgetObservability' -v`
  - `rule` TR-6.2: 暂停开始/恢复日志各至少一条且含原因（日志捕获断言）

## Task 7: 场景集成测试（多来源/过滤/跨账户/重复/隔离）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 5, Task 6
- **Description**:
  - 新增 `server/jetstream_catchup_budget_test.go`：
    - 多来源独立性（一个极小预算，另两个无预算；断言暂停期间其他来源 lag 持续下降、直接发布不受影响）。
    - FilterSubject / SubjectTransforms（Mirror 多过滤）：仅匹配消息计费、变换正确、最终一致。
    - External 跨账户：双账户 import/export 下 mirror/source 暂停恢复与最终一致。
    - 重复消息：利用现有 dedupe/header 路径制造跳过，断言无重复落地、无 seq 缺口、预算继续工作。
    - 隔离性：暂停窗口内目标流普通发布 ack 正常、活跃消费者按时收到投递；lag 回落后自动切 realtime 且 paused 清零、FC 放行。
- **Acceptance Criteria Addressed**: AC-8, AC-9, AC-10, AC-11, AC-13, AC-2
- **Test Requirements**:
  - `rule` TR-7.1: `go test ./server -run 'TestJetStreamCatchUpBudget(MultiSource|Filter|External|Duplicates|Isolation)' -v` 全部通过
  - `rubric` TR-7.2: 场景覆盖与断言质量；scale 1-5；anchors 1=只测快乐路径，3=主要场景有断言但时序弱，5=暂停/恢复/最终一致三时点断言；threshold >= 4；评审证据

## Task 8: 集群高可用测试（stepdown/重启/复制因子变化）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 7
- **Description**:
  - 在 R3 集群（复用 createJetStreamClusterExplicit 等测试 helper）中：
    - 追赶暂停窗口内触发 stream stepdown，断言新领导者从持久化进度续传、最终一致、不残留 paused。
    - 重启原领导者后同上。
    - Replicas 1→3→1 更新过程中预算配置保留、行为自洽、最终一致。
    - 断言集群模式 FC 放行经 Raft（追随者不会错误放行 held FC；旧消费者失效后 heldFC 被清理）。
- **Acceptance Criteria Addressed**: AC-12, AC-15
- **Test Requirements**:
  - `rule` TR-8.1: `go test ./server -run 'TestJetStreamClusterCatchUpBudget' -v -timeout 180s` 通过
  - `rule` TR-8.2: 每种拓扑变化后消息总数与上游一致、新领导者 catchup 状态自洽（无 paused 僵尸态）

## Task 9: 全量回归、race 与独立评审修复
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 8
- **Description**:
  - 运行新增测试全集 + 既有 mirror/source/cluster 相关测试（含 -race）；修复发现的问题。
  - gofmt/静态检查；确认锁序文档更新；确认未配置路径零分配（可通过代码走读/benchmark 佐证）。
  - 配合独立 Review（review.md），把 actionable 发现物化为 Issue 并修复至 pass。
- **Acceptance Criteria Addressed**: AC-2, AC-15, AC-16
- **Test Requirements**:
  - `rule` TR-9.1: `go test ./server -run 'CatchUp|JetStreamSource|JetStreamMirror' -race -count=1` 全绿
  - `rule` TR-9.2: `go test ./server -run 'TestJetStream' -count=1`（或按评审约定的既有相关子集）无新增失败
  - `rule` TR-9.3: `gofmt -l` 对改动文件无输出；`go vet ./server` 无新增告警
  - `rubric` TR-9.4: 整体契合度 AC-16 复核 >= 4；独立评审证据
