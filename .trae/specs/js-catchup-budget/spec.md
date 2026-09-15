# JetStream Source/Mirror 分层追赶预算（Catch-up Budget）- 产品需求文档

## Overview
- **Summary**: 为 JetStream 的 Mirror 与每个 Source 同步关系增加可配置的"分层追赶预算"：管理员可分别限制单位时间窗口内允许同步的消息数量、字节数量、允许在途（已拉取未落地）的消息数量，以及一次连续追赶的最长持续时间。预算仅作用于"追赶流量"；预算耗尽后同步进入可观察的暂停状态，窗口恢复/暂停时长结束后自动继续，直到欠账清零。
- **Purpose**: 当目标流落后上游较多（新接入、长时间断连、节点重启、扩容迁移后）时，避免追赶同步在短时间内挤占网络、CPU、存储与复制资源，影响实时复制、普通发布和消费者投递；同时让管理员对追赶节奏拥有确定性的控制与可观察性。
- **Target Users**: JetStream 流配置管理员（通过 `$JS.STREAM.*` API、nats CLI 或声明式配置创建/更新流的运维与开发者）。

## Goals
- 在 `StreamSource`（同时用于 `Mirror` 与 `Sources[]` 的每个条目）上提供可选的追赶预算配置，四类限制可独立设置：窗口消息数、窗口字节数、最大在途拉取量、最长连续追赶时间。
- 精确区分"追赶流量"与"实时流量"：只有存在欠账（lag 超过阈值）的同步消息消耗预算；欠账归零后的实时同步、普通客户端发布、消费者投递完全不受预算影响。
- 预算耗尽（消息/字节额度用尽或连续追赶超时）时进入暂停：目标侧停止落地追赶消息并暂缓向上游确认 Flow Control（从源头停止继续推送）；暂停原因、暂停截止时间、窗口用量、在途量、欠账可通过 Stream Info / 监控端点观察，并输出服务端日志。
- 窗口恢复（或暂停时长结束）后自动恢复追赶，最终把上游消息完整、有序、不丢不重地同步到目标流。
- 在多来源、主题过滤/变换、跨账户 External 同步、重复消息、复制因子变化、节点重启、领导者切换等场景下，同步进度、欠账、暂停原因与最终一致性不发生失真。
- 未配置预算（字段为空或全零）时，Source/Mirror 的全部既有行为保持不变（零行为差异）。

## Non-Goals
- 不对普通客户端发布（publish）路径、消费者投递（consumer delivery）路径、Raft 实时复制路径做任何限速或排队改造。
- 不实现流级别的跨来源共享总预算（本期预算以每个 Source 条目 / Mirror 为单位独立生效）。
- 不持久化预算窗口计数与暂停状态（进度仍沿用现有 `StreamSourceState` 持久化机制；预算运行态在新领导者/重启后按当前欠账重新评估）。
- 不改造上游 Push Consumer 的 Flow Control 批量协议本身；并发拉取量通过目标侧"暂缓 Flow Control 确认"在协议允许范围内实现。
- 不为外部预置消费者（`StreamSource.Consumer`，durable sourcing）启用预算（该消费者由外部所有，目标侧不掌握其 FC 生命周期），该组合在配置校验阶段被拒绝。
- 不新增 nats CLI 子命令或新的 REST 端点（复用 Stream Info / `$JS.API.STREAM.INFO` / monitor varz 暴露状态）。

## Background & Context
- 现有实现位于 `server/stream.go`：Mirror 与 Sources 通过内部创建的上游 Push Consumer（`AckNone` + `FlowControl: true` + Heartbeat）接收消息；上游在在途字节超过阈值后发送 FlowControl 请求（`NATS/1.0 100 FlowControl Request`，reply 为 FC 主题），目标侧在 `handleFlowControl` 中回复后上游才继续推送；Idle Heartbeat 可携带 `Nats-Consumer-Stalled` 提示目标侧补回 FC。
- 每条同步消息的 ack reply subject 携带 `pending`（该消息之后上游仍积压的消息数），现有代码据此维护 `sourceInfo.lag`（= pending-1）。这是区分追赶/实时的天然信号。
- Mirror 与 Sources 的消息分别进入各自有序队列（mirror: `sourceInfo.msgs`+`processMirrorMsgs`；sources: 共享 `mset.smsgs`+`processAllSourceMsgs`，每个 `inMsg` 带 `si *sourceInfo`），均在流领导者上运行；FC 在集群模式下经 Raft `Propose` 回复（`handleFlowControl`）。
- 同步进度（sseq/ident）已通过存储层 `StreamSourceState` 持久化，消费者重建时从 `si.sseq+1` 续传；去重由现有消息头与 dedupe 机制覆盖。
- 已确认的设计决策（用户确认）：
  1. 预算粒度：每个来源独立（配置挂在 `StreamSource` 上，Mirror 与每个 Source 条目各持有独立窗口/暂停/在途状态）。
  2. 追赶判定：lag > 阈值即追赶；阈值可配，默认复用现有 `dgetCaughtUpThresh`（10）；lag 回落至阈值内即切回实时，实时消息永不被预算阻塞，并立即放行任何暂缓的 FC。
  3. 暂停恢复：引入可选 `pause_duration`，缺省等于窗口长度；消息/字节额度耗尽时暂停到下一窗口补满，连续追赶超时时暂停 `pause_duration`。
  4. 状态持久化：预算计数与暂停态为运行态，不持久化、不随 Raft 复制；进度持久化与最终一致性沿用现有机制。

## Functional Requirements
- **FR-1（配置模型）**：`StreamSource` 增加可选字段 `CatchUpBudget *CatchUpBudget`（JSON: `catchup_budget`）。`CatchUpBudget` 包含：
  - `Window time.Duration`（`window`）：速率窗口长度；当配置了任一限制且为 0 时规范化为 1 秒。
  - `MaxMsgs int64`（`max_msgs`）：每个窗口允许落地的追赶消息数；0 表示不限制消息数。
  - `MaxBytes int64`（`max_bytes`）：每个窗口允许落地的追赶消息负载字节数（按消息 payload 累计）；0 表示不限制字节数。
  - `MaxConcurrent int`（`max_concurrent`）：追赶期间允许在途（已从上游拉取、尚未在目标侧落地）的最大消息数；通过控制 FC 放行时机实现；0 表示不限制在途量。
  - `MaxDuration time.Duration`（`max_duration`）：一次连续追赶的最长时间；达到后强制暂停；0 表示不限制连续追赶时长。
  - `PauseDuration time.Duration`（`pause_duration`）：连续追赶超时后的暂停时长；0 时取 `Window`。
  - `LagThreshold uint64`（`lag_threshold`）：追赶判定阈值；0 时取默认值 10。
- **FR-2（配置校验）**：在 `checkStreamCfgLocked` 中对 Mirror 与每个 Source 的预算做校验：所有数值字段不得为负；`Window` 与 `PauseDuration` 必须 >0（或留空走默认值）；预算与 `StreamSource.Consumer`（外部预置 durable 消费者）互斥；非法配置返回明确的 Stream 配置错误。空（nil）或全零预算等价于未配置。配置经 Stream Update 变更后，对应来源在下一次消费者/领导者初始化时使用新预算。
- **FR-3（追赶判定与模式）**：每个来源的预算运行态维护三种模式：`realtime`（实时）、`catching_up`（追赶中）、`paused`（已暂停）。依据每条入站同步消息携带的 pending 计算 lag：lag > LagThreshold 时进入/保持追赶模式；lag ≤ LagThreshold 时回到实时模式并复位连续追赶计时。判定对 Mirror 与每个 Source 独立进行。
- **FR-4（消息/字节窗口预算）**：追赶模式下，每条追赶消息在**落地前**申请预算（消息 +1、字节 += payload 长度）；窗口内计数超过任一项额度时，进入 `paused`，原因为 `max_msgs` 或 `max_bytes`，暂停截止时间为当前窗口结束时刻；窗口滚动补满后自动恢复为追赶模式。实时模式消息不申请、不计数、不阻塞。
- **FR-5（连续追赶时长）**：追赶模式维护本次连续追赶起点（进入追赶模式的时刻；暂停后恢复时以恢复时刻重新起算，回到实时模式则清零）。若配置了 `MaxDuration` 且连续追赶时长达到该值，进入 `paused`，原因为 `max_duration`，暂停截止时间为当前时刻 + `PauseDuration`；到期后回到追赶模式（窗口按恢复时刻重新计）。
- **FR-6（并发拉取量控制）**：目标侧暂缓对在途 FC 请求的确认，直到：未处于暂停态、且当前在途追赶消息数 ≤ `MaxConcurrent`（未配置则只受 FR-4/FR-5 约束）。FC 放行时携带最新的 delivery/stream 序号。在实时模式下立即放行 FC。因目标侧主动暂缓 FC 而收到的 `Nats-Consumer-Stalled` 心跳提示在暂缓期间被忽略（不补 FC），避免上游被意外放行。
- **FR-7（暂停期间行为）**：暂停期间：(a) 已在目标侧队列中的追赶消息停止落地（保留顺序）；(b) 新的上游推送因 FC 未确认而停止（在途量被 FC 批量天然限定）；(c) 实时消息（lag 已回落）不被阻塞，到达即落地并触发模式切回与 FC 放行；(d) 暂停不触发"消费者僵死"重建（心跳仍在到达，`si.last` 持续刷新）。
- **FR-8（自动恢复）**：暂停截止时刻到达后自动恢复：追赶消息按序继续落地，暂缓的 FC 按 FR-6 规则放行；无需人工干预、不重建消费者（除非现有的健康检查机制独立判定需要）。
- **FR-9（可观察性）**：`StreamSourceInfo` 增加可选的追赶状态对象（JSON `catchup`），Mirror 与每个 Source 独立上报，至少包含：是否启用、当前模式、是否暂停、暂停原因（`max_msgs`/`max_bytes`/`max_duration`/空）、暂停截止时间、lag 阈值、当前窗口起点与窗口内消息/字节计数、在途消息数、连续追赶起点、累计暂停次数。该状态经 `$JS.API.STREAM.INFO`、Stream Info 响应、monitor（varz/jsz）与集群 info 汇总链路自动可见。进入暂停与恢复时分别输出一条服务端日志（带来源流名与原因）。未配置预算时该字段为空（omitempty）。
- **FR-10（多来源）**：多个 Source 各自维护独立的预算窗口、在途计数与暂停状态；一个来源暂停不影响其他来源，也不影响目标流上的直接发布。
- **FR-11（主题过滤/变换）**：预算在过滤与 subject transform 之前作用于同步流量本身；配置 `FilterSubject` 或 `SubjectTransforms`（含 Mirror 的多过滤）时，预算对该来源条目匹配到的全部消息一致生效，不破坏现有过滤、变换与重叠校验语义。
- **FR-12（跨账户/External）**：`External`（跨账户/跨域）同步走同一内部订阅与 FC 路径，预算在目标侧生效，与账户/域前缀无关；跨账户场景下 FC 暂缓、恢复与状态上报行为与同账户一致。
- **FR-13（重复与异常消息）**：被判定为重复（如 `errMsgIdDuplicate`）而跳过的追赶消息照常消耗预算（它确实被拉取），但不影响 sseq/dseq 记账与续传正确性；现有重试、skip、deseq 不匹配重建消费者等错误恢复逻辑保持不变，预算状态不得导致消息丢失、重复落地或乱序。
- **FR-14（拓扑变化与高可用）**：
  - 复制因子变化（R1↔R3 等）、领导者切换、节点重启后，新领导者基于持久化的 `StreamSourceState` 续传，重新创建预算运行态（窗口重置、不残留旧暂停态），依据新消费者报告的 lag 重新判定模式；暂缓中的 FC 随旧消费者一起失效，不会向新消费者错误放行。
  - 集群模式下 FC 的暂缓与放行只在领导者上发生，且放行消息仍经现有 Raft 复制路径（`handleFlowControl` 的 Propose 分支）。
- **FR-15（兼容性）**：未配置预算时，Mirror/Source 同步与改动前完全一致：FC 立即回复、无额外队列/计时器/锁竞争、Stream Info 不出现新字段、旧版本 JSON 配置可正常解析（未知/缺失字段零值）。

## Non-Functional Requirements
- **NFR-1（正确性/并发安全）**：预算运行态的锁独立于 `mset.mu`，允许在处理协程与恢复计时器中并发访问；不得新增与 `locksordering.txt` 冲突的锁序（禁止在持有预算锁时获取 stream/server/client 锁或执行 outq 发送）。
- **NFR-2（性能）**：未配置预算时热路径零额外分配、零额外系统调用 goroutine/计时器；配置预算时每条消息的判定为常数时间、无阻塞 I/O；暂停/恢复只使用单个计时器，不产生 goroutine 泄漏（来源消费者取消、流关闭、领导者丢失时计时器与回调被正确回收）。
- **NFR-3（可测试性）**：预算核心状态机可脱离网络进行单元测试（可注入时钟）；端到端行为通过独立服务器与 R3 集群测试覆盖。
- **NFR-4（可维护性）**：预算核心逻辑放在独立文件，与 stream.go 接线解耦；公共配置类型可被 nats.go 客户端等价结构以 JSON 无缝交互。
- **NFR-5（向后兼容）**：通过既有 Go 测试全集（至少 `server` 包内 mirror/source/cluster 相关测试）与 `-race` 下的新增测试，无新增数据竞争。

## Constraints
- **Technical**:
  - Go 1.26，代码位于 `github.com/nats-io/nats-server/v2/server`；只能使用仓库现有依赖（`golang.org/x/time` 等已在 go.mod 中，但优先用标准库自实现以避免额外行为耦合）。
  - 不得修改 Wire 协议、存储格式、消费者创建协议；只允许利用现有 Push Consumer FC 语义实现反压。
  - FC 在集群模式必须经 Raft 回复；目标侧任何 FC 放行都要走与 `handleFlowControl` 相同的路径。
  - 内部投递是有序的（同一 deliver subject），预算实现不得重排消息。
- **Business**:
  - 默认行为必须不变（特性严格 opt-in）。
  - 配置形态需与 nats-server 公共 API 风格一致（snake_case JSON、`omitempty`、`StreamSource` 复用）。
- **Dependencies**: 无外部新依赖；复用 `ipQueue`、`sourceInfo`、Stream Info/monitor 既有链路。

## Assumptions
- "单位时间"通过显式 `Window` 表达（默认 1 秒）；消息数与字节数都是每个固定窗口的额度（固定窗口算法），不做令牌桶平滑。
- "并发拉取量"在 Push FC 协议下解释为"已拉取未落地的在途追赶消息数上限"，通过 FC 放行时机做稳态约束；FC 批量由上游按字节触发，首批在途量可能超过该上限（协议限制），上限在后续 FC 周期被严格遵守。
- 字节计量以消息 payload（`len(msg)`）为准，不含协议头。
- 预算只在流领导者上生效；追随者不运行来源消费者，这与现有架构一致。
- 被暂停的来源在上游看来与一个"较慢的合法消费者"无异，不会引发服务端资源告警；心跳间隔（`sourceHealthHB`）小于健康检查间隔，暂停不会被误判为僵死。
- Stream Update 变更预算后，允许在当前来源消费者下次重建时完全生效（与其他来源配置更新的生效时机一致）；不要求对正在进行的消费者做即时热切换，但已建立的 gate 应在处理循环中立即使用新配置（实现上以"更新后重建对应 gate、状态复位"为验收口径）。

## Acceptance Criteria

### AC-1: 预算配置可被设置、校验、持久化回读
- **Type**: `rule`
- **Given**: 一个启用 JetStream 的服务器
- **When**: 通过 Stream Create/Update API 为 Mirror 或 Source 提交合法 `catchup_budget`（含 window/max_msgs/max_bytes/max_concurrent/max_duration/pause_duration/lag_threshold 的任意子集）
- **Then**: 流创建/更新成功；Stream Info 的 `config` 中原样回显配置（默认值规范化后的值生效）；服务器重启后配置仍存在；非法值（负数、与 `source.consumer` 组合）返回明确的配置 API 错误且流不被创建/更新
- **Pass Condition**: 合法配置往返一致（默认值按 FR-1 规范化）、非法配置被拒绝并返回 400 级 ApiError；证据为新增配置校验测试输出
- **Evidence**: `go test ./server -run 'TestJetStreamCatchUpBudgetConfig' -v` 通过

### AC-2: 未配置预算时行为零变化
- **Type**: `rule`
- **Given**: 未设置 `catchup_budget` 的 Mirror/Source 流
- **When**: 发生大量历史积压追赶与实时同步
- **Then**: FC 立即回复、无暂停态、Stream Info 无 `catchup` 字段；既有全部 mirror/source 测试通过
- **Pass Condition**: 新增"默认行为"断言通过，且既有相关测试全集不回归
- **Evidence**: `go test ./server -run 'TestJetStreamSource|TestJetStreamMirror|TestJetStreamCatchUp' -v` 全绿

### AC-3: 消息数窗口预算触发暂停并在窗口恢复后继续
- **Type**: `rule`
- **Given**: 一个 Source/Mirror 流配置 `max_msgs=N, window=W`，上游已存在 N 倍以上积压
- **When**: 同步开始
- **Then**: 每个窗口落地的追赶消息不超过 N；超额后 Stream Info 显示 `catchup.paused=true、pause_reason=max_msgs、pause_until≈窗口结束`；窗口恢复后继续，最终全部消息有序同步完成；暂停期间实时发布（lag 回落后的消息）不被阻塞
- **Pass Condition**: 观测到至少一次暂停（原因 max_msgs）、各窗口计数 ≤ N、最终消息数与上游一致且 seq 连续；证据为集成测试检查点
- **Evidence**: `go test ./server -run 'TestJetStreamCatchUpBudgetMirror|TestJetStreamCatchUpBudgetSource' -v` 通过

### AC-4: 字节窗口预算触发暂停
- **Type**: `rule`
- **Given**: 配置 `max_bytes=B, window=W`，上游积压多条已知大小消息
- **When**: 同步追赶
- **Then**: 每窗口落地字节 ≤ B（允许最后一条消息带来的边界超额被暂停吸收，即暂停前不超额落地）；暂停原因 `max_bytes`；恢复后最终一致
- **Pass Condition**: 窗口字节计数断言与暂停原因断言成立、最终一致；证据为集成测试
- **Evidence**: 同 AC-3 测试命令中字节子用例通过

### AC-5: 最长连续追赶时间触发强制暂停
- **Type**: `rule`
- **Given**: 配置 `max_duration=D, pause_duration=P`，上游积压足以使追赶持续超过 D
- **When**: 同步追赶
- **Then**: 连续追赶 D 后出现 `pause_reason=max_duration` 的暂停，暂停时刻 ≈ 到达 D 的时刻，恢复时刻 ≈ +P；暂停期间不落地追赶消息，恢复后继续直至追平
- **Pass Condition**: 暂停/恢复时间边界与原因断言成立，最终一致；证据为集成测试
- **Evidence**: catchup 预算集成测试中 max_duration 子用例通过

### AC-6: 并发拉取量受控且 FC 不被提前放行
- **Type**: `rule`
- **Given**: 配置 `max_concurrent=C`（配合较小消息速率预算使在途可观测），上游有大积压
- **When**: 同步追赶且 FC 请求到达
- **Then**: 只要在途追赶消息数 > C 且仍在追赶中，FC 不被回复；在途量降到 ≤ C 且未暂停时才放行 FC；追赶期间 `Nats-Consumer-Stalled` 心跳提示不导致提前放行；实时模式下 FC 立即回复
- **Pass Condition**: 通过 FC 回复时序/在途计数断言验证上限；证据为单元测试（gate）+ 集成测试钩子
- **Evidence**: `go test ./server -run 'TestCatchUpGate|TestJetStreamCatchUpBudgetConcurrent' -v` 通过

### AC-7: 暂停状态完整可观察且包含原因与用量
- **Type**: `rule`
- **Given**: 任一原因导致的暂停
- **When**: 拉取 Stream Info（或 monitor 端点）
- **Then**: Mirror/Source 的 `catchup` 对象包含模式、paused、pause_reason、pause_until、lag_threshold、窗口起点、窗口消息/字节计数、inflight、连续追赶起点、累计暂停次数；暂停开始/恢复各有一条服务端日志；未配置时字段缺省
- **Pass Condition**: 状态字段在暂停与恢复两个时点分别断言正确；日志出现；证据为测试输出
- **Evidence**: 集成测试中 Stream Info 检查点 + 日志捕获通过

### AC-8: 多来源相互独立
- **Type**: `rule`
- **Given**: 一个聚合流配置 3 个 Source，仅其中 1 个配置极小预算且该来源有大积压
- **When**: 三个来源同时同步
- **Then**: 被限流来源反复暂停，另外两个来源不受影响地追平；目标流直接发布不受影响；三者在 Stream Info 中各自上报独立状态
- **Pass Condition**: 未限流来源在限流来源暂停期间持续推进（lag 下降）的断言成立；证据为多来源集成测试
- **Evidence**: `go test ./server -run 'TestJetStreamCatchUpBudgetMultiSource' -v` 通过

### AC-9: 主题过滤/变换下预算正确
- **Type**: `rule`
- **Given**: Source 配置 `FilterSubject` 或 `SubjectTransforms`（Mirror 多过滤），同时配置预算
- **When**: 上游发布匹配与不匹配过滤的消息形成积压
- **Then**: 只有匹配并被同步的消息参与预算与暂停；变换后的消息正确落地；过滤/变换语义与无预算时一致，最终匹配消息全部同步
- **Pass Condition**: 落地内容、主题变换结果与最终一致性断言成立；证据为集成测试
- **Evidence**: `go test ./server -run 'TestJetStreamCatchUpBudgetFilter' -v` 通过

### AC-10: 跨账户 External 同步下预算生效
- **Type**: `rule`
- **Given**: 两个账户，目标流通过 `External` api/deliver 前缀跨账户 Source/Mirror 并配置预算
- **When**: 上游形成积压
- **Then**: 暂停/恢复/FC 行为与同账户一致，最终同步完成，状态正常上报
- **Pass Condition**: 跨账户集成测试观察到暂停并最终一致；证据为测试输出
- **Evidence**: `go test ./server -run 'TestJetStreamCatchUpBudgetExternal' -v` 通过

### AC-11: 重复消息不破坏记账与预算
- **Type**: `rule`
- **Given**: 追赶中出现被去重跳过的消息（现有 dedupe/header 机制）
- **When**: 预算开启
- **Then**: 重复消息被跳过、不重复落地；sseq/dseq 与续传位置正确；预算继续工作且最终一致
- **Pass Condition**: 去重后目标流消息数/内容与上游语义一致、无重试风暴；证据为集成测试
- **Evidence**: `go test ./server -run 'TestJetStreamCatchUpBudgetDuplicates' -v` 通过

### AC-12: 领导者切换、节点重启、复制因子变化后正确性保持
- **Type**: `rule`
- **Given**: R3 集群中配置预算的 Source/Mirror 流正在追赶上（含暂停窗口内）
- **When**: 触发领导者 stepdown / 重启原领导者 / 把 Replicas 从 1 改为 3（及改回 1）
- **Then**: 新领导者从持久化进度续传，不丢不重；暂停态不残留（新领导者初始不处于 paused，除非新 lag 再次触发）；旧 FC 不被错误回复；最终全部追平
- **Pass Condition**: 每种拓扑变化后最终一致（消息数与上游一致）、新领导者状态自洽；证据为集群集成测试
- **Evidence**: `go test ./server -run 'TestJetStreamClusterCatchUpBudget' -v -timeout 120s` 通过

### AC-13: 追赶流量与实时/发布/消费者投递隔离
- **Type**: `rule`
- **Given**: 来源被预算暂停期间
- **When**: 目标流有普通发布者持续发消息、目标流上有活跃消费者持续投递，且上游积压被逐步清空（lag 回落）
- **Then**: 普通发布与消费者投递不被阻塞（延迟无数量级恶化）；lag 回落后的实时同步消息立即落地、预算自动切回实时模式并放行 FC
- **Pass Condition**: 暂停窗口内普通发布 ack 正常、消费者在限定时间内收到消息；实时切换后 paused 清零；证据为集成测试时序断言
- **Evidence**: 隔离性集成测试通过

### AC-14: 预算核心状态机的正确性（单元级）
- **Type**: `rule`
- **Given**: 使用虚拟时钟的 gate 单元测试
- **When**: 覆盖窗口滚动、双维度超额（先/后触发 msgs 与 bytes）、max_duration 暂停/恢复、lag 阈值双向切换、在途 FC 放行判定、暂停期间 stalled 心跳忽略、停止后计时器回收
- **Then**: 每种序列的模式转移、原因、截止时间、计数与可观察快照与预期完全一致
- **Pass Condition**: 表驱动单元测试全部通过；证据为测试输出
- **Evidence**: `go test ./server -run 'TestCatchUpGate' -v -race` 通过

### AC-15: 并发安全与资源回收
- **Type**: `rule`
- **Given**: `-race` 下运行全部新增测试与既有 mirror/source 测试，并反复执行 创建→暂停中删除流→重建、领导者切换、配置更新
- **When**: 全流程
- **Then**: 无 data race、无 goroutine/计时器泄漏（暂停计时器在消费者取消/流关闭/丢失领导者时停止）、无向已关闭 outq/队列发送
- **Pass Condition**: race 检测零报告、泄漏检查（goroutine 数前后稳定或有界）通过；证据为带 `-race` 的测试输出
- **Evidence**: `go test ./server -run 'TestCatchUpGate|TestJetStreamCatchUpBudget' -race -v` 全绿

### AC-16: 实现与既有架构的契合度
- **Type**: `rubric`
- **Dimension**: 代码契合度（最小侵入、锁序、复用现有 FC/info/配置链路、命名与风格一致性）
- **Scale**: 1-5
- **Anchors**: 1 = 新增旁路协议或大段重复逻辑、锁序违规；3 = 功能正确但接线分散、存在可避免的重复；5 = 预算核心独立内聚，stream.go 仅薄接线，完全复用 handleFlowControl/StreamSourceInfo/checkStreamCfg 链路，零默认行为侵入
- **Pass Threshold**: >= 4
- **Evidence**: 独立评审对 diff 的结构化评价

## Open Questions
- 无（四个关键设计点已经用户确认：每来源独立、lag 阈值判定、可配暂停时长默认等于窗口、运行态不持久化）。
