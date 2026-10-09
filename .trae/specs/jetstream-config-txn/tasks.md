# JetStream 跨资源原子配置事务 - 实施计划

> 说明：任务按依赖排序，每个任务是可独立验证的纵向切片。集群/单机共用平台无关的类型与校验引擎；
> 新 meta entryOp 必须追加在 `resetSeqOp` 之后。

## Task 1: 公共契约层——API 类型、主题、错误码与限额
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - `server/jetstream_txn.go`：四个 TXN 主题/响应类型/状态机/操作枚举/JSApiTxnStatus 等契约；TTL 与 op 数限额 helper（缺省 60s/上限 5m/256 ops）。
  - errors.json 新增 10228-10233 六个错误标识并重新生成 `jetstream_errors_generated.go`。
  - opts.go：JSLimitOpts 增加 `max_txn_timeout`/`max_txn_ops` 字段与配置解析。
  - `JSApiLevel` 5→6（事务 API level 门控）。
  - `go build ./...`、`go vet ./server/` 通过；`go test ./server/ -run TestTxn` 通过（JSON 往返、未知 op、TTL 夹取、保留窗口、资源 key）。
- **Description**:
  - 新增文件 `server/jetstream_txn.go`（类型与平台无关逻辑）。
  - 主题常量：`JSApiTxnCreate = "$JS.API.TXN.CREATE"`、`JSApiTxnCommit(T)="$JS.API.TXN.COMMIT.%s"`、`JSApiTxnCancel(T)`、`JSApiTxnInfo(T)`，并在 [jetstream_api.go](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_api.go#L54-L150) 常量区与 dispatch 表（L1108-L1135）登记。
  - 类型：`JSApiTxnCreateRequest{Id, TTL, Ops []JSApiTxnOp}`、`JSApiTxnOp{Op, Stream, Consumer, StreamConfig *StreamConfig, ConsumerConfig *ConsumerConfig, ExpectedVersion uint64, HasExpectedVersion bool}`、`JSApiTxnCommitResponse`、`JSApiTxnCancelResponse`、`JSApiTxnInfoResponse`（含状态机字段、逐资源进度 `[]JSApiTxnOpProgress`、冲突明细 `[]JSApiTxnConflict{Resource, Kind, ExpectedVersion, ActualVersion, Reason}`、最终 `Version`、时间戳、TTL），及对应 `io.nats.jetstream.api.v1.txn_*_response` Type 常量。
  - 状态/原因枚举：`prepared|committing|committed|aborted`；reason：`validation|permission|quota|conflict|cancelled|timeout|leader_change|restart|cluster_unavailable`。
  - 在 `errors.json`/生成器或 `jetstream_errors.go` 增加错误：事务不存在、事务状态冲突（已终结）、事务 op 数超限、TTL 超限、事务版本冲突（含资源名+期望/实际版本字段）、事务校验失败（包装具体资源错误）。生成 `jetstream_errors_generated.go` 对应条目（遵循仓库生成流程并运行生成命令）。
  - [opts.go](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/opts.go#L383) `JetStreamLimits` 增加 `MaxTxnTimeout time.Duration`、`MaxTxnOps int`（含 config 解析，0 走缺省：TTL 60s/上限 5m，op 上限 256）。
  - 常量：`cfgTxnVersionMetadataKey = "_nats.cfg.ver"`；`JSApiLevel` 提升到 6，事务端点按既有 `errorOnRequiredApiLevel` 机制门控。
- **Acceptance Criteria Addressed**: AC-1, AC-14
- **Test Requirements**:
  - `rule` TR-1.1: `go build ./...` 与 `go vet ./server/` 通过；新类型 JSON 编往返序列化测试（含 omitempty、未知 op 拒绝）通过。
  - `rule` TR-1.2: 缺省/上限 TTL 与 op 数限制的单元测试通过。
- **Notes**: 不注册任何行为逻辑，仅契约与常量；响应 Type 字符串遵循既有命名。

## Task 2: 账户级配置版本记账
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - jsAccount 增加 `cfgVersion` 计数与 current/seed/bump helper（jetstream.go / jetstream_txn.go）；`_nats.cfg.ver` 元数据读写 helper（jetstream_versioning.go），setStatic* 清除客户端伪造版本。
  - 四个单机入口记账：流 create/update/delete、消费者 create/delete（jetstream_api.go）；共享 addStream/addConsumer 路径从恢复/权威配置 seed 版本；copy*Metadata 携带 cfg.ver 保证幂等再创建可 DeepEqual。
  - TR-2.1/2.2 测试 `TestStandaloneConfigVersionAccounting`（连续变更版本 1→7、伪造版本被拒）与 `TestStandaloneConfigVersionRestartSeed`（重启后 seed 单调）通过；既有 `TestJetStreamVersioning|TestJetStreamMetadata` 回归通过。
- **Description**:
  - 集群：meta 状态中按账户维护单调版本计数（`cc.cfgVersions map[string]uint64`，`js.mu` 保护），仅在 meta 日志 apply 路径修改；单机：在 `jsAccount` 增加等价计数字段。
  - 提交一次配置变更（assign/update/remove stream、assign/remove consumer，无论是否来自事务）使该账户版本 +1；**一个事务整体只 +1**：在 Task 5 的事务 apply 包装中对每账户只递增一次。
  - 应用变更时把当前版本写入流/消费者配置元数据 `_nats.cfg.ver`（静态元数据，机制同 `setStaticStreamMetadata`；扩展该函数或在 apply 前统一 stamp）。
  - 查询辅助：`currentConfigVersion(account, resource)`（含 inflight 视图）、读取资源元数据版本的 helper。
  - 快照携带版本计数（在 Task 5 快照信封中落地，本任务先定义结构与单机持久）。
- **Acceptance Criteria Addressed**: AC-5, AC-15
- **Test Requirements**:
  - `rule` TR-2.1: 单机连续 create/update/delete 流与消费者，版本严格 +1/次；元数据键值正确；既有 stream/consumer info 响应字段集合不变（快照式 JSON 断言仅多出保留元数据键）。
  - `rule` TR-2.2: 版本在进程重启后（单机从现有配置目录）仍单调正确（读取元数据/计数恢复）。
- **Notes**: 非事务接口响应**结构**不得改变；`_nats.cfg.ver` 与 `_nats.req.level` 同属保留静态元数据。

## Task 3: 平台无关事务校验引擎
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - `server/jetstream_txn_plan.go`：txnResourceView 接口、txnEngine.prepare/recheckAtCommit；归一化去重、确定性排序（stream upsert→consumer upsert→consumer delete→stream delete）、引用/mirror/sources 成环、stream.delete 消费者显式覆盖、subject 重叠、CAS baseline、配额与副本可放置 hook。
  - 表驱动测试（14 场景）+ drift recheck + 随机输入排序稳定性全部通过（`TestTxnEnginePrepare/RecheckDrift/OrderingStable`）。
- **Description**:
  - 在 `jetstream_txn.go` 实现事务归一化与校验，输入：账户、操作集、“资源快照视图”接口（由集群/单机各自实现：查 streamAssignment/stream 与 consumerAssignment/consumer、inflight 提案）。
  - 归一化：校验 op 枚举与参数完整性；同资源多 op 去重规则（后写覆盖/冲突报错，取确定性策略：同一资源只允许一个最终 op，重复即 `validation` 错误）；确定性排序（删除反序：consumer.delete → stream.delete；创建正序：stream → consumer；update 按 stream 先于 consumer）。
  - 引用校验：消费者 op 的 stream 必须存在或同事务创建；`stream.delete` 必须显式同事务删除其全部现存消费者；mirror/sources 目标在“事务后视图”中存在且不成环。
  - 复用既有规则：`checkStreamCfg`、`checkConsumerCfg`、subjectsOverlap（在事务后视图上做跨流 subject 冲突）、`jsClusteredStreamLimitsCheck`/`jsNonClusteredStreamLimitsCheck` 的总账计算（删除释放、创建占用按合并后计算）。
  - 副本/存储：复用 `createGroupForStream/createGroupForConsumer` 的可用性判定逻辑（抽取可在不建组前提下预检的 helper：在线 peer 数 ≥ Replicas、storage 放置可行）。
  - CAS：按 `expected_version`（缺省用准备时快照版本，0=必须不存在）逐资源记录期望版本。
  - 输出：归一化有序计划、每资源基准版本、结构化冲突/错误列表 `[]JSApiTxnConflict`。
  - 提供两阶段入口：`prepare()` 与 `recheckAtCommit()`，后者额外判定快照漂移。
- **Acceptance Criteria Addressed**: AC-2, AC-4, AC-7
- **Test Requirements**:
  - `rule` TR-3.1: 表驱动测试覆盖：消费者引用缺失、mirror 缺失、sources 成环、stream.delete 隐式消费者、subject 重叠、配额合并计算（删除额度可复用）、Replicas>在线节点、存储不满足——全部拒绝且错误定位到资源；合法组合（建流+建消费者、删消费者+删流、同事务内引用）通过。
  - `rule` TR-3.2: CAS 三态（expected=0 资源已存在、版本不符、版本相符）与“缺省 expected 时快照漂移”判定单测通过。
  - `rule` TR-3.3: 归一化排序在随机输入下稳定（100 次随机用例结果一致）。
- **Notes**: 引擎不得直接触碰网络/Raft/存储落盘，纯函数 + 视图接口，保证集群/单机行为一致。

## Task 4: 单机事务管理与 journal 持久化
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 新增单机事务管理器（挂在 `jsAccount` 或 `jetStream` 上）：内存事务表（id→record）+ TTL 定时器；CREATE→引擎 prepare→落 prepared journal；COMMIT→recheck→写 commit-intent journal(fsync)→按计划顺序调用既有单机增删改（addStreamPedantic/更新/删除、consumer 增删改，均走现有函数）→置 committed 并写终结 journal；任一失败置 aborted 并保证此前已应用步骤通过重做/补偿达到计划一致状态（利用 commit-intent 在重启时幂等重做；运行期失败用同样重做路径收敛）。
  - journal 位置：JetStream store 目录下 `txns/<accHash>/<txnId>.json`；record 含状态、版本、ops、时间戳、原因、进度。
  - 启动恢复：扫描 journal——commit-intent 未终结：按确定性顺序幂等重做（create 用 upsert 语义/已存在则校验配置一致，delete 幂等）→committed；prepared：置 aborted(restart) 并保留可查询；终结记录按保留窗口清理。
  - INFO/CANCEL/超时/幂等（同 id 返回首终结态）全部实现；保留窗口=max(2×TTL,10m)。
  - 单机 API handler 复用与集群相同的请求/响应类型与错误。
- **Acceptance Criteria Addressed**: AC-1, AC-6, AC-8, AC-9, AC-10, AC-12, AC-14
- **Test Requirements**:
  - `rule` TR-4.1: 单机 happy path：多流+消费者 create/update/delete 一次提交，全部成功且版本一致；INFO 各状态字段正确。
  - `rule` TR-4.2: 失败注入（第 N 个资源操作失败）后资源集合与提交前一致或按 intent 重做收敛；INFO=aborted 且原因齐全。
  - `rule` TR-4.3: kill -9 重启测试（prepared 与 commit-intent 两时点）：状态与资源集合断言；journal 重做幂等（重复重启结果一致）。
  - `rule` TR-4.4: 幂等（重复 CREATE/COMMIT 三次结果一致、只生效一次）、取消、TTL 超时测试通过。
- **Notes**: 单机路径不经过 meta Raft；原子性以 commit-intent journal + 幂等重做保证。

## Task 5: 集群 meta 层事务条目、apply 与快照扩展
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 3
- **Completion Evidence**:
  - entryOp 追加 `configTxnPrepareOp/CommitOp/AbortOp`；`metaTxn` 负载与三态 apply（`jetstream_txn_cluster.go`）。
  - applyMetaEntries 重构为复用 `applyMetaAssignmentOp`：普通条目与事务步骤统一按账户确定性 +1 并 stamp `_nats.cfg.ver`；commit 单条目整体只 +1，按有序步骤调用既有五个 process*/recoveryUpdates 路径。
  - cc 增加 `cfgVersions/txns`；meta 快照升级为 format=1 信封（streams+versions+txns），旧数组快照仍可解码并从资产元数据重建版本；压缩重放（collectStreamAndConsumerChanges）同步维护版本与事务步骤折叠。
  - go build/vet 通过；两个既有快照测试调用点已适配新签名。集群端到端测试在 Task 6/7 后补齐。
- **Description**:
  - entryOp 追加：`configTxnPrepareOp`、`configTxnCommitOp`、`configTxnAbortOp`（在 `resetSeqOp` 之后）。
  - 编码/解码：`txnRecord`（id、账户、client(forProposal)、reply/subject、ops 归一化计划、期望版本、状态、原因、TTL/时间戳、账户版本、进度所需字段）；三 op 各自 encode/decode。
  - [applyMetaEntries](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_cluster.go#L3411-L3551) 增加分支：
    - prepare：写入 `cc.txns` 事务表（replica 一致），启动/恢复 TTL 责任在 leader；
    - commit：**一个条目内**按 Task 3 有序计划依次调用既有 `processStreamAssignment/processUpdateStreamAssignment/processStreamRemoval/processConsumerAssignment/processConsumerRemoval`（恢复路径走 recoveryUpdates 同一套），对该账户版本只 +1 并在每个 assignment 配置上 stamp 同一版本；置 committed；
    - abort：置 aborted(reason)。
    - recoveryUpdates 扩展事务感知（与现有快照 supersede 协调：事务 commit 内的增删同样进入 ru，顺序保证引用可解析）。
  - 快照：扩展 [encodeMetaSnapshot/decodeMetaSnapshot](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_cluster.go#L2566-L2650) 为带魔数/版本的信封（旧格式：裸 streams JSON 仍可解码；新格式：streams + cfgVersions + 保留窗口内 txns）。压缩/重建快照调用点（L2131/L2135）同步适配。
  - 新副本快照恢复后：事务表、版本计数、assignment 三者一致；超窗口终结事务可物理丢弃。
- **Acceptance Criteria Addressed**: AC-5, AC-10, AC-11, AC-12, AC-13, AC-16
- **Test Requirements**:
  - `rule` TR-5.1: 3 节点集群中一次多资源事务提交，meta 日志仅 1 条 commit op（decode 断言），所有资源 `_nats.cfg.ver` 相同且引用即时成立。
  - `rule` TR-5.2: 旧格式快照字节流可被新 decodeMetaSnapshot 解码（用历史编码形态构造夹具）；新快照在模拟旧节点未知 op 场景行为符合“仅全 level-6 集群可用事务”的门控。
  - `rule` TR-5.3: 新副本通过快照+catchup 追平后，streams/consumers/版本计数/保留窗口事务与 leader 一致（状态哈希比对）。
  - `rule` TR-5.4: 重启任一节点（含 leader）后重放日志，事务最终状态与资源一致，无重复应用导致的版本跳跃。
- **Notes**: 严格复用五个既有 process* 函数，禁止另写资源生效路径。

## Task 6: meta leader 事务协调器（prepare/commit/cancel、TTL、幂等、回复与进度）
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 5
- **Description**:
  - 仅 meta leader 服务写请求（沿用现有 leader gate 与 leaderless 拒绝）；CREATE：引擎 prepare（视图含 inflight 提案）→ 提议 prepare op（携带 reply/ci）→ 返回 prepared 与基准版本。
  - COMMIT：持 `js.mu` 在原子临界内 recheck（CAS/配额/副本/存储/权限），通过则提议 commit op，否则提议 abort 并返回冲突明细；状态置 committing。
  - 回复派发：commit apply 后由 leader 按 record 中的 reply/ci 发送最终响应（版本+进度）；客户端等待机制复用 stream/consumer assignment 结果回流（`processStreamAssignmentResults/processConsumerAssignmentResults` 增加 txn 进度聚合：逐资源 assigned/applied 计数写入事务表）。
  - 领导者切换：新 leader 选主后扫描 `cc.txns`——committing 且已 committed 的补发最终回复（去重 reply 一次）、prepared 重置 TTL 定时器、aborted 保留查询；客户端重试 COMMIT 幂等。
  - TTL sweeper：leader 周期性/定时器对到期 prepared 提议 abort(timeout)；切换时停止旧定时器。
  - 幂等：同 id CREATE/COMMIT/CANCEL 直接回放首终结态；终结记录保留窗口由 leader 清理并（可选）记 abort cleanup。
  - 并发：prepared 事务资源集合 + inflight 视图纳入版本判定；非事务单资源路径在提交 CAS 复核时自然产生冲突，不阻塞其执行。
- **Acceptance Criteria Addressed**: AC-6, AC-7, AC-8, AC-9, AC-10, AC-11, AC-14, AC-16
- **Test Requirements**:
  - `rule` TR-6.1: 混沌测试：prepare 后、commit 提议窗口、commit 已追加未回复三时点 `meta stepdown`/kill leader；客户端（重试/INFO）最终拿到唯一 committed 与最终版本，无重复回复、无版本回退。
  - `rule` TR-6.2: 两事务资源集相交/与单资源 API 竞争的 -race 并发测试：后提交者 aborted(conflict)，明细含资源与期望/实际版本；正确 CAS 重放成功。
  - `rule` TR-6.3: 提交大事务（多 raft group）期间 INFO 进度计数单调推进至全部 applied，随后 committed。
  - `rule` TR-6.4: leaderless/非 leader 节点上写请求行为与现有单资源接口一致（静默丢弃转发 / cluster unavailable 错误口径一致）。
- **Notes**: 锁序遵循 locksordering.txt；不得在持有 `js.mu` 时做磁盘 IO/网络发送。

## Task 7: API 接入、逐操作权限与审计
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 4, Task 6
- **Description**:
  - 在 dispatch 注册四个主题（pairs 表），实现 `jsTxnCreateRequest/jsTxnCommitRequest/jsTxnCancelRequest/jsTxnInfoRequest`：走 `getRequestInfo`、`unmarshalRequest`、API level 门控、账户 JetStream 启用检查，与现有 handler 同构。
  - 权限：事务内每个 op 按等价单资源主题做发布权限判定（定位并复用连接的权限检查 helper，例如对 `$JS.API.STREAM.CREATE.<s>` / `...CONSUMER.CREATE.<s>.<c>` / UPDATE / DELETE 逐一校验）；任一拒绝→整体 `permission` 拒绝，不返回其余资源细节之外的信息；INFO/COMMIT/CANCEL 校验该 txn 归属客户端账户。
  - 响应走 `sendAPIResponse/sendAPIErrResponse`，自动获得 API audit advisory；错误码与 Type 遵循 Task 1 契约。
  - 集群/单机分发：handler 内按 `JetStreamIsClustered()` 走对应管理器。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-14, AC-15
- **Test Requirements**:
  - `rule` TR-7.1: 权限三态（全允许/缺一/全拒）测试；跨账户访问他人 txnId 被拒。
  - `rule` TR-7.2: 端到端冒烟（nats client 风格原始 $JS.API 发布）四端点在集群与单机均工作。
  - `rule` TR-7.3: 未知主题/错误 JSON/未启用 JetStream 等异常输入行为与现有 handler 口径一致。

## Task 8: 回归、竞态与静态检查
- **Status**: `pending`
- **Priority**: high
- **Depends On**: Task 7
- **Description**:
  - 运行 `go build ./...`、`go vet ./server/`、`go test ./server/ -race -count=1 -run 'Txn|Transaction'`。
  - 运行既有 JetStream 全量回归（可分包执行避免超时）：`go test ./server/ -run 'JetStream|Stream|Consumer' -count=1`，修复一切由版本记账/快照扩展引入的不兼容。
  - 对照 AC-15 抽样固化单资源响应 JSON 字段集合断言。
- **Acceptance Criteria Addressed**: AC-15, AC-17
- **Test Requirements**:
  - `rule` TR-8.1: 上述命令全部退出码 0；输出留档到任务 Completion Evidence。
  - `rubric` TR-8.2: 验证充分性；scale 1-5；anchors 1=仅 happy path，3=主要失败分支覆盖，5=含 -race 并发、3 节点混沌（切主/重启/加副本/快照兼容）、幂等与 TTL 全自动化；threshold >= 4；evidence=测试清单与运行记录。
