# JetStream 跨资源原子配置事务 - 产品需求文档

## Overview
- **Summary**: 为 JetStream 管理面增加“配置事务”能力：客户端在一次事务中对多个流与消费者执行创建、修改、删除；提交前完成全部校验；提交后所有资源以同一个单调递增的配置版本原子可见。事务具备并发冲突检测、幂等提交、取消、超时能力，并在领导者切换、节点重启、副本恢复后保持状态一致与可查询。
- **Purpose**: 当前流/消费者管理接口（`$JS.API.STREAM.*`、`$JS.API.CONSUMER.*`）各自独立走 meta Raft 提议，多资源变更无法原子化：部分成功会留下悬空引用（消费者指向不存在的流）、配额/副本约束只在单资源视角成立、失败后无法统一回滚或查询结果。
- **Target Users**: 需要批量、一致性地编排 JetStream 拓扑的运维平台、控制面与应用开发者。

## Goals
- 单事务包含任意数量的流与消费者 create/update/delete 操作。
- 提交前一次性完成：引用关系、权限、账户配额、复制因子、存储条件校验；任一不通过则整体不生效。
- 提交是一个原子单元：集群模式为**一条** meta Raft 日志条目；所有资源在同一配置版本下可见，流与消费者约束同时生效。
- 提供事务状态查询：提交进度、冲突原因（精确到资源与版本）、最终配置版本。
- 支持乐观并发控制（expected version CAS）、重复提交幂等、取消、TTL 超时。
- 领导者切换、节点重启、副本（含快照/catchup）恢复后：已提交事务不丢不重，未提交事务状态仍可查询。
- 不使用事务时，现有单资源管理接口的请求参数、返回结构与错误语义保持不变。
- 单机（非集群）JetStream 提供与集群一致的事务 API 与状态语义。

## Non-Goals
- 不改变消息面发布/消费语义；事务仅覆盖**配置**（流配置、消费者配置），不包含消息发布、purge、msg delete、stream snapshot/restore、peer remove/evacuate/move 等运维操作。
- 不提供多阶段暂存（BEGIN 后反复 ADD 操作）；CREATE 事务时必须携带全量操作列表。
- 不提供跨账户事务；一个事务的所有操作必须属于同一账户。
- 不提供跨 super-cluster /跨 JetStream domain 的事务。
- 不改变现有单资源接口的 URL、请求/响应 JSON 字段与错误码；版本号仅通过既有元数据机制透出。
- 不实现事务之间的锁等待/排队；冲突采用乐观并发控制，立即返回冲突结果。

## Background & Context
- 管理 API 入口在 [jetstream_api.go](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_api.go#L1108-L1135) 注册；集群请求仅在 meta leader 上处理（如 [jsStreamCreateRequest](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_api.go#L1520-L1634)）。
- 集群配置变更通过 meta Raft 提议五种 entryOp 实现：`assignStreamOp / assignConsumerOp / removeStreamOp / removeConsumerOp / updateStreamOp`，编码与提议见 [jetstream_cluster.go](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_cluster.go#L11522-L11550)，所有副本在 [applyMetaEntries](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_cluster.go#L3411-L3551) 按日志顺序应用；快照只编码 `cc.streams`（[metaSnapshot](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_cluster.go#L2382-L2387)）。
- meta leader 用 `inflightStreams / inflightConsumers` 跟踪在途提议（[trackInflightStreamProposal](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_cluster.go#L1626-L1713)），创建/更新结果通过内部 system subject 回流（`processStreamAssignmentResults`）。
- 现有校验分散在：`checkStreamCfg`、`subjectsOverlap`（跨流 subject 冲突）、`jsClusteredStreamLimitsCheck`、`createGroupForStream/createGroupForConsumer`（副本/放置）、[checkConsumerCfg](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/consumer.go#L729)；单机路径为 `jsNonClusteredStreamLimitsCheck`、`acc.addStreamPedantic`。
- 版本化现状：仅有 API level 与 server version 元数据（[jetstream_versioning.go](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/server/jetstream_versioning.go)），没有单调配置版本，需要新增。`entryOp` 常量只允许**追加**，不能在中间插入（见常量注释）。

## Functional Requirements

### API 形态（已确认：创建即带全量操作）
新增四个账户级 API（经 `$JS.API` 体系导出、走既有审计/鉴权链路）：
- `$JS.API.TXN.CREATE`：请求体携带 `id`（可选，缺省服务端生成）、`ttl`（可选）、`ops[]` 全量操作；成功后事务进入 `prepared`。
- `$JS.API.TXN.COMMIT.<txnId>`：提交；空 body。
- `$JS.API.TXN.CANCEL.<txnId>`：取消；可携带原因。
- `$JS.API.TXN.INFO.<txnId>`：查询状态、进度、冲突原因与最终版本。

单个资源操作 `op`：
- `op`：`stream.create | stream.update | stream.delete | consumer.create | consumer.update | consumer.delete`。
- `stream`、`consumer`（消费者操作必填）、`stream_config`、`consumer_config`（按操作类型携带）。
- `expected_version`（可选，uint64）：乐观锁；缺省表示以 CREATE 时校验快照为准，提交时若资源被改动则冲突；`0` 表示资源必须不存在（仅 create 语义）。

### 行为
- **FR-1 准备校验**：CREATE 时在 meta leader（集群）/本机（单机）对整组操作做合并视图校验，但不改变任何资源；返回 `prepared` 与每个资源解析出的基准版本。
- **FR-2 提交时复核**：COMMIT 时在持锁的原子步骤内复核 CAS、引用、权限、配额、副本、存储条件；全部通过才应用并递增版本；任一失败则事务置 `aborted` 并给出冲突原因，资源零变更。
- **FR-3 原子应用顺序**：同一事务内部确定性排序——先流（按名称排序）后消费者；删除按反序（先消费者后流），保证引用在任何时刻可解析；同一条 meta 日志条目内完成， followers 恢复/重放同样如此。
- **FR-4 配置版本**：每个账户维护单调递增 uint64 配置版本，初值 0；任何配置变更（含非事务单资源接口）提交一次即 +1；事务提交只 +1（而非按操作数递增）。版本以保留元数据键 `_nats.cfg.ver` 写入流/消费者配置（机制与现有 `_nats.req.level` 静态元数据相同），并在事务提交/INFO 响应中返回。
- **FR-5 状态机**：`prepared → committing → committed`，或任意未提交态 → `aborted`（reason: `validation | permission | quota | conflict | cancelled | timeout | leader_change | restart | cluster_unavailable`）。
- **FR-6 幂等**：用客户端 `id` 去重；对同一 txnId 的重复 CREATE/COMMIT/CANCEL 返回首个终结状态与同一版本，不重复生效。
- **FR-7 TTL**：CREATE 可指定 TTL（缺省 60s，上限受 `JetStreamLimits.MaxTxnTimeout` 约束，未配置时上限 5m）；超时由 meta leader 写 abort 条目；单机由本地 TTL 定时器终结。
- **FR-8 查询**：终结后事务记录保留到 TTL 截止（至少 2×TTL 且不少于 10 分钟），期间 INFO 可查；到期后物理清理。
- **FR-9 进度**：committing 期间 INFO 返回各资源 assignment 结果计数（assigned/applied 数），复用现有 stream/consumer assignment 结果回流机制。
- **FR-10 集群可靠性**：prepared/committed/aborted 状态全部由 meta Raft 日志承载；meta leader 切换后，新 leader 接管 TTL 扫描、提交回复与进度跟踪；重启与副本 catchup/快照后状态一致。
- **FR-11 单机可靠性**：事务记录以 JSON journal（JetStream store 目录下 `txns/`）持久化 prepare 意图与 commit 意图；启动时重放：prepared 置 `aborted(restart)`（可查询），commit-intent 按确定性顺序幂等重做至 `committed`。
- **FR-12 兼容性**：新增 meta entryOp 追加在 `resetSeqOp` 之后；旧节点收到未知 op 的行为沿用现有“unknown meta entry op”防护（不支持 mixed-version 集群中使用事务，由 API level 门控，事务要求 API level 6）。

## Non-Functional Requirements
- **NFR-1 性能**：事务校验在提议前完成，失败不产生 Raft 日志垃圾（abort 除外，abort 也写日志以保证可查询）；无事务负载下既有 API 吞吐无可观测回退（benchmark 对比在噪声范围内）。
- **NFR-2 正确性**：全部新代码具备单元/集成测试；集群路径覆盖 leader 切换、重启、副本恢复混沌场景；`go test ./server/ -run 'Txn|Transaction' -race -count=1` 通过。
- **NFR-3 可观测**：事务提交/中止发送与现有 API audit advisory 一致的审计事件；关键错误带资源名与版本信息。
- **NFR-4 安全**：事务中每个操作按“等价单资源 API 主题”做权限校验（如 stream.create S 需具备 `$JS.API.STREAM.CREATE.S` 的发布权限），任一不满足整体拒绝，错误不泄露其余资源细节。
- **NFR-5 代码质量**：通过仓库既有 lint/vet；锁序遵循 [locksordering.txt](file:///Users/tog/Documents/ChatGPT/SWE/workspaces/nats-server-t04-T04/locksordering.txt)；复用既有校验函数，不复制校验逻辑。

## Constraints
- **Technical**: Go、仓库内实现；meta entryOp 仅可追加；meta 快照格式变更需向后兼容（旧快照可解码）；流/消费者 assignment 处理、raft group 创建结果回流等既有机制必须复用。
- **Business**: 不破坏既有单资源 API 契约；不引入新的外部依赖。
- **Dependencies**: meta Raft、raft group 分配结果回流、账户权限体系、JetStreamLimits。

## Assumptions
- 单事务操作数设置上限（默认 256 个 op，可由 `JetStreamLimits.MaxTxnOps` 覆盖），超限拒绝；这是防滥用的服务端约束。
- 事务不做两阶段跨 raft group 锁；原子性边界定义为 **meta 层配置状态原子切换**，数据面（流消息）不受影响；group 建立/副本收敛按既有异步机制进行，进度通过 FR-9 暴露。
- prepared 事务不阻塞非事务单资源 API；漂移在 COMMIT 复核时以冲突检出。这保证单资源接口的错误语义在无并发事务时完全不变，在并发冲突场景返回标准冲突错误（新增错误码，仅出现在并发场景）。
- 删除流时其下消费者由该事务隐式覆盖：若同事务未显式处理，校验阶段报错要求显式声明（`stream.delete` 前必须同事务 `consumer.delete` 该流全部现存消费者），避免隐式行为分歧。
- 事务中的 create/update 不允许改变 sealed 状态之外的既有硬性规则；所有单资源校验规则原样适用于事务内操作。

## Acceptance Criteria

### AC-1: 事务创建与准备契约
- **Type**: `rule`
- **Given**: 启用 JetStream（集群或单机）的账户，客户端具备相应权限。
- **When**: 向 `$JS.API.TXN.CREATE` 提交包含若干流/消费者操作的合法请求。
- **Then**: 返回 200 响应，含 txnId、状态 `prepared`、ttl、每个资源的基准版本；不创建/修改/删除任何实际资源（STREAM.INFO/CONSUMER.INFO 视角无变化）。
- **Pass Condition**: 响应字段齐全且资源状态未变；非法 op 组合在 CREATE 即收到明确错误且无 txnId 颁发。
- **Evidence**: 新增单元/集成测试断言响应与资源侧 INFO。

### AC-2: 提交前引用关系校验
- **Type**: `rule`
- **Given**: 事务包含消费者指向不存在且同事务未创建的流、或 mirror/sources 引用缺失/成环。
- **When**: CREATE 或 COMMIT。
- **Then**: 事务被拒绝/置 aborted，reason 为 `validation`，冲突原因指出具体操作与缺失引用；无任何资源生效。
- **Pass Condition**: 构造三类坏引用（消费者流缺失、mirror 缺失、sources 成环）均被拦截，且同事务内“先建流再建消费者”的合法组合通过。
- **Evidence**: 表驱动测试。

### AC-3: 逐操作权限校验
- **Type**: `rule`
- **Given**: 客户端具备事务 API 权限但缺少其中某资源（如删除某流）的等价单资源权限。
- **When**: CREATE 或 COMMIT 含该操作的事务。
- **Then**: 整体拒绝，权限错误定位到该资源操作；部分有权限的操作也不生效。
- **Pass Condition**: 权限用例（全允许/缺一/全拒）三态测试通过。
- **Evidence**: 鉴权集成测试。

### AC-4: 配额、复制因子与存储条件校验
- **Type**: `rule`
- **Given**: 事务合并后超出账户 max_streams/max_consumers/max_bytes、或 Replicas 大于可用在线节点数、或内存/文件存储条件不满足。
- **When**: CREATE（可预先判定的）与 COMMIT（合并配额复核）。
- **Then**: 对应 reason（`quota`/`validation`）拒绝，错误语义与单资源接口同类错误一致；部分场景（如提交前节点掉线导致副本不足）在 COMMIT 复核时拦截。
- **Pass Condition**: 配额按“事务后总账”计算（同事务删除释放的额度可被创建使用）；副本不足在 CREATE 与 COMMIT 两阶段均有覆盖测试。
- **Evidence**: 配额与副本选择测试。

### AC-5: 原子提交与同一配置版本可见
- **Type**: `rule`
- **Given**: `prepared` 事务含跨流/消费者的多个操作。
- **When**: COMMIT 成功。
- **Then**: 集群 meta 日志只新增 1 条 commit 条目；所有目标资源在同一账户配置版本 V 下可见；流/消费者配置元数据均带 `_nats.cfg.ver=V`；同事务消费者立即能引用同事务新建的流；非事务单资源变更也使版本 +1。
- **Pass Condition**: 检查 meta 条目数、所有资源版本一致、引用即时生效；V 单调且严格大于提交前版本。
- **Evidence**: 集群集成测试读取资源 INFO 元数据与内部 applied index。

### AC-6: 失败回到可查询的未提交结果
- **Type**: `rule`
- **Given**: COMMIT 复核失败（冲突/配额/权限/校验）。
- **When**: 随后 INFO 查询。
- **Then**: 状态 `aborted`，含 reason、失败时间、各失败原因列表；实际资源与提交前完全一致；在保留窗口内可重复查询。
- **Pass Condition**: 提交前后 STREAM.LIST/CONSUMER.LIST 快照一致；INFO 字段完整。
- **Evidence**: 失败注入测试 + 列表快照比对。

### AC-7: 并发修改检测
- **Type**: `rule`
- **Given**: 事务 prepared 后，另一客户端（事务或单资源 API）改动了事务涉及的资源；或两事务资源集相交。
- **When**: 受影响事务 COMMIT。
- **Then**: COMMIT 置 aborted，reason=`conflict`，冲突项包含资源名、expected_version、actual_version；未相交资源不受影响；带正确 expected_version 的重放事务可成功。
- **Pass Condition**: 单资源 API 抢先修改、两事务交叉、CAS 正确重放三个场景测试通过。
- **Evidence**: 并发集成测试（含 -race）。

### AC-8: 重复提交幂等
- **Type**: `rule`
- **Given**: 已 committed 或 aborted 的 txnId。
- **When**: 客户端因超时重发相同 CREATE/COMMIT。
- **Then**: 返回首个终结状态、同一版本与同一结果，不产生第二次变更，版本不重复递增。
- **Pass Condition**: 连续 3 次重复提交结果字节级一致；资源只变更一次。
- **Evidence**: 幂等测试。

### AC-9: 取消事务
- **Type**: `rule`
- **Given**: prepared 事务。
- **When**: CANCEL。
- **Then**: 状态 aborted(reason=cancelled)；此后 COMMIT 返回 aborted/cancelled（不复活）；资源零变更。
- **Pass Condition**: 取消后提交、重复取消、取消不存在事务三种响应符合定义。
- **Evidence**: 单元测试。

### AC-10: TTL 超时
- **Type**: `rule`
- **Given**: prepared 事务 TTL=短时间（如 1s）且不提交。
- **When**: TTL 到期。
- **Then**: 系统自动置 aborted(reason=timeout)，INFO 可查；TTL 上限与缺省值生效（超出上限的 TTL 请求被夹到上限或拒绝，取其一并在文档/错误中明确）。
- **Evidence**: 超时测试。
- **Pass Condition**: 到期后状态、回调时机、INFO 结果正确。

### AC-11: 领导者切换下事务一致
- **Type**: `rule`
- **Given**: 3 节点 meta 集群，事务处于 prepared 或 committing。
- **When**: 触发 meta leader stepdown/重启 leader。
- **Then**: prepared 事务在新 leader 上仍可 INFO/COMMIT/CANCEL；committing 事务最终收敛为 committed 且客户端收到（或重连后 INFO 查到）最终版本；不出现双提交或版本回退。
- **Pass Condition**: 在 prepare 后、commit 提议窗口、commit 已追加未回复三个时点分别切主，结果均一致。
- **Evidence**: 混沌集成测试（kill/stepdown）。

### AC-12: 节点重启后事务状态一致
- **Type**: `rule`
- **Given**: （集群）事务 prepared/committed 后重启某节点；（单机）在 prepared 与 committing 两阶段 kill -9 后重启。
- **When**: 节点恢复完成。
- **Then**: 集群侧该节点 meta 状态与 leader 一致；单机侧 prepared 事务变为可查询的 aborted(restart)，commit-intent 事务重做后所有目标资源一致到达 committed 状态，无半提交。
- **Pass Condition**: 资源集合（名称/配置版本/存在性）与事务声明完全一致；INFO 与资源实际状态吻合。
- **Evidence**: 重启恢复测试（单机含 journal 重放断言）。

### AC-13: 副本恢复（快照/catchup）后状态一致
- **Type**: `rule`
- **Given**: 新 meta 副本加入或落后副本通过快照恢复，期间存在已提交事务与账户版本。
- **When**: 副本追平。
- **Then**: 其流/消费者 assignment 与账户配置版本和 leader 一致；事务记录（保留窗口内）可在该副本服务 INFO 时返回一致结果（或稳定转发给 leader）。
- **Pass Condition**: 快照扩展后旧快照仍可解码；新副本追平后状态哈希一致。
- **Evidence**: 快照兼容测试 + 加副本恢复测试。

### AC-14: 提交进度、冲突原因与最终版本查询
- **Type**: `rule`
- **Given**: 任意状态事务。
- **When**: `$JS.API.TXN.INFO.<id>`。
- **Then**: 返回状态、phase、create/commit 时间戳、ttl、ops 概览、逐资源进度（assigned/applied/失败原因）、终结版本或冲突明细（资源名+期望/实际版本+原因码）。
- **Pass Condition**: 字段契约与示例在测试中固化；不存在的 id 返回标准 not found 错误。
- **Evidence**: INFO 契约测试。

### AC-15: 非事务单资源接口完全兼容
- **Type**: `rule`
- **Given**: 不使用事务 API 的既有客户端。
- **When**: 调用全部流/消费者 create/update/delete 及 info/list 接口。
- **Then**: 请求参数、响应 JSON 结构、错误码与错误语义与变更前一致（配置元数据中新增保留键 `_nats.cfg.ver` 与既有 `_nats.*` 静态元数据同机制，不构成结构变更）；既有相关测试套件零修改通过。
- **Pass Condition**: `go test ./server/ -run 'JetStream'` 既有用例全绿；抽样比对响应 JSON 字段集合。
- **Evidence**: 既有测试全量运行结果。

### AC-16: 与 meta 恢复/快照/分配回流路径的集成质量
- **Type**: `rubric`
- **Dimension**: 复用正确性与侵入控制
- **Scale**: 1-5
- **Anchors**: 1 = 另起一套并行机制，恢复路径行为分叉；3 = 部分复用，少量分叉逻辑需人工保证一致；5 = 完全复用 assignment 编解码、apply、recoveryUpdates、快照与结果回流，仅以“条目包装 + 版本记账 + 事务表”增量接入。
- **Pass Threshold**: >= 4
- **Evidence**: 代码走查（复用点清单）与故障注入测试结果。

### AC-17: 测试覆盖与工程质量
- **Type**: `rubric`
- **Dimension**: 验证充分性
- **Scale**: 1-5
- **Anchors**: 1 = 仅 happy path；3 = happy path + 主要失败分支单机测试；5 = 表驱动校验、权限、并发 -race、3 节点混沌（切主/重启/加副本/快照）、幂等与 TTL 全部自动化且稳定重复运行。
- **Pass Threshold**: >= 4
- **Evidence**: 测试清单与 `-race -count=1` 运行记录。

## Open Questions
- [x] API 形态：创建即带全量操作（用户确认）。
- [x] 部署形态：集群与单机均支持（用户确认）。
- [x] 版本可见性：事务响应 + 资源配置保留元数据（用户确认）。
- [ ] 事务审计 advisory 是否需要独立事件类型（实现阶段默认复用 API audit，不新增事件）。
