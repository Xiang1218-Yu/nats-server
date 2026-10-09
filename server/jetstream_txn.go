// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import "time"

// Configuration transactions allow atomically creating, updating and
// deleting multiple JetStream streams and consumers in a single unit of
// configuration change. The API is intentionally a one-shot model: a CREATE
// request carries the full, ordered set of resource operations, after which
// the transaction can only be committed, cancelled or queried.
const (
	// JSApiTxnCreate is the subject used to create (prepare) a configuration
	// transaction. The full set of operations must be carried in the request.
	JSApiTxnCreate = "$JS.API.TXN.CREATE"

	// JSApiTxnCommitT is the subject template for committing a transaction.
	JSApiTxnCommitT = "$JS.API.TXN.COMMIT.%s"

	// JSApiTxnCancelT is the subject template for cancelling a transaction.
	JSApiTxnCancelT = "$JS.API.TXN.CANCEL.%s"

	// JSApiTxnInfoT is the subject template for querying transaction status.
	JSApiTxnInfoT = "$JS.API.TXN.INFO.%s"
)

// Response type strings for the transaction API.
const (
	JSApiTxnCreateResponseType = "io.nats.jetstream.api.v1.txn_create_response"
	JSApiTxnCommitResponseType = "io.nats.jetstream.api.v1.txn_commit_response"
	JSApiTxnCancelResponseType = "io.nats.jetstream.api.v1.txn_cancel_response"
	JSApiTxnInfoResponseType   = "io.nats.jetstream.api.v1.txn_info_response"
)

// JSCfgVersionMetadataKey is the reserved, static metadata key under which the
// account-wide monotonically increasing configuration version is recorded on
// each stream and consumer configuration produced by a configuration change.
const JSCfgVersionMetadataKey = "_nats.cfg.ver"

// Transaction state machine values.
const (
	txnStatePrepared   = "prepared"
	txnStateCommitting = "committing"
	txnStateCommitted  = "committed"
	txnStateAborted    = "aborted"
)

// Transaction abort reasons.
const (
	txnReasonValidation         = "validation"
	txnReasonPermission         = "permission"
	txnReasonQuota              = "quota"
	txnReasonConflict           = "conflict"
	txnReasonCancelled          = "cancelled"
	txnReasonTimeout            = "timeout"
	txnReasonLeaderChange       = "leader_change"
	txnReasonRestart            = "restart"
	txnReasonClusterUnavailable = "cluster_unavailable"
)

const (
	// defaultTxnTTL is used when a CREATE request does not specify a TTL.
	defaultTxnTTL = 60 * time.Second

	// maxTxnTTL bounds the TTL a client may request when the server does not
	// configure an explicit JetStreamLimits.MaxTxnTimeout.
	maxTxnTTL = 5 * time.Minute

	// defaultMaxTxnOps is the default maximum number of resource operations a
	// single transaction may contain.
	defaultMaxTxnOps = 256

	// txnRetentionMin is the minimum retention window for finalized
	// transaction records after their TTL expired.
	txnRetentionMin = 10 * time.Minute
)

// JSApiTxnAction identifies a single resource operation within a transaction.
type JSApiTxnAction string

const (
	JSApiTxnStreamCreate   JSApiTxnAction = "stream.create"
	JSApiTxnStreamUpdate   JSApiTxnAction = "stream.update"
	JSApiTxnStreamDelete   JSApiTxnAction = "stream.delete"
	JSApiTxnConsumerCreate JSApiTxnAction = "consumer.create"
	JSApiTxnConsumerUpdate JSApiTxnAction = "consumer.update"
	JSApiTxnConsumerDelete JSApiTxnAction = "consumer.delete"
)

// Valid reports whether the action is a known transaction operation.
func (a JSApiTxnAction) Valid() bool {
	switch a {
	case JSApiTxnStreamCreate, JSApiTxnStreamUpdate, JSApiTxnStreamDelete,
		JSApiTxnConsumerCreate, JSApiTxnConsumerUpdate, JSApiTxnConsumerDelete:
		return true
	}
	return false
}

// IsStream reports whether the action targets a stream.
func (a JSApiTxnAction) IsStream() bool {
	switch a {
	case JSApiTxnStreamCreate, JSApiTxnStreamUpdate, JSApiTxnStreamDelete:
		return true
	}
	return false
}

// IsConsumer reports whether the action targets a consumer.
func (a JSApiTxnAction) IsConsumer() bool {
	switch a {
	case JSApiTxnConsumerCreate, JSApiTxnConsumerUpdate, JSApiTxnConsumerDelete:
		return true
	}
	return false
}

// IsCreate reports whether the action creates a resource.
func (a JSApiTxnAction) IsCreate() bool {
	return a == JSApiTxnStreamCreate || a == JSApiTxnConsumerCreate
}

// IsUpdate reports whether the action updates a resource.
func (a JSApiTxnAction) IsUpdate() bool {
	return a == JSApiTxnStreamUpdate || a == JSApiTxnConsumerUpdate
}

// IsDelete reports whether the action deletes a resource.
func (a JSApiTxnAction) IsDelete() bool {
	return a == JSApiTxnStreamDelete || a == JSApiTxnConsumerDelete
}

// JSApiTxnOp is a single resource change in a transaction.
type JSApiTxnOp struct {
	// Op is the action to perform.
	Op JSApiTxnAction `json:"op"`

	// Stream is the target stream name. Required for all operations.
	Stream string `json:"stream"`

	// Consumer is the target consumer name. Required for consumer operations.
	Consumer string `json:"consumer,omitempty"`

	// StreamConfig carries the configuration for stream.create/stream.update.
	StreamConfig *StreamConfig `json:"stream_config,omitempty"`

	// ConsumerConfig carries the configuration for consumer.create/update.
	ConsumerConfig *ConsumerConfig `json:"consumer_config,omitempty"`

	// ExpectedVersion is the optimistic concurrency token.
	// A nil value means the version captured at prepare time must still be
	// current at commit. Zero means the resource must not exist (create).
	ExpectedVersion *uint64 `json:"expected_version,omitempty"`
}

// JSApiTxnCreateRequest is the body of $JS.API.TXN.CREATE.
type JSApiTxnCreateRequest struct {
	// Id is an optional client supplied identifier for idempotency. When
	// empty, the server generates one.
	Id string `json:"id,omitempty"`

	// TTL is the time after which an uncommitted transaction is aborted.
	// Zero means the server default applies.
	TTL time.Duration `json:"ttl,omitempty"`

	// Ops is the full, authoritative set of resource operations.
	Ops []JSApiTxnOp `json:"ops"`
}

// JSApiTxnCancelRequest is the optional body of $JS.API.TXN.CANCEL.<id>.
type JSApiTxnCancelRequest struct {
	Reason string `json:"reason,omitempty"`
}

// JSApiTxnOpProgress reports per-resource assignment progress while a
// transaction is committing.
type JSApiTxnOpProgress struct {
	Op       JSApiTxnAction `json:"op"`
	Stream   string         `json:"stream"`
	Consumer string         `json:"consumer,omitempty"`

	// Assigned is true once the resource assignment has been produced on the
	// meta leader.
	Assigned bool `json:"assigned,omitempty"`

	// Applied is true once the assigned raft group has reported the change as
	// applied. For single-server mode this flips together with Assigned.
	Applied bool `json:"applied,omitempty"`

	// Error carries an API error if this resource failed while applying.
	Error *ApiError `json:"error,omitempty"`
}

// JSApiTxnConflict describes a single validation, permission, quota or
// version conflict that prevented a transaction from being prepared or
// committed.
type JSApiTxnConflict struct {
	// Resource is "stream:<name>" or "consumer:<stream>/<consumer>".
	Resource string `json:"resource"`

	// Op is the action that produced the conflict.
	Op JSApiTxnAction `json:"op,omitempty"`

	// Reason is the machine readable abort reason category.
	Reason string `json:"reason"`

	// ExpectedVersion and ActualVersion are populated for version conflicts.
	ExpectedVersion uint64 `json:"expected_version,omitempty"`
	ActualVersion   uint64 `json:"actual_version,omitempty"`

	// Description is a human readable description.
	Description string `json:"description,omitempty"`

	// ApiError is the underlying JetStream API error, when available.
	ApiError *ApiError `json:"api_error,omitempty"`
}

// JSApiTxnStatus is the common status view returned by create, commit and info.
type JSApiTxnStatus struct {
	Id      string `json:"id"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Version uint64 `json:"version,omitempty"`

	// TTL is the configured transaction lifetime as nanoseconds.
	TTL time.Duration `json:"ttl,omitempty"`

	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated,omitempty"`
	Committed time.Time `json:"committed,omitempty"`
	Aborted   time.Time `json:"aborted,omitempty"`

	// Ops is the echo of the prepared, normalized operation set.
	Ops []JSApiTxnOp `json:"ops,omitempty"`

	// Progress reports per-resource commit progress.
	Progress []JSApiTxnOpProgress `json:"progress,omitempty"`

	// Conflicts lists the reasons a transaction was rejected or aborted.
	Conflicts []JSApiTxnConflict `json:"conflicts,omitempty"`
}

// JSApiTxnCreateResponse is returned by $JS.API.TXN.CREATE.
type JSApiTxnCreateResponse struct {
	ApiResponse
	JSApiTxnStatus
}

// JSApiTxnCommitResponse is returned by $JS.API.TXN.COMMIT.<id>.
type JSApiTxnCommitResponse struct {
	ApiResponse
	JSApiTxnStatus
}

// JSApiTxnCancelResponse is returned by $JS.API.TXN.CANCEL.<id>.
type JSApiTxnCancelResponse struct {
	ApiResponse
	JSApiTxnStatus
}

// JSApiTxnInfoResponse is returned by $JS.API.TXN.INFO.<id>.
type JSApiTxnInfoResponse struct {
	ApiResponse
	JSApiTxnStatus
}

// txnResourceKey returns the canonical key for the resource targeted by an op.
func (op *JSApiTxnOp) txnResourceKey() string {
	if op.Consumer != _EMPTY_ {
		return "consumer:" + op.Stream + "/" + op.Consumer
	}
	return "stream:" + op.Stream
}

// effectiveTxnTTL returns the TTL to apply, clamped to the configured maximum.
func effectiveTxnTTL(configured time.Duration, s *Server) time.Duration {
	max := maxTxnTTL
	if lim := s.getOpts().JetStreamLimits.MaxTxnTimeout; lim > 0 {
		max = lim
	}
	switch {
	case configured <= 0:
		d := defaultTxnTTL
		if d > max {
			return max
		}
		return d
	case configured > max:
		return max
	default:
		return configured
	}
}

// maxTxnOps returns the maximum number of operations per transaction.
func maxTxnOps(s *Server) int {
	if m := s.getOpts().JetStreamLimits.MaxTxnOps; m > 0 {
		return m
	}
	return defaultMaxTxnOps
}

// txnRetentionWindow returns how long a finalized transaction record must
// remain queryable.
func txnRetentionWindow(ttl time.Duration) time.Duration {
	w := 2 * ttl
	if w < txnRetentionMin {
		w = txnRetentionMin
	}
	return w
}

// currentConfigVersion returns the current account-wide configuration version.
func (jsa *jsAccount) currentConfigVersion() uint64 {
	jsa.cfvMu.Lock()
	defer jsa.cfvMu.Unlock()
	return jsa.cfgVersion
}

// seedConfigVersion advances the in-memory configuration version counter to
// at least ver without consuming a new version. It is used while recovering
// assets from disk and when a configuration carrying an authoritative version
// (stamped by the meta layer or the transaction manager) is applied locally.
func (jsa *jsAccount) seedConfigVersion(ver uint64) {
	if ver == 0 {
		return
	}
	jsa.cfvMu.Lock()
	if ver > jsa.cfgVersion {
		jsa.cfgVersion = ver
	}
	jsa.cfvMu.Unlock()
}

// bumpConfigVersion consumes and returns the next account-wide configuration
// version. The counter is strictly monotonic.
func (jsa *jsAccount) bumpConfigVersion() uint64 {
	jsa.cfvMu.Lock()
	jsa.cfgVersion++
	ver := jsa.cfgVersion
	jsa.cfvMu.Unlock()
	return ver
}

// bumpStreamConfigVersion assigns the next configuration version to a stream
// configuration applied through a non-transactional standalone API path.
func (jsa *jsAccount) bumpStreamConfigVersion(cfg *StreamConfig) uint64 {
	ver := jsa.bumpConfigVersion()
	setStreamConfigVersion(cfg, ver)
	return ver
}

// bumpConsumerConfigVersion assigns the next configuration version to a
// consumer configuration applied through a non-transactional standalone API
// path.
func (jsa *jsAccount) bumpConsumerConfigVersion(cfg *ConsumerConfig) uint64 {
	ver := jsa.bumpConfigVersion()
	setConsumerConfigVersion(cfg, ver)
	return ver
}
