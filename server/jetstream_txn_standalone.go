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

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nuid"
)

const txnsDirName = "txns"

// txnRecord is the runtime state of a configuration transaction. The same
// shape is used by the standalone manager (journal persisted) and the meta
// layer (replicated through raft).
type txnRecord struct {
	Id        string
	Account   string
	State     string
	Reason    string
	TTL       time.Duration
	Created   time.Time
	Updated   time.Time
	Committed time.Time
	Aborted   time.Time

	// Plan is the normalized, ordered operation set produced at prepare time.
	Plan *txnPlan

	// Version is the account-wide configuration version produced by a
	// successful commit.
	Version uint64

	// Conflicts carries rejection/abort reasons.
	Conflicts []JSApiTxnConflict

	// Progress is the per-resource apply view.
	Progress []JSApiTxnOpProgress

	// Standalone only: expiry timer management.
	timer *time.Timer

	// Clustered only: originating client/reply for the final response and the
	// meta raft term the record was last proposed in.
	client *ClientInfo
	reply  string
	term   uint64
}

// status converts a runtime record into the public status view.
func (r *txnRecord) status() JSApiTxnStatus {
	st := JSApiTxnStatus{
		Id:        r.Id,
		State:     r.State,
		Reason:    r.Reason,
		Version:   r.Version,
		TTL:       r.TTL,
		Created:   r.Created,
		Updated:   r.Updated,
		Committed: r.Committed,
		Aborted:   r.Aborted,
		Progress:  r.Progress,
		Conflicts: r.Conflicts,
	}
	if r.Plan != nil {
		st.Ops = r.Plan.ops
	}
	return st
}

// localTxnJournal is the on-disk representation of a standalone transaction.
type localTxnJournal struct {
	Id        string                 `json:"id"`
	Account   string                 `json:"account"`
	State     string                 `json:"state"`
	Reason    string                 `json:"reason,omitempty"`
	TTL       time.Duration          `json:"ttl"`
	Created   time.Time              `json:"created"`
	Updated   time.Time              `json:"updated,omitempty"`
	Committed time.Time              `json:"committed,omitempty"`
	Aborted   time.Time              `json:"aborted,omitempty"`
	Ops       []JSApiTxnOp           `json:"ops"`
	Baseline  map[string]uint64      `json:"baseline"`
	Version   uint64                 `json:"version,omitempty"`
	Conflicts []JSApiTxnConflict     `json:"conflicts,omitempty"`
	Progress  []JSApiTxnOpProgress   `json:"progress,omitempty"`
}

func (r *txnRecord) journal() *localTxnJournal {
	j := &localTxnJournal{
		Id: r.Id, Account: r.Account, State: r.State, Reason: r.Reason, TTL: r.TTL,
		Created: r.Created, Updated: r.Updated, Committed: r.Committed, Aborted: r.Aborted,
		Version: r.Version, Conflicts: r.Conflicts, Progress: r.Progress,
		Baseline: map[string]uint64{},
	}
	if r.Plan != nil {
		j.Ops = r.Plan.ops
		for k, v := range r.Plan.baseline {
			j.Baseline[k] = v
		}
	}
	return j
}

// standaloneTxnView implements txnResourceView over live standalone registries.
type standaloneTxnView struct {
	s   *Server
	acc *Account
	jsa *jsAccount
}

func (v *standaloneTxnView) currentStream(name string) (*StreamConfig, uint64, bool) {
	mset, err := v.acc.lookupStream(name)
	if err != nil || mset == nil {
		return nil, 0, false
	}
	cfg := mset.config()
	return &cfg, configVersionFromMetadata(cfg.Metadata), true
}

func (v *standaloneTxnView) currentConsumer(stream, name string) (*ConsumerConfig, uint64, bool) {
	mset, err := v.acc.lookupStream(stream)
	if err != nil || mset == nil {
		return nil, 0, false
	}
	o := mset.lookupConsumer(name)
	if o == nil {
		return nil, 0, false
	}
	o.mu.RLock()
	cfg := o.cfg
	o.mu.RUnlock()
	return &cfg, configVersionFromMetadata(cfg.Metadata), true
}

func (v *standaloneTxnView) consumersOf(stream string) []string {
	mset, err := v.acc.lookupStream(stream)
	if err != nil || mset == nil {
		return nil
	}
	mset.mu.RLock()
	out := make([]string, 0, len(mset.consumers))
	for name := range mset.consumers {
		out = append(out, name)
	}
	mset.mu.RUnlock()
	sort.Strings(out)
	return out
}

func (v *standaloneTxnView) feasibleStream(cfg *StreamConfig) *ApiError {
	if v.s.JetStreamIsClustered() {
		// Standalone manager must never be used while clustered.
		return NewJSClusterNotAvailError()
	}
	if !v.s.standAloneMode() && cfg.Replicas > 1 {
		return ApiErrors[JSStreamReplicasNotSupportedErr]
	}
	if cfg.Replicas > 1 {
		return ApiErrors[JSStreamReplicasNotSupportedErr]
	}
	return nil
}

// checkLimits validates the simulated post-transaction world against the
// account and server limits using the same reservation primitives as the
// single-resource APIs, recomputed from scratch instead of incrementally.
func (v *standaloneTxnView) checkLimits(postStreams map[string]*StreamConfig, consumerCounts map[string]int) *JSApiTxnConflict {
	jsa := v.jsa
	js := jsa.js
	acc := v.acc

	jsa.usageMu.RLock()
	defer jsa.usageMu.RUnlock()
	js.mu.RLock()
	defer js.mu.RUnlock()
	jsa.mu.RLock()
	defer jsa.mu.RUnlock()

	// Current (live) reservations, per tier and storage, excluding the streams
	// that the transaction touches (their post configs replace them).
	liveCount := map[string]int{}
	liveDisk, liveMem := map[string]int64{}, map[string]int64{}
	for _, mset := range jsa.streams {
		mset.cfgMu.RLock()
		cfg := mset.cfg
		mset.cfgMu.RUnlock()
		_, tier, _ := jsa.selectLimits(cfg.Replicas)
		liveCount[tier]++
		if cfg.MaxBytes > 0 {
			res := accountReservation(tier, cfg.Replicas, cfg.MaxBytes)
			if cfg.Storage == FileStorage {
				liveDisk[tier] += res
			} else {
				liveMem[tier] += res
			}
		}
	}

	postCount := map[string]int{}
	postDisk, postMem := map[string]int64{}, map[string]int64{}
	for name, cfg := range postStreams {
		if cfg == nil {
			continue
		}
		selected, tier, _, apiErr := acc.selectLimits(cfg.Replicas)
		if apiErr != nil {
			c := JSApiTxnConflict{Resource: "stream:" + name, Reason: txnReasonQuota, Description: apiErr.Description, ApiError: apiErr}
			return &c
		}
		postCount[tier]++
		if cfg.MaxBytes > 0 {
			res := accountReservation(tier, cfg.Replicas, cfg.MaxBytes)
			if cfg.Storage == FileStorage {
				postDisk[tier] += res
			} else {
				postMem[tier] += res
			}
		}
		if selected.MaxConsumers > 0 && consumerCounts[name] > selected.MaxConsumers {
			c := JSApiTxnConflict{Resource: "stream:" + name, Reason: txnReasonQuota,
				Description: fmt.Sprintf("stream would host %d consumers, account limit is %d", consumerCounts[name], selected.MaxConsumers),
				ApiError:    NewJSMaximumConsumersLimitError()}
			return &c
		}
	}

	for tier, selected := range jsa.limits {
		lim := selected
		if lim.MaxStreams > 0 && postCount[tier] > lim.MaxStreams {
			c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota,
				Description: fmt.Sprintf("post-transaction stream count %d exceeds account limit %d", postCount[tier], lim.MaxStreams),
				ApiError:    NewJSMaximumStreamsLimitError()}
			return &c
		}
		// Account-wide durable consumers.
		if lim.MaxConsumers > 0 {
			totalConsumers := 0
			for _, n := range consumerCounts {
				totalConsumers += n
			}
			if totalConsumers > lim.MaxConsumers {
				c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota,
					Description: fmt.Sprintf("post-transaction consumer count %d exceeds account limit %d", totalConsumers, lim.MaxConsumers),
					ApiError:    NewJSAccountResourcesExceededError()}
				return &c
			}
		}
		// Bytes. Delta post vs live is applied onto the current server/account
		// reservation counters.
		if lim.MaxStore >= 0 && postDisk[tier] > liveDisk[tier] {
			need := postDisk[tier] - liveDisk[tier]
			if js.storeReserved+need > js.config.MaxStore || int64(lim.MaxStore) >= 0 && liveReservedAccount(jsa.usage, tier, true)+need > int64(lim.MaxStore) {
				c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota, Description: "post-transaction disk reservation exceeds account limit", ApiError: NewJSStorageResourcesExceededError()}
				return &c
			}
		}
		if lim.MaxMemory >= 0 && postMem[tier] > liveMem[tier] {
			need := postMem[tier] - liveMem[tier]
			if js.memReserved+need > js.config.MaxMemory || int64(lim.MaxMemory) >= 0 && liveReservedAccount(jsa.usage, tier, false)+need > int64(lim.MaxMemory) {
				c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota, Description: "post-transaction memory reservation exceeds account limit", ApiError: NewJSMemoryResourcesExceededError()}
				return &c
			}
		}
	}
	return nil
}

func liveReservedAccount(usage map[string]*jsaStorage, tier string, disk bool) int64 {
	u, ok := usage[tier]
	if !ok {
		return 0
	}
	if disk {
		return u.total.store
	}
	return u.total.mem
}

func (v *standaloneTxnView) accountConfigVersion() uint64 {
	return v.jsa.currentConfigVersion()
}

// standaloneTxnManager owns the lifecycle of configuration transactions on a
// non-clustered server.
type standaloneTxnManager struct {
	s    *Server
	acc  *Account
	jsa  *jsAccount
	dir  string
	mu   sync.Mutex
	txns map[string]*txnRecord
	once sync.Once
}

// txnMgrForServer returns the standalone configuration transaction manager for
// an account, or nil when the account has no JetStream enabled locally.
func (a *Account) txnMgrForServer(_ *Server) *standaloneTxnManager {
	_, _, jsa := a.getJetStreamFromAccount()
	if jsa == nil {
		return nil
	}
	return jsa.txnManager()
}

func (jsa *jsAccount) txnManager() *standaloneTxnManager {
	jsa.txnOnce.Do(func() {
		if jsa.account == nil || jsa.js == nil || jsa.js.srv == nil {
			return
		}
		m := &standaloneTxnManager{
			s:    jsa.js.srv,
			acc:  jsa.account,
			jsa:  jsa,
			dir:  filepath.Join(jsa.storeDir, txnsDirName),
			txns: make(map[string]*txnRecord),
		}
		m.recover()
		jsa.txnMgr = m
	})
	return jsa.txnMgr
}

// create prepares a transaction. Conflicts (if any) are returned alongside a
// nil record; a prepared record is returned otherwise.
func (m *standaloneTxnManager) create(req *JSApiTxnCreateRequest) (*txnRecord, []JSApiTxnConflict) {
	m.once.Do(m.recover)
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(req.Ops) > maxTxnOps(m.s) {
		return nil, []JSApiTxnConflict{{Resource: "transaction", Reason: txnReasonValidation,
			Description: fmt.Sprintf("transaction has too many operations: %d exceeds %d", len(req.Ops), maxTxnOps(m.s)),
			ApiError:    NewJSTxnTooManyOpsError(len(req.Ops), maxTxnOps(m.s))}}
	}

	id := req.Id
	if id == _EMPTY_ {
		id = nuid.Next()
	}
	ttl := effectiveTxnTTL(req.TTL, m.s)
	if req.TTL < 0 {
		return nil, []JSApiTxnConflict{{Resource: "transaction", Reason: txnReasonValidation, Description: "transaction ttl is invalid", ApiError: NewJSTxnTTLInvalidError(req.TTL)}}
	}

	if existing := m.txns[id]; existing != nil {
		// Idempotent create: only valid with an identical operation set.
		if plansEqualOps(existing.Plan, req.Ops) {
			return existing, nil
		}
		apiErr := NewJSTxnInvalidStateError(id, existing.State)
		return nil, []JSApiTxnConflict{{Resource: "transaction", Reason: txnReasonValidation, Description: apiErr.Description, ApiError: apiErr}}
	}

	engine := newTxnEngine(m.s, m.acc, &standaloneTxnView{s: m.s, acc: m.acc, jsa: m.jsa})
	plan, conflicts := engine.prepare(req.Ops)
	if conflicts != nil {
		return nil, conflicts
	}

	now := time.Now().UTC()
	rec := &txnRecord{
		Id: id, Account: m.acc.Name, State: txnStatePrepared, TTL: ttl,
		Created: now, Updated: now, Plan: plan,
		Progress: makeProgress(plan),
	}
	if err := m.persist(rec); err != nil {
		apiErr := NewJSStreamGeneralError(err, Unless(err))
		return nil, []JSApiTxnConflict{{Resource: "transaction", Reason: txnReasonValidation, Description: err.Error(), ApiError: apiErr}}
	}
	m.txns[id] = rec
	m.armTimeoutLocked(rec)
	return rec, nil
}

func makeProgress(plan *txnPlan) []JSApiTxnOpProgress {
	if plan == nil {
		return nil
	}
	out := make([]JSApiTxnOpProgress, 0, len(plan.ops))
	for _, op := range plan.ops {
		out = append(out, JSApiTxnOpProgress{Op: op.Op, Stream: op.Stream, Consumer: op.Consumer})
	}
	return out
}

func plansEqualOps(plan *txnPlan, ops []JSApiTxnOp) bool {
	if plan == nil {
		return len(ops) == 0
	}
	if len(plan.ops) != len(ops) {
		return false
	}
	// Compare the normalized plan against the raw incoming ops independent of
	// ordering: same multiset of (action, resource).
	type key struct {
		op       JSApiTxnAction
		stream   string
		consumer string
	}
	counts := map[key]int{}
	for _, op := range plan.ops {
		counts[key{op.Op, op.Stream, op.Consumer}]++
	}
	for _, op := range ops {
		k := key{op.Op, op.Stream, op.Consumer}
		counts[k]--
		if counts[k] < 0 {
			return false
		}
	}
	return true
}

// commit commits a prepared transaction or replays a terminal/idempotent one.
func (m *standaloneTxnManager) commit(id string) (*txnRecord, []JSApiTxnConflict) {
	m.once.Do(m.recover)
	m.mu.Lock()
	defer m.mu.Unlock()

	rec := m.txns[id]
	if rec == nil {
		return nil, []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: txnReasonValidation, Description: "transaction not found", ApiError: NewJSTxnNotFoundError()}}
	}
	switch rec.State {
	case txnStateCommitted:
		return rec, nil
	case txnStateAborted:
		apiErr := NewJSTxnInvalidStateError(id, rec.State)
		return rec, []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: rec.Reason, Description: apiErr.Description, ApiError: apiErr}}
	case txnStateCommitting:
		// Redrive an interrupted commit.
		conflicts := m.applyLocked(rec)
		return rec, conflicts
	}

	// Recheck under the manager lock; prepared txns are not visible elsewhere,
	// so drift here comes from concurrent non-transactional APIs.
	engine := newTxnEngine(m.s, m.acc, &standaloneTxnView{s: m.s, acc: m.acc, jsa: m.jsa})
	if conflicts := engine.recheckAtCommit(rec.Plan); conflicts != nil {
		m.abortLocked(rec, txnReasonConflict, conflicts)
		return rec, conflicts
	}

	rec.State = txnStateCommitting
	rec.Updated = time.Now().UTC()
	// Consume the single account-wide version for the whole transaction.
	rec.Version = m.jsa.bumpConfigVersion()
	if err := m.persist(rec); err != nil {
		rec.Version = 0
		rec.State = txnStatePrepared
		apiErr := NewJSStreamGeneralError(err, Unless(err))
		return nil, []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: txnReasonValidation, Description: err.Error(), ApiError: apiErr}}
	}
	conflicts := m.applyLocked(rec)
	return rec, conflicts
}

// applyLocked applies the ordered plan using existing standalone management
// functions. It is idempotent and reused when redriving an interrupted commit
// after a crash. The caller holds the manager lock.
func (m *standaloneTxnManager) applyLocked(rec *txnRecord) []JSApiTxnConflict {
	var failures []JSApiTxnConflict
	allApplied := true
	for i := range rec.Plan.ops {
		op := &rec.Plan.ops[i]
		progress := &rec.Progress[i]
		if progress.Applied {
			continue
		}
		if err := m.applyOp(rec.Version, op); err != nil {
			allApplied = false
			progress.Error = NewJSStreamGeneralError(err, Unless(err))
			failures = append(failures, JSApiTxnConflict{
				Resource: op.txnResourceKey(), Op: op.Op, Reason: txnReasonValidation,
				Description: err.Error(), ApiError: progress.Error,
			})
			continue
		}
		progress.Assigned = true
		progress.Applied = true
		progress.Error = nil
		// Persist progress incrementally so a crash can resume the redo.
		_ = m.persist(rec)
	}

	now := time.Now().UTC()
	rec.Updated = now
	if allApplied {
		rec.State = txnStateCommitted
		rec.Committed = now
		rec.Version = m.jsa.currentConfigVersion()
		m.stopTimerLocked(rec)
	} else {
		rec.State = txnStateCommitting
	}
	if err := m.persist(rec); err != nil {
		m.s.Warnf("Failed persisting txn %q progress: %v", rec.Id, err)
	}
	if allApplied {
		m.scheduleCleanupLocked(rec)
	}
	return failures
}

func (m *standaloneTxnManager) applyOp(ver uint64, op *JSApiTxnOp) error {
	switch op.Op {
	case JSApiTxnStreamCreate, JSApiTxnStreamUpdate:
		cfg := *op.StreamConfig
		cfg.Name = op.Stream
		setStaticStreamMetadata(&cfg)
		setStreamConfigVersion(&cfg, ver)
		if op.Op == JSApiTxnStreamCreate {
			mset, err := m.acc.addStreamPedantic(&cfg, false)
			if err != nil {
				return err
			}
			m.jsa.seedConfigVersion(ver)
			_ = mset
			return nil
		}
		mset, err := m.acc.lookupStream(op.Stream)
		if err != nil {
			return err
		}
		if err := mset.updatePedantic(&cfg, false); err != nil {
			return err
		}
		m.jsa.seedConfigVersion(ver)
		return nil
	case JSApiTxnStreamDelete:
		mset, err := m.acc.lookupStream(op.Stream)
		if err != nil {
			return err
		}
		return mset.delete()
	case JSApiTxnConsumerCreate, JSApiTxnConsumerUpdate:
		mset, err := m.acc.lookupStream(op.Stream)
		if err != nil {
			return err
		}
		cfg := *op.ConsumerConfig
		cfg.Name = op.Consumer
		setStaticConsumerMetadata(&cfg)
		setConsumerConfigVersion(&cfg, ver)
		_, err = mset.addConsumerWithAction(&cfg, ActionCreateOrUpdate, false)
		if err != nil {
			return err
		}
		m.jsa.seedConfigVersion(ver)
		return nil
	case JSApiTxnConsumerDelete:
		mset, err := m.acc.lookupStream(op.Stream)
		if err != nil {
			return err
		}
		o := mset.lookupConsumer(op.Consumer)
		if o == nil {
			return nil // already gone; idempotent
		}
		return o.delete()
	}
	return fmt.Errorf("unknown transaction op %q", op.Op)
}

// cancel aborts a prepared transaction.
func (m *standaloneTxnManager) cancel(id, reason string) (*txnRecord, []JSApiTxnConflict) {
	m.once.Do(m.recover)
	m.mu.Lock()
	defer m.mu.Unlock()

	rec := m.txns[id]
	if rec == nil {
		return nil, []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: txnReasonValidation, Description: "transaction not found", ApiError: NewJSTxnNotFoundError()}}
	}
	switch rec.State {
	case txnStatePrepared:
		r := reason
		if r == _EMPTY_ {
			r = txnReasonCancelled
		}
		m.abortLocked(rec, txnReasonCancelled, []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: txnReasonCancelled, Description: r}})
		return rec, nil
	case txnStateAborted, txnStateCommitted:
		return rec, nil
	default:
		apiErr := NewJSTxnInvalidStateError(id, rec.State)
		return rec, []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: rec.Reason, Description: apiErr.Description, ApiError: apiErr}}
	}
}

func (m *standaloneTxnManager) abortLocked(rec *txnRecord, reason string, conflicts []JSApiTxnConflict) {
	now := time.Now().UTC()
	rec.State = txnStateAborted
	rec.Reason = reason
	rec.Conflicts = conflicts
	rec.Aborted = now
	rec.Updated = now
	m.stopTimerLocked(rec)
	_ = m.persist(rec)
	m.scheduleCleanupLocked(rec)
}

// info returns a record by id.
func (m *standaloneTxnManager) info(id string) (*txnRecord, *ApiError) {
	m.once.Do(m.recover)
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.txns[id]
	if rec == nil {
		return nil, NewJSTxnNotFoundError()
	}
	return rec, nil
}

func (m *standaloneTxnManager) armTimeoutLocked(rec *txnRecord) {
	rec.timer = time.AfterFunc(rec.TTL, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		cur := m.txns[rec.Id]
		if cur != nil && cur.State == txnStatePrepared {
			m.abortLocked(cur, txnReasonTimeout, []JSApiTxnConflict{{Resource: "transaction:" + cur.Id, Reason: txnReasonTimeout, Description: "transaction timed out before commit"}})
		}
	})
}

func (m *standaloneTxnManager) stopTimerLocked(rec *txnRecord) {
	if rec.timer != nil {
		rec.timer.Stop()
		rec.timer = nil
	}
}

func (m *standaloneTxnManager) scheduleCleanupLocked(rec *txnRecord) {
	window := txnRetentionWindow(rec.TTL)
	time.AfterFunc(window, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		_ = os.Remove(m.journalPath(rec.Id))
		delete(m.txns, rec.Id)
	})
}

func (m *standaloneTxnManager) journalPath(id string) string {
	return filepath.Join(m.dir, id+".json")
}

func (m *standaloneTxnManager) persist(rec *txnRecord) error {
	if err := os.MkdirAll(m.dir, defaultDirPerms); err != nil {
		return err
	}
	buf, err := json.MarshalIndent(rec.journal(), _EMPTY_, "  ")
	if err != nil {
		return err
	}
	tmp := m.journalPath(rec.Id) + ".tmp"
	if err := os.WriteFile(tmp, buf, defaultFilePerms); err != nil {
		return err
	}
	return os.Rename(tmp, m.journalPath(rec.Id))
}

// recover loads journals from disk. It runs once, after standalone assets have
// been recovered from disk: prepared transactions become queryable
// aborted(restart) records, and commit intents are redriven idempotently.
func (m *standaloneTxnManager) recover() {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		m.s.Warnf("Failed reading configuration transaction directory %q: %v", m.dir, err)
		return
	}
	now := time.Now().UTC()
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".json" {
			continue
		}
		buf, err := os.ReadFile(filepath.Join(m.dir, ent.Name()))
		if err != nil {
			continue
		}
		var j localTxnJournal
		if err := json.Unmarshal(buf, &j); err != nil {
			m.s.Warnf("Skipping corrupt transaction journal %q: %v", ent.Name(), err)
			continue
		}
		rec := &txnRecord{
			Id: j.Id, Account: j.Account, State: j.State, Reason: j.Reason, TTL: j.TTL,
			Created: j.Created, Updated: j.Updated, Committed: j.Committed, Aborted: j.Aborted,
			Version: j.Version, Conflicts: j.Conflicts, Progress: j.Progress,
			Plan:    &txnPlan{ops: j.Ops, baseline: j.Baseline},
		}
		if rec.Progress == nil {
			rec.Progress = makeProgress(rec.Plan)
		}
		if rec.TTL == 0 {
			rec.TTL = defaultTxnTTL
		}

		switch rec.State {
		case txnStatePrepared:
			// Never prepared across a restart: expose as queryable aborted.
			rec.State = txnStateAborted
			rec.Reason = txnReasonRestart
			rec.Aborted = now
			rec.Updated = now
			rec.Conflicts = []JSApiTxnConflict{{Resource: "transaction:" + rec.Id, Reason: txnReasonRestart, Description: "server restarted before the transaction was committed"}}
			_ = m.persist(rec)
		case txnStateCommitting:
			// Redrive the commit intent idempotently.
			if rec.Version == 0 {
				rec.Version = m.jsa.bumpConfigVersion()
			}
			m.jsa.seedConfigVersion(rec.Version)
			m.applyLocked(rec)
		}
		m.txns[rec.Id] = rec
		if rec.State == txnStateAborted || rec.State == txnStateCommitted {
			m.scheduleCleanupLocked(rec)
		}
	}
}
