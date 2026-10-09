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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// clusteredTxnView implements txnResourceView over meta assignments plus the
// inflight proposal maps.
type clusteredTxnView struct {
	s    *Server
	js   *jetStream
	cc   *jetStreamCluster
	acc  *Account
	ci   *ClientInfo
	lock bool // when true, js.mu is already held by the caller
}

func (v *clusteredTxnView) rlock() func() {
	if v.lock {
		return func() {}
	}
	v.js.mu.RLock()
	return v.js.mu.RUnlock
}

func (v *clusteredTxnView) currentStream(name string) (*StreamConfig, uint64, bool) {
	defer v.rlock()()
	sa := v.js.streamAssignmentOrInflight(v.acc.Name, name)
	if sa == nil || sa.Config == nil {
		return nil, 0, false
	}
	cfg := *sa.Config
	return &cfg, configVersionFromMetadata(sa.Config.Metadata), true
}

func (v *clusteredTxnView) currentConsumer(stream, name string) (*ConsumerConfig, uint64, bool) {
	defer v.rlock()()
	ca := v.js.consumerAssignmentOrInflight(v.acc.Name, stream, name)
	if ca == nil || ca.Config == nil {
		return nil, 0, false
	}
	cfg := *ca.Config
	return &cfg, configVersionFromMetadata(ca.Config.Metadata), true
}

func (v *clusteredTxnView) consumersOf(stream string) []string {
	defer v.rlock()()
	sa := v.js.streamAssignment(v.acc.Name, stream)
	if sa == nil {
		return nil
	}
	out := make([]string, 0, len(sa.consumers))
	for name := range sa.consumers {
		out = append(out, name)
	}
	return out
}

func (v *clusteredTxnView) feasibleStream(cfg *StreamConfig) *ApiError {
	release := v.rlock()
	defer release()
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}
	cluster := v.ci.Cluster
	if cfg.Placement != nil && cfg.Placement.Cluster != _EMPTY_ {
		cluster = cfg.Placement.Cluster
	}
	peers, err := v.cc.selectPeerGroup(replicas, cluster, cfg, nil, 0, nil)
	if len(peers) < replicas {
		if err != nil {
			return NewJSClusterNoPeersError(err)
		}
		return NewJSClusterNoPeersError(fmt.Errorf("only %d online peers for %d replicas", len(peers), replicas))
	}
	return nil
}

func (v *clusteredTxnView) checkLimits(postStreams map[string]*StreamConfig, consumerCounts map[string]int) *JSApiTxnConflict {
	release := v.rlock()
	defer release()

	// Post stream count and reservation per tier.
	type agg struct {
		count  int
		disk   int64
		mem    int64
		limits *JetStreamAccountLimits
		tier   string
		repCfg *StreamConfig
	}
	aggs := map[string]*agg{}
	for name, cfg := range postStreams {
		if cfg == nil {
			continue
		}
		selected, tier, _, apiErr := v.acc.selectLimits(cfg.Replicas)
		if apiErr != nil {
			c := JSApiTxnConflict{Resource: "stream:" + name, Reason: txnReasonQuota, ApiError: apiErr, Description: apiErr.Description}
			return &c
		}
		a := aggs[tier]
		if a == nil {
			a = &agg{limits: selected, tier: tier, repCfg: cfg}
			aggs[tier] = a
		}
		a.count++
		res := accountReservation(tier, cfg.Replicas, cfg.MaxBytes)
		if cfg.Storage == FileStorage {
			a.disk += res
		} else {
			a.mem += res
		}
		// Per stream consumer cap.
		if selected.MaxConsumers > 0 && consumerCounts[name] > selected.MaxConsumers {
			c := JSApiTxnConflict{Resource: "stream:" + name, Reason: txnReasonQuota,
				Description: fmt.Sprintf("stream would host %d consumers, account limit is %d", consumerCounts[name], selected.MaxConsumers),
				ApiError:    NewJSMaximumConsumersLimitError()}
			return &c
		}
	}
	// Account wide consumer total.
	totalConsumers := 0
	for _, n := range consumerCounts {
		totalConsumers += n
	}
	for _, a := range aggs {
		if a.limits.MaxStreams > 0 && a.count > a.limits.MaxStreams {
			c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota,
				Description: fmt.Sprintf("post-transaction stream count %d exceeds account limit %d", a.count, a.limits.MaxStreams),
				ApiError:    NewJSMaximumStreamsLimitError()}
			return &c
		}
		if a.limits.MaxConsumers > 0 && totalConsumers > a.limits.MaxConsumers {
			c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota,
				Description: fmt.Sprintf("post-transaction consumer count %d exceeds account limit %d", totalConsumers, a.limits.MaxConsumers),
				ApiError:    NewJSAccountResourcesExceededError()}
			return &c
		}
		if a.limits.MaxStore >= 0 && a.disk > a.limits.MaxStore {
			c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota,
				Description: "post-transaction disk reservation exceeds account limit",
				ApiError:    NewJSStorageResourcesExceededError()}
			return &c
		}
		if a.limits.MaxMemory >= 0 && a.mem > a.limits.MaxMemory {
			c := JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota,
				Description: "post-transaction memory reservation exceeds account limit",
				ApiError:    NewJSMemoryResourcesExceededError()}
			return &c
		}
	}
	return nil
}

func (v *clusteredTxnView) accountConfigVersion() uint64 {
	return v.js.currentMetaConfigVersion(v.acc.Name)
}

// ---- Leader coordinator entry points (called by the API handlers) ----

func (s *Server) jsClusteredTxnCreate(ci *ClientInfo, acc *Account, subject, reply string, req *JSApiTxnCreateRequest) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}
	if len(req.Ops) > maxTxnOps(s) {
		resp := &JSApiTxnCreateResponse{}
		resp.Type = JSApiTxnCreateResponseType
		resp.Error = NewJSTxnTooManyOpsError(len(req.Ops), maxTxnOps(s))
		s.sendAPIErrResponse(ci, acc, subject, reply, _EMPTY_, s.jsonResponse(resp))
		return
	}
	ttl := effectiveTxnTTL(req.TTL, s)
	if req.TTL < 0 {
		resp := &JSApiTxnCreateResponse{}
		resp.Type = JSApiTxnCreateResponseType
		resp.Error = NewJSTxnTTLInvalidError(req.TTL)
		s.sendAPIErrResponse(ci, acc, subject, reply, _EMPTY_, s.jsonResponse(resp))
		return
	}

	id := req.Id
	if id == _EMPTY_ {
		id = nuid.Next()
	}

	// Full validation runs without js.mu (checkStreamCfg takes it itself).
	view := &clusteredTxnView{s: s, js: js, cc: cc, acc: acc, ci: ci}
	engine := newTxnEngine(s, acc, view)
	plan, conflicts := engine.prepare(req.Ops)
	if conflicts != nil {
		resp := &JSApiTxnCreateResponse{}
		resp.Type = JSApiTxnCreateResponseType
		resp.Error = conflictsToApiError(conflicts)
		resp.Conflicts = conflicts
		s.sendAPIErrResponse(ci, acc, subject, reply, _EMPTY_, s.jsonResponse(resp))
		return
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	if cc.meta == nil {
		return
	}
	// Idempotency under the lock.
	if existing := cc.txns[acc.Name][id]; existing != nil {
		s.txnSendRecord(existing, ci, acc, subject, reply, JSApiTxnCreateResponseType, nil)
		return
	}

	now := time.Now().UTC()
	mtx := &metaTxn{
		Id: id, Account: acc.Name, Phase: mtxPrepare, Client: ci.forProposal(),
		Subject: subject, Reply: reply, TTL: int64(ttl), Created: now,
		Ops: plan.ops, Baseline: plan.baseline,
	}
	if err := cc.meta.Propose(cc.term, encodeMetaTxn(configTxnPrepareOp, mtx)); err != nil {
		return
	}
}

func (s *Server) jsClusteredTxnCommit(ci *ClientInfo, acc *Account, subject, reply, id string) {
	s.clusteredTxnTerminal(ci, acc, subject, reply, id, false)
}

func (s *Server) jsClusteredTxnCancel(ci *ClientInfo, acc *Account, subject, reply, id, reason string) {
	s.clusteredTxnTerminal(ci, acc, subject, reply, id, true)
}

func (s *Server) jsClusteredTxnInfo(ci *ClientInfo, acc *Account, id string) *JSApiTxnInfoResponse {
	js, cc := s.getJetStreamCluster()
	resp := &JSApiTxnInfoResponse{}
	resp.Type = JSApiTxnInfoResponseType
	if js == nil || cc == nil {
		resp.Error = NewJSClusterNotAvailError()
		return resp
	}
	js.mu.RLock()
	rec := cc.txns[acc.Name][id]
	var st JSApiTxnStatus
	if rec != nil {
		st = rec.status()
		st.Progress = s.txnProgressLocked(acc.Name, rec)
	}
	ver := cc.cfgVersions[acc.Name]
	js.mu.RUnlock()
	if rec == nil {
		resp.Error = NewJSTxnNotFoundError()
		return resp
	}
	_ = ver
	st.Version = s.effectiveTxnVersion(acc.Name, rec)
	resp.JSApiTxnStatus = st
	return resp
}

func (s *Server) clusteredTxnTerminal(ci *ClientInfo, acc *Account, subject, reply, id string, cancel bool) {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	if cc.meta == nil {
		return
	}
	rec := cc.txns[acc.Name][id]
	if rec == nil {
		s.txnSendNotFound(ci, acc, subject, reply, cancel)
		return
	}
	switch rec.State {
	case txnStateCommitted:
		s.txnSendRecord(rec, ci, acc, subject, reply, JSApiTxnCommitResponseType, nil)
		return
	case txnStateAborted:
		s.txnSendRecord(rec, ci, acc, subject, reply, cancelResponseType(cancel), rec.Conflicts)
		return
	case txnStateCommitting:
		// Redrive not possible without rebuilding; report current progress.
		s.txnSendRecord(rec, ci, acc, subject, reply, JSApiTxnCommitResponseType, nil)
		return
	}

	if cancel {
		rec.State = txnStateAborted
		rec.Reason = txnReasonCancelled
		rec.Aborted = time.Now().UTC()
		mtx := &metaTxn{Id: id, Account: acc.Name, Phase: mtxAbort,
			Client: ci.forProposal(), Subject: subject, Reply: reply,
			Reason: txnReasonCancelled, Created: rec.Created}
		if err := cc.meta.Propose(cc.term, encodeMetaTxn(configTxnAbortOp, mtx)); err != nil {
			return
		}
		return
	}

	// Commit: atomic recheck under the js lock.
	view := &clusteredTxnView{s: s, js: js, cc: cc, acc: acc, ci: ci, lock: true}
	engine := newTxnEngine(s, acc, view)
	if conflicts := engine.recheckAtCommit(rec.Plan); conflicts != nil {
		rec.State = txnStateAborted
		rec.Reason = txnReasonConflict
		rec.Conflicts = conflicts
		rec.Aborted = time.Now().UTC()
		mtx := &metaTxn{Id: id, Account: acc.Name, Phase: mtxAbort,
			Client: ci.forProposal(), Subject: subject, Reply: reply,
			Reason: txnReasonConflict, Conflicts: conflicts, Created: rec.Created}
		if err := cc.meta.Propose(cc.term, encodeMetaTxn(configTxnAbortOp, mtx)); err != nil {
			return
		}
		s.txnSendRecord(rec, ci, acc, subject, reply, JSApiTxnCommitResponseType, conflicts)
		return
	}

	steps, apiErr := s.buildTxnStepsLocked(ci, acc, rec)
	if apiErr != nil {
		conflicts := []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: txnReasonValidation, Description: apiErr.Description, ApiError: apiErr}}
		rec.State = txnStateAborted
		rec.Reason = txnReasonValidation
		rec.Conflicts = conflicts
		rec.Aborted = time.Now().UTC()
		mtx := &metaTxn{Id: id, Account: acc.Name, Phase: mtxAbort,
			Client: ci.forProposal(), Subject: subject, Reply: reply,
			Reason: txnReasonValidation, Conflicts: conflicts, Created: rec.Created}
		_ = cc.meta.Propose(cc.term, encodeMetaTxn(configTxnAbortOp, mtx))
		s.txnSendRecord(rec, ci, acc, subject, reply, JSApiTxnCommitResponseType, conflicts)
		return
	}

	rec.State = txnStateCommitting
	rec.reply = reply
	rec.client = ci
	mtx := &metaTxn{Id: id, Account: acc.Name, Phase: mtxCommit,
		Client: ci.forProposal(), Subject: subject, Reply: reply,
		Created: rec.Created, Steps: steps}
	if err := cc.meta.Propose(cc.term, encodeMetaTxn(configTxnCommitOp, mtx)); err != nil {
		rec.State = txnStatePrepared
		return
	}
}

func cancelResponseType(cancel bool) string {
	if cancel {
		return JSApiTxnCancelResponseType
	}
	return JSApiTxnCommitResponseType
}

// buildTxnStepsLocked converts the ordered plan into standard meta assignment
// payloads, creating raft groups for new assets. js.mu must be held.
func (s *Server) buildTxnStepsLocked(ci *ClientInfo, acc *Account, rec *txnRecord) ([][]byte, *ApiError) {
	js, cc := s.getJetStreamCluster()
	postSA := make(map[string]*streamAssignment)
	steps := make([][]byte, 0, len(rec.Plan.ops))
	now := time.Now().UTC()

	for i := range rec.Plan.ops {
		op := &rec.Plan.ops[i]
		switch op.Op {
		case JSApiTxnStreamCreate:
			cfg := op.StreamConfig
			rg, err := js.createGroupForStream(ci, cfg)
			if err != nil {
				return nil, NewJSClusterNoPeersError(err)
			}
			rg.setPreferred(s)
			sa := &streamAssignment{Group: rg, Sync: syncSubjForStream(), Config: cfg,
				Client: ci.forProposal(), Created: now}
			postSA[cfg.Name] = sa
			steps = append(steps, encodeAddStreamAssignment(sa))
		case JSApiTxnStreamUpdate:
			osa := postSA[op.Stream]
			if osa == nil {
				osa = js.streamAssignmentOrInflight(acc.Name, op.Stream)
			}
			if osa == nil {
				return nil, NewJSStreamNotFoundError()
			}
			csa := osa.copyGroup()
			csa.Config = op.StreamConfig
			csa.Client = ci.forProposal()
			csa.Reply = _EMPTY_
			csa.Subject = _EMPTY_
			postSA[op.Stream] = csa
			steps = append(steps, encodeUpdateStreamAssignment(csa))
		case JSApiTxnStreamDelete:
			osa := postSA[op.Stream]
			if osa == nil {
				osa = js.streamAssignmentOrInflight(acc.Name, op.Stream)
			}
			if osa == nil {
				return nil, NewJSStreamNotFoundError()
			}
			sa := &streamAssignment{Group: osa.Group, Config: osa.Config, Client: ci.forProposal(), Created: osa.Created}
			steps = append(steps, encodeDeleteStreamAssignment(sa))
		case JSApiTxnConsumerCreate, JSApiTxnConsumerUpdate:
			sa := postSA[op.Stream]
			if sa == nil {
				sa = js.streamAssignmentOrInflight(acc.Name, op.Stream)
			}
			if sa == nil {
				return nil, NewJSStreamNotFoundError()
			}
			cfg := *op.ConsumerConfig
			var rg *raftGroup
			if op.Op == JSApiTxnConsumerCreate {
				var err *selectPeerError
				rg, err = cc.createGroupForConsumer(&cfg, sa)
				if err != nil {
					return nil, NewJSInsufficientResourcesError()
				}
				rg.setPreferred(s)
				rg.Cluster = sa.Group.Cluster
			} else {
				oca := js.consumerAssignmentOrInflight(acc.Name, op.Stream, op.Consumer)
				if oca == nil {
					return nil, NewJSConsumerNotFoundError()
				}
				rg = oca.Group
			}
			ca := &consumerAssignment{Group: rg, Stream: op.Stream, Name: op.Consumer,
				Config: &cfg, Client: ci.forProposal(), Created: now}
			steps = append(steps, encodeAddConsumerAssignment(ca))
		case JSApiTxnConsumerDelete:
			oca := js.consumerAssignmentOrInflight(acc.Name, op.Stream, op.Consumer)
			if oca == nil {
				return nil, NewJSConsumerNotFoundError()
			}
			ca := &consumerAssignment{Group: oca.Group, Stream: oca.Stream, Name: oca.Name,
				Config: oca.Config, Client: ci.forProposal(), Created: oca.Created}
			steps = append(steps, encodeDeleteConsumerAssignment(ca))
		}
	}
	return steps, nil
}

// ---- Response helpers ----

func conflictsToApiError(conflicts []JSApiTxnConflict) *ApiError {
	for _, c := range conflicts {
		if c.ApiError != nil {
			return c.ApiError
		}
	}
	if len(conflicts) > 0 {
		return NewJSTxnValidationError(errors.New(conflicts[0].Description))
	}
	return NewJSTxnValidationError(errors.New("transaction rejected"))
}

func (s *Server) txnSendNotFound(ci *ClientInfo, acc *Account, subject, reply string, cancel bool) {
	t := JSApiTxnCommitResponseType
	if cancel {
		t = JSApiTxnCancelResponseType
	}
	if strings.Contains(subject, "CREATE") {
		t = JSApiTxnCreateResponseType
	}
	resp := &JSApiTxnCommitResponse{}
	resp.Type = t
	resp.Error = NewJSTxnNotFoundError()
	s.sendAPIErrResponse(ci, acc, subject, reply, _EMPTY_, s.jsonResponse(resp))
}

// txnSendRecord sends the public status of a record to the waiting client.
func (s *Server) txnSendRecord(rec *txnRecord, ci *ClientInfo, acc *Account, subject, reply, typ string, conflicts []JSApiTxnConflict) {
	if reply == _EMPTY_ {
		return
	}
	st := rec.status()
	if s.JetStreamIsClustered() {
		st.Progress = s.txnProgressFor(acc.Name, rec)
		st.Version = s.effectiveTxnVersion(acc.Name, rec)
	}
	if conflicts != nil {
		st.Conflicts = conflicts
	}
	var resp any
	switch typ {
	case JSApiTxnCreateResponseType:
		r := &JSApiTxnCreateResponse{}
		r.Type = typ
		r.JSApiTxnStatus = st
		resp = r
	case JSApiTxnCancelResponseType:
		r := &JSApiTxnCancelResponse{}
		r.Type = typ
		r.JSApiTxnStatus = st
		resp = r
	default:
		r := &JSApiTxnCommitResponse{}
		r.Type = JSApiTxnCommitResponseType
		r.JSApiTxnStatus = st
		resp = r
	}
	if rec.State == txnStateAborted {
		if ae := apiResponseFor(typ, conflicts); ae != nil {
			switch r := resp.(type) {
			case *JSApiTxnCommitResponse:
				r.Error = ae
			case *JSApiTxnCancelResponse:
				r.Error = ae
			case *JSApiTxnCreateResponse:
				r.Error = ae
			}
		}
		s.sendAPIErrResponse(ci, acc, subject, reply, _EMPTY_, s.jsonResponse(resp))
		return
	}
	s.sendAPIResponse(ci, acc, subject, reply, _EMPTY_, s.jsonResponse(resp))
}

func apiResponseFor(_ string, conflicts []JSApiTxnConflict) *ApiError {
	if len(conflicts) > 0 {
		return conflictsToApiError(conflicts)
	}
	return NewJSTxnInvalidStateError("", txnStateAborted)
}

// effectiveTxnVersion returns the version to surface for the record: its
// committed version, else the current account version.
func (s *Server) effectiveTxnVersion(accName string, rec *txnRecord) uint64 {
	if rec.State == txnStateCommitted {
		return rec.Version
	}
	js := s.getJetStream()
	if js == nil || js.cluster == nil {
		return 0
	}
	return js.currentMetaConfigVersion(accName)
}

// txnProgressLocked computes per-op progress from current meta state; js.mu
// read lock must be held.
func (s *Server) txnProgressLocked(accName string, rec *txnRecord) []JSApiTxnOpProgress {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil || rec.Plan == nil {
		return nil
	}
	return s.txnProgressForLocked(accName, rec, cc)
}

func (s *Server) txnProgressFor(accName string, rec *txnRecord) []JSApiTxnOpProgress {
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil || rec.Plan == nil {
		return nil
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return s.txnProgressForLocked(accName, rec, cc)
}

func (s *Server) txnProgressForLocked(accName string, rec *txnRecord, cc *jetStreamCluster) []JSApiTxnOpProgress {
	out := make([]JSApiTxnOpProgress, 0, len(rec.Plan.ops))
	for _, op := range rec.Plan.ops {
		p := JSApiTxnOpProgress{Op: op.Op, Stream: op.Stream, Consumer: op.Consumer}
		if rec.State == txnStatePrepared {
			out = append(out, p)
			continue
		}
		switch {
		case op.Op.IsStream():
			sa := cc.streams[accName][op.Stream]
			p.Applied = sa != nil && configVersionFromMetadata(sa.Config.Metadata) == rec.Version
			p.Assigned = sa != nil
		default:
			sa := cc.streams[accName][op.Stream]
			var ok bool
			if sa != nil {
				ca := sa.consumers[op.Consumer]
				ok = ca != nil && configVersionFromMetadata(ca.Config.Metadata) == rec.Version
			}
			p.Applied = ok
			p.Assigned = sa != nil
		}
		out = append(out, p)
	}
	return out
}

// ---- Leader notification / TTL ----

// txnPhaseNotify is invoked on every member right after a transaction entry is
// applied. Only the leader acts, delivering the pending client response once.
func (js *jetStream) txnPhaseNotify(rec *txnRecord, mtx *metaTxn, phase metaTxnPhase) {
	s := js.srv
	if !s.JetStreamIsLeader() {
		return
	}
	ci := rec.client
	reply := rec.reply
	subject := mtx.Subject
	// Consume the reply so the response is delivered at most once.
	rec.reply = _EMPTY_
	rec.client = nil
	if ci == nil {
		ci = mtx.Client
	}
	if reply == _EMPTY_ {
		return
	}
	acc, _ := s.LookupAccount(rec.Account)
	if acc == nil {
		return
	}
	switch phase {
	case mtxPrepare:
		s.txnSendRecord(rec, ci, acc, subject, reply, JSApiTxnCreateResponseType, nil)
	case mtxCommit:
		s.txnSendRecord(rec, ci, acc, subject, reply, JSApiTxnCommitResponseType, nil)
	case mtxAbort:
		typ := JSApiTxnCommitResponseType
		if strings.Contains(subject, "CANCEL") {
			typ = JSApiTxnCancelResponseType
		} else if strings.Contains(subject, "CREATE") {
			typ = JSApiTxnCreateResponseType
		}
		s.txnSendRecord(rec, ci, acc, subject, reply, typ, rec.Conflicts)
	}
}

// txnLeaderInit runs on the meta leader after winning leadership: it arms the
// TTL sweeper and delivers any responses that were pending across the
// leadership change.
func (js *jetStream) txnLeaderInit() {
	s := js.srv
	if !s.JetStreamIsLeader() {
		return
	}

	js.mu.Lock()
	cc := js.cluster
	var pending []*txnRecord
	if cc != nil {
		if cc.txnStop != nil {
			select {
			case <-cc.txnStop:
			default:
				close(cc.txnStop)
			}
		}
		cc.txnStop = make(chan struct{})
		for _, txns := range cc.txns {
			for _, rec := range txns {
				if rec.reply != _EMPTY_ && (rec.State == txnStateCommitted || rec.State == txnStateAborted) {
					pending = append(pending, rec)
				}
			}
		}
	}
	stop := cc.txnStop
	js.mu.Unlock()

	for _, rec := range pending {
		subject := JSApiTxnCommitT
		if rec.State == txnStateAborted && rec.Reason == txnReasonCancelled {
			subject = JSApiTxnCancelT
		}
		phase := mtxCommit
		if rec.State == txnStateAborted {
			phase = mtxAbort
		}
		js.txnPhaseNotify(rec, &metaTxn{Subject: subject, Reply: rec.reply, Client: rec.client}, phase)
	}

	s.startGoRoutine(func() {
		defer s.grWG.Done()
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-s.quitCh:
				return
			case <-t.C:
				js.txnSweepTimeouts(stop)
			}
		}
	})
}

func (js *jetStream) txnSweepTimeouts(stop chan struct{}) {
	s := js.srv
	type expire struct {
		acc string
		id  string
		mtx *metaTxn
	}
	var due []expire

	js.mu.RLock()
	cc := js.cluster
	if cc != nil {
		now := time.Now().UTC()
		for acc, txns := range cc.txns {
			for id, rec := range txns {
				if rec.State != txnStatePrepared {
					continue
				}
				ttl := rec.TTL
				if ttl == 0 {
					ttl = defaultTxnTTL
				}
				if now.Sub(rec.Created) > ttl {
					due = append(due, expire{acc: acc, id: id, mtx: &metaTxn{
						Id: id, Account: acc, Phase: mtxAbort, Created: rec.Created,
						Reason:    txnReasonTimeout,
						Conflicts: []JSApiTxnConflict{{Resource: "transaction:" + id, Reason: txnReasonTimeout, Description: "transaction timed out before commit"}},
					}})
				}
			}
		}
	}
	js.mu.RUnlock()

	for _, e := range due {
		select {
		case <-stop:
			return
		default:
		}
		js.mu.Lock()
		cc2 := js.cluster
		if cc2 != nil && cc2.meta != nil && s.JetStreamIsLeader() {
			// Only propose if still prepared (a commit could have won the race).
			if rec := cc2.txns[e.acc][e.id]; rec != nil && rec.State == txnStatePrepared {
				if err := cc2.meta.Propose(cc2.term, encodeMetaTxn(configTxnAbortOp, e.mtx)); err == nil {
					rec.State = txnStateAborted
					rec.Reason = txnReasonTimeout
				}
			}
		}
		js.mu.Unlock()
	}
}

// txnLeaderStop terminates leader-only transaction activity.
func (js *jetStream) txnLeaderStop() {
	js.mu.Lock()
	defer js.mu.Unlock()
	if cc := js.cluster; cc != nil && cc.txnStop != nil {
		select {
		case <-cc.txnStop:
		default:
			close(cc.txnStop)
		}
		cc.txnStop = nil
	}
}
