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
	"bytes"
	"encoding/json"
	"time"
)

// metaTxnPhase identifies the kind of replicated transaction entry.
type metaTxnPhase uint8

const (
	mtxPrepare metaTxnPhase = iota
	mtxCommit
	mtxAbort
)

// metaTxn is the replicated payload of a configuration transaction meta
// entry. Prepare entries make every meta member track the open transaction and
// its plan; commit entries carry the ordered, fully built assignment steps
// that constitute the atomic change; abort entries record cancellation,
// timeout or commit-time conflicts.
type metaTxn struct {
	Id        string             `json:"id"`
	Account   string             `json:"account"`
	Phase     metaTxnPhase       `json:"phase"`
	Client    *ClientInfo        `json:"client,omitempty"`
	Subject   string             `json:"subject,omitempty"`
	Reply     string             `json:"reply,omitempty"`
	TTL       int64              `json:"ttl,omitempty"` // nanoseconds
	Created   time.Time          `json:"created"`
	Reason    string             `json:"reason,omitempty"`
	Conflicts []JSApiTxnConflict `json:"conflicts,omitempty"`

	// Ops and Baseline carry the ordered, normalized plan on prepare entries.
	Ops      []JSApiTxnOp      `json:"ops,omitempty"`
	Baseline map[string]uint64 `json:"baseline,omitempty"`

	// Steps is the ordered set of existing meta assignment operations
	// (assignStreamOp/updateStreamOp/removeStreamOp/assignConsumerOp/
	// removeConsumerOp), each prefixed by its entryOp byte. Only present on
	// commit entries.
	Steps [][]byte `json:"steps,omitempty"`
}

// snapshotTxn is the transaction representation embedded in meta snapshots.
// Client reply information and committed step payloads are not retained.
type snapshotTxn struct {
	Id        string             `json:"id"`
	State     string             `json:"state"`
	Reason    string             `json:"reason,omitempty"`
	TTL       int64              `json:"ttl,omitempty"`
	Created   time.Time          `json:"created"`
	Updated   time.Time          `json:"updated,omitempty"`
	Committed time.Time          `json:"committed,omitempty"`
	Aborted   time.Time          `json:"aborted,omitempty"`
	Ops       []JSApiTxnOp       `json:"ops,omitempty"`
	Baseline  map[string]uint64  `json:"baseline,omitempty"`
	Version   uint64             `json:"version,omitempty"`
	Conflicts []JSApiTxnConflict `json:"conflicts,omitempty"`
}

func encodeMetaTxn(phase entryOp, mtx *metaTxn) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(phase))
	_ = json.NewEncoder(&bb).Encode(mtx)
	return bb.Bytes()
}

func decodeMetaTxn(buf []byte) (*metaTxn, error) {
	var mtx metaTxn
	if err := json.Unmarshal(buf, &mtx); err != nil {
		return nil, err
	}
	return &mtx, nil
}

// bumpMetaConfigVersion returns the next account-wide configuration version.
// Assignment process functions take js.mu themselves, so callers must not hold
// it.
func (js *jetStream) bumpMetaConfigVersion(account string) uint64 {
	js.mu.Lock()
	defer js.mu.Unlock()
	cc := js.cluster
	if cc == nil {
		return 0
	}
	if cc.cfgVersions == nil {
		cc.cfgVersions = make(map[string]uint64)
	}
	cc.cfgVersions[account]++
	return cc.cfgVersions[account]
}

func (js *jetStream) currentMetaConfigVersion(account string) uint64 {
	js.mu.RLock()
	defer js.mu.RUnlock()
	if js.cluster == nil || js.cluster.cfgVersions == nil {
		return 0
	}
	return js.cluster.cfgVersions[account]
}

// stampStreamAssignmentVersion writes the version into the assignment's
// configuration and keeps ConfigJSON consistent.
func stampStreamAssignmentVersion(sa *streamAssignment, ver uint64) {
	if sa == nil || sa.Config == nil {
		return
	}
	setStreamConfigVersion(sa.Config, ver)
	if buf, err := json.Marshal(sa.Config); err == nil {
		sa.ConfigJSON = buf
	}
}

func stampConsumerAssignmentVersion(ca *consumerAssignment, ver uint64) {
	if ca == nil || ca.Config == nil {
		return
	}
	setConsumerConfigVersion(ca.Config, ver)
	if buf, err := json.Marshal(ca.Config); err == nil {
		ca.ConfigJSON = buf
	}
}

// applyMetaAssignmentOp applies one standard stream/consumer assignment meta
// op. When ver is zero it consumes the next account configuration version
// (ordinary single-resource entries); transaction commit steps pass the shared
// commit version explicitly.
func (js *jetStream) applyMetaAssignmentOp(op entryOp, payload []byte, ru *recoveryUpdates, ver uint64) error {
	decode := func() (account string, sa *streamAssignment, ca *consumerAssignment, err error) {
		switch op {
		case assignStreamOp, removeStreamOp, updateStreamOp:
			sa, err = decodeStreamAssignment(js.srv, payload)
			if err == nil {
				account = sa.Client.serviceAccount()
			}
		case assignConsumerOp:
			ca, err = decodeConsumerAssignment(payload)
			if err == nil {
				account = ca.Client.serviceAccount()
			}
		case assignCompressedConsumerOp:
			ca, err = decodeConsumerAssignmentCompressed(payload)
			if err == nil {
				account = ca.Client.serviceAccount()
			}
		case removeConsumerOp:
			ca, err = decodeConsumerAssignment(payload)
			if err == nil {
				account = ca.Client.serviceAccount()
			}
		default:
			err = errBadEntryOp
		}
		if err != nil {
			js.srv.Errorf("JetStream cluster failed to decode meta assignment op %d: %q", op, payload)
		}
		return account, sa, ca, err
	}

	account, sa, ca, err := decode()
	if err != nil {
		return err
	}
	if ver == 0 {
		ver = js.bumpMetaConfigVersion(account)
	}
	if sa != nil {
		stampStreamAssignmentVersion(sa, ver)
	}
	if ca != nil {
		stampConsumerAssignmentVersion(ca, ver)
	}

	switch op {
	case assignStreamOp:
		if ru != nil {
			js.setStreamAssignmentRecovering(sa)
			ru.addStream(sa)
		} else {
			js.processStreamAssignment(sa)
		}
	case removeStreamOp:
		if ru != nil {
			js.setStreamAssignmentRecovering(sa)
			ru.removeStream(sa)
		} else {
			js.processStreamRemoval(sa)
		}
	case updateStreamOp:
		if ru != nil {
			js.setStreamAssignmentRecovering(sa)
			ru.updateStream(sa)
		} else {
			js.processUpdateStreamAssignment(sa)
		}
	case assignConsumerOp, assignCompressedConsumerOp:
		if ru != nil {
			js.setConsumerAssignmentRecovering(ca)
			ru.addOrUpdateConsumer(ca)
		} else {
			js.processConsumerAssignment(ca)
		}
	case removeConsumerOp:
		if ru != nil {
			js.setConsumerAssignmentRecovering(ca)
			ru.removeConsumer(ca)
		} else {
			js.processConsumerRemoval(ca)
		}
	}
	return nil
}

// applyConfigTxn applies a replicated transaction entry. All cluster members
// (leader and followers) apply it deterministically.
func (js *jetStream) applyConfigTxn(mtx *metaTxn, ru *recoveryUpdates) {
	now := time.Now().UTC()
	isRecovering := ru != nil

	js.mu.Lock()
	cc := js.cluster
	if cc != nil && cc.txns == nil {
		cc.txns = make(map[string]map[string]*txnRecord)
	}
	if cc != nil && cc.cfgVersions == nil {
		cc.cfgVersions = make(map[string]uint64)
	}
	if cc != nil && cc.txns[mtx.Account] == nil {
		cc.txns[mtx.Account] = make(map[string]*txnRecord)
	}
	var rec *txnRecord
	if cc != nil {
		rec = cc.txns[mtx.Account][mtx.Id]
	}

	if mtx.Phase == mtxPrepare {
		if rec == nil {
			rec = &txnRecord{
				Id: mtx.Id, Account: mtx.Account, State: txnStatePrepared,
				TTL: time.Duration(mtx.TTL), Created: mtx.Created, Updated: now,
				client: mtx.Client, reply: mtx.Reply,
				Plan: &txnPlan{ops: mtx.Ops, baseline: mtx.Baseline},
			}
			if rec.Plan.baseline == nil {
				rec.Plan.baseline = map[string]uint64{}
			}
			cc.txns[mtx.Account][mtx.Id] = rec
		}
		js.mu.Unlock()
		if !isRecovering {
			js.txnPhaseNotify(rec, mtx, mtxPrepare)
		}
		return
	}

	if mtx.Phase == mtxAbort {
		if rec == nil {
			rec = &txnRecord{Id: mtx.Id, Account: mtx.Account, Created: mtx.Created,
				Plan: &txnPlan{ops: js.stepsToTxnOps(mtx.Steps), baseline: map[string]uint64{}}}
			cc.txns[mtx.Account][mtx.Id] = rec
		}
		rec.State = txnStateAborted
		rec.Reason = mtx.Reason
		rec.Conflicts = mtx.Conflicts
		rec.Aborted = now
		rec.Updated = now
		rec.reply = mtx.Reply
		rec.client = mtx.Client
		if rec.TTL == 0 {
			rec.TTL = time.Duration(mtx.TTL)
		}
		js.mu.Unlock()
		if !isRecovering {
			js.txnPhaseNotify(rec, mtx, mtxAbort)
		}
		return
	}

	// Commit: a single account configuration version for the whole
	// transaction.
	var ver uint64
	if isRecovering {
		// During recovery the stamped versions are already part of the state;
		// keep the counter at least as high as the surviving entry set.
		cc.cfgVersions[mtx.Account]++
		ver = cc.cfgVersions[mtx.Account]
	} else {
		cc.cfgVersions[mtx.Account]++
		ver = cc.cfgVersions[mtx.Account]
	}
	if rec == nil {
		rec = &txnRecord{Id: mtx.Id, Account: mtx.Account, Created: mtx.Created,
			Plan: &txnPlan{ops: js.stepsToTxnOps(mtx.Steps), baseline: map[string]uint64{}}}
		cc.txns[mtx.Account][mtx.Id] = rec
	}
	rec.State = txnStateCommitted
	rec.Version = ver
	rec.Committed = now
	rec.Updated = now
	rec.reply = mtx.Reply
	rec.client = mtx.Client
	rec.Progress = makeProgress(rec.Plan)
	for i := range rec.Progress {
		rec.Progress[i].Assigned = true
		rec.Progress[i].Applied = true
	}
	js.mu.Unlock()

	// Apply steps deterministically in their encoded order. Assignment
	// functions take js.mu themselves, so it must not be held here.
	for _, step := range mtx.Steps {
		if len(step) == 0 {
			continue
		}
		if err := js.applyMetaAssignmentOp(entryOp(step[0]), step[1:], ru, ver); err != nil {
			js.srv.Errorf("JetStream cluster failed to apply transaction %q step: %v", mtx.Id, err)
		}
	}
	if !isRecovering {
		js.txnPhaseNotify(rec, mtx, mtxCommit)
	}
}

// stepsToTxnOps reconstructs a minimal ordered op list from assignment steps
// for INFO display after failover/snapshot.
func (js *jetStream) stepsToTxnOps(steps [][]byte) []JSApiTxnOp {
	ops := make([]JSApiTxnOp, 0, len(steps))
	for _, step := range steps {
		if len(step) == 0 {
			continue
		}
		var op JSApiTxnOp
		switch entryOp(step[0]) {
		case assignStreamOp:
			if sa, err := decodeStreamAssignment(js.srv, step[1:]); err == nil && sa.Config != nil {
				op = JSApiTxnOp{Op: JSApiTxnStreamCreate, Stream: sa.Config.Name}
			}
		case updateStreamOp:
			if sa, err := decodeStreamAssignment(js.srv, step[1:]); err == nil && sa.Config != nil {
				op = JSApiTxnOp{Op: JSApiTxnStreamUpdate, Stream: sa.Config.Name}
			}
		case removeStreamOp:
			if sa, err := decodeStreamAssignment(js.srv, step[1:]); err == nil && sa.Config != nil {
				op = JSApiTxnOp{Op: JSApiTxnStreamDelete, Stream: sa.Config.Name}
			}
		case assignConsumerOp, assignCompressedConsumerOp:
			if ca, err := decodeConsumerAssignment(step[1:]); err == nil {
				op = JSApiTxnOp{Op: JSApiTxnConsumerCreate, Stream: ca.Stream, Consumer: ca.Name}
			}
		case removeConsumerOp:
			if ca, err := decodeConsumerAssignment(step[1:]); err == nil {
				op = JSApiTxnOp{Op: JSApiTxnConsumerDelete, Stream: ca.Stream, Consumer: ca.Name}
			}
		}
		if op.Op != "" {
			ops = append(ops, op)
		}
	}
	return ops
}
