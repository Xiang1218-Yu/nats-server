// Copyright 2019-2026 The NATS Authors
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nuid"
)

// readSnapshotMsgOverhead mirrors the fixed per-message accounting used by the
// memory store (subject/hdr/msg + two 8 byte scalars).
const readSnapshotMsgOverhead = 16

// Server side defaults and hard caps for read snapshots. Stream layer
// configuration can lower the caps but never raise them past the hard values.
const (
	defaultReadSnapshotTTL      = 2 * time.Minute
	maxReadSnapshotTTL          = 10 * time.Minute
	defaultMaxReadSnapshotBytes = int64(64 * 1024 * 1024)
	defaultMaxReadSnapshots     = 32
	defaultReadSnapshotBatch    = 100
	maxReadSnapshotBatch        = 1000
	maxReadSnapshotBatchBytes   = int64(16 * 1024 * 1024)
)

// readSnapshotLiveStore is the read-only subset of StreamStore used by read
// snapshots to merge live messages with messages retained after the snapshot
// was created. Both fileStore and memStore satisfy it.
type readSnapshotLiveStore interface {
	LoadNextMsg(filter string, wc bool, start uint64, smp *StoreMsg) (*StoreMsg, uint64, error)
	FilteredState(seq uint64, subject string) (SimpleState, error)
}

// retainedMsg is a deleted message held for a read snapshot. The byte slices
// are independent copies: fileStore's cache buffers can be recycled and
// memStore entries overwritten.
type retainedMsg struct {
	subj string
	hdr  []byte
	msg  []byte
	ts   int64
	size int64
}

// ReadSnapshot is a short lived, consistent read view over a stream store.
// It pins no physical storage: messages deleted after creation are copied into
// a bounded retained area. If that area would exceed MaxBytes, or a structural
// operation (purge/compact/truncate/leadership loss/shutdown) occurs, the
// snapshot is invalidated and reports a reason.
type ReadSnapshot struct {
	mgr      *readSnapshotManager
	store    readSnapshotLiveStore
	id       string
	filter   string
	wc       bool
	startSeq uint64
	endSeq   uint64
	ttl      time.Duration
	maxBytes int64
	created  time.Time
	expires  time.Time

	// readMu serializes batch reads against each other.
	readMu sync.Mutex

	// mu protects all fields below.
	mu        sync.Mutex
	status    string
	reason    string
	cursor    uint64
	retained  map[uint64]*retainedMsg
	keys      *avl.SequenceSet
	retBytes  int64
	ttlTimer  *time.Timer
	tombTimer *time.Timer
}

// ID returns the snapshot identifier.
func (s *ReadSnapshot) ID() string {
	return s.id
}

// readSnapshotManager owns the read snapshots of a single store instance.
// Capture hooks run under the store write lock and never block: when no
// snapshot is active a single atomic load short-circuits the whole path.
type readSnapshotManager struct {
	store   readSnapshotLiveStore
	mu      sync.RWMutex
	snaps   map[string]*ReadSnapshot
	nactive atomic.Int32
}

func newReadSnapshotManager(store readSnapshotLiveStore) *readSnapshotManager {
	return &readSnapshotManager{
		store: store,
		snaps: make(map[string]*ReadSnapshot),
	}
}

// create validates the request against the current store bounds and registers
// a new snapshot. It must be called while holding the store lock so the
// first/last fence is consistent with concurrent structural operations.
func (mgr *readSnapshotManager) create(cfg ReadSnapshotConfig, firstSeq, lastSeq uint64) (*ReadSnapshot, error) {
	filter := cfg.Filter
	if filter == _EMPTY_ {
		filter = fwcs
	}
	if !IsValidSubject(filter) {
		return nil, ErrReadSnapshotBadRange
	}

	start, end := cfg.StartSeq, cfg.EndSeq
	if start == 0 {
		start = firstSeq
	}
	if end == 0 {
		end = lastSeq
	}
	// An empty store (first > last after a purge) has no view to snapshot.
	if firstSeq > lastSeq || start < firstSeq || end > lastSeq || start > end {
		return nil, ErrReadSnapshotBadRange
	}
	if cfg.TTL <= 0 {
		return nil, ErrReadSnapshotBadRange
	}

	id := cfg.ID
	if id == _EMPTY_ {
		id = nuid.Next()
	}
	now := time.Now().UTC()
	s := &ReadSnapshot{
		mgr:      mgr,
		store:    mgr.store,
		id:       id,
		filter:   filter,
		wc:       subjectHasWildcard(filter),
		startSeq: start,
		endSeq:   end,
		ttl:      cfg.TTL,
		maxBytes: cfg.MaxBytes,
		created:  now,
		expires:  now.Add(cfg.TTL),
		status:   ReadSnapshotActive,
		cursor:   start,
		retained: make(map[uint64]*retainedMsg),
		keys:     &avl.SequenceSet{},
	}

	mgr.mu.Lock()
	if _, exists := mgr.snaps[id]; exists {
		mgr.mu.Unlock()
		return nil, ErrReadSnapshotBadRange
	}
	if cfg.MaxSnaps > 0 && len(mgr.snaps) >= cfg.MaxSnaps {
		mgr.mu.Unlock()
		return nil, ErrReadSnapshotLimit
	}
	mgr.snaps[id] = s
	mgr.nactive.Add(1)
	s.ttlTimer = time.AfterFunc(cfg.TTL, s.onTTLExpiry)
	mgr.mu.Unlock()

	return s, nil
}

func (s *ReadSnapshot) onTTLExpiry() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Both active and completed snapshots expire; released/already expired
	// tombstones do nothing.
	if s.status != ReadSnapshotActive && s.status != ReadSnapshotComplete {
		return
	}
	wasActive := s.status == ReadSnapshotActive
	s.status = ReadSnapshotInvalidated
	s.reason = ReadSnapshotReasonExpired
	if wasActive {
		s.mgr.nactive.Add(-1)
	}
	s.clearRetainedLocked()
	if s.ttlTimer != nil {
		s.ttlTimer.Stop()
		s.ttlTimer = nil
	}
	if s.tombTimer == nil {
		s.tombTimer = time.AfterFunc(s.ttl, func() { s.mgr.removeTombstone(s.id) })
	}
}

// removeTombstone drops an invalidated/complete snapshot once its query grace
// elapsed. Active snapshots are left untouched.
func (mgr *readSnapshotManager) removeTombstone(id string) {
	mgr.mu.Lock()
	s := mgr.snaps[id]
	if s == nil {
		mgr.mu.Unlock()
		return
	}
	s.mu.Lock()
	active := s.status == ReadSnapshotActive
	s.stopTimersLocked()
	s.mu.Unlock()
	if !active {
		delete(mgr.snaps, id)
	}
	mgr.mu.Unlock()
}

// lookup returns the snapshot with the given id or nil.
func (mgr *readSnapshotManager) lookup(id string) *ReadSnapshot {
	mgr.mu.RLock()
	s := mgr.snaps[id]
	mgr.mu.RUnlock()
	return s
}

// list returns all snapshots known to the manager.
func (mgr *readSnapshotManager) list() []*ReadSnapshot {
	mgr.mu.RLock()
	snaps := make([]*ReadSnapshot, 0, len(mgr.snaps))
	for _, s := range mgr.snaps {
		snaps = append(snaps, s)
	}
	mgr.mu.RUnlock()
	return snaps
}

// release explicitly deletes a snapshot and frees its retained area.
// It returns false if no such snapshot exists, so callers can treat it as
// idempotent at the API level.
func (mgr *readSnapshotManager) release(id string) bool {
	mgr.mu.Lock()
	s := mgr.snaps[id]
	if s != nil {
		delete(mgr.snaps, id)
	}
	mgr.mu.Unlock()
	if s == nil {
		return false
	}

	s.mu.Lock()
	if s.status == ReadSnapshotActive {
		mgr.nactive.Add(-1)
	}
	s.status = ReadSnapshotInvalidated
	s.clearRetainedLocked()
	s.stopTimersLocked()
	s.mu.Unlock()
	return true
}

// invalidateAll deactivates every active snapshot with the given reason. Used
// by structural store operations (purge/compact/truncate/shutdown). It is
// deliberately coarse: structural operations must never be blocked or slowed
// by per-record retention work.
func (mgr *readSnapshotManager) invalidateAll(reason string) {
	if mgr.nactive.Load() == 0 {
		return
	}
	mgr.mu.Lock()
	for _, s := range mgr.snaps {
		s.mu.Lock()
		if s.status == ReadSnapshotActive {
			s.deactivateLocked(ReadSnapshotInvalidated, reason)
		}
		s.mu.Unlock()
	}
	mgr.mu.Unlock()
}

// close stops every timer and drops all snapshots on store shutdown/delete.
func (mgr *readSnapshotManager) close() {
	mgr.mu.Lock()
	for _, s := range mgr.snaps {
		s.mu.Lock()
		if s.status == ReadSnapshotActive {
			mgr.nactive.Add(-1)
		}
		s.status = ReadSnapshotInvalidated
		if s.reason == _EMPTY_ {
			s.reason = ReadSnapshotReasonStoreStopped
		}
		s.clearRetainedLocked()
		s.stopTimersLocked()
		s.mu.Unlock()
	}
	mgr.snaps = make(map[string]*ReadSnapshot)
	mgr.mu.Unlock()
}

// capture offers a message that is about to be physically deleted to every
// active snapshot. Called from the store deletion paths while the store write
// lock (and for fileStore the block lock) is held, therefore it must avoid
// IO, channel operations and any call back into the store. Retained copies
// are bounded per snapshot; overflow invalidates the snapshot rather than
// failing or blocking the deletion.
func (mgr *readSnapshotManager) capture(seq uint64, subj string, hdr, msg []byte, ts int64) {
	if mgr.nactive.Load() == 0 {
		return
	}
	mgr.mu.RLock()
	for _, s := range mgr.snaps {
		s.retain(seq, subj, hdr, msg, ts)
	}
	mgr.mu.RUnlock()
}

// retain stores a private copy of a deleted message if it belongs to this
// snapshot's view. s.mu is taken on top of the manager RLock and the store
// lock; the critical section only performs bounded copies and map work.
func (s *ReadSnapshot) retain(seq uint64, subj string, hdr, msg []byte, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.status != ReadSnapshotActive || seq < s.startSeq || seq > s.endSeq {
		return
	}
	if !subjectIsSubsetMatch(subj, s.filter) {
		return
	}
	if _, ok := s.retained[seq]; ok {
		return
	}

	size := int64(len(subj) + len(hdr) + len(msg) + readSnapshotMsgOverhead)
	if s.maxBytes > 0 && s.retBytes+size > s.maxBytes {
		// Bounded retention: yield the snapshot instead of blocking the delete.
		s.deactivateLocked(ReadSnapshotInvalidated, ReadSnapshotReasonSpaceLimit)
		return
	}

	rm := &retainedMsg{
		subj: string(append([]byte(nil), subj...)),
		ts:   ts,
		size: size,
	}
	if len(hdr) > 0 {
		rm.hdr = append([]byte(nil), hdr...)
	}
	if len(msg) > 0 {
		rm.msg = append([]byte(nil), msg...)
	}
	s.retained[seq] = rm
	s.keys.Insert(seq)
	s.retBytes += size
}

// deactivateLocked moves an active snapshot to a terminal status. Completion
// keeps the TTL timer running (a fully read snapshot still expires); an
// invalidation stops it and arms the tombstone removal timer.
// s.mu must be held.
func (s *ReadSnapshot) deactivateLocked(status, reason string) {
	if s.status != ReadSnapshotActive {
		return
	}
	s.status = status
	s.reason = reason
	s.mgr.nactive.Add(-1)
	s.clearRetainedLocked()
	if status == ReadSnapshotInvalidated {
		if s.ttlTimer != nil {
			s.ttlTimer.Stop()
			s.ttlTimer = nil
		}
		if s.tombTimer == nil {
			s.tombTimer = time.AfterFunc(s.ttl, func() { s.mgr.removeTombstone(s.id) })
		}
	}
}

func (s *ReadSnapshot) clearRetainedLocked() {
	s.retained = make(map[uint64]*retainedMsg)
	if s.keys != nil {
		s.keys.Empty()
	}
	s.retBytes = 0
}

func (s *ReadSnapshot) stopTimersLocked() {
	if s.ttlTimer != nil {
		s.ttlTimer.Stop()
		s.ttlTimer = nil
	}
	if s.tombTimer != nil {
		s.tombTimer.Stop()
		s.tombTimer = nil
	}
}

// retainedFirstLocked returns the smallest retained seq >= cursor.
// s.mu must be held.
func (s *ReadSnapshot) retainedFirstLocked(cursor uint64) (uint64, *retainedMsg, bool) {
	var rseq uint64
	s.keys.Range(func(seq uint64) bool {
		if seq >= cursor {
			rseq = seq
			return false
		}
		return true
	})
	rm, ok := s.retained[rseq]
	return rseq, rm, ok
}

// retainedCountLocked counts retained seqs >= cursor.
// s.mu must be held.
func (s *ReadSnapshot) retainedCountLocked(cursor uint64) (n uint64) {
	s.keys.Range(func(seq uint64) bool {
		if seq >= cursor {
			n++
		}
		return true
	})
	return n
}

// dropRetainedBelowLocked discards retained messages already delivered
// (seq < cursor), bounding the retained area to the unread remainder.
// s.mu must be held.
func (s *ReadSnapshot) dropRetainedBelowLocked(cursor uint64) {
	var drop []uint64
	s.keys.Range(func(seq uint64) bool {
		if seq < cursor {
			drop = append(drop, seq)
			return true
		}
		return false
	})
	for _, seq := range drop {
		if rm := s.retained[seq]; rm != nil {
			s.retBytes -= rm.size
			if s.retBytes < 0 {
				s.retBytes = 0
			}
		}
		delete(s.retained, seq)
		s.keys.Delete(seq)
	}
}

// ReadBatch returns up to batch messages starting at reqCursor (zero means the
// server side cursor), ordered by stream sequence. It merges live messages
// with retained (deleted post creation) ones so the snapshot view stays stable.
// It returns the messages, the next cursor, remaining message count and a
// complete flag.
func (s *ReadSnapshot) ReadBatch(reqCursor uint64, batch int, maxBytes int64) ([]*StoreMsg, uint64, uint64, bool, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()

	s.mu.Lock()
	if s.status == ReadSnapshotInvalidated {
		reason := s.reason
		s.mu.Unlock()
		return nil, 0, 0, false, &ReadSnapshotInvalidError{Reason: reason}
	}
	cursor := s.cursor
	if reqCursor != 0 && reqCursor != cursor {
		s.mu.Unlock()
		return nil, 0, 0, false, ErrReadSnapshotCursor
	}
	if batch <= 0 {
		batch = defaultReadSnapshotBatch
	}
	filter, wc, end := s.filter, s.wc, s.endSeq
	s.mu.Unlock()

	var (
		msgs   []*StoreMsg
		nbytes int64
		cur    = cursor
	)

	for cur <= end && len(msgs) < batch {
		// Live candidate is loaded without holding s.mu (LoadNextMsg takes the
		// store lock; taking it under s.mu would invert the lock order).
		var smv StoreMsg
		lsm, lseq, lerr := s.store.LoadNextMsg(filter, wc, cur, &smv)
		if lerr != nil && lerr != ErrStoreEOF && lerr != ErrStoreMsgNotFound {
			return nil, 0, 0, false, lerr
		}
		haveL := lsm != nil && lseq <= end && lseq >= cur

		// Reconcile against the retained area under s.mu: a message the live
		// scan saw can be captured concurrently; the retained copy then wins
		// so a sequence is never served twice or skipped.
		s.mu.Lock()
		rseq, rm, haveR := s.retainedFirstLocked(cur)
		if haveL {
			if _, captured := s.retained[lseq]; captured {
				haveL = false
			}
		}

		var chosen *StoreMsg
		switch {
		case haveR && (!haveL || rseq < lseq || rseq == lseq):
			chosen = &StoreMsg{subj: rm.subj, hdr: rm.hdr, msg: rm.msg, seq: rseq, ts: rm.ts}
		case haveL:
			chosen = lsm
		}
		s.mu.Unlock()

		if chosen == nil {
			// No live or retained match in [cur, end]; the filtered view is done.
			cur = end + 1
			break
		}
		size := int64(len(chosen.subj) + len(chosen.hdr) + len(chosen.msg))
		if maxBytes > 0 && nbytes > 0 && nbytes+size > maxBytes {
			break
		}
		msgs = append(msgs, chosen)
		nbytes += size
		cur = chosen.seq + 1
	}

	s.mu.Lock()
	s.cursor = cur
	s.dropRetainedBelowLocked(cur)
	if cur > end {
		s.deactivateLocked(ReadSnapshotComplete, _EMPTY_)
	}

	// Remaining = retained unread + live matching messages in [cur, end].
	var remaining uint64
	status := s.status
	if status == ReadSnapshotActive {
		remaining = s.retainedCountLocked(cur)
	}
	s.mu.Unlock()

	if status == ReadSnapshotActive {
		ss1, _ := s.store.FilteredState(cur, filter)
		ss2, _ := s.store.FilteredState(end+1, filter)
		live := ss1.Msgs
		if ss2.Msgs < live {
			live -= ss2.Msgs
		} else {
			live = 0
		}
		remaining += live
	}

	return msgs, cur, remaining, cur > end, nil
}

// State returns the observable snapshot state.
func (s *ReadSnapshot) State() ReadSnapshotState {
	s.mu.Lock()
	st := ReadSnapshotState{
		ID:            s.id,
		Status:        s.status,
		Reason:        s.reason,
		Filter:        s.filter,
		StartSeq:      s.startSeq,
		EndSeq:        s.endSeq,
		Cursor:        s.cursor,
		RetainedMsgs:  len(s.retained),
		RetainedBytes: s.retBytes,
		MaxBytes:      s.maxBytes,
		Created:       s.created,
		Expires:       s.expires,
	}
	if s.status == ReadSnapshotActive {
		st.RemainingMsgs = s.retainedCountLocked(s.cursor)
	}
	cursor, end, filter := s.cursor, s.endSeq, s.filter
	s.mu.Unlock()

	if st.Status == ReadSnapshotActive {
		ss1, _ := s.store.FilteredState(cursor, filter)
		ss2, _ := s.store.FilteredState(end+1, filter)
		live := ss1.Msgs
		if ss2.Msgs < live {
			live -= ss2.Msgs
		} else {
			live = 0
		}
		st.RemainingMsgs += live
	}

	return st
}
