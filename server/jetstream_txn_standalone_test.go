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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func natsStream(name string, subjects ...string) *nats.StreamConfig {
	if len(subjects) == 0 {
		subjects = []string{name}
	}
	return &nats.StreamConfig{Name: name, Subjects: subjects, Storage: nats.FileStorage}
}

func natsStreamCfg(name string, subjects ...string) *nats.StreamConfig {
	return natsStream(name, subjects...)
}

func standaloneTxnSetup(t *testing.T, opts *Options) (*Server, *Account, *jsAccount, *standaloneTxnManager) {
	if opts == nil {
		o := DefaultTestOptions
		o.Port = -1
		o.JetStream = true
		o.StoreDir = t.TempDir()
		opts = &o
	}
	s := RunServer(opts)
	t.Cleanup(s.Shutdown)
	acc := s.GlobalAccount()
	_, _, jsa := acc.getJetStreamFromAccount()
	require_True(t, jsa != nil)
	return s, acc, jsa, jsa.txnManager()
}

func TestStandaloneTxnCommitHappyPath(t *testing.T) {
	_, acc, jsa, mgr := standaloneTxnSetup(t, nil)

	ops := []JSApiTxnOp{
		consumerCreateOp("S1", "C1"),
		streamCreateOp("S1"),
		streamCreateOp("S2", "s2"),
		{Op: JSApiTxnConsumerCreate, Stream: "S2", Consumer: "C2",
			ConsumerConfig: &ConsumerConfig{Durable: "C2", AckPolicy: AckExplicit}},
	}
	rec, conflicts := mgr.create(&JSApiTxnCreateRequest{Id: "txn-happy", Ops: ops})
	require_True(t, len(conflicts) == 0)
	require_True(t, rec != nil)
	require_Equal(t, rec.State, txnStatePrepared)

	// Nothing exists yet.
	_, err := acc.lookupStream("S1")
	require_True(t, err != nil)

	rec, conflicts = mgr.commit("txn-happy")
	require_True(t, len(conflicts) == 0)
	require_Equal(t, rec.State, txnStateCommitted)

	// A single account-wide version for the whole transaction.
	require_True(t, rec.Version >= 1)
	require_Equal(t, jsa.currentConfigVersion(), rec.Version)

	for _, name := range []string{"S1", "S2"} {
		mset, err := acc.lookupStream(name)
		require_NoError(t, err)
		require_Equal(t, configVersionFromMetadata(mset.config().Metadata), rec.Version)
		for _, cname := range mgrViewConsumers(mset) {
			o := mset.lookupConsumer(cname)
			require_Equal(t, configVersionFromMetadata(o.cfg.Metadata), rec.Version)
		}
	}
	// The new consumer immediately references the txn-created stream.
	mset, err := acc.lookupStream("S1")
	require_NoError(t, err)
	require_True(t, mset.lookupConsumer("C1") != nil)

	// INFO reports the terminal state.
	info, apiErr := mgr.info("txn-happy")
	require_True(t, apiErr == nil)
	require_Equal(t, info.State, txnStateCommitted)
	for _, p := range info.Progress {
		require_True(t, p.Applied)
	}
}

func mgrViewConsumers(mset *stream) []string {
	mset.mu.RLock()
	defer mset.mu.RUnlock()
	var out []string
	for n := range mset.consumers {
		out = append(out, n)
	}
	return out
}

func TestStandaloneTxnIdempotentReplay(t *testing.T) {
	_, _, _, mgr := standaloneTxnSetup(t, nil)

	req := &JSApiTxnCreateRequest{Id: "idem", Ops: []JSApiTxnOp{streamCreateOp("S")}}
	_, c := mgr.create(req)
	require_True(t, len(c) == 0)
	rec1, c := mgr.commit("idem")
	require_True(t, len(c) == 0)
	require_Equal(t, rec1.State, txnStateCommitted)
	v1 := rec1.Version

	// Duplicate CREATE returns the same terminal record, duplicate COMMIT does
	// not bump the version again.
	dup, c := mgr.create(req)
	require_True(t, len(c) == 0)
	require_Equal(t, dup.State, txnStateCommitted)
	rec2, c := mgr.commit("idem")
	require_True(t, len(c) == 0)
	require_Equal(t, rec2.Version, v1)
	require_Equal(t, rec2.State, txnStateCommitted)
}

func TestStandaloneTxnCancel(t *testing.T) {
	_, acc, _, mgr := standaloneTxnSetup(t, nil)
	_, c := mgr.create(&JSApiTxnCreateRequest{Id: "cx", Ops: []JSApiTxnOp{streamCreateOp("S")}})
	require_True(t, len(c) == 0)
	rec, c := mgr.cancel("cx", "")
	require_True(t, len(c) == 0)
	require_Equal(t, rec.State, txnStateAborted)
	require_Equal(t, rec.Reason, txnReasonCancelled)

	// Commit after cancel stays aborted.
	rec, c = mgr.commit("cx")
	require_Equal(t, rec.State, txnStateAborted)
	require_True(t, len(c) == 1)

	// Resource never created.
	_, err := acc.lookupStream("S")
	require_True(t, err != nil)
}

func TestStandaloneTxnTTLAbort(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	opts.JetStreamLimits.MaxTxnTimeout = 2 * time.Second
	_, _, _, mgr := standaloneTxnSetup(t, &opts)

	_, c := mgr.create(&JSApiTxnCreateRequest{Id: "ttl", TTL: 100 * time.Millisecond, Ops: []JSApiTxnOp{streamCreateOp("S")}})
	require_True(t, len(c) == 0)

	time.Sleep(500 * time.Millisecond)
	rec, apiErr := mgr.info("ttl")
	require_True(t, apiErr == nil)
	require_Equal(t, rec.State, txnStateAborted)
	require_Equal(t, rec.Reason, txnReasonTimeout)
}

func TestStandaloneTxnConflictAtCommit(t *testing.T) {
	s, acc, _, mgr := standaloneTxnSetup(t, nil)
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Pre-existing stream at version 1.
	_, err := js.AddStream(natsStream("BASE"))
	require_NoError(t, err)

	ver := uint64(1)
	_, c := mgr.create(&JSApiTxnCreateRequest{Id: "cas", Ops: []JSApiTxnOp{{
		Op: JSApiTxnStreamUpdate, Stream: "BASE", ExpectedVersion: &ver,
		StreamConfig: &StreamConfig{Name: "BASE", Subjects: []string{"BASE", "x"}, Storage: FileStorage},
	}}})
	require_True(t, len(c) == 0)

	// Concurrent non-transactional update.
	_, err = js.UpdateStream(natsStreamCfg("BASE", "BASE", "y"))
	require_NoError(t, err)

	rec, c := mgr.commit("cas")
	require_True(t, len(c) >= 1)
	require_Equal(t, rec.State, txnStateAborted)
	require_Equal(t, c[0].Reason, txnReasonConflict)
	_ = acc
}

func TestStandaloneTxnPreparedAbortsOnRestart(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = dir

	s := RunServer(&opts)
	acc := s.GlobalAccount()
	_, _, jsa := acc.getJetStreamFromAccount()
	mgr := jsa.txnManager()
	_, c := mgr.create(&JSApiTxnCreateRequest{Id: "prep-restart", Ops: []JSApiTxnOp{streamCreateOp("S")}})
	require_True(t, len(c) == 0)
	s.Shutdown()

	// Restart: journal recovers as aborted(restart), queryable.
	s = RunServer(&opts)
	defer s.Shutdown()
	acc = s.GlobalAccount()
	_, _, jsa = acc.getJetStreamFromAccount()
	mgr = jsa.txnManager()
	rec, apiErr := mgr.info("prep-restart")
	require_True(t, apiErr == nil)
	require_Equal(t, rec.State, txnStateAborted)
	require_Equal(t, rec.Reason, txnReasonRestart)
}

func TestStandaloneTxnCommitIntentRedrivenOnRestart(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = dir

	s := RunServer(&opts)
	acc := s.GlobalAccount()
	_, _, jsa := acc.getJetStreamFromAccount()
	mgr := jsa.txnManager()
	// Prepare a txn to learn its layout, then emulate a crash mid-commit by
	// writing a commit-intent journal directly.
	rec, c := mgr.create(&JSApiTxnCreateRequest{Id: "redo-restart", Ops: []JSApiTxnOp{
		streamCreateOp("S"),
		consumerCreateOp("S", "D"),
	}})
	require_True(t, len(c) == 0)
	rec.State = txnStateCommitting
	rec.Version = 5
	require_NoError(t, mgr.persist(rec))
	s.Shutdown()

	// Restart: the commit intent must be redriven to completion exactly once.
	s = RunServer(&opts)
	defer s.Shutdown()
	acc = s.GlobalAccount()
	_, _, jsa = acc.getJetStreamFromAccount()
	mgr = jsa.txnManager()
	got, apiErr := mgr.info("redo-restart")
	require_True(t, apiErr == nil)
	require_Equal(t, got.State, txnStateCommitted)
	require_Equal(t, got.Version, uint64(5))
	require_True(t, jsa.currentConfigVersion() >= 5)

	mset, err := acc.lookupStream("S")
	require_NoError(t, err)
	require_Equal(t, configVersionFromMetadata(mset.config().Metadata), uint64(5))
	require_True(t, mset.lookupConsumer("D") != nil)

	// Journal file exists and is well-formed.
	buf, err := os.ReadFile(filepath.Join(jsa.storeDir, txnsDirName, "redo-restart.json"))
	require_NoError(t, err)
	require_True(t, len(buf) > 0)
}
