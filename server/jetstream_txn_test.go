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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestTxnContractJSONRoundtrip(t *testing.T) {
	exp := uint64(7)
	req := JSApiTxnCreateRequest{
		Id:  "txn-1",
		TTL: time.Second,
		Ops: []JSApiTxnOp{
			{Op: JSApiTxnStreamCreate, Stream: "S1", StreamConfig: &StreamConfig{Name: "S1", Storage: FileStorage}},
			{Op: JSApiTxnConsumerCreate, Stream: "S1", Consumer: "C1", ConsumerConfig: &ConsumerConfig{Durable: "C1"}, ExpectedVersion: &exp},
			{Op: JSApiTxnConsumerDelete, Stream: "S1", Consumer: "C2"},
		},
	}
	b, err := json.Marshal(&req)
	require_NoError(t, err)

	var got JSApiTxnCreateRequest
	require_NoError(t, json.Unmarshal(b, &got))
	require_Equal(t, got.Id, "txn-1")
	require_Equal(t, len(got.Ops), 3)
	require_Equal(t, got.Ops[0].Op, JSApiTxnStreamCreate)
	require_True(t, got.Ops[1].ExpectedVersion != nil)
	require_Equal(t, *got.Ops[1].ExpectedVersion, uint64(7))
	// A nil expected version round-trips as nil (omitempty pointer).
	require_True(t, got.Ops[2].ExpectedVersion == nil)
}

func TestTxnContractUnknownAction(t *testing.T) {
	require_False(t, JSApiTxnAction("nope").Valid())
	require_True(t, JSApiTxnStreamDelete.Valid())
	require_True(t, JSApiTxnConsumerCreate.IsConsumer())
	require_False(t, JSApiTxnStreamCreate.IsConsumer())
	require_True(t, JSApiTxnStreamDelete.IsDelete())
	require_False(t, JSApiTxnConsumerCreate.IsDelete())
}

func TestTxnEffectiveTTLAndMaxOps(t *testing.T) {
	s := &Server{}
	s.opts = &Options{}

	// Default TTL and op limit.
	require_Equal(t, effectiveTxnTTL(0, s), defaultTxnTTL)
	require_Equal(t, effectiveTxnTTL(time.Second, s), time.Second)
	require_Equal(t, effectiveTxnTTL(time.Hour, s), maxTxnTTL)
	require_Equal(t, maxTxnOps(s), defaultMaxTxnOps)

	// Configured overrides.
	s.opts.JetStreamLimits.MaxTxnTimeout = 2 * time.Second
	s.opts.JetStreamLimits.MaxTxnOps = 3
	require_Equal(t, effectiveTxnTTL(0, s), 2*time.Second)
	require_Equal(t, effectiveTxnTTL(time.Hour, s), 2*time.Second)
	require_Equal(t, maxTxnOps(s), 3)
}

func TestTxnRetentionWindow(t *testing.T) {
	require_Equal(t, txnRetentionWindow(time.Second), txnRetentionMin)
	require_True(t, txnRetentionWindow(10*time.Minute) >= 20*time.Minute)
}

func TestStandaloneConfigVersionAccounting(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	acc := s.GlobalAccount()
	_, _, jsa := acc.getJetStreamFromAccount()
	require_True(t, jsa != nil)

	streamVer := func(name string) uint64 {
		mset, err := acc.lookupStream(name)
		require_NoError(t, err)
		return configVersionFromMetadata(mset.config().Metadata)
	}
	consumerVer := func(stream, consumer string) uint64 {
		mset, err := acc.lookupStream(stream)
		require_NoError(t, err)
		o := mset.lookupConsumer(consumer)
		require_True(t, o != nil)
		return configVersionFromMetadata(o.cfg.Metadata)
	}

	_, err := js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	require_Equal(t, streamVer("S1"), uint64(1))
	require_Equal(t, jsa.currentConfigVersion(), uint64(1))

	// A second stream bumps the account-wide version independently.
	_, err = js.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	require_Equal(t, streamVer("S2"), uint64(2))

	// Consumer create bumps once.
	_, err = js.AddConsumer("S1", &nats.ConsumerConfig{Durable: "C1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	require_Equal(t, consumerVer("S1", "C1"), uint64(3))

	// Stream update bumps and re-stamps; the stream's stored version is the
	// account version at update time, strictly newer than at create time.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1", "s1b"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	require_Equal(t, streamVer("S1"), uint64(4))

	// Client supplied version metadata must never be honored.
	si, err := js.StreamInfo("S2")
	require_NoError(t, err)
	si.Config.Metadata[JSCfgVersionMetadataKey] = "999"
	_, err = js.UpdateStream(&si.Config)
	require_NoError(t, err)
	require_Equal(t, streamVer("S2"), uint64(5))

	// Consumer delete and stream delete each consume a version.
	require_NoError(t, js.DeleteConsumer("S1", "C1"))
	require_Equal(t, jsa.currentConfigVersion(), uint64(6))
	require_NoError(t, js.DeleteStream("S2"))
	require_Equal(t, jsa.currentConfigVersion(), uint64(7))
}

func TestStandaloneConfigVersionRestartSeed(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = dir
	s := RunServer(&opts)

	nc, js := jsClientConnect(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	nc.Close()
	s.Shutdown()

	// Restart against the same store directory.
	s = RunServer(&opts)
	defer s.Shutdown()
	acc := s.GlobalAccount()
	_, _, jsa := acc.getJetStreamFromAccount()
	mset, err := acc.lookupStream("S1")
	require_NoError(t, err)
	// Recovered metadata seeds the counter at the stamped version.
	require_Equal(t, configVersionFromMetadata(mset.config().Metadata), jsa.currentConfigVersion())
	require_True(t, jsa.currentConfigVersion() >= 1)

	// New changes after restart keep the version strictly monotonic.
	nc2, js2 := jsClientConnect(t, s)
	defer nc2.Close()
	prev := jsa.currentConfigVersion()
	_, err = js2.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"s2"}, Storage: nats.FileStorage})
	require_NoError(t, err)
	require_Equal(t, jsa.currentConfigVersion(), prev+1)
}

// fakeTxnView is an in-memory txnResourceView for engine tests, backed by a
// real standalone server/account so checkStreamCfg/checkConsumerCfg exercise
// the production validation rules.
type fakeTxnView struct {
	s             *Server
	acc           *Account
	streams       map[string]*StreamConfig
	consumers     map[string]map[string]*ConsumerConfig
	versions      map[string]uint64
	limitConflict *JSApiTxnConflict
	feasibleErr   *ApiError
}

func newFakeTxnView(t *testing.T) *fakeTxnView {
	s := RunBasicJetStreamServer(t)
	t.Cleanup(s.Shutdown)
	return &fakeTxnView{
		s:         s,
		acc:       s.GlobalAccount(),
		streams:   make(map[string]*StreamConfig),
		consumers: make(map[string]map[string]*ConsumerConfig),
		versions:  make(map[string]uint64),
	}
}

func (v *fakeTxnView) addStream(name string, subjects ...string) {
	cfg := &StreamConfig{Name: name, Subjects: subjects, Storage: FileStorage, Replicas: 1}
	if len(subjects) == 0 {
		cfg.Subjects = []string{name}
	}
	v.streams[name] = cfg
	v.versions["stream:"+name]++
}

func (v *fakeTxnView) addConsumer(stream, name string) {
	if v.consumers[stream] == nil {
		v.consumers[stream] = make(map[string]*ConsumerConfig)
	}
	v.consumers[stream][name] = &ConsumerConfig{Name: name, Durable: name, AckPolicy: AckExplicit, Replicas: 1}
	v.versions["consumer:"+stream+"/"+name]++
}

func (v *fakeTxnView) currentStream(name string) (*StreamConfig, uint64, bool) {
	cfg, ok := v.streams[name]
	return cfg, v.versions["stream:"+name], ok
}

func (v *fakeTxnView) currentConsumer(stream, name string) (*ConsumerConfig, uint64, bool) {
	cfg, ok := v.consumers[stream][name]
	return cfg, v.versions["consumer:"+stream+"/"+name], ok
}

func (v *fakeTxnView) consumersOf(stream string) []string {
	var out []string
	for name := range v.consumers[stream] {
		out = append(out, name)
	}
	return out
}

func (v *fakeTxnView) feasibleStream(cfg *StreamConfig) *ApiError {
	if v.feasibleErr != nil {
		return v.feasibleErr
	}
	if cfg.Replicas > 1 {
		return ApiErrors[JSStreamReplicasNotSupportedErr]
	}
	return nil
}

func (v *fakeTxnView) checkLimits(_ map[string]*StreamConfig, _ map[string]int) *JSApiTxnConflict {
	return v.limitConflict
}

func (v *fakeTxnView) accountConfigVersion() uint64 { return 0 }

func streamCreateOp(name string, subjects ...string) JSApiTxnOp {
	subs := subjects
	if len(subs) == 0 {
		subs = []string{name}
	}
	return JSApiTxnOp{Op: JSApiTxnStreamCreate, Stream: name, StreamConfig: &StreamConfig{Name: name, Subjects: subs, Storage: FileStorage}}
}

func consumerCreateOp(stream, consumer string) JSApiTxnOp {
	return JSApiTxnOp{Op: JSApiTxnConsumerCreate, Stream: stream, Consumer: consumer,
		ConsumerConfig: &ConsumerConfig{Durable: consumer, AckPolicy: AckExplicit}}
}

func TestTxnEnginePrepare(t *testing.T) {
	tests := []struct {
		name         string
		setup        func(v *fakeTxnView)
		ops          func(v *fakeTxnView) []JSApiTxnOp
		wantConflict bool
		wantReason   string
		wantOrder    []string // ordered resource keys
	}{
		{
			name: "valid create stream and consumer",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{consumerCreateOp("S", "C"), streamCreateOp("S")}
			},
			wantOrder: []string{"stream:S", "consumer:S/C"},
		},
		{
			name: "consumer missing stream",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{consumerCreateOp("NOPE", "C")}
			},
			wantConflict: true,
			wantReason:   txnReasonValidation,
		},
		{
			name:  "stream delete requires consumer delete",
			setup: func(v *fakeTxnView) { v.addStream("S"); v.addConsumer("S", "C") },
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{{Op: JSApiTxnStreamDelete, Stream: "S"}}
			},
			wantConflict: true,
		},
		{
			name:  "stream delete with consumer delete",
			setup: func(v *fakeTxnView) { v.addStream("S"); v.addConsumer("S", "C") },
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{
					{Op: JSApiTxnStreamDelete, Stream: "S"},
					{Op: JSApiTxnConsumerDelete, Stream: "S", Consumer: "C"},
				}
			},
			wantOrder: []string{"consumer:S/C", "stream:S"},
		},
		{
			name: "subject overlap",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{streamCreateOp("S1", "a"), streamCreateOp("S2", "a")}
			},
			wantConflict: true,
		},
		{
			name: "mirror target missing",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{{Op: JSApiTxnStreamCreate, Stream: "S",
					StreamConfig: &StreamConfig{Name: "S", Storage: FileStorage, Mirror: &StreamSource{Name: "M"}}}}
			},
			wantConflict: true,
		},
		{
			name: "source cycle",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{
					{Op: JSApiTxnStreamCreate, Stream: "S1", StreamConfig: &StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: FileStorage, Sources: []*StreamSource{{Name: "S2"}}}},
					{Op: JSApiTxnStreamCreate, Stream: "S2", StreamConfig: &StreamConfig{Name: "S2", Subjects: []string{"s2"}, Storage: FileStorage, Sources: []*StreamSource{{Name: "S1"}}}},
				}
			},
			wantConflict: true,
		},
		{
			name: "duplicate resource ops",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{streamCreateOp("S"), streamCreateOp("S")}
			},
			wantConflict: true,
		},
		{
			name:  "create existing stream",
			setup: func(v *fakeTxnView) { v.addStream("S") },
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{streamCreateOp("S")}
			},
			wantConflict: true,
		},
		{
			name: "update missing stream",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				return []JSApiTxnOp{{Op: JSApiTxnStreamUpdate, Stream: "S", StreamConfig: &StreamConfig{Name: "S", Storage: FileStorage}}}
			},
			wantConflict: true,
		},
		{
			name: "replicas infeasible standalone",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				op := streamCreateOp("S")
				op.StreamConfig.Replicas = 3
				return []JSApiTxnOp{op}
			},
			wantConflict: true,
		},
		{
			name: "quota exceeded post transaction",
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				v.limitConflict = &JSApiTxnConflict{Resource: "account", Reason: txnReasonQuota, Description: "too many streams"}
				return []JSApiTxnOp{streamCreateOp("S")}
			},
			wantConflict: true,
			wantReason:   txnReasonQuota,
		},
		{
			name:  "expected version mismatch at prepare",
			setup: func(v *fakeTxnView) { v.addStream("S") },
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				wrong := uint64(42)
				return []JSApiTxnOp{{Op: JSApiTxnStreamUpdate, Stream: "S", ExpectedVersion: &wrong,
					StreamConfig: &StreamConfig{Name: "S", Subjects: []string{"S"}, Storage: FileStorage}}}
			},
			wantConflict: true,
			wantReason:   txnReasonConflict,
		},
		{
			name:  "valid update with expected version",
			setup: func(v *fakeTxnView) { v.addStream("S") },
			ops: func(v *fakeTxnView) []JSApiTxnOp {
				ver := v.versions["stream:S"]
				return []JSApiTxnOp{{Op: JSApiTxnStreamUpdate, Stream: "S", ExpectedVersion: &ver,
					StreamConfig: &StreamConfig{Name: "S", Subjects: []string{"S", "x"}, Storage: FileStorage}}}
			},
			wantOrder: []string{"stream:S"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := newFakeTxnView(t)
			if tc.setup != nil {
				tc.setup(v)
			}
			e := newTxnEngine(v.s, v.acc, v)
			plan, conflicts := e.prepare(tc.ops(v))
			if tc.wantConflict {
				require_True(t, len(conflicts) > 0)
				if tc.wantReason != _EMPTY_ {
					found := false
					for _, c := range conflicts {
						if c.Reason == tc.wantReason {
							found = true
						}
					}
					require_True(t, found)
				}
				require_True(t, plan == nil)
				return
			}
			require_True(t, len(conflicts) == 0)
			require_True(t, plan != nil)
			if tc.wantOrder != nil {
				require_Equal(t, len(plan.ops), len(tc.wantOrder))
				for i, want := range tc.wantOrder {
					require_Equal(t, plan.ops[i].txnResourceKey(), want)
				}
			}
		})
	}
}

func TestTxnEngineRecheckDrift(t *testing.T) {
	v := newFakeTxnView(t)
	v.addStream("S")
	e := newTxnEngine(v.s, v.acc, v)
	ver := v.versions["stream:S"]
	plan, conflicts := e.prepare([]JSApiTxnOp{{Op: JSApiTxnStreamUpdate, Stream: "S", ExpectedVersion: &ver,
		StreamConfig: &StreamConfig{Name: "S", Subjects: []string{"S", "x"}, Storage: FileStorage}}})
	require_True(t, len(conflicts) == 0)
	require_True(t, plan != nil)

	// No drift -> no conflict.
	require_True(t, len(e.recheckAtCommit(plan)) == 0)

	// Resource mutated between prepare and commit -> conflict with versions.
	v.versions["stream:S"]++
	rc := e.recheckAtCommit(plan)
	require_True(t, len(rc) == 1)
	require_Equal(t, rc[0].Reason, txnReasonConflict)
	require_Equal(t, rc[0].Resource, "stream:S")
	require_True(t, rc[0].ActualVersion > rc[0].ExpectedVersion)
}

func TestTxnEngineOrderingStable(t *testing.T) {
	v := newFakeTxnView(t)
	e := newTxnEngine(v.s, v.acc, v)
	ops := []JSApiTxnOp{
		consumerCreateOp("S2", "Cz"),
		streamCreateOp("S2"),
		streamCreateOp("S1"),
		consumerCreateOp("S1", "Ca"),
	}
	plan1, c1 := e.prepare(ops)
	require_True(t, len(c1) == 0)
	plan2, c2 := e.prepare([]JSApiTxnOp{ops[2], ops[3], ops[1], ops[0]})
	require_True(t, len(c2) == 0)
	require_Equal(t, len(plan1.ops), len(plan2.ops))
	for i := range plan1.ops {
		require_Equal(t, plan1.ops[i].txnResourceKey(), plan2.ops[i].txnResourceKey())
	}
}

func TestTxnResourceKey(t *testing.T) {
	op := JSApiTxnOp{Op: JSApiTxnStreamDelete, Stream: "S"}
	require_Equal(t, op.txnResourceKey(), "stream:S")
	op = JSApiTxnOp{Op: JSApiTxnConsumerDelete, Stream: "S", Consumer: "C"}
	require_Equal(t, op.txnResourceKey(), "consumer:S/C")
}
