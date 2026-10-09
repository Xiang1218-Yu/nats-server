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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func txnAPIRequest(t *testing.T, nc *nats.Conn, subject string, req any, v any) {
	t.Helper()
	buf, err := json.Marshal(req)
	require_NoError(t, err)
	msg, err := nc.Request(subject, buf, 5*time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(msg.Data, v))
}

func txnCreateReq(ops ...JSApiTxnOp) JSApiTxnCreateRequest {
	return JSApiTxnCreateRequest{Ops: ops, TTL: 30 * time.Second}
}

func TestTxnAPIStandaloneEndToEnd(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, _ := jsClientConnect(t, s)
	defer nc.Close()

	ops := []JSApiTxnOp{
		{Op: JSApiTxnStreamCreate, Stream: "S1", StreamConfig: &StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: FileStorage}},
		{Op: JSApiTxnStreamCreate, Stream: "S2", StreamConfig: &StreamConfig{Name: "S2", Subjects: []string{"s2"}, Storage: FileStorage}},
		{Op: JSApiTxnConsumerCreate, Stream: "S1", Consumer: "C1", ConsumerConfig: &ConsumerConfig{Durable: "C1", AckPolicy: AckExplicit}},
	}
	var cr JSApiTxnCreateResponse
	txnAPIRequest(t, nc, JSApiTxnCreate, txnCreateReq(ops...), &cr)
	require_True(t, cr.Error == nil)
	require_Equal(t, cr.State, txnStatePrepared)
	require_True(t, cr.Id != _EMPTY_)
	id := cr.Id

	// Resources not visible yet.
	msg, err := nc.Request("$JS.API.STREAM.INFO.S1", nil, time.Second)
	require_NoError(t, err)
	var probe streamInfoResponse
	require_NoError(t, json.Unmarshal(msg.Data, &probe))
	require_True(t, probe.Error != nil)

	var ir JSApiTxnInfoResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnInfoT, id), nil, &ir)
	require_True(t, ir.Error == nil)
	require_Equal(t, ir.State, txnStatePrepared)

	var commit JSApiTxnCommitResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnCommitT, id), nil, &commit)
	require_True(t, commit.Error == nil)
	require_Equal(t, commit.State, txnStateCommitted)
	require_True(t, commit.Version > 0)
	ver := commit.Version

	// Both streams and the consumer are visible at the SAME version.
	for _, name := range []string{"S1", "S2"} {
		si, err := streamInfoRaw(nc, name)
		require_NoError(t, err)
		require_Equal(t, si.Config.Metadata[JSCfgVersionMetadataKey], fmt.Sprintf("%d", ver))
	}
	ci, err := consumerInfoRaw(nc, "S1", "C1")
	require_NoError(t, err)
	require_Equal(t, ci.Config.Metadata[JSCfgVersionMetadataKey], fmt.Sprintf("%d", ver))

	// Idempotent re-commit returns the same terminal record/version.
	var commit2 JSApiTxnCommitResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnCommitT, id), nil, &commit2)
	require_True(t, commit2.Error == nil)
	require_Equal(t, commit2.Version, ver)
}

func TestTxnAPIStandaloneCancelAndConflict(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Cancel path.
	ops := []JSApiTxnOp{{Op: JSApiTxnStreamCreate, Stream: "S1", StreamConfig: &StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: FileStorage}}}
	var cr JSApiTxnCreateResponse
	txnAPIRequest(t, nc, JSApiTxnCreate, txnCreateReq(ops...), &cr)
	require_True(t, cr.Error == nil)
	var cancel JSApiTxnCancelResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnCancelT, cr.Id), JSApiTxnCancelRequest{}, &cancel)
	require_True(t, cancel.Error == nil)
	require_Equal(t, cancel.State, txnStateAborted)
	require_Equal(t, cancel.Reason, txnReasonCancelled)
	_, err := js.StreamInfo("S1")
	require_True(t, err != nil)

	// CAS conflict path.
	_, err = js.AddStream(natsStream("BASE"))
	require_NoError(t, err)
	v := uint64(1)
	conflictOps := []JSApiTxnOp{{Op: JSApiTxnStreamUpdate, Stream: "BASE", ExpectedVersion: &v,
		StreamConfig: &StreamConfig{Name: "BASE", Subjects: []string{"BASE", "x"}, Storage: FileStorage}}}
	var cr2 JSApiTxnCreateResponse
	txnAPIRequest(t, nc, JSApiTxnCreate, txnCreateReq(conflictOps...), &cr2)
	require_True(t, cr2.Error == nil)
	_, err = js.UpdateStream(natsStreamCfg("BASE", "BASE", "y"))
	require_NoError(t, err)
	var commit JSApiTxnCommitResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnCommitT, cr2.Id), nil, &commit)
	require_True(t, commit.Error != nil)
	require_Equal(t, commit.State, txnStateAborted)
	require_Equal(t, commit.Reason, txnReasonConflict)
	require_True(t, len(commit.Conflicts) >= 1)

	// INFO remains queryable.
	var ir JSApiTxnInfoResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnInfoT, cr2.Id), nil, &ir)
	require_True(t, ir.Error == nil)
	require_Equal(t, ir.State, txnStateAborted)
}

func TestTxnAPIClusterEndToEnd(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ops := []JSApiTxnOp{
		{Op: JSApiTxnStreamCreate, Stream: "S1", StreamConfig: &StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: FileStorage, Replicas: 3}},
		{Op: JSApiTxnConsumerCreate, Stream: "S1", Consumer: "D1", ConsumerConfig: &ConsumerConfig{Durable: "D1", AckPolicy: AckExplicit, Replicas: 3}},
	}
	var cr JSApiTxnCreateResponse
	txnAPIRequest(t, nc, JSApiTxnCreate, txnCreateReq(ops...), &cr)
	require_True(t, cr.Error == nil)
	require_Equal(t, cr.State, txnStatePrepared)
	id := cr.Id

	var commit JSApiTxnCommitResponse
	txnAPIRequest(t, nc, fmt.Sprintf(JSApiTxnCommitT, id), nil, &commit)
	require_True(t, commit.Error == nil)
	require_Equal(t, commit.State, txnStateCommitted)
	ver := commit.Version
	require_True(t, ver > 0)

	// The transaction is a single meta entry; all resources carry that version.
	checkCluster := func(srv *Server) {
		mset, err := srv.GlobalAccount().lookupStream("S1")
		require_NoError(t, err)
		require_Equal(t, configVersionFromMetadata(mset.config().Metadata), ver)
		o := mset.lookupConsumer("D1")
		require_True(t, o != nil)
		o.mu.RLock()
		ccver := configVersionFromMetadata(o.cfg.Metadata)
		o.mu.RUnlock()
		require_Equal(t, ccver, ver)
	}
	// Wait for the assignments to be applied on every member.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		for _, srv := range c.servers {
			acc := srv.GlobalAccount()
			mset, err := acc.lookupStream("S1")
			if err != nil || mset == nil {
				return fmt.Errorf("stream not yet on %s", srv.Name())
			}
			if mset.lookupConsumer("D1") == nil {
				return fmt.Errorf("consumer not yet on %s", srv.Name())
			}
		}
		return nil
	})
	for _, srv := range c.servers {
		checkCluster(srv)
	}

	// A normal non-transaction update keeps versions monotonic.
	si, err := streamInfoRaw(nc, "S1")
	require_NoError(t, err)
	si.Config.Subjects = []string{"s1", "s1b"}
	_, err = nc.Request("$JS.API.STREAM.UPDATE.S1", mustJSON(t, si.Config), 5*time.Second)
	require_NoError(t, err)
	si2, err := streamInfoRaw(nc, "S1")
	require_NoError(t, err)
	require_True(t, metaVersion(t, si2.Config.Metadata) > ver)
}

func TestTxnAPIClusterLeaderSwitch(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, _ := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	ops := []JSApiTxnOp{
		{Op: JSApiTxnStreamCreate, Stream: "S1", StreamConfig: &StreamConfig{Name: "S1", Subjects: []string{"s1"}, Storage: FileStorage, Replicas: 3}},
		{Op: JSApiTxnConsumerCreate, Stream: "S1", Consumer: "D1", ConsumerConfig: &ConsumerConfig{Durable: "D1", AckPolicy: AckExplicit, Replicas: 3}},
	}
	var cr JSApiTxnCreateResponse
	txnAPIRequest(t, nc, JSApiTxnCreate, txnCreateReq(ops...), &cr)
	require_True(t, cr.Error == nil)
	id := cr.Id

	// Step the meta leader down while the transaction is prepared.
	c.leader().Shutdown()
	c.waitOnLeader()

	// The prepared transaction remains queryable through replicated state on
	// the new leader.
	nc2, _ := jsClientConnect(t, c.leader())
	defer nc2.Close()
	var ir JSApiTxnInfoResponse
	txnAPIRequest(t, nc2, fmt.Sprintf(JSApiTxnInfoT, id), nil, &ir)
	require_True(t, ir.Error == nil)
	require_Equal(t, ir.State, txnStatePrepared)

	// Commit against the new leader succeeds.
	var commit JSApiTxnCommitResponse
	txnAPIRequest(t, nc2, fmt.Sprintf(JSApiTxnCommitT, id), nil, &commit)
	require_True(t, commit.Error == nil)
	require_Equal(t, commit.State, txnStateCommitted)

	checkFor(t, 10*time.Second, 200*time.Millisecond, func() error {
		mset, err := c.leader().GlobalAccount().lookupStream("S1")
		if err != nil || mset == nil || mset.lookupConsumer("D1") == nil {
			return fmt.Errorf("not applied yet")
		}
		return nil
	})
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require_NoError(t, err)
	return b
}

func streamInfoRaw(nc *nats.Conn, name string) (*nats.StreamInfo, error) {
	msg, err := nc.Request(fmt.Sprintf("$JS.API.STREAM.INFO.%s", name), nil, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var sr streamInfoResponse
	if err := json.Unmarshal(msg.Data, &sr); err != nil {
		return nil, err
	}
	if sr.Error != nil {
		return nil, fmt.Errorf("%s", sr.Error.Description)
	}
	return sr.StreamInfo, nil
}

type streamInfoResponse struct {
	*nats.StreamInfo
	Error *struct {
		Description string `json:"description"`
	} `json:"error,omitempty"`
}

func consumerInfoRaw(nc *nats.Conn, stream, consumer string) (*nats.ConsumerInfo, error) {
	msg, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.INFO.%s.%s", stream, consumer), nil, 5*time.Second)
	if err != nil {
		return nil, err
	}
	var sr consumerInfoResponse
	if err := json.Unmarshal(msg.Data, &sr); err != nil {
		return nil, err
	}
	if sr.Error != nil {
		return nil, fmt.Errorf("%s", sr.Error.Description)
	}
	return sr.ConsumerInfo, nil
}

type consumerInfoResponse struct {
	*nats.ConsumerInfo
	Error *struct {
		Description string `json:"description"`
	} `json:"error,omitempty"`
}

func metaVersion(t *testing.T, md map[string]string) uint64 {
	t.Helper()
	var v uint64
	_, err := fmt.Sscanf(md[JSCfgVersionMetadataKey], "%d", &v)
	require_NoError(t, err)
	return v
}
