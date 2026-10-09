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
	"fmt"
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"
)

// objectStoreClusterName is the backing stream name used in cluster tests.
const (
	objClusterBucket    = "CLB"
	objClusterAccount   = "$G"
)

func objClusterStreamName() string { return objStreamName(objClusterBucket) }

// connectClient starts a test client against a specific cluster server.
func connectClient(t testing.TB, s *Server) *objTestClient {
	t.Helper()
	nc, err := natsc.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect %s: %v", s.Name(), err)
	}
	t.Cleanup(nc.Close)
	return &objTestClient{nc: nc}
}

// nonLeaderServer returns a cluster server that is not the stream leader.
func nonLeaderServer(t testing.TB, c *cluster) *Server {
	leader := c.streamLeader(objClusterAccount, objClusterStreamName())
	for _, s := range c.servers {
		if s != leader && s.Running() {
			return s
		}
	}
	t.Fatal("no non-leader server")
	return nil
}

// startClusterSession starts (or resumes) the R3 session used by cluster tests.
func startClusterSession(t testing.TB, client *objTestClient, name string, numChunks uint32, chunkSize uint32) *objStartResponse {
	t.Helper()
	return client.start(t, objUploadStartRequest{
		Bucket:    objClusterBucket,
		Name:      name,
		ChunkSize: chunkSize,
		NumChunks: numChunks,
		Replicas:  3,
	})
}

// uploadChunks writes a contiguous range of chunks from data.
func uploadChunks(t testing.TB, client *objTestClient, sid string, data []byte, chunkSize uint32, from, to uint32) {
	t.Helper()
	for n := from; n <= to; n++ {
		lo := n * chunkSize
		hi := lo + chunkSize
		if int(hi) > len(data) {
			hi = uint32(len(data))
		}
		client.sendChunk(t, objClusterBucket, sid, n, data[lo:hi], false)
	}
}

// waitStreamPeerCount waits until the stream Raft group reports the expected
// number of members (leader + peers).
func waitStreamPeerCount(t testing.TB, nc *natsc.Conn, stream string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var resp JSApiStreamInfoResponse
		msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, stream), nil, 5*time.Second)
		if err == nil && json.Unmarshal(msg.Data, &resp) == nil &&
			resp.StreamInfo != nil && resp.StreamInfo.Cluster != nil {
			count := 1 + len(resp.StreamInfo.Cluster.Replicas)
			if count == want {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("stream %q peer count never reached %d", stream, want)
}

// updateReplicas changes the backing stream replication factor.
func updateReplicas(t testing.TB, nc *natsc.Conn, stream string, replicas int) {
	t.Helper()

	var info JSApiStreamInfoResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, stream), nil, 5*time.Second)
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if err := json.Unmarshal(msg.Data, &info); err != nil || info.StreamInfo == nil {
		t.Fatalf("info decode: %v", err)
	}
	cfg := info.StreamInfo.Config
	cfg.Replicas = replicas

	var resp JSApiStreamUpdateResponse
	body, _ := json.Marshal(StreamConfigRequest{StreamConfig: cfg})
	reply, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, stream), body, 10*time.Second)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("update decode: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("update error: %v", resp.Error)
	}
}

// TestObjectStoreClusterResumeAcrossServers verifies that a new client talking
// to a different server discovers the in-progress session and completes it.
func TestObjectStoreClusterResumeAcrossServers(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "OBS", 3)
	defer c.shutdown()

	const chunkSize = uint32(64 * 1024)
	data := randomData(int(chunkSize)*5 + 999)
	name := "resume/object.bin"

	client1 := connectClient(t, c.leader())
	st := startClusterSession(t, client1, name, 6, chunkSize)
	sid := st.Session.ID
	uploadChunks(t, client1, sid, data, chunkSize, 0, 2)

	// New client instance connected to a non-leader server.
	server2 := nonLeaderServer(t, c)
	client2 := connectClient(t, server2)
	resume := startClusterSession(t, client2, name, 6, chunkSize)
	if resume.Session.ID != sid {
		t.Fatalf("sid changed across server: %s != %s", resume.Session.ID, sid)
	}
	if !equalU32(resume.Received, []uint32{0, 1, 2}) {
		t.Fatalf("resume received: %v", resume.Received)
	}

	uploadChunks(t, client2, sid, data, chunkSize, 3, 5)
	info := client2.commit(t, objClusterBucket, sid)
	if info.Size != uint64(len(data)) {
		t.Fatalf("commit size %d", info.Size)
	}
	got := client2.getRange(t, objClusterBucket, name, 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("cluster resumed object mismatch")
	}
}

// TestObjectStoreClusterReplicaChange exercises Replicas changes while an
// upload is open and after commit, in both directions.
func TestObjectStoreClusterReplicaChange(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "OBS", 3)
	defer c.shutdown()

	const chunkSize = uint32(64 * 1024)
	data := randomData(int(chunkSize)*3 + 11)
	stream := objClusterStreamName()
	name := "replica/object.bin"

	client := connectClient(t, c.leader())
	st := startClusterSession(t, client, name, 4, chunkSize)
	sid := st.Session.ID
	uploadChunks(t, client, sid, data, chunkSize, 0, 1)

	// R3 -> R1 mid-upload.
	updateReplicas(t, client.nc, stream, 1)
	waitStreamPeerCount(t, client.nc, stream, 1)

	uploadChunks(t, client, sid, data, chunkSize, 2, 3)
	info := client.commit(t, objClusterBucket, sid)
	if info.Size != uint64(len(data)) {
		t.Fatalf("size after R1 commit: %d", info.Size)
	}
	got := client.getRange(t, objClusterBucket, name, 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("object mismatch after R3->R1")
	}

	// R1 -> R3 after commit; data must be copied to the new peers.
	updateReplicas(t, client.nc, stream, 3)
	waitStreamPeerCount(t, client.nc, stream, 3)
	// Read through a non-lever server to prove replicas have the data.
	client2 := connectClient(t, nonLeaderServer(t, c))
	got = client2.getRange(t, objClusterBucket, name, 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("object mismatch after R1->R3")
	}
}

// TestObjectStoreClusterStepdown verifies continued upload after a stream
// leader change.
func TestObjectStoreClusterStepdown(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "OBS", 3)
	defer c.shutdown()

	const chunkSize = uint32(64 * 1024)
	data := randomData(int(chunkSize)*3)
	name := "stepdown/object.bin"

	client := connectClient(t, c.leader())
	st := startClusterSession(t, client, name, 3, chunkSize)
	sid := st.Session.ID
	uploadChunks(t, client, sid, data, chunkSize, 0, 0)

	// Step down to a non-leader server.
	preferred := nonLeaderServer(t, c)
	req, _ := json.Marshal(JSApiLeaderStepdownRequest{Placement: &Placement{Preferred: preferred.Name()}})
	if _, err := client.nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, objClusterStreamName()), req, 10*time.Second); err != nil {
		t.Fatalf("stepdown: %v", err)
	}
	c.waitOnStreamLeader(objClusterAccount, objClusterStreamName())
	if sl := c.streamLeader(objClusterAccount, objClusterStreamName()); sl != preferred {
		t.Fatalf("leader did not move to %s", preferred.Name())
	}

	// Continue from a client on the new leader.
	client2 := connectClient(t, preferred)
	resume := startClusterSession(t, client2, name, 3, chunkSize)
	if resume.Session.ID != sid {
		t.Fatalf("sid changed after stepdown: %s", resume.Session.ID)
	}
	uploadChunks(t, client2, sid, data, chunkSize, 1, 2)
	client2.commit(t, objClusterBucket, sid)
	got := client2.getRange(t, objClusterBucket, name, 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("mismatch after stepdown")
	}
}

// TestObjectStoreClusterNodeRestart verifies session recovery after the stream
// leader process is shut down and restarted.
func TestObjectStoreClusterNodeRestart(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "OBS", 3)
	defer c.shutdown()

	const chunkSize = uint32(64 * 1024)
	data := randomData(int(chunkSize)*4 + 7)
	name := "restart/object.bin"

	leader := c.leader()
	client := connectClient(t, leader)
	st := startClusterSession(t, client, name, 5, chunkSize)
	sid := st.Session.ID
	uploadChunks(t, client, sid, data, chunkSize, 0, 2)

	// Shut down the leader process, then restart with same config.
	leader.Shutdown()
	leader.WaitForShutdown()
	c.restartServer(leader)
	c.waitOnStreamLeader(objClusterAccount, objClusterStreamName())

	// Connect to whichever server is the (possibly same) stream leader.
	srv := c.streamLeader(objClusterAccount, objClusterStreamName())
	if srv == nil {
		t.Fatal("no stream leader after restart")
	}
	client2 := connectClient(t, srv)

	resume := startClusterSession(t, client2, name, 5, chunkSize)
	if resume.Session.ID != sid {
		t.Fatalf("sid changed after restart: %s != %s", resume.Session.ID, sid)
	}
	if !equalU32(resume.Received, []uint32{0, 1, 2}) {
		t.Fatalf("received after restart: %v", resume.Received)
	}
	uploadChunks(t, client2, sid, data, chunkSize, 3, 4)
	client2.commit(t, objClusterBucket, sid)
	got := client2.getRange(t, objClusterBucket, name, 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("mismatch after node restart")
	}
}
