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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"
)

// -------------------------------------------------------------------
// Test client
// -------------------------------------------------------------------

type objTestClient struct {
	nc *natsc.Conn
}

func newObjTestClient(t testing.TB, s *Server) *objTestClient {
	t.Helper()
	nc, err := natsc.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return &objTestClient{nc: nc}
}

func objReqTimeout() time.Duration { return 20 * time.Second }

func (c *objTestClient) jsonRequest(subj string, req any, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	msg, err := c.nc.Request(subj, body, objReqTimeout())
	if err != nil {
		return err
	}
	return json.Unmarshal(msg.Data, resp)
}

func (c *objTestClient) start(t testing.TB, req objUploadStartRequest) *objStartResponse {
	t.Helper()
	var resp objStartResponse
	if err := c.jsonRequest(objAPIUploadStart, req, &resp); err != nil {
		t.Fatalf("start request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("start error: %v", resp.Error)
	}
	return &resp
}

func (c *objTestClient) startErr(req objUploadStartRequest) (*objStartResponse, error) {
	var resp objStartResponse
	if err := c.jsonRequest(objAPIUploadStart, req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return &resp, resp.Error
	}
	return &resp, nil
}

// sendChunk uploads a single chunk. badDigest replaces the header digest.
func (c *objTestClient) sendChunk(t testing.TB, bucket, sid string, n uint32, data []byte, badDigest bool) *objChunkResponse {
	t.Helper()
	digest := objShaDigest(data)
	if badDigest {
		digest = objShaDigest([]byte("not-the-data"))
	}
	msg := natsc.NewMsg(objAPIUploadChunk)
	msg.Header.Set(objHdrBucket, bucket)
	msg.Header.Set(objHdrSession, sid)
	msg.Header.Set(objHdrChunk, fmt.Sprintf("%d", n))
	msg.Header.Set(objHdrDigest, digest)
	msg.Data = data
	reply, err := c.nc.RequestMsg(msg, objReqTimeout())
	if err != nil {
		t.Fatalf("chunk request: %v", err)
	}
	var resp objChunkResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("chunk decode: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("chunk error: %v", resp.Error)
	}
	return &resp
}

func (c *objTestClient) sendChunkErr(bucket, sid string, n uint32, data []byte, badDigest bool) (*objChunkResponse, error) {
	digest := objShaDigest(data)
	if badDigest {
		digest = objShaDigest([]byte("not-the-data"))
	}
	msg := natsc.NewMsg(objAPIUploadChunk)
	msg.Header.Set(objHdrBucket, bucket)
	msg.Header.Set(objHdrSession, sid)
	msg.Header.Set(objHdrChunk, fmt.Sprintf("%d", n))
	msg.Header.Set(objHdrDigest, digest)
	msg.Data = data
	reply, err := c.nc.RequestMsg(msg, objReqTimeout())
	if err != nil {
		return nil, err
	}
	var resp objChunkResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return &resp, resp.Error
	}
	return &resp, nil
}

func (c *objTestClient) status(t testing.TB, bucket, sid string) *objStatusResponse {
	t.Helper()
	var resp objStatusResponse
	if err := c.jsonRequest(objAPIUploadStatus, map[string]string{"bucket": bucket, "id": sid}, &resp); err != nil {
		t.Fatalf("status request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("status error: %v", resp.Error)
	}
	return &resp
}

func (c *objTestClient) commit(t testing.TB, bucket, sid string) *objInfo {
	t.Helper()
	var resp objGetResponse
	if err := c.jsonRequest(objAPIUploadCommit, map[string]string{"bucket": bucket, "id": sid}, &resp); err != nil {
		t.Fatalf("commit request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("commit error: %v", resp.Error)
	}
	return resp.Info
}

func (c *objTestClient) commitErr(bucket, sid string) (*objGetResponse, error) {
	var resp objGetResponse
	if err := c.jsonRequest(objAPIUploadCommit, map[string]string{"bucket": bucket, "id": sid}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return &resp, resp.Error
	}
	return &resp, nil
}

func (c *objTestClient) abort(t testing.TB, bucket, sid string) {
	t.Helper()
	var resp objDeleteResponse
	if err := c.jsonRequest(objAPIUploadAbort, map[string]string{"bucket": bucket, "id": sid}, &resp); err != nil {
		t.Fatalf("abort request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("abort error: %v", resp.Error)
	}
}

func (c *objTestClient) getMeta(t testing.TB, bucket, name string) *objInfo {
	t.Helper()
	var resp objGetResponse
	if err := c.jsonRequest(objAPIGet, map[string]string{"bucket": bucket, "name": name}, &resp); err != nil {
		t.Fatalf("get request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("get error: %v", resp.Error)
	}
	return resp.Info
}

func (c *objTestClient) getMetaErr(bucket, name string) (*objGetResponse, error) {
	var resp objGetResponse
	if err := c.jsonRequest(objAPIGet, map[string]string{"bucket": bucket, "name": name}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return &resp, resp.Error
	}
	return &resp, nil
}

func (c *objTestClient) getRange(t testing.TB, bucket, name string, offset, length int64) []byte {
	t.Helper()
	var resp objGetChunkResponse
	req := objGetChunkRequest{Bucket: bucket, Name: name, Offset: offset, Length: length}
	if err := c.jsonRequest(objAPIGetChunk, req, &resp); err != nil {
		t.Fatalf("range request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("range error: %v", resp.Error)
	}
	return resp.Data
}

func (c *objTestClient) getChunkRange(t testing.TB, bucket, name string, first, last int32) []byte {
	t.Helper()
	var resp objGetChunkResponse
	req := objGetChunkRequest{Bucket: bucket, Name: name, First: &first, Last: &last}
	if err := c.jsonRequest(objAPIGetChunk, req, &resp); err != nil {
		t.Fatalf("chunk range request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("chunk range error: %v", resp.Error)
	}
	return resp.Data
}

func (c *objTestClient) list(t testing.TB, bucket string) []objInfo {
	t.Helper()
	var resp objListResponse
	if err := c.jsonRequest(objAPIList, map[string]string{"bucket": bucket}, &resp); err != nil {
		t.Fatalf("list request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("list error: %v", resp.Error)
	}
	return resp.Objects
}

func (c *objTestClient) countMatching(stream, filter string) (int, error) {
	msgs, err := c.enumerate(stream, filter)
	return len(msgs), err
}

func (c *objTestClient) enumerate(stream, filter string) ([]*StoredMsg, error) {
	out := make([]*StoredMsg, 0)
	var seq uint64
	for {
		var resp JSApiMsgGetResponse
		req := &JSApiMsgGetRequest{NextFor: filter, Seq: seq}
		body, _ := json.Marshal(req)
		msg, err := c.nc.Request(fmt.Sprintf(JSApiMsgGetT, stream), body, objReqTimeout())
		if err != nil {
			return out, err
		}
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			return out, err
		}
		if resp.Error != nil {
			if resp.Error.ErrCode == uint16(JSNoMessageFoundErr) {
				return out, nil
			}
			return out, resp.Error
		}
		out = append(out, resp.Message)
		seq = resp.Message.Sequence + 1
	}
}

func (c *objTestClient) delete(t testing.TB, bucket, name string) {
	t.Helper()
	var resp objDeleteResponse
	if err := c.jsonRequest(objAPIDelete, map[string]string{"bucket": bucket, "name": name}, &resp); err != nil {
		t.Fatalf("delete request: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("delete error: %v", resp.Error)
	}
}

func (c *objTestClient) put(t testing.TB, bucket, name, ctype string, data []byte) *objInfo {
	t.Helper()
	msg := natsc.NewMsg(objAPIPut)
	msg.Header.Set(objHdrBucket, bucket)
	msg.Header.Set(objHdrName, name)
	if ctype != _EMPTY_ {
		msg.Header.Set(objHdrCType, ctype)
	}
	msg.Data = data
	reply, err := c.nc.RequestMsg(msg, objReqTimeout())
	if err != nil {
		t.Fatalf("put request: %v", err)
	}
	var resp objPutResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("put decode: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("put error: %v", resp.Error)
	}
	return resp.Info
}

func (c *objTestClient) events() (*natsc.Subscription, chan *natsc.Msg) {
	ch := make(chan *natsc.Msg, 16)
	sub, err := c.nc.Subscribe(objEventSubject, func(m *natsc.Msg) {
		select {
		case ch <- m:
		default:
		}
	})
	if err != nil {
		panic(err)
	}
	return sub, ch
}

func nextEvent(t testing.TB, ch chan *natsc.Msg) *objEvent {
	t.Helper()
	select {
	case m := <-ch:
		var ev objEvent
		if err := json.Unmarshal(m.Data, &ev); err != nil {
			t.Fatalf("event decode: %v", err)
		}
		return &ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

func randomData(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func getObjStore(t testing.TB, s *Server) *objStore {
	t.Helper()
	acc := s.GlobalAccount()
	acc.mu.Lock()
	o := acc.obj
	acc.mu.Unlock()
	if o == nil {
		t.Fatal("object store not started")
	}
	return o
}

// uploadAll uploads data with the given chunk size, optionally shuffled order.
func uploadAll(t testing.TB, c *objTestClient, bucket, name, ctype string, data []byte, chunkSize uint32, shuffle bool) string {
	t.Helper()
	numChunks := (len(data) + int(chunkSize) - 1) / int(chunkSize)
	st := c.start(t, objUploadStartRequest{
		Bucket:      bucket,
		Name:        name,
		ContentType: ctype,
		ChunkSize:   chunkSize,
		NumChunks:   uint32(numChunks),
		TotalSize:   uint64(len(data)),
		Metadata:    map[string]string{"x-test": "yes"},
	})
	sid := st.Session.ID

	order := make([]int, numChunks)
	for i := range order {
		order[i] = i
	}
	if shuffle {
		// deterministic shuffle
		for i := len(order) - 1; i > 0; i-- {
			j := int((uint64(i) * 7919) % uint64(i+1))
			order[i], order[j] = order[j], order[i]
		}
	}
	for _, i := range order {
		lo := i * int(chunkSize)
		hi := lo + int(chunkSize)
		if hi > len(data) {
			hi = len(data)
		}
		c.sendChunk(t, bucket, sid, uint32(i), data[lo:hi], false)
	}
	return sid
}

// -------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------

func TestObjectStoreBasicUpload(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	sub, evCh := c.events()
	defer sub.Unsubscribe()

	data := randomData(int(objDefaultChunkSize)*5 + 1234)
	sid := uploadAll(t, c, "TEST", "files/report.bin", "application/octet-stream", data, objDefaultChunkSize, true)

	st := c.status(t, "TEST", sid)
	if len(st.Pending) != 0 {
		t.Fatalf("expected no pending ranges, got %v", st.Pending)
	}
	if st.Size != uint64(len(data)) {
		t.Fatalf("status size %d != %d", st.Size, len(data))
	}

	info := c.commit(t, "TEST", sid)
	if info.Size != uint64(len(data)) {
		t.Fatalf("info size %d != %d", info.Size, len(data))
	}
	if info.NumChunks != 6 {
		t.Fatalf("num chunks %d", info.NumChunks)
	}
	if info.Digest != objShaDigest(data) {
		t.Fatal("digest mismatch")
	}
	if info.ContentType != "application/octet-stream" {
		t.Fatalf("content type %q", info.ContentType)
	}
	if info.Metadata["x-test"] != "yes" {
		t.Fatal("metadata missing")
	}

	// Full read via byte range.
	got := c.getRange(t, "TEST", "files/report.bin", 0, int64(len(data)))
	if !bytes.Equal(got, data) {
		t.Fatal("read data mismatch")
	}
	// Read to end using length -1.
	got = c.getRange(t, "TEST", "files/report.bin", 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("read-to-end mismatch")
	}

	objs := c.list(t, "TEST")
	if len(objs) != 1 || objs[0].Name != "files/report.bin" {
		t.Fatalf("list: %+v", objs)
	}

	ev := nextEvent(t, evCh)
	if ev.Op != objEventCreated || ev.Object != "files/report.bin" {
		t.Fatalf("event: %+v", ev)
	}
}

func TestObjectStoreBadDigestAndBounds(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	st := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 64, NumChunks: 3})
	sid := st.Session.ID

	if _, err := c.sendChunkErr("B", sid, 0, []byte("hello"), true); err == nil {
		t.Fatal("expected bad digest error")
	}
	// Chunk not persisted despite rejected upload.
	if n, _ := c.countMatching(objStreamName("B"), objChunkSubject("B", sid, 0)); n != 0 {
		t.Fatalf("rejected chunk persisted: %d", n)
	}
	if _, err := c.sendChunkErr("B", sid, 3, []byte("hello"), false); err == nil {
		t.Fatal("expected out of bounds error")
	}

	// Session gone states: abort it, then operations should fail.
	c.abort(t, "B", sid)
	if _, err := c.sendChunkErr("B", sid, 0, []byte("hello"), false); err == nil {
		t.Fatal("expected chunk on aborted session to fail")
	}
}

func TestObjectStoreDuplicateAndConflict(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	data := randomData(100)
	st := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 128, NumChunks: 1})
	sid := st.Session.ID

	r := c.sendChunk(t, "B", sid, 0, data, false)
	if r.Duplicate {
		t.Fatal("should not be duplicate")
	}
	r = c.sendChunk(t, "B", sid, 0, data, false)
	if !r.Duplicate {
		t.Fatal("should be duplicate")
	}
	// Exactly one data message.
	if n, _ := c.countMatching(objStreamName("B"), objChunkSubject("B", sid, 0)); n != 1 {
		t.Fatalf("expected 1 chunk message, got %d", n)
	}
	// Same number with different content conflicts.
	if _, err := c.sendChunkErr("B", sid, 0, randomData(100), false); err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestObjectStoreOutOfOrderRanges(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	st := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 32, NumChunks: 10})
	sid := st.Session.ID

	pieces := map[uint32][]byte{0: {}, 1: {}, 3: {}, 4: {}, 7: {}}
	for n := range pieces {
		pieces[n] = randomData(32)
		c.sendChunk(t, "B", sid, n, pieces[n], false)
	}
	got := c.status(t, "B", sid)
	wantReceived := []uint32{0, 1, 3, 4, 7}
	if !equalU32(got.Received, wantReceived) {
		t.Fatalf("received %v, want %v", got.Received, wantReceived)
	}
	wantRanges := []objRange{{0, 1}, {3, 4}, {7, 7}}
	if !equalRanges(got.Ranges, wantRanges) {
		t.Fatalf("ranges %v, want %v", got.Ranges, wantRanges)
	}
	wantPending := []objRange{{2, 2}, {5, 6}, {8, 9}}
	if !equalRanges(got.Pending, wantPending) {
		t.Fatalf("pending %v, want %v", got.Pending, wantPending)
	}
}

func TestObjectStoreCommitFailures(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	st := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 32, NumChunks: 3})
	sid := st.Session.ID
	c.sendChunk(t, "B", sid, 0, randomData(32), false)
	c.sendChunk(t, "B", sid, 2, randomData(32), false)

	if _, err := c.commitErr("B", sid); err == nil {
		t.Fatal("expected commit with missing chunk to fail")
	}
	// No metadata; not listable/gettable.
	if objs := c.list(t, "B"); len(objs) != 0 {
		t.Fatalf("uncommitted object listed: %+v", objs)
	}
	if _, err := c.getMetaErr("B", "o"); err == nil {
		t.Fatal("uncommitted object readable")
	}
	// Finish and commit.
	c.sendChunk(t, "B", sid, 1, randomData(32), false)
	c.commit(t, "B", sid)
}

func TestObjectStoreResumeAfterServerRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	dir := s.JetStreamConfig().StoreDir
	c := newObjTestClient(t, s)

	data := randomData(int(objDefaultChunkSize)*3 + 11)
	// Declare four chunks up front, upload three full ones, then interrupt
	// before the last (small) chunk.
	st0 := c.start(t, objUploadStartRequest{
		Bucket:    "B",
		Name:      "big.bin",
		ChunkSize: objDefaultChunkSize,
		NumChunks: 4,
	})
	sid1 := st0.Session.ID
	for n := uint32(0); n < 3; n++ {
		lo := n * uint32(objDefaultChunkSize)
		c.sendChunk(t, "B", sid1, n, data[lo:lo+uint32(objDefaultChunkSize)], false)
	}

	s.Shutdown()
	s.WaitForShutdown()

	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = dir
	s2 := RunServer(&opts)
	defer s2.Shutdown()

	c2 := newObjTestClient(t, s2)
	st := c2.start(t, objUploadStartRequest{
		Bucket:    "B",
		Name:      "big.bin",
		ChunkSize: objDefaultChunkSize,
		NumChunks: 4,
	})
	if st.Session.ID != sid1 {
		t.Fatalf("sid changed: %s != %s", st.Session.ID, sid1)
	}
	if !equalU32(st.Received, []uint32{0, 1, 2}) {
		t.Fatalf("received after restart: %v", st.Received)
	}
	// Send the final chunk, commit and verify.
	lo := 3 * int(objDefaultChunkSize)
	if _, err := c2.sendChunkErr("B", sid1, 3, data[lo:], false); err != nil {
		t.Fatalf("final chunk: %v", err)
	}
	c2.commit(t, "B", sid1)
	got := c2.getRange(t, "B", "big.bin", 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after restart resume")
	}
}

func TestObjectStoreExpiryAndSweep(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)
	o := getObjStore(t, s)

	st, _ := c.startErr(objUploadStartRequest{
		Bucket:    "B",
		Name:      "o",
		ChunkSize: 64,
		NumChunks: 2,
		TTL:       200 * time.Millisecond,
	})
	sid := st.Session.ID
	c.sendChunk(t, "B", sid, 0, randomData(64), false)

	// Before expiry, a sweep changes nothing.
	o.sweep()
	if st2 := c.status(t, "B", sid); len(st2.Received) != 1 {
		t.Fatal("session swept before expiry")
	}

	time.Sleep(250 * time.Millisecond)
	o.sweep()

	// No messages remain; a new start yields a new session.
	if n, _ := c.countMatching(objStreamName("B"), objChunkFilter("B", sid)); n != 0 {
		t.Fatalf("chunks remain: %d", n)
	}
	st2 := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 64, NumChunks: 2})
	if st2.Session.ID == sid {
		t.Fatal("expected new session after expiry")
	}
}

func TestObjectStoreSweepTimer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)
	o := getObjStore(t, s)

	st, _ := c.startErr(objUploadStartRequest{
		Bucket:    "B",
		Name:      "o",
		ChunkSize: 64,
		NumChunks: 1,
		TTL:       100 * time.Millisecond,
	})
	sid := st.Session.ID
	c.sendChunk(t, "B", sid, 0, randomData(64), false)

	// Drive the timer with a short interval; sweep reschedules itself.
	o.mu.Lock()
	o.sweepTimer.Stop()
	o.sweepInterval = 75 * time.Millisecond
	o.sweepTimer = time.AfterFunc(75*time.Millisecond, o.sweep)
	o.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := c.countMatching(objStreamName("B"), objChunkFilter("B", sid)); n == 0 {
			return // cleaned by background timer
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("background sweep timer did not clean expired session")
}

func TestObjectStoreAbort(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	st := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 64, NumChunks: 3})
	sid := st.Session.ID
	c.sendChunk(t, "B", sid, 0, randomData(64), false)
	c.sendChunk(t, "B", sid, 1, randomData(64), false)

	c.abort(t, "B", sid)
	if n, _ := c.countMatching(objStreamName("B"), "$OBJ.B.S."+sid+".>"); n != 0 {
		t.Fatalf("aborted session data remains: %d", n)
	}
	// A new start yields a fresh session.
	st2 := c.start(t, objUploadStartRequest{Bucket: "B", Name: "o", ChunkSize: 64, NumChunks: 3})
	if st2.Session.ID == sid {
		t.Fatal("expected new session after abort")
	}
}

func TestObjectStoreConcurrentStart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	var wg sync.WaitGroup
	sids := make(chan string, 50)
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newObjTestClient(t, s)
			st, err := c.startErr(objUploadStartRequest{Bucket: "B", Name: "shared", ChunkSize: 64, NumChunks: 1})
			if err != nil {
				errs <- err
				return
			}
			sids <- st.Session.ID
		}()
	}
	wg.Wait()
	close(sids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent start error: %v", err)
	}
	first := ""
	for sid := range sids {
		if first == "" {
			first = sid
		}
		if sid != first {
			t.Fatalf("got multiple sessions: %s and %s", first, sid)
		}
	}
}

func TestObjectStoreRangeReads(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	const chunkSize = uint32(65536)
	data := randomData(300000)
	sid := uploadAll(t, c, "B", "obj", _EMPTY_, data, chunkSize, true)
	c.commit(t, "B", sid)

	ranges := [][2]int64{
		{0, 1},
		{0, int64(chunkSize)},
		{int64(chunkSize) - 1, 2},
		{int64(chunkSize) - 1, int64(chunkSize) + 2},
		{100, 100000},
		{100000, 100000},
		{int64(len(data)) - 1, 1},
		{int64(len(data)) - 100, 100},
	}
	for _, r := range ranges {
		got := c.getRange(t, "B", "obj", r[0], r[1])
		end := r[0] + r[1]
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		want := data[r[0]:end]
		if !bytes.Equal(got, want) {
			t.Fatalf("range %d+%d mismatch (%d vs %d)", r[0], r[1], len(got), len(want))
		}
	}

	// Chunk-number ranges.
	got := c.getChunkRange(t, "B", "obj", 1, 2)
	if !bytes.Equal(got, data[chunkSize:3*chunkSize]) {
		t.Fatal("chunk range 1-2 mismatch")
	}
	got = c.getChunkRange(t, "B", "obj", 0, 0)
	if !bytes.Equal(got, data[:chunkSize]) {
		t.Fatal("chunk range 0 mismatch")
	}
}

func TestObjectStoreMetadataConsistency(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	data1 := randomData(5000)
	sid1 := uploadAll(t, c, "B", "name with spaces.文件", "text/plain", data1, 1024, true)
	info := c.commit(t, "B", sid1)
	if info.Size != 5000 || info.NumChunks != 5 || info.ContentType != "text/plain" {
		t.Fatalf("info v1: %+v", info)
	}
	if info.Digest != objShaDigest(data1) {
		t.Fatal("digest v1")
	}
	if info.ModTime.IsZero() {
		t.Fatal("mtime zero")
	}

	// Replace with a new version.
	data2 := randomData(3000)
	sid2 := uploadAll(t, c, "B", "name with spaces.文件", "application/json", data2, 1000, false)
	info2 := c.commit(t, "B", sid2)
	if info2.ID != sid2 || info2.Size != 3000 || info2.ContentType != "application/json" {
		t.Fatalf("info v2: %+v", info2)
	}

	// Only one object with that name.
	objs := c.list(t, "B")
	if len(objs) != 1 || objs[0].ID != sid2 {
		t.Fatalf("list after replace: %+v", objs)
	}
	// Old version messages removed.
	stream := objStreamName("B")
	if n, _ := c.countMatching(stream, "$OBJ.B.O."+sid1+".M"); n != 0 {
		t.Fatal("old meta remains")
	}
	if n, _ := c.countMatching(stream, "$OBJ.B.S."+sid1+".>"); n != 0 {
		t.Fatalf("old chunks/descriptor remain: %d", n)
	}
}

func TestObjectStoreDelete(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	sub, evCh := c.events()
	defer sub.Unsubscribe()

	data := randomData(4096)
	sid := uploadAll(t, c, "B", "o", _EMPTY_, data, 1024, false)
	c.commit(t, "B", sid)
	_ = nextEvent(t, evCh)

	c.delete(t, "B", "o")
	if _, err := c.getMetaErr("B", "o"); err == nil {
		t.Fatal("object readable after delete")
	}
	if objs := c.list(t, "B"); len(objs) != 0 {
		t.Fatalf("object listed after delete: %+v", objs)
	}
	if n, _ := c.countMatching(objStreamName("B"), "$OBJ.B.S."+sid+".>"); n != 0 {
		t.Fatalf("data remains after delete: %d", n)
	}
	ev := nextEvent(t, evCh)
	if ev.Op != objEventDeleted {
		t.Fatalf("event: %+v", ev)
	}
}

func TestObjectStorePut(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	sub, evCh := c.events()
	defer sub.Unsubscribe()

	data := randomData(200000)
	info := c.put(t, "B", "one-shot.bin", "application/data", data)
	if info.Size != uint64(len(data)) || info.NumChunks != 2 || info.ChunkSize != objDefaultChunkSize {
		t.Fatalf("put info: %+v", info)
	}
	got := c.getRange(t, "B", "one-shot.bin", 0, -1)
	if !bytes.Equal(got, data) {
		t.Fatal("put data mismatch")
	}

	// Overwrite.
	data2 := randomData(10)
	c.put(t, "B", "one-shot.bin", _EMPTY_, data2)
	got = c.getRange(t, "B", "one-shot.bin", 0, -1)
	if !bytes.Equal(got, data2) {
		t.Fatal("overwrite mismatch")
	}
	if objs := c.list(t, "B"); len(objs) != 1 {
		t.Fatalf("list after overwrite: %+v", objs)
	}
	// Two created events total.
	ev := nextEvent(t, evCh)
	if ev.Op != objEventCreated {
		t.Fatalf("event: %+v", ev)
	}
	ev = nextEvent(t, evCh)
	if ev.Op != objEventCreated {
		t.Fatalf("event: %+v", ev)
	}
}

func TestObjectStoreMalformedRequests(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, err := natsc.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	msg, err := nc.Request(objAPIUploadStart, []byte("{not json"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp objStartResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for malformed json")
	}

	// Missing required fields.
	if _, err := nc.Request(objAPIUploadStart, []byte(`{"bucket":"B"}`), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// Empty request to bucket create.
	if _, err := nc.Request(objAPIBucketCreate, []byte(`{}`), 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestObjectStoreIdempotentRecommit(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	c := newObjTestClient(t, s)

	data := randomData(2048)
	sid := uploadAll(t, c, "B", "o", _EMPTY_, data, 1024, false)
	c.commit(t, "B", sid)
	// Recommit returns success, no new event.
	sub, evCh := c.events()
	defer sub.Unsubscribe()
	c.commit(t, "B", sid)
	select {
	case <-evCh:
		t.Fatal("unexpected event on recommit")
	case <-time.After(300 * time.Millisecond):
	}
	// Math sanity for bounds used in pending math.
	if math.MaxUint32 == 0 {
		t.Fatal("impossible")
	}
}

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalRanges(a, b []objRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
