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
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nuid"
)

// Object Store protocol constants.
//
// Every bucket is backed by a single JetStream stream named "OBJ_<bucket>"
// with subject space "$OBJ.<bucket>.>". Messages inside the stream:
//
//	$OBJ.<bucket>.B                 bucket information message
//	$OBJ.<bucket>.S.<sid>           upload session descriptor (JSON)
//	$OBJ.<bucket>.S.<sid>.C.<n>     chunk n payload (n starts at 0)
//	$OBJ.<bucket>.O.<sid>.M         object metadata message (commit marker)
//
// An object is readable/listable only while its M message exists. Committing
// is therefore atomic: it is a single metadata message write. Because every
// session and chunk is a normal JetStream message, replication, leader changes
// and server restarts are handled by JetStream/Raft.
const (
	objPrefix    = "$OBJ"
	objAPIPrefix = "$OBJ.API"

	// Request/response API endpoints (account scoped).
	objAPIBucketCreate = "$OBJ.API.BUCKET.CREATE"
	objAPIPut          = "$OBJ.API.PUT"
	objAPIUploadStart  = "$OBJ.API.UPLOAD.START"
	objAPIUploadChunk  = "$OBJ.API.UPLOAD.CHUNK"
	objAPIUploadStatus = "$OBJ.API.UPLOAD.STATUS"
	objAPIUploadCommit = "$OBJ.API.UPLOAD.COMMIT"
	objAPIUploadAbort  = "$OBJ.API.UPLOAD.ABORT"
	objAPIGet          = "$OBJ.API.OBJECT.GET"
	objAPIGetChunk     = "$OBJ.API.OBJECT.GET.CHUNK"
	objAPIList         = "$OBJ.API.OBJECT.LIST"
	objAPIDelete       = "$OBJ.API.OBJECT.DELETE"

	// Change notifications.
	objEventSubject = "$OBJ.EVENT"

	// Backing stream naming and subject templates.
	objStreamPrefix = "OBJ_"
	objStreamNameT   = "OBJ_%s"

	objBucketInfoT  = "$OBJ.%s.B"
	objSessionT     = "$OBJ.%s.S.%s"
	objChunkT       = "$OBJ.%s.S.%s.C.%d"
	objChunkFilterT = "$OBJ.%s.S.%s.C.*"
	objMetaT        = "$OBJ.%s.O.%s.M"
	objMetaFilterT  = "$OBJ.%s.O.>"
	objSessFilterT  = "$OBJ.%s.S.*"
	objSubjectsT    = "$OBJ.%s.>"

	// Headers used by binary payload endpoints (chunk, put).
	objHdrBucket    = "Nats-Object-Bucket"
	objHdrSession   = "Nats-Object-Session"
	objHdrChunk     = "Nats-Object-Chunk"
	objHdrDigest    = "Nats-Object-Digest"
	objHdrName      = "Nats-Object-Name"
	objHdrDesc      = "Nats-Object-Description"
	objHdrCType     = "Nats-Object-Content-Type"
	objHdrChunkSize = "Nats-Object-Chunk-Size"

	// Defaults.
	objDefaultChunkSize     = 128 * 1024
	objDefaultSessionTTL    = 2 * time.Hour
	objDefaultSweepInterval = time.Minute
	objRequestTimeout       = 15 * time.Second
	objMaxEnumerate         = 100_000
	objInboxPrefix          = "_INBOX."

	objShaPrefix = "SHA-256="
)

// Session states.
const (
	objSessionOpen      = "open"
	objSessionCommitted = "committed"
	objSessionAborted   = "aborted"
)

// Event operations.
const (
	objEventCreated = "created"
	objEventDeleted = "deleted"
	objEventAborted = "aborted"
)

// Wire response/type tags.
const (
	objResponseType = "io.nats.object.v1.response"
	objEventType    = "io.nats.object.v1.event"
)

var (
	errObjBadRequest     = errors.New("objectstore: bad request")
	errObjSessionGone    = errors.New("objectstore: session not found or no longer active")
	errObjDigestMismatch = errors.New("objectstore: digest mismatch")
	errObjChunkConflict  = errors.New("objectstore: chunk exists with different content")
)

// objBucketInfo is persisted at the bucket information subject.
type objBucketInfo struct {
	Bucket      string        `json:"bucket"`
	Description string        `json:"description,omitempty"`
	Replicas    int           `json:"num_replicas"`
	Storage     StorageType   `json:"storage"`
	MaxBytes    int64         `json:"max_bytes,omitempty"`
	Created     time.Time     `json:"created"`
	TTL         time.Duration `json:"ttl,omitempty"`
}

// objUploadStartRequest starts (or resumes) an upload session.
type objUploadStartRequest struct {
	Bucket      string            `json:"bucket"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	ChunkSize   uint32            `json:"chunk_size,omitempty"`
	NumChunks   uint32            `json:"num_chunks,omitempty"`
	TotalSize   uint64            `json:"total_size,omitempty"`
	Digest      string            `json:"digest,omitempty"`
	TTL         time.Duration     `json:"ttl,omitempty"`
	Replicas    int               `json:"num_replicas,omitempty"`
	Storage     StorageType       `json:"storage,omitempty"`
	MaxBytes    int64             `json:"max_bytes,omitempty"`
}

// objSession is the persisted session descriptor.
type objSession struct {
	ID          string            `json:"id"`
	Bucket      string            `json:"bucket"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	ChunkSize   uint32            `json:"chunk_size"`
	NumChunks   uint32            `json:"num_chunks"`
	TotalSize   uint64            `json:"total_size,omitempty"`
	Digest      string            `json:"digest,omitempty"`
	Created     time.Time         `json:"created"`
	Deadline    time.Time         `json:"deadline"`
	State       string            `json:"state"`
}

// objChunkInfo describes a single committed chunk.
type objChunkInfo struct {
	Num    uint32 `json:"num"`
	Size   uint32 `json:"size"`
	Digest string `json:"digest"`
}

// objMeta is the committed object metadata message.
type objMeta struct {
	ID          string            `json:"id"`
	Bucket      string            `json:"bucket"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Size        uint64            `json:"size"`
	ChunkSize   uint32            `json:"chunk_size"`
	NumChunks   uint32            `json:"num_chunks"`
	Digest      string            `json:"digest,omitempty"`
	Chunks      []objChunkInfo    `json:"chunks"`
	ModTime     time.Time         `json:"mtime"`
}

// objInfo is the public object information view.
type objInfo struct {
	ID          string            `json:"id"`
	Bucket      string            `json:"bucket"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Size        uint64            `json:"size"`
	ChunkSize   uint32            `json:"chunk_size"`
	NumChunks   uint32            `json:"num_chunks"`
	Digest      string            `json:"digest,omitempty"`
	ModTime     time.Time         `json:"mtime"`
}

func (m *objMeta) info() *objInfo {
	return &objInfo{
		ID:          m.ID,
		Bucket:      m.Bucket,
		Name:        m.Name,
		Description: m.Description,
		ContentType: m.ContentType,
		Metadata:    m.Metadata,
		Size:        m.Size,
		ChunkSize:   m.ChunkSize,
		NumChunks:   m.NumChunks,
		Digest:      m.Digest,
		ModTime:     m.ModTime,
	}
}

// objRange is an inclusive chunk number interval.
type objRange struct {
	First uint32 `json:"first"`
	Last  uint32 `json:"last"`
}

// API response envelopes.
type objResponse struct {
	Type  string    `json:"type"`
	Error *ApiError `json:"error,omitempty"`
}

func newObjResponse() objResponse {
	return objResponse{Type: objResponseType}
}

type objStartResponse struct {
	objResponse
	Created bool         `json:"created,omitempty"`
	Session *objSession  `json:"session,omitempty"`
	Received []uint32    `json:"received,omitempty"`
	Ranges   []objRange  `json:"ranges,omitempty"`
}

type objChunkResponse struct {
	objResponse
	Num       uint32 `json:"num"`
	Size      uint32 `json:"size"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

type objStatusResponse struct {
	objResponse
	Session  *objSession `json:"session,omitempty"`
	Received []uint32    `json:"received,omitempty"`
	Ranges   []objRange  `json:"ranges,omitempty"`
	Pending  []objRange  `json:"pending,omitempty"`
	Size     uint64      `json:"size"`
}

type objGetResponse struct {
	objResponse
	Info *objInfo `json:"info,omitempty"`
}

type objGetChunkRequest struct {
	Bucket string  `json:"bucket"`
	Name   string  `json:"name"`
	ID     string  `json:"id,omitempty"`
	Offset int64   `json:"offset,omitempty"`
	Length int64   `json:"length,omitempty"`
	First  *int32  `json:"first,omitempty"`
	Last   *int32  `json:"last,omitempty"`
}

type objGetChunkResponse struct {
	objResponse
	Num  uint32 `json:"num,omitempty"`
	Size uint32 `json:"size"`
	Data []byte `json:"data,omitempty"`
}

type objListResponse struct {
	objResponse
	Objects []objInfo `json:"objects"`
}

type objDeleteResponse struct {
	objResponse
	Success bool `json:"success"`
}

type objPutResponse struct {
	objResponse
	Info *objInfo `json:"info,omitempty"`
}

// objEvent is published to objEventSubject on object/object lifecycle changes.
type objEvent struct {
	Type   string    `json:"type"`
	Bucket string    `json:"bucket"`
	Object string    `json:"object"`
	ID     string    `json:"id"`
	Op     string    `json:"op"`
	Time   time.Time `json:"time"`
}

// objStore is the account scoped Object Store manager. It holds no object
// state itself: every operation is reconstructed from the backing streams.
type objStore struct {
	acc   *Account
	srv   *Server

	createMu sync.Mutex

	mu            sync.Mutex
	closed        bool
	subs          []*subscription
	sweepTimer    *time.Timer
	sweepInterval time.Duration
	quitCh        chan struct{}
	jq            *objInternalSendQ
}

// objInternalSendQ is an account scoped outbound queue that delivers through
// a JETSTREAM-kind internal client. Server-side JS API dispatch only honors
// requests from JETSTREAM clients (SYSTEM clients are ignored), so the generic
// account sendq cannot be used.
type objInternalSendQ struct {
	s *Server
	a *Account
	q *ipQueue[*outMsg]
}

func (os *objStore) sendQueue() *objInternalSendQ {
	os.mu.Lock()
	defer os.mu.Unlock()
	if os.jq == nil {
		jq := &objInternalSendQ{s: os.srv, a: os.acc, q: newIPQueue[*outMsg](os.srv, "ObjectStore sendQ")}
		os.srv.startGoRoutine(jq.loop)
		os.jq = jq
	}
	return os.jq
}

func (jq *objInternalSendQ) send(subj, rply string, hdr, msg []byte) {
	out := outMsgPool.Get().(*outMsg)
	out.subj, out.rply = subj, rply
	out.hdr = append(out.hdr[:0], hdr...)
	out.msg = append(out.msg[:0], msg...)
	jq.q.push(out)
}

func (jq *objInternalSendQ) loop() {
	s := jq.s
	defer s.grWG.Done()

	c := s.createInternalJetStreamClient()
	c.registerWithAccount(jq.a)
	defer c.closeConnection(ClientClosed)

	var (
		subj [256]byte
		rply [256]byte
	)

	for s.isRunning() {
		select {
		case <-s.quitCh:
			return
		case <-jq.q.ch:
			pms := jq.q.pop()
			for _, pm := range pms {
				c.pa.subject = append(subj[:0], pm.subj...)
				c.pa.size = len(pm.msg) + len(pm.hdr)
				c.pa.szb = strconv.AppendInt(nil, int64(c.pa.size), 10)
				if len(pm.rply) > 0 {
					c.pa.reply = append(rply[:0], pm.rply...)
				} else {
					c.pa.reply = nil
				}
				var msg []byte
				if len(pm.hdr) > 0 {
					c.pa.hdr = len(pm.hdr)
					c.pa.hdb = strconv.AppendInt(nil, int64(c.pa.hdr), 10)
					msg = append(msg[:0], pm.hdr...)
					msg = append(msg, pm.msg...)
					msg = append(msg, _CRLF_...)
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					msg = append(msg[:0], pm.msg...)
					msg = append(msg, _CRLF_...)
				}
				c.processInboundClientMsg(msg)
				outMsgPool.Put(pm)
			}
			c.flushClients(0)
			jq.q.recycle(&pms)
		}
	}
}

// startObjectStore starts the Object Store subsystem for an account. It is a
// no-op if it is already running.
func (a *Account) startObjectStore() {
	a.mu.Lock()
	if a.obj != nil {
		a.mu.Unlock()
		return
	}
	s := a.srv
	os := &objStore{
		acc:           a,
		srv:           s,
		sweepInterval: objDefaultSweepInterval,
		quitCh:        make(chan struct{}),
	}
	a.obj = os
	a.mu.Unlock()

	os.start()
}

func (a *Account) stopObjectStore() {
	a.mu.Lock()
	os := a.obj
	a.obj = nil
	a.mu.Unlock()
	if os != nil {
		os.stop()
	}
}

func (os *objStore) start() {
	endpoints := map[string]msgHandler{
		objAPIBucketCreate: os.bucketCreateRequest,
		objAPIPut:          os.putRequest,
		objAPIUploadStart:  os.uploadStartRequest,
		objAPIUploadChunk:  os.uploadChunkRequest,
		objAPIUploadStatus: os.uploadStatusRequest,
		objAPIUploadCommit: os.uploadCommitRequest,
		objAPIUploadAbort:  os.uploadAbortRequest,
		objAPIGet:          os.objectGetRequest,
		objAPIGetChunk:     os.objectGetChunkRequest,
		objAPIList:         os.objectListRequest,
		objAPIDelete:       os.objectDeleteRequest,
	}
	// Deterministic registration order for stable logs/tests.
	subjects := make([]string, 0, len(endpoints))
	for subj := range endpoints {
		subjects = append(subjects, subj)
	}
	for i := 0; i < len(subjects); i++ {
		for j := i + 1; j < len(subjects); j++ {
			if subjects[j] < subjects[i] {
				subjects[i], subjects[j] = subjects[j], subjects[i]
			}
		}
	}
	for _, subj := range subjects {
		sub, err := os.acc.subscribeInternal(subj, endpoints[subj])
		if err != nil {
			os.srv.Errorf("Object Store: failed to subscribe %q for account %q: %v", subj, os.acc.Name, err)
			continue
		}
		os.mu.Lock()
		os.subs = append(os.subs, sub)
		os.mu.Unlock()
	}

	os.mu.Lock()
	os.sweepTimer = time.AfterFunc(os.sweepInterval, os.sweep)
	os.mu.Unlock()
}

func (os *objStore) stop() {
	os.mu.Lock()
	if os.closed {
		os.mu.Unlock()
		return
	}
	os.closed = true
	close(os.quitCh)
	if os.sweepTimer != nil {
		os.sweepTimer.Stop()
		os.sweepTimer = nil
	}
	subs := append([]*subscription(nil), os.subs...)
	os.subs = nil
	os.mu.Unlock()

	for _, sub := range subs {
		os.acc.unsubscribeInternal(sub)
	}
}

// -------------------------------------------------------------------
// Internal messaging primitives
// -------------------------------------------------------------------

// send delivers raw bytes within the account through the JETSTREAM internal
// outbound queue.
func (os *objStore) send(subj, rply string, hdr, msg []byte) {
	os.sendQueue().send(subj, rply, hdr, msg)
}

func (os *objStore) respond(reply string, v any) {
	if reply == _EMPTY_ {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		os.srv.Errorf("Object Store: failed to marshal response: %v", err)
		return
	}
	os.send(reply, _EMPTY_, nil, b)
}

// request performs a JetStream API style request within the account and
// unmarshals the JSON reply into resp. resp must embed objResponse-like error
// access; callers inspect response errors separately.
func (os *objStore) request(subj string, req any, resp any) error {
	var body []byte
	if req != nil {
		b, err := json.Marshal(req)
		if err != nil {
			return err
		}
		body = b
	}

	inbox := objInboxPrefix + nuid.Next()
	ch := make(chan []byte, 16)
	sub, err := os.acc.subscribeInternal(inbox, func(_ *subscription, c *client, _ *Account, _, _ string, m []byte) {
		select {
		case ch <- append([]byte(nil), m...):
		default:
		}
	})
	if err != nil {
		return err
	}
	defer os.acc.unsubscribeInternal(sub)

	os.send(subj, inbox, nil, body)

	timer := time.NewTimer(objRequestTimeout)
	defer timer.Stop()
	select {
	case b := <-ch:
		return json.Unmarshal(b, resp)
	case <-timer.C:
		return fmt.Errorf("objectstore: request to %q timed out", subj)
	case <-os.quitCh:
		return ErrServerNotRunning
	}
}

// getLastMsg fetches the last message for a subject. It returns
// (nil, nil) when no such message exists.
func (os *objStore) getLastMsg(stream, subj string) (*StoredMsg, error) {
	var resp JSApiMsgGetResponse
	if err := os.request(fmt.Sprintf(JSApiMsgGetT, stream), &JSApiMsgGetRequest{LastFor: subj}, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrCode == uint16(JSNoMessageFoundErr) {
			return nil, nil
		}
		return nil, resp.Error
	}
	return resp.Message, nil
}

// enumerate walks messages matching the (optionally wildcard) filter using
// NextFor requests, starting from seq 0. It stops at no-message. The NextFor
// sequence is inclusive, so the next request starts after the returned seq.
func (os *objStore) enumerate(stream, filter string) ([]*StoredMsg, error) {
	msgs := make([]*StoredMsg, 0)
	var seq uint64
	for {
		if len(msgs) >= objMaxEnumerate {
			break
		}
		var resp JSApiMsgGetResponse
		if err := os.request(fmt.Sprintf(JSApiMsgGetT, stream), &JSApiMsgGetRequest{NextFor: filter, Seq: seq}, &resp); err != nil {
			return msgs, err
		}
		if resp.Error != nil {
			if resp.Error.ErrCode == uint16(JSNoMessageFoundErr) {
				return msgs, nil
			}
			return msgs, resp.Error
		}
		if resp.Message == nil {
			return msgs, nil
		}
		msgs = append(msgs, resp.Message)
		seq = resp.Message.Sequence + 1
	}
	return msgs, nil
}

// countMatching returns the number of messages matching filter.
func (os *objStore) countMatching(stream, filter string) (int, error) {
	msgs, err := os.enumerate(stream, filter)
	return len(msgs), err
}

// purgeSubject purges messages matching the subject filter from the backing
// stream (wildcards allowed).
func (os *objStore) purgeSubject(stream, filter string) (uint64, error) {
	var resp JSApiStreamPurgeResponse
	if err := os.request(fmt.Sprintf(JSApiStreamPurgeT, stream), &JSApiStreamPurgeRequest{Subject: filter}, &resp); err != nil {
		return 0, err
	}
	if resp.Error != nil {
		return 0, resp.Error
	}
	return resp.Purged, nil
}

// publishExpectAck publishes raw bytes and waits for the stream PubAck.
func (os *objStore) publishExpectAck(subj string, hdr, data []byte) (*PubAck, error) {
	inbox := objInboxPrefix + nuid.Next()
	ch := make(chan []byte, 4)
	sub, err := os.acc.subscribeInternal(inbox, func(_ *subscription, c *client, _ *Account, _, _ string, m []byte) {
		select {
		case ch <- append([]byte(nil), m...):
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer os.acc.unsubscribeInternal(sub)

	os.send(subj, inbox, hdr, data)

	timer := time.NewTimer(objRequestTimeout)
	defer timer.Stop()
	select {
	case b := <-ch:
		var ack PubAck
		if err := json.Unmarshal(b, &ack); err != nil {
			return nil, err
		}
		return &ack, nil
	case <-timer.C:
		return nil, fmt.Errorf("objectstore: publish to %q timed out waiting for ack", subj)
	case <-os.quitCh:
		return nil, ErrServerNotRunning
	}
}

// publishEvent publishes a change notification.
func (os *objStore) publishEvent(bucket, name, id, op string) {
	ev := &objEvent{
		Type:   objEventType,
		Bucket: bucket,
		Object: name,
		ID:     id,
		Op:     op,
		Time:   time.Now().UTC(),
	}
	if err := os.srv.sendInternalAccountMsg(os.acc, objEventSubject, ev); err != nil {
		os.srv.Debugf("Object Store: failed to publish event: %v", err)
	}
}

// -------------------------------------------------------------------
// Digest helpers
// -------------------------------------------------------------------

func objShaDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return objShaPrefix + base64.StdEncoding.EncodeToString(sum[:])
}

func objDigestMatches(data []byte, digest string) bool {
	if digest == _EMPTY_ {
		return true
	}
	return objShaDigest(data) == digest
}

// headerValue extracts a header value from a persisted/raw NATS header block.
func objHeaderValue(hdr []byte, key string) string {
	if len(hdr) == 0 {
		return _EMPTY_
	}
	tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(hdr)))
	if _, err := tp.ReadLine(); err != nil { // leading "NATS/1.0" line
		return _EMPTY_
	}
	mh, err := tp.ReadMIMEHeader()
	if err != nil {
		return _EMPTY_
	}
	return mh.Get(key)
}

// -------------------------------------------------------------------
// Subject builders
// -------------------------------------------------------------------

func objStreamName(bucket string) string {
	return objStreamPrefix + bucket
}

func objBucketInfoSubject(bucket string) string {
	return fmt.Sprintf(objBucketInfoT, bucket)
}

func objSessionSubject(bucket, sid string) string {
	return fmt.Sprintf(objSessionT, bucket, sid)
}

func objChunkSubject(bucket, sid string, n uint32) string {
	return fmt.Sprintf(objChunkT, bucket, sid, n)
}

func objChunkFilter(bucket, sid string) string {
	return fmt.Sprintf(objChunkFilterT, bucket, sid)
}

func objMetaSubject(bucket, sid string) string {
	return fmt.Sprintf(objMetaT, bucket, sid)
}

func chunkNumFromSubject(subj string) (uint32, error) {
	idx := strings.LastIndexByte(subj, '.')
	if idx < 0 {
		return 0, fmt.Errorf("bad chunk subject %q", subj)
	}
	n, err := strconv.ParseUint(subj[idx+1:], 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(n), nil
}

// -------------------------------------------------------------------
// Session helpers
// -------------------------------------------------------------------

func (os *objStore) loadSession(bucket, sid string) (*objSession, error) {
	sm, err := os.getLastMsg(objStreamName(bucket), objSessionSubject(bucket, sid))
	if err != nil {
		return nil, err
	}
	if sm == nil {
		return nil, errObjSessionGone
	}
	var sess objSession
	if err := json.Unmarshal(sm.Data, &sess); err != nil {
		return nil, err
	}
	return &sess, nil
}

// sessionChunks returns the received chunk messages for a session.
func (os *objStore) sessionChunks(sess *objSession) ([]*StoredMsg, error) {
	return os.enumerate(objStreamName(sess.Bucket), objChunkFilter(sess.Bucket, sess.ID))
}

// latestSessionDescriptors returns the newest descriptor per session id in a
// bucket. The descriptor subject holds the full history of rewrites (open ->
// committed/aborted), so callers must only consider the newest entry.
func (os *objStore) latestSessionDescriptors(bucket string) (map[string]*objSession, error) {
	stream := objStreamName(bucket)
	descs, err := os.enumerate(stream, fmt.Sprintf(objSessFilterT, bucket))
	if err != nil {
		return nil, err
	}

	type entry struct {
		sess objSession
		seq  uint64
	}
	latest := make(map[string]*entry)
	for _, sm := range descs {
		var sess objSession
		if err := json.Unmarshal(sm.Data, &sess); err != nil {
			continue
		}
		if e, ok := latest[sess.ID]; !ok || sm.Sequence > e.seq {
			latest[sess.ID] = &entry{sess: sess, seq: sm.Sequence}
		}
	}

	out := make(map[string]*objSession, len(latest))
	for id, e := range latest {
		s := e.sess
		out[id] = &s
	}
	return out, nil
}

// findActiveSession returns the open, unexpired session for an object name.
func (os *objStore) findActiveSession(bucket, name string) (*objSession, error) {
	latest, err := os.latestSessionDescriptors(bucket)
	if err != nil {
		return nil, err
	}
	var found *objSession
	for _, sess := range latest {
		if sess.Name != name || sess.State != objSessionOpen {
			continue
		}
		if !sess.Deadline.IsZero() && time.Now().After(sess.Deadline) {
			continue
		}
		// Multiple open sessions for the same name should not exist; newest
		// created wins as a deterministic tiebreak.
		if found == nil || sess.Created.After(found.Created) {
			s := *sess
			found = &s
		}
	}
	return found, nil
}

// receivedSet builds a sorted chunk number set plus contiguous ranges from
// chunk messages.
func chunkSet(msgs []*StoredMsg) (received []uint32, ranges []objRange, size uint64, err error) {
	set := make(map[uint32]bool)
	for _, sm := range msgs {
		n, e := chunkNumFromSubject(sm.Subject)
		if e != nil {
			return nil, nil, 0, e
		}
		if !set[n] {
			set[n] = true
			received = append(received, n)
		}
		size += uint64(len(sm.Data))
	}
	// order
	for i := 0; i < len(received); i++ {
		for j := i + 1; j < len(received); j++ {
			if received[j] < received[i] {
				received[i], received[j] = received[j], received[i]
			}
		}
	}
	ranges = contiguousRanges(received)
	return received, ranges, size, nil
}

func contiguousRanges(sorted []uint32) []objRange {
	if len(sorted) == 0 {
		return nil
	}
	ranges := make([]objRange, 0)
	first, last := sorted[0], sorted[0]
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == last+1 {
			last = sorted[i]
			continue
		}
		ranges = append(ranges, objRange{First: first, Last: last})
		first, last = sorted[i], sorted[i]
	}
	ranges = append(ranges, objRange{First: first, Last: last})
	return ranges
}

// pendingRanges returns chunk intervals missing from received within
// [0, numChunks).
func pendingRanges(received []uint32, numChunks uint32) []objRange {
	if numChunks == 0 {
		return nil
	}
	set := make(map[uint32]bool, len(received))
	for _, n := range received {
		set[n] = true
	}
	ranges := make([]objRange, 0)
	var first, last uint32
	open := false
	for n := uint32(0); n < numChunks; n++ {
		if !set[n] {
			if !open {
				first, last, open = n, n, true
			} else {
				last = n
			}
		} else if open {
			ranges = append(ranges, objRange{First: first, Last: last})
			open = false
		}
	}
	if open {
		ranges = append(ranges, objRange{First: first, Last: last})
	}
	return ranges
}

// -------------------------------------------------------------------
// Bucket management
// -------------------------------------------------------------------

// ensureBucket creates the backing stream and bucket info message if missing.
// It returns whether it created the bucket.
func (os *objStore) ensureBucket(info *objBucketInfo) (bool, error) {
	if info == nil || info.Bucket == _EMPTY_ {
		return false, errObjBadRequest
	}
	stream := objStreamName(info.Bucket)

	// Check for existing stream.
	var infoResp JSApiStreamInfoResponse
	iErr := os.request(fmt.Sprintf(JSApiStreamInfoT, stream), nil, &infoResp)
	if iErr == nil && infoResp.Error == nil && infoResp.StreamInfo != nil {
		return false, nil
	}
	if iErr == nil && infoResp.Error != nil &&
		infoResp.Error.ErrCode != uint16(JSStreamNotFoundErr) {
		return false, infoResp.Error
	}

	cfg := StreamConfig{
		Name:        stream,
		Description: info.Description,
		Subjects:    []string{fmt.Sprintf(objSubjectsT, info.Bucket)},
		Retention:   LimitsPolicy,
		Storage:     info.Storage,
		Replicas:    info.Replicas,
		MaxBytes:    info.MaxBytes,
		Discard:     DiscardOld,
		AllowDirect: true,
	}
	if cfg.Storage == 0 {
		cfg.Storage = FileStorage
	}
	if cfg.Replicas < 1 {
		cfg.Replicas = 1
	}

	var createResp JSApiStreamCreateResponse
	if err := os.request(fmt.Sprintf(JSApiStreamCreateT, stream), &StreamConfigRequest{StreamConfig: cfg}, &createResp); err != nil {
		return false, err
	}
	if createResp.Error != nil {
		if createResp.Error.ErrCode == uint16(JSStreamNameExistErr) ||
			createResp.Error.ErrCode == uint16(JSStreamNotFoundErr) {
			// Race with a concurrent create; bucket now exists.
			return false, nil
		}
		return false, createResp.Error
	}

	info.Created = time.Now().UTC()
	if info.Storage == 0 {
		info.Storage = cfg.Storage
	}
	info.Replicas = cfg.Replicas
	b, _ := json.Marshal(info)
	if _, err := os.publishExpectAck(objBucketInfoSubject(info.Bucket), nil, b); err != nil {
		return true, err
	}
	return true, nil
}

func (os *objStore) bucketCreateRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req objUploadStartRequest
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	if req.Bucket == _EMPTY_ {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	info := &objBucketInfo{
		Bucket:      req.Bucket,
		Description: _EMPTY_,
		Replicas:    req.Replicas,
		Storage:     req.Storage,
		MaxBytes:    req.MaxBytes,
		TTL:         req.TTL,
	}
	if _, err := os.ensureBucket(info); err != nil {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	os.respond(reply, objStartResponse{objResponse: newObjResponse()})
}

func objResponseWithError(e *ApiError) objResponse {
	return objResponse{Type: objResponseType, Error: e}
}

// -------------------------------------------------------------------
// Upload start (and resume)
// -------------------------------------------------------------------

func (os *objStore) uploadStartRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req objUploadStartRequest
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	if req.Bucket == _EMPTY_ || req.Name == _EMPTY_ {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}

	os.createMu.Lock()
	defer os.createMu.Unlock()

	binfo := &objBucketInfo{
		Bucket:   req.Bucket,
		Replicas: req.Replicas,
		Storage:  req.Storage,
		MaxBytes: req.MaxBytes,
	}
	if _, err := os.ensureBucket(binfo); err != nil {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	// Resume an active session if one exists for this object name.
	if sess, err := os.findActiveSession(req.Bucket, req.Name); err == nil && sess != nil {
		chunks, cerr := os.sessionChunks(sess)
		if cerr != nil {
			os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(cerr)))})
			return
		}
		received, ranges, _, _ := chunkSet(chunks)
		os.respond(reply, objStartResponse{
			objResponse: newObjResponse(),
			Created:     false,
			Session:     sess,
			Received:    received,
			Ranges:      ranges,
		})
		return
	} else if err != nil {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	// New session.
	chunkSize := req.ChunkSize
	if chunkSize == 0 {
		chunkSize = objDefaultChunkSize
	}
	numChunks := req.NumChunks
	if numChunks == 0 && req.TotalSize > 0 {
		numChunks = uint32((req.TotalSize + uint64(chunkSize) - 1) / uint64(chunkSize))
	}
	if numChunks == 0 {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = objDefaultSessionTTL
	}
	now := time.Now().UTC()
	sess := &objSession{
		ID:          nuid.Next(),
		Bucket:      req.Bucket,
		Name:        req.Name,
		Description: req.Description,
		ContentType: req.ContentType,
		Metadata:    req.Metadata,
		ChunkSize:   chunkSize,
		NumChunks:   numChunks,
		TotalSize:   req.TotalSize,
		Digest:      req.Digest,
		Created:     now,
		Deadline:    now.Add(ttl),
		State:       objSessionOpen,
	}
	b, _ := json.Marshal(sess)
	if _, err := os.publishExpectAck(objSessionSubject(req.Bucket, sess.ID), nil, b); err != nil {
		os.respond(reply, objStartResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	os.respond(reply, objStartResponse{
		objResponse: newObjResponse(),
		Created:     true,
		Session:     sess,
		Received:    []uint32{},
		Ranges:      nil,
	})
}

// -------------------------------------------------------------------
// Chunk upload
// -------------------------------------------------------------------

func (os *objStore) uploadChunkRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if c == nil {
		return
	}
	hdr, data := c.msgParts(rmsg)
	// Messages delivered via account internal subscriptions retain the
	// trailing CRLF; it is not part of the payload.
	if len(data) >= LEN_CR_LF {
		data = data[:len(data)-LEN_CR_LF]
	}
	bucket := objHeaderValue(hdr, objHdrBucket)
	sid := objHeaderValue(hdr, objHdrSession)
	numStr := objHeaderValue(hdr, objHdrChunk)
	digest := objHeaderValue(hdr, objHdrDigest)
	if bucket == _EMPTY_ || sid == _EMPTY_ || numStr == _EMPTY_ {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	num64, err := strconv.ParseUint(numStr, 10, 32)
	if err != nil {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	num := uint32(num64)

	sess, err := os.loadSession(bucket, sid)
	if err != nil {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	if sess.State != objSessionOpen {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjSessionGone)))})
		return
	}
	if !sess.Deadline.IsZero() && time.Now().After(sess.Deadline) {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjSessionGone)))})
		return
	}
	if num >= sess.NumChunks {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	// Verify payload digest before anything is persisted.
	if digest != _EMPTY_ && !objDigestMatches(data, digest) {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjDigestMismatch)))})
		return
	}

	csubj := objChunkSubject(bucket, sid, num)
	existing, err := os.getLastMsg(objStreamName(bucket), csubj)
	if err != nil {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	if existing != nil {
		if objShaDigest(existing.Data) == objShaDigest(data) {
			os.respond(reply, objChunkResponse{
				objResponse: newObjResponse(),
				Num:         num,
				Size:        uint32(len(data)),
				Duplicate:   true,
			})
			return
		}
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjChunkConflict)))})
		return
	}

	// Persist with the verified digest header so it can be re-checked at commit.
	chdr := bytes.NewBuffer(nil)
	chdr.WriteString("NATS/1.0\r\n")
	chdr.WriteString(objHdrDigest + ": " + digest + "\r\n\r\n")
	ack, err := os.publishExpectAck(csubj, chdr.Bytes(), data)
	if err != nil {
		os.respond(reply, objChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	_ = ack
	os.respond(reply, objChunkResponse{
		objResponse: newObjResponse(),
		Num:         num,
		Size:        uint32(len(data)),
	})
}

// -------------------------------------------------------------------
// Upload status
// -------------------------------------------------------------------

func (os *objStore) uploadStatusRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req struct {
		Bucket string `json:"bucket"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objStatusResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	sess, err := os.loadSession(req.Bucket, req.ID)
	if err != nil {
		os.respond(reply, objStatusResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	if sess.State != objSessionOpen {
		os.respond(reply, objStatusResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjSessionGone))), Session: sess})
		return
	}
	chunks, err := os.sessionChunks(sess)
	if err != nil {
		os.respond(reply, objStatusResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	received, ranges, size, err := chunkSet(chunks)
	if err != nil {
		os.respond(reply, objStatusResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	if received == nil {
		received = []uint32{}
	}
	os.respond(reply, objStatusResponse{
		objResponse: newObjResponse(),
		Session:     sess,
		Received:    received,
		Ranges:      ranges,
		Pending:     pendingRanges(received, sess.NumChunks),
		Size:        size,
	})
}

// -------------------------------------------------------------------
// Atomic commit
// -------------------------------------------------------------------

func (os *objStore) uploadCommitRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req struct {
		Bucket string `json:"bucket"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	sess, err := os.loadSession(req.Bucket, req.ID)
	if err != nil {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	// Idempotent re-commit.
	existingMeta := os.findMeta(sess.Bucket, sess.ID)
	if sess.State == objSessionCommitted && existingMeta != nil {
		os.respond(reply, objGetResponse{objResponse: newObjResponse(), Info: existingMeta.info()})
		return
	}
	if sess.State != objSessionOpen {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjSessionGone)))})
		return
	}
	if !sess.Deadline.IsZero() && time.Now().After(sess.Deadline) {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjSessionGone)))})
		return
	}

	chunks, err := os.sessionChunks(sess)
	if err != nil {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	meta, err := os.buildMeta(sess, chunks)
	if err != nil {
		// Verification failure: no metadata is written; object stays invisible.
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	// Locate a previous committed version of this object (different id, same
	// name) so it can be removed after the new metadata is visible.
	var oldMeta *objMeta
	for _, sm := range os.allMetas(sess.Bucket) {
		if sm.Name == sess.Name && sm.ID != sess.ID {
			if oldMeta == nil || sm.ModTime.After(oldMeta.ModTime) {
				oldMeta = sm
			}
		}
	}

	// Atomic commit: single metadata message write.
	mb, _ := json.Marshal(meta)
	if _, err := os.publishExpectAck(objMetaSubject(sess.Bucket, sess.ID), nil, mb); err != nil {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	// Mark session descriptor committed (latest descriptor wins on load).
	sess.State = objSessionCommitted
	sb, _ := json.Marshal(sess)
	_, _ = os.publishExpectAck(objSessionSubject(sess.Bucket, sess.ID), nil, sb)

	// Remove previous version only after new metadata is visible.
	if oldMeta != nil {
		os.removeObjectData(oldMeta)
	}

	os.publishEvent(meta.Bucket, meta.Name, meta.ID, objEventCreated)
	os.respond(reply, objGetResponse{objResponse: newObjResponse(), Info: meta.info()})
}

// buildMeta verifies every chunk and assembles the object metadata.
func (os *objStore) buildMeta(sess *objSession, chunks []*StoredMsg) (*objMeta, error) {
	if uint32(len(chunks)) != sess.NumChunks {
		return nil, fmt.Errorf("objectstore: expected %d chunks, have %d", sess.NumChunks, len(chunks))
	}
	byNum := make(map[uint32]*StoredMsg, sess.NumChunks)
	for _, sm := range chunks {
		n, err := chunkNumFromSubject(sm.Subject)
		if err != nil {
			return nil, err
		}
		if _, dup := byNum[n]; dup {
			return nil, fmt.Errorf("objectstore: duplicate chunk %d", n)
		}
		byNum[n] = sm
	}

	infos := make([]objChunkInfo, 0, sess.NumChunks)
	hasher := sha256.New()
	var total uint64
	for n := uint32(0); n < sess.NumChunks; n++ {
		sm, ok := byNum[n]
		if !ok {
			return nil, fmt.Errorf("objectstore: missing chunk %d", n)
		}
		data := sm.Data
		// Re-verify against the digest carried when the chunk was uploaded.
		chunkDigest := objHeaderValue(sm.Header, objHdrDigest)
		if chunkDigest != _EMPTY_ && !objDigestMatches(data, chunkDigest) {
			return nil, fmt.Errorf("%w: chunk %d", errObjDigestMismatch, n)
		}
		if chunkDigest == _EMPTY_ {
			chunkDigest = objShaDigest(data)
		}
		// Fixed chunk size except for the last chunk.
		if n+1 < sess.NumChunks && uint32(len(data)) != sess.ChunkSize {
			return nil, fmt.Errorf("objectstore: chunk %d has unexpected size %d", n, len(data))
		}
		if n+1 == sess.NumChunks && uint32(len(data)) > sess.ChunkSize {
			return nil, fmt.Errorf("objectstore: last chunk %d larger than chunk size", n)
		}
		infos = append(infos, objChunkInfo{Num: n, Size: uint32(len(data)), Digest: chunkDigest})
		hasher.Write(data)
		total += uint64(len(data))
	}
	if sess.TotalSize > 0 && total != sess.TotalSize {
		return nil, fmt.Errorf("objectstore: total size %d does not match expected %d", total, sess.TotalSize)
	}
	digest := objShaPrefix + base64.StdEncoding.EncodeToString(hasher.Sum(nil))
	if sess.Digest != _EMPTY_ && sess.Digest != digest {
		return nil, fmt.Errorf("%w: whole object", errObjDigestMismatch)
	}

	return &objMeta{
		ID:          sess.ID,
		Bucket:      sess.Bucket,
		Name:        sess.Name,
		Description: sess.Description,
		ContentType: sess.ContentType,
		Metadata:    sess.Metadata,
		Size:        total,
		ChunkSize:   sess.ChunkSize,
		NumChunks:   sess.NumChunks,
		Digest:      digest,
		Chunks:      infos,
		ModTime:     time.Now().UTC(),
	}, nil
}

// allMetas returns parsed metadata messages in a bucket.
func (os *objStore) allMetas(bucket string) []*objMeta {
	msgs, err := os.enumerate(objStreamName(bucket), fmt.Sprintf(objMetaFilterT, bucket))
	if err != nil {
		return nil
	}
	metas := make([]*objMeta, 0, len(msgs))
	for _, sm := range msgs {
		var m objMeta
		if err := json.Unmarshal(sm.Data, &m); err != nil {
			continue
		}
		metas = append(metas, &m)
	}
	return metas
}

// findMeta returns the metadata message for a specific object id.
func (os *objStore) findMeta(bucket, id string) *objMeta {
	sm, err := os.getLastMsg(objStreamName(bucket), objMetaSubject(bucket, id))
	if err != nil || sm == nil {
		return nil
	}
	var m objMeta
	if err := json.Unmarshal(sm.Data, &m); err != nil {
		return nil
	}
	return &m
}

// findMetaByName returns the newest committed metadata for an object name.
func (os *objStore) findMetaByName(bucket, name string) *objMeta {
	var found *objMeta
	for _, m := range os.allMetas(bucket) {
		if m.Name != name {
			continue
		}
		if found == nil || m.ModTime.After(found.ModTime) {
			found = m
		}
	}
	return found
}

// removeObjectData purges metadata, chunks and the descriptor of an object.
func (os *objStore) removeObjectData(m *objMeta) {
	stream := objStreamName(m.Bucket)
	_, _ = os.purgeSubject(stream, objMetaSubject(m.Bucket, m.ID))
	_, _ = os.purgeSubject(stream, objChunkFilter(m.Bucket, m.ID))
	_, _ = os.purgeSubject(stream, objSessionSubject(m.Bucket, m.ID))
}

// -------------------------------------------------------------------
// Abort
// -------------------------------------------------------------------

func (os *objStore) uploadAbortRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req struct {
		Bucket string `json:"bucket"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objDeleteResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	sess, err := os.loadSession(req.Bucket, req.ID)
	if err != nil {
		os.respond(reply, objDeleteResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	stream := objStreamName(sess.Bucket)
	_, _ = os.purgeSubject(stream, objChunkFilter(sess.Bucket, sess.ID))
	_, _ = os.purgeSubject(stream, objSessionSubject(sess.Bucket, sess.ID))
	os.publishEvent(sess.Bucket, sess.Name, sess.ID, objEventAborted)
	os.respond(reply, objDeleteResponse{objResponse: newObjResponse(), Success: true})
}

// -------------------------------------------------------------------
// Background sweeper
// -------------------------------------------------------------------

func (os *objStore) sweep() {
	os.mu.Lock()
	interval := os.sweepInterval
	closed := os.closed
	os.mu.Unlock()
	defer func() {
		os.mu.Lock()
		if !os.closed {
			os.sweepTimer = time.AfterFunc(interval, os.sweep)
		}
		os.mu.Unlock()
	}()
	if closed {
		return
	}

	names := os.allBucketStreams()
	for _, stream := range names {
		bucket := strings.TrimPrefix(stream, objStreamPrefix)
		descs, err := os.latestSessionDescriptors(bucket)
		if err != nil {
			os.srv.Debugf("Object Store: sweep enumerate %q failed: %v", stream, err)
			continue
		}
		for _, sess := range descs {
			if sess.State != objSessionOpen {
				continue
			}
			if sess.Deadline.IsZero() || time.Now().Before(sess.Deadline) {
				continue
			}
			n, err := os.purgeSubject(stream, objChunkFilter(bucket, sess.ID))
			if err == nil {
				_, err = os.purgeSubject(stream, objSessionSubject(bucket, sess.ID))
			}
			if err != nil {
				os.srv.Debugf("Object Store: sweep purge session %s failed: %v", sess.ID, err)
			} else {
				os.srv.Debugf("Object Store: swept expired session %s (%d chunks)", sess.ID, n)
			}
		}
	}
}

// allBucketStreams lists backing streams (OBJ_ prefix) in the account.
func (os *objStore) allBucketStreams() []string {
	out := make([]string, 0)
	var offset int
	for {
		var resp JSApiStreamNamesResponse
		err := os.request(JSApiStreams, &JSApiStreamNamesRequest{
			ApiPagedRequest: ApiPagedRequest{Offset: offset},
		}, &resp)
		if err != nil {
			return out
		}
		if resp.Error != nil {
			return out
		}
		for _, n := range resp.Streams {
			if strings.HasPrefix(n, objStreamPrefix) {
				out = append(out, n)
			}
		}
		offset += len(resp.Streams)
		if resp.Total <= offset || len(resp.Streams) == 0 {
			break
		}
	}
	return out
}

// -------------------------------------------------------------------
// Object get / range reads / list / delete
// -------------------------------------------------------------------

func (os *objStore) objectGetRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req struct {
		Bucket string `json:"bucket"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	meta := os.findMetaByName(req.Bucket, req.Name)
	if meta == nil {
		os.respond(reply, objGetResponse{objResponse: objResponseWithError(NewJSNoMessageFoundError())})
		return
	}
	os.respond(reply, objGetResponse{objResponse: newObjResponse(), Info: meta.info()})
}

func (os *objStore) objectGetChunkRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req objGetChunkRequest
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}

	var meta *objMeta
	switch {
	case req.ID != _EMPTY_:
		meta = os.findMeta(req.Bucket, req.ID)
	case req.Name != _EMPTY_:
		meta = os.findMetaByName(req.Bucket, req.Name)
	default:
		os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	if meta == nil {
		os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSNoMessageFoundError())})
		return
	}

	// Chunk-number range.
	if req.First != nil || req.Last != nil {
		first, last := int32(0), int32(meta.NumChunks) - 1
		if req.First != nil {
			first = *req.First
		}
		if req.Last != nil {
			last = *req.Last
		}
		if first < 0 || last > int32(meta.NumChunks)-1 || first > last {
			os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
			return
		}
		var buf bytes.Buffer
		stream := objStreamName(meta.Bucket)
		for n := uint32(first); n <= uint32(last); n++ {
			sm, err := os.getLastMsg(stream, objChunkSubject(meta.Bucket, meta.ID, n))
			if err != nil || sm == nil {
				os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSNoMessageFoundError())})
				return
			}
			buf.Write(sm.Data)
		}
		data := buf.Bytes()
		os.respond(reply, objGetChunkResponse{
			objResponse: newObjResponse(),
			Num:         uint32(first),
			Size:        uint32(len(data)),
			Data:        data,
		})
		return
	}

	// Byte range across chunks.
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	if uint64(offset) >= meta.Size && meta.Size > 0 {
		os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	length := req.Length
	if length < 0 || uint64(offset+length) > meta.Size {
		length = int64(meta.Size) - offset
	}

	stream := objStreamName(meta.Bucket)
	var buf bytes.Buffer
	startChunk := uint64(offset) / uint64(meta.ChunkSize)
	// Running position across the object. All chunks except possibly the last
	// are fixed size, so position starts at startChunk * chunk size.
	pos := int64(startChunk) * int64(meta.ChunkSize)
	for n := uint32(startChunk); uint64(n) < uint64(meta.NumChunks); n++ {
		sm, err := os.getLastMsg(stream, objChunkSubject(meta.Bucket, meta.ID, n))
		if err != nil || sm == nil {
			os.respond(reply, objGetChunkResponse{objResponse: objResponseWithError(NewJSNoMessageFoundError())})
			return
		}
		chunkStart := pos
		chunkEnd := pos + int64(len(sm.Data))
		// Slice intersection with requested [offset, offset+length).
		lo := int64(0)
		if offset > chunkStart {
			lo = offset - chunkStart
		}
		hi := int64(len(sm.Data))
		if chunkEnd > offset+length {
			hi = offset + length - chunkStart
		}
		if lo < hi && lo < int64(len(sm.Data)) {
			buf.Write(sm.Data[lo:hi])
		}
		pos = chunkEnd
		if pos >= offset+length {
			break
		}
	}
	data := buf.Bytes()
	os.respond(reply, objGetChunkResponse{
		objResponse: newObjResponse(),
		Size:        uint32(len(data)),
		Data:        data,
	})
}

func (os *objStore) objectListRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req struct {
		Bucket string `json:"bucket"`
	}
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objListResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	metas := os.allMetas(req.Bucket)
	objects := make([]objInfo, 0, len(metas))
	for _, m := range metas {
		objects = append(objects, *m.info())
	}
	os.respond(reply, objListResponse{objResponse: newObjResponse(), Objects: objects})
}

func (os *objStore) objectDeleteRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	var req struct {
		Bucket string `json:"bucket"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal(rmsg, &req); err != nil {
		os.respond(reply, objDeleteResponse{objResponse: objResponseWithError(NewJSInvalidJSONError(err))})
		return
	}
	meta := os.findMetaByName(req.Bucket, req.Name)
	if meta == nil {
		os.respond(reply, objDeleteResponse{objResponse: objResponseWithError(NewJSNoMessageFoundError())})
		return
	}
	os.removeObjectData(meta)
	os.publishEvent(meta.Bucket, meta.Name, meta.ID, objEventDeleted)
	os.respond(reply, objDeleteResponse{objResponse: newObjResponse(), Success: true})
}

// -------------------------------------------------------------------
// One-shot put (server side single write)
// -------------------------------------------------------------------

func (os *objStore) putRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if c == nil {
		return
	}
	hdr, data := c.msgParts(rmsg)
	// Account internal subscription delivery retains the trailing CRLF.
	if len(data) >= LEN_CR_LF {
		data = data[:len(data)-LEN_CR_LF]
	}
	bucket := objHeaderValue(hdr, objHdrBucket)
	name := objHeaderValue(hdr, objHdrName)
	if bucket == _EMPTY_ || name == _EMPTY_ {
		os.respond(reply, objPutResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(errObjBadRequest)))})
		return
	}
	chunkSizeStr := objHeaderValue(hdr, objHdrChunkSize)
	chunkSize := uint32(objDefaultChunkSize)
	if chunkSizeStr != _EMPTY_ {
		if v, err := strconv.ParseUint(chunkSizeStr, 10, 32); err == nil && v > 0 {
			chunkSize = uint32(v)
		}
	}
	numChunks := uint32((len(data) + int(chunkSize) - 1) / int(chunkSize))
	if numChunks == 0 {
		numChunks = 1
	}

	os.createMu.Lock()
	defer os.createMu.Unlock()

	if _, err := os.ensureBucket(&objBucketInfo{Bucket: bucket, Replicas: 1, Storage: FileStorage}); err != nil {
		os.respond(reply, objPutResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	// A one-shot put replaces any active session: abort an in-progress upload
	// of the same name first.
	if sess, err := os.findActiveSession(bucket, name); err == nil && sess != nil {
		stream := objStreamName(bucket)
		_, _ = os.purgeSubject(stream, objChunkFilter(bucket, sess.ID))
		_, _ = os.purgeSubject(stream, objSessionSubject(bucket, sess.ID))
	}

	now := time.Now().UTC()
	sess := &objSession{
		ID:          nuid.Next(),
		Bucket:      bucket,
		Name:        name,
		Description: objHeaderValue(hdr, objHdrDesc),
		ContentType: objHeaderValue(hdr, objHdrCType),
		ChunkSize:   chunkSize,
		NumChunks:   numChunks,
		TotalSize:   uint64(len(data)),
		Created:     now,
		Deadline:    now.Add(objDefaultSessionTTL),
		State:       objSessionOpen,
	}
	sb, _ := json.Marshal(sess)
	if _, err := os.publishExpectAck(objSessionSubject(bucket, sess.ID), nil, sb); err != nil {
		os.respond(reply, objPutResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	chunks := make([]*StoredMsg, 0, numChunks)
	for n := uint32(0); n < numChunks; n++ {
		lo := n * chunkSize
		hi := lo + chunkSize
		if int(hi) > len(data) {
			hi = uint32(len(data))
		}
		piece := data[lo:hi]
		digest := objShaDigest(piece)
		chdr := bytes.NewBuffer(nil)
		chdr.WriteString("NATS/1.0\r\n")
		chdr.WriteString(objHdrDigest + ": " + digest + "\r\n\r\n")
		ack, err := os.publishExpectAck(objChunkSubject(bucket, sess.ID, n), chdr.Bytes(), piece)
		if err != nil {
			os.respond(reply, objPutResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
			return
		}
		chunks = append(chunks, &StoredMsg{Subject: objChunkSubject(bucket, sess.ID, n), Sequence: ack.Sequence, Data: piece, Header: chdr.Bytes()})
	}

	meta, err := os.buildMeta(sess, chunks)
	if err != nil {
		os.respond(reply, objPutResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}

	// Replace prior committed version.
	var oldMeta *objMeta
	for _, m := range os.allMetas(bucket) {
		if m.Name == name && m.ID != sess.ID {
			oldMeta = m
		}
	}

	mb, _ := json.Marshal(meta)
	if _, err := os.publishExpectAck(objMetaSubject(bucket, sess.ID), nil, mb); err != nil {
		os.respond(reply, objPutResponse{objResponse: objResponseWithError(NewJSBadRequestError(Unless(err)))})
		return
	}
	sess.State = objSessionCommitted
	sb2, _ := json.Marshal(sess)
	_, _ = os.publishExpectAck(objSessionSubject(bucket, sess.ID), nil, sb2)
	if oldMeta != nil {
		os.removeObjectData(oldMeta)
	}
	os.publishEvent(bucket, name, sess.ID, objEventCreated)
	os.respond(reply, objPutResponse{objResponse: newObjResponse(), Info: meta.info()})
}
