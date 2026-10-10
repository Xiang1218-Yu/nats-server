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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	natsc "github.com/nats-io/nats.go"
)

// ---- store level helpers ----

type rsTestStore struct {
	name    string
	st      StreamStore
	cleanup func()
}

func newRSStore(t *testing.T, name string, cfg StreamConfig, fcfg *FileStoreConfig) StreamStore {
	t.Helper()
	switch cfg.Storage {
	case MemoryStorage:
		ms, err := newMemStore(&cfg)
		require_NoError(t, err)
		t.Cleanup(func() { ms.Stop() })
		return ms
	case FileStorage:
		dir := t.TempDir()
		fsCfg := FileStoreConfig{StoreDir: dir}
		if fcfg != nil {
			fsCfg = *fcfg
			fsCfg.StoreDir = dir
		}
		fs, err := newFileStore(fsCfg, cfg)
		require_NoError(t, err)
		t.Cleanup(func() { fs.Stop() })
		return fs
	default:
		t.Fatalf("unknown storage type")
		return nil
	}
}

func rsPublish(t *testing.T, st StreamStore, subj string, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		_, _, err := st.StoreMsg(subj, nil, []byte(fmt.Sprintf("m%d", i)), 0)
		require_NoError(t, err)
	}
}

func rsReadAll(t *testing.T, snap *ReadSnapshot, batch int) []uint64 {
	t.Helper()
	var (
		out    []uint64
		cursor uint64
		reads  int
	)
	for {
		msgs, next, remaining, complete, err := snap.ReadBatch(cursor, batch, 0)
		require_NoError(t, err)
		if reads == 0 {
			// First call uses the implicit server cursor; afterwards always pass
			// the returned cursor explicitly to exercise resume.
		} else if cursor != 0 && next < cursor {
			t.Fatalf("cursor went backwards: %d -> %d", cursor, next)
		}
		cursor = next
		reads++
		for _, sm := range msgs {
			out = append(out, sm.seq)
		}
		if complete {
			if remaining != 0 {
				t.Fatalf("expected zero remaining on complete, got %d", remaining)
			}
			return out
		}
		if len(msgs) == 0 {
			t.Fatalf("no progress before completion at cursor %d", cursor)
		}
		if reads > 100000 {
			t.Fatalf("read loop did not terminate")
		}
	}
}

func rsSnapCfg(filter string) ReadSnapshotConfig {
	return ReadSnapshotConfig{
		Filter:   filter,
		TTL:      time.Minute,
		MaxBytes: 1 << 30,
		MaxSnaps: 10,
	}
}

func rsInvalidReason(t *testing.T, err error) string {
	t.Helper()
	var ie *ReadSnapshotInvalidError
	if !errors.As(err, &ie) {
		t.Fatalf("expected ReadSnapshotInvalidError, got %v", err)
	}
	return ie.Reason
}

// ---- store level tests, parameterized across mem and file stores ----

func TestReadSnapshotStableView(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage, Subjects: []string{"foo.*", "bar.*"}}
			st := newRSStore(t, storage.String(), cfg, nil)

			rsPublish(t, st, "foo.a", 1, 5)
			rsPublish(t, st, "bar.b", 1, 3) // seqs 6..8

			// Delete one before snapshot creation; it must not be in the view.
			_, err := st.RemoveMsg(2)
			require_NoError(t, err)

			snap, err := st.CreateReadSnapshot(rsSnapCfg("foo.*"))
			require_NoError(t, err)
			state := snap.State()
			if state.StartSeq != 1 || state.EndSeq != 8 || state.Cursor != 1 {
				t.Fatalf("unexpected initial state: %+v", state)
			}
			if state.Status != ReadSnapshotActive {
				t.Fatalf("unexpected status %q", state.Status)
			}

			// Published after the fence must stay invisible.
			rsPublish(t, st, "foo.a", 9, 10) // seqs 9,10

			got := rsReadAll(t, snap, 2)
			expect := []uint64{1, 3, 4, 5}
			if len(got) != len(expect) {
				t.Fatalf("expected %v, got %v", expect, got)
			}
			for i := range expect {
				if got[i] != expect[i] {
					t.Fatalf("expected %v, got %v", expect, got)
				}
			}

			state = snap.State()
			if state.Status != ReadSnapshotComplete || state.Cursor != 9 {
				t.Fatalf("unexpected final state: %+v", state)
			}
		})
	}
}

func TestReadSnapshotDeletesRetained(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 8)

			snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
			require_NoError(t, err)

			// Remove and erase messages that the cursor has not reached yet.
			_, err = st.RemoveMsg(3)
			require_NoError(t, err)
			_, err = st.EraseMsg(4)
			require_NoError(t, err)

			got := rsReadAll(t, snap, 3)
			if len(got) != 8 {
				t.Fatalf("expected all 8 messages retained, got %v", got)
			}
			for i, seq := range got {
				if seq != uint64(i+1) {
					t.Fatalf("gap/dup in retained read: %v", got)
				}
			}
		})
	}
}

func TestReadSnapshotExpiredMessagesRetained(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{
				Name:        "RS",
				Storage:     storage,
				AllowMsgTTL: true,
				Subjects:    []string{"foo"},
				Discard:     DiscardOld,
			}
			st := newRSStore(t, storage.String(), cfg, nil)

			// Long lived message. ttl argument is in seconds.
			_, _, err := st.StoreMsg("foo", nil, []byte("keep"), 3600)
			require_NoError(t, err)
			// Short lived message (1 second).
			_, _, err = st.StoreMsg("foo", nil, []byte("expire"), 1)
			require_NoError(t, err)

			snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
			require_NoError(t, err)

			checkFor(t, 3*time.Second, 50*time.Millisecond, func() error {
				if sm, lerr := st.LoadMsg(2, &StoreMsg{}); lerr == nil && sm != nil {
					return fmt.Errorf("seq 2 not expired yet")
				}
				return nil
			})

			got := rsReadAll(t, snap, 10)
			if len(got) != 2 {
				t.Fatalf("expected expired msg retained, got %v", got)
			}
		})
	}
}

func TestReadSnapshotMaxAgeRetention(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage, MaxAge: 250 * time.Millisecond}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 3)

			snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
			require_NoError(t, err)

			checkFor(t, 3*time.Second, 100*time.Millisecond, func() error {
				state := st.State()
				if state.Msgs != 0 {
					return fmt.Errorf("still %d msgs", state.Msgs)
				}
				return nil
			})

			got := rsReadAll(t, snap, 10)
			if len(got) != 3 {
				t.Fatalf("expected MaxAge expired msgs retained, got %v", got)
			}
		})
	}
}

func TestReadSnapshotSpaceLimit(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			// Two messages around 10 bytes each with overhead.
			rsPublish(t, st, "foo", 1, 2)

			snapCfg := rsSnapCfg(">")
			// Each message is ~21 bytes (subject+payload+overhead), so one
			// retained message fits but two do not.
			snapCfg.MaxBytes = 25
			snap, err := st.CreateReadSnapshot(snapCfg)
			require_NoError(t, err)

			// First deletion retained.
			removed, err := st.RemoveMsg(1)
			if err != nil || !removed {
				t.Fatalf("remove failed: %v %v", removed, err)
			}
			// Second deletion must still succeed and invalidate the snapshot.
			removed, err = st.RemoveMsg(2)
			if err != nil || !removed {
				t.Fatalf("remove blocked by snapshot: %v %v", removed, err)
			}

			_, _, _, _, rerr := snap.ReadBatch(0, 10, 0)
			if rerr == nil {
				t.Fatalf("expected invalidation error")
			}
			if reason := rsInvalidReason(t, rerr); reason != ReadSnapshotReasonSpaceLimit {
				t.Fatalf("expected space_limit, got %q", reason)
			}
			st2 := snap.State()
			if st2.RetainedBytes != 0 || st2.RetainedMsgs != 0 {
				t.Fatalf("retained area not released: %+v", st2)
			}
		})
	}
}

func TestReadSnapshotTTLExpiryAndTombstone(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 2)

			scfg := rsSnapCfg(">")
			scfg.TTL = 200 * time.Millisecond
			snap, err := st.CreateReadSnapshot(scfg)
			require_NoError(t, err)
			id := snap.ID()

			checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
				_, _, _, _, rerr := snap.ReadBatch(0, 10, 0)
				if rerr == nil {
					return fmt.Errorf("still active")
				}
				return nil
			})

			_, _, _, _, rerr := snap.ReadBatch(0, 10, 0)
			if reason := rsInvalidReason(t, rerr); reason != ReadSnapshotReasonExpired {
				t.Fatalf("expected expired, got %q", reason)
			}
			// Still queryable as a tombstone.
			if got := st.LookupReadSnapshot(id); got == nil {
				t.Fatalf("tombstone should still be queryable")
			} else if got.State().Reason != ReadSnapshotReasonExpired {
				t.Fatalf("bad tombstone reason: %s", got.State().Reason)
			}

			// After the tombstone grace it is gone.
			checkFor(t, 3*time.Second, 20*time.Millisecond, func() error {
				if st.LookupReadSnapshot(id) != nil {
					return fmt.Errorf("tombstone not reaped")
				}
				return nil
			})
		})
	}
}

func TestReadSnapshotRelease(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 2)

			snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
			require_NoError(t, err)
			id := snap.ID()

			if !st.ReleaseReadSnapshot(id) {
				t.Fatalf("release failed")
			}
			if st.LookupReadSnapshot(id) != nil {
				t.Fatalf("snapshot still present after release")
			}
			// Idempotent.
			if st.ReleaseReadSnapshot(id) {
				t.Fatalf("second release should report false")
			}
		})
	}
}

func TestReadSnapshotMaxSnapshots(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 2)

			scfg := rsSnapCfg(">")
			scfg.MaxSnaps = 2
			s1, err := st.CreateReadSnapshot(scfg)
			require_NoError(t, err)
			s2, err := st.CreateReadSnapshot(scfg)
			require_NoError(t, err)
			_, err = st.CreateReadSnapshot(scfg)
			if !errors.Is(err, ErrReadSnapshotLimit) {
				t.Fatalf("expected limit error, got %v", err)
			}
			st.ReleaseReadSnapshot(s1.ID())
			if _, err = st.CreateReadSnapshot(scfg); err != nil {
				t.Fatalf("expected create after release, got %v", err)
			}
			st.ReleaseReadSnapshot(s2.ID())
		})
	}
}

func TestReadSnapshotBadRange(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 3)

			good := rsSnapCfg(">")
			for _, bad := range []ReadSnapshotConfig{
				{TTL: time.Minute, StartSeq: 2, EndSeq: 1, MaxSnaps: 10},
				{TTL: time.Minute, StartSeq: 100, MaxSnaps: 10},
				{TTL: time.Minute, EndSeq: 100, MaxSnaps: 10},
				{TTL: time.Minute, Filter: "foo..bar", MaxSnaps: 10},
				{MaxSnaps: 10}, // no TTL
			} {
				if _, err := st.CreateReadSnapshot(bad); !errors.Is(err, ErrReadSnapshotBadRange) {
					t.Fatalf("expected bad range for %+v, got %v", bad, err)
				}
			}
			if _, err := st.CreateReadSnapshot(good); err != nil {
				t.Fatalf("valid create failed: %v", err)
			}
		})
	}
}

func TestReadSnapshotStructuralInvalidations(t *testing.T) {
	cases := []struct {
		reason string
		apply  func(st StreamStore)
	}{
		{ReadSnapshotReasonPurged, func(st StreamStore) { _, _ = st.Purge() }},
		{ReadSnapshotReasonCompacted, func(st StreamStore) { _, _ = st.Compact(4) }},
		{ReadSnapshotReasonTruncated, func(st StreamStore) { _ = st.Truncate(3) }},
	}
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		for _, tc := range cases {
			name := storage.String() + "-" + tc.reason
			t.Run(name, func(t *testing.T) {
				cfg := StreamConfig{Name: "RS", Storage: storage}
				st := newRSStore(t, name, cfg, nil)
				rsPublish(t, st, "foo", 1, 6)

				snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
				require_NoError(t, err)

				tc.apply(st)

				_, _, _, _, rerr := snap.ReadBatch(0, 10, 0)
				if rerr == nil {
					t.Fatalf("expected invalidation on %s", tc.reason)
				}
				if reason := rsInvalidReason(t, rerr); reason != tc.reason {
					t.Fatalf("expected %s, got %s", tc.reason, reason)
				}
				if snap.State().Reason != tc.reason {
					t.Fatalf("state reason mismatch: %s", snap.State().Reason)
				}
			})
		}
	}
}

func TestReadSnapshotCursorMismatch(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			rsPublish(t, st, "foo", 1, 4)
			snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
			require_NoError(t, err)

			// Advance the cursor.
			_, next, _, _, err := snap.ReadBatch(0, 2, 0)
			require_NoError(t, err)
			if next != 3 {
				t.Fatalf("expected cursor 3, got %d", next)
			}
			// Replaying an older cursor is rejected.
			_, _, _, _, err = snap.ReadBatch(1, 2, 0)
			if !errors.Is(err, ErrReadSnapshotCursor) {
				t.Fatalf("expected cursor error, got %v", err)
			}
			// Resuming at the server cursor continues cleanly.
			got := rsReadAll(t, snap, 2)
			if len(got) != 2 || got[0] != 3 {
				t.Fatalf("unexpected remainder: %v", got)
			}
		})
	}
}

func TestReadSnapshotFilteredPurgeExMem(t *testing.T) {
	// memStore PurgeEx removes per message, so unrelated view data survives and
	// purged matching messages are retained by active snapshots.
	cfg := StreamConfig{Name: "RS", Storage: MemoryStorage}
	st := newRSStore(t, "mem", cfg, nil)
	rsPublish(t, st, "keep.1", 1, 3) // seqs 1..3
	rsPublish(t, st, "gone.1", 1, 3) // seqs 4..6

	snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
	require_NoError(t, err)

	purged, err := st.PurgeEx("gone.1", 0, 0)
	require_NoError(t, err)
	if purged != 3 {
		t.Fatalf("expected 3 purged, got %d", purged)
	}

	got := rsReadAll(t, snap, 10)
	if len(got) != 6 {
		t.Fatalf("expected full view retained across PurgeEx, got %v", got)
	}
}

// Whole block limits eviction in the file store invalidates snapshots rather
// than pinning blocks; the memory store evicts per message and retains them.
func TestReadSnapshotLimitsEviction(t *testing.T) {
	t.Run("mem-per-message", func(t *testing.T) {
		cfg := StreamConfig{Name: "RS", Storage: MemoryStorage, MaxMsgs: 6, Discard: DiscardOld}
		st := newRSStore(t, "mem", cfg, nil)
		rsPublish(t, st, "foo", 1, 6)
		snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
		require_NoError(t, err)

		rsPublish(t, st, "foo", 7, 30) // evicts 1..24
		got := rsReadAll(t, snap, 10)
		if len(got) != 6 {
			t.Fatalf("expected fenced view 1..6 retained, got %v", got)
		}
	})

	t.Run("file-whole-block", func(t *testing.T) {
		// Shrinking MaxMsgs via a config update makes limits enforcement evict
		// whole first blocks (purgeMsgBlock) when the excess exceeds a block.
		cfg := StreamConfig{Name: "RS", Storage: FileStorage, MaxMsgs: 1000, Discard: DiscardOld}
		fcfg := &FileStoreConfig{BlockSize: 512}
		st := newRSStore(t, "file", cfg, fcfg).(*fileStore)

		// Fill multiple small blocks.
		rsPublish(t, st, "foo", 1, 30)
		if len(st.blks) < 2 {
			t.Fatalf("test setup: expected multiple blocks, got %d", len(st.blks))
		}
		snap, err := st.CreateReadSnapshot(rsSnapCfg(">"))
		require_NoError(t, err)

		shrunk := cfg
		shrunk.MaxMsgs = 1
		require_NoError(t, st.UpdateConfig(&shrunk))

		_, _, _, _, rerr := snap.ReadBatch(0, 10, 0)
		if rerr == nil {
			t.Fatalf("expected snapshot invalidation under whole block eviction")
		}
		if reason := rsInvalidReason(t, rerr); reason != ReadSnapshotReasonPurged {
			t.Fatalf("expected purged, got %q", reason)
		}
	})
}

func TestReadSnapshotConcurrentReadDelete(t *testing.T) {
	for _, storage := range []StorageType{MemoryStorage, FileStorage} {
		t.Run(storage.String(), func(t *testing.T) {
			cfg := StreamConfig{Name: "RS", Storage: storage}
			st := newRSStore(t, storage.String(), cfg, nil)
			const n = 200
			rsPublish(t, st, "foo", 1, n)

			snapCfg := rsSnapCfg(">")
			snapCfg.MaxBytes = 1 << 30
			snap, err := st.CreateReadSnapshot(snapCfg)
			require_NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(2)

			// Deleter removes the upper half concurrently.
			go func() {
				defer wg.Done()
				for seq := uint64(n/2 + 1); seq <= n; seq++ {
					_, _ = st.RemoveMsg(seq)
				}
			}()

			// Reader drains the snapshot.
			var mu sync.Mutex
			seen := make(map[uint64]bool)
			go func() {
				defer wg.Done()
				var cursor uint64
				for i := 0; i < 10000; i++ {
					msgs, next, _, complete, err := snap.ReadBatch(cursor, 7, 0)
					if err != nil {
						// Invalidation is an acceptable terminal outcome, but with
						// per message deletes the view should remain intact.
						return
					}
					mu.Lock()
					for _, sm := range msgs {
						if seen[sm.seq] {
							t.Errorf("dup seq %d", sm.seq)
						}
						seen[sm.seq] = true
					}
					mu.Unlock()
					cursor = next
					if complete {
						return
					}
				}
			}()

			wg.Wait()
			if len(seen) != n {
				t.Fatalf("expected %d unique messages, got %d", n, len(seen))
			}
		})
	}
}

// ---- API level tests ----

type jsAPIError struct {
	Code        int    `json:"code"`
	ErrCode     int    `json:"err_code"`
	Description string `json:"description"`
}

func (e *jsAPIError) Error() string { return e.Description }

type rsAPISnapshot struct {
	SnapshotID    string    `json:"snapshot_id"`
	Status        string    `json:"status"`
	Reason        string    `json:"reason"`
	Filter        string    `json:"filter"`
	StartSeq      uint64    `json:"start_seq"`
	EndSeq        uint64    `json:"end_seq"`
	Cursor        uint64    `json:"cursor"`
	RemainingMsgs uint64    `json:"remaining_messages"`
	RetainedBytes int64     `json:"retained_bytes"`
	Created       time.Time `json:"created"`
	Expires       time.Time `json:"expires"`
}

type rsStoredMsg struct {
	Subject  string    `json:"subject"`
	Sequence uint64    `json:"seq"`
	Data     []byte    `json:"data"`
	Time     time.Time `json:"time"`
}

type rsAPIEnvelope struct {
	Error *jsAPIError `json:"error,omitempty"`
	Type  string      `json:"type"`
}

func jsAPIRequest(t *testing.T, nc *natsc.Conn, subj string, body any, out any) *jsAPIError {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		require_NoError(t, err)
	}
	resp, err := nc.Request(subj, payload, 5*time.Second)
	if err != nil {
		t.Fatalf("request %s failed: %v", subj, err)
	}
	if err := json.Unmarshal(resp.Data, out); err != nil {
		t.Fatalf("bad response %q: %v", resp.Data, err)
	}
	// Reflect the embedded error field out via a second decode when present.
	var probe struct {
		Error *jsAPIError `json:"error,omitempty"`
	}
	_ = json.Unmarshal(resp.Data, &probe)
	return probe.Error
}

func rsSetupAPITest(t *testing.T, opts *Options) (*Server, *natsc.Conn) {
	t.Helper()
	if opts == nil {
		opts = &DefaultTestOptions
	}
	opts.Port = -1
	opts.JetStream = true
	if opts.StoreDir == _EMPTY_ {
		opts.StoreDir = t.TempDir()
	}
	s := RunServer(opts)
	t.Cleanup(s.Shutdown)
	nc, err := natsc.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return s, nc
}

func rsCreateAPIStream(t *testing.T, nc *natsc.Conn, name string, storage string, subjects ...string) {
	t.Helper()
	body := map[string]any{"name": name, "storage": storage, "subjects": subjects}
	var env rsAPIEnvelope
	if ae := jsAPIRequest(t, nc, fmt.Sprintf(JSApiStreamCreateT, name), body, &env); ae != nil {
		t.Fatalf("create stream: %v", ae)
	}
}

func TestJetStreamReadSnapshotAPILifecycle(t *testing.T) {
	s, nc := rsSetupAPITest(t, nil)
	_ = s
	rsCreateAPIStream(t, nc, "RS", "memory", "foo.*")

	for i := 1; i <= 6; i++ {
		require_NoError(t, nc.Publish("foo.a", []byte(fmt.Sprintf("m%d", i))))
	}
	// Flush publishes through JS.
	nc.Flush()
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		si, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "RS"), nil, time.Second)
		if err != nil {
			return err
		}
		var probe struct {
			State struct {
				Messages uint64 `json:"messages"`
			} `json:"state"`
		}
		_ = json.Unmarshal(si.Data, &probe)
		if probe.State.Messages != 6 {
			return fmt.Errorf("msgs=%d", probe.State.Messages)
		}
		return nil
	})

	// Create with filter and range.
	createSubj := fmt.Sprintf(JSApiReadSnapshotCreateT, "RS")
	var cresp struct {
		rsAPIEnvelope
		Snapshot *rsAPISnapshot `json:"snapshot"`
	}
	ae := jsAPIRequest(t, nc, createSubj, map[string]any{"filter": "foo.*", "ttl": int64(time.Minute)}, &cresp)
	if ae != nil {
		t.Fatalf("create: %v", ae)
	}
	if cresp.Snapshot == nil || cresp.Snapshot.SnapshotID == _EMPTY_ {
		t.Fatalf("missing snapshot: %+v", cresp)
	}
	id := cresp.Snapshot.SnapshotID
	if cresp.Snapshot.EndSeq != 6 || cresp.Snapshot.Cursor != 1 || cresp.Snapshot.Status != "active" {
		t.Fatalf("bad snapshot state: %+v", cresp.Snapshot)
	}

	// Read in batches of 4.
	readSubj := fmt.Sprintf(JSApiReadSnapshotReadT, "RS")
	var rresp struct {
		rsAPIEnvelope
		Snapshot          *rsAPISnapshot `json:"snapshot"`
		Messages          []rsStoredMsg  `json:"messages"`
		Cursor            uint64         `json:"cursor"`
		RemainingMessages uint64         `json:"remaining_messages"`
		Complete          bool           `json:"complete"`
	}
	ae = jsAPIRequest(t, nc, readSubj, map[string]any{"snapshot_id": id, "batch": 4, "max_bytes": 1 << 20}, &rresp)
	if ae != nil {
		t.Fatalf("read1: %v", ae)
	}
	if len(rresp.Messages) != 4 || rresp.Cursor != 5 || rresp.RemainingMessages != 2 || rresp.Complete {
		t.Fatalf("unexpected first batch: %+v", rresp)
	}

	// Delete an unread message while cursor is at 5; it must stay readable.
	var derr struct {
		Success bool `json:"success"`
	}
	delBody, _ := json.Marshal(map[string]uint64{"seq": 5})
	ae = jsAPIRequest(t, nc, fmt.Sprintf(JSApiMsgDeleteT, "RS"), json.RawMessage(delBody), &derr)
	if ae != nil {
		t.Fatalf("msg delete: %v", ae)
	}

	ae = jsAPIRequest(t, nc, readSubj, map[string]any{"snapshot_id": id, "batch": 10, "cursor": rresp.Cursor}, &rresp)
	if ae != nil {
		t.Fatalf("read2: %v", ae)
	}
	if len(rresp.Messages) != 2 || rresp.Messages[0].Sequence != 5 || !rresp.Complete {
		t.Fatalf("unexpected second batch: %+v", rresp.Messages)
	}

	// Stale cursor -> 409.
	var probe rsAPIEnvelope
	ae = jsAPIRequest(t, nc, readSubj, map[string]any{"snapshot_id": id, "cursor": 1}, &probe)
	if ae == nil || ae.Code != 409 {
		t.Fatalf("expected 409 cursor mismatch, got %v", ae)
	}

	// Unknown snapshot -> 404.
	ae = jsAPIRequest(t, nc, readSubj, map[string]any{"snapshot_id": "nope"}, &probe)
	if ae == nil || ae.Code != 404 {
		t.Fatalf("expected 404, got %v", ae)
	}

	// Bad range -> 400.
	var cresp2 struct {
		rsAPIEnvelope
	}
	ae = jsAPIRequest(t, nc, createSubj, map[string]any{"start_seq": 1000}, &cresp2)
	if ae == nil || ae.Code != 400 {
		t.Fatalf("expected 400, got %v", ae)
	}

	// INFO and LIST.
	infoSubj := fmt.Sprintf(JSApiReadSnapshotInfoT, "RS")
	var iresp struct {
		rsAPIEnvelope
		Snapshot *rsAPISnapshot `json:"snapshot"`
	}
	ae = jsAPIRequest(t, nc, infoSubj, map[string]string{"snapshot_id": id}, &iresp)
	if ae != nil {
		t.Fatalf("info: %v", ae)
	}
	if iresp.Snapshot.Status != "complete" {
		t.Fatalf("expected complete, got %+v", iresp.Snapshot)
	}

	var lresp struct {
		rsAPIEnvelope
		Total     int             `json:"total"`
		Snapshots []rsAPISnapshot `json:"snapshots"`
	}
	ae = jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotListT, "RS"), struct{}{}, &lresp)
	if ae != nil {
		t.Fatalf("list: %v", ae)
	}
	if lresp.Total != 1 || lresp.Snapshots[0].SnapshotID != id {
		t.Fatalf("unexpected list: %+v", lresp)
	}

	// DELETE then 404.
	var dresp struct {
		Success bool `json:"success"`
	}
	ae = jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotDeleteT, "RS"), map[string]string{"snapshot_id": id}, &dresp)
	if ae != nil || !dresp.Success {
		t.Fatalf("delete: %v %+v", ae, dresp)
	}
	ae = jsAPIRequest(t, nc, infoSubj, map[string]string{"snapshot_id": id}, &probe)
	if ae == nil || ae.Code != 404 {
		t.Fatalf("expected 404 after release, got %v", ae)
	}
	// Idempotent delete succeeds.
	ae = jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotDeleteT, "RS"), map[string]string{"snapshot_id": id}, &dresp)
	if ae != nil || !dresp.Success {
		t.Fatalf("idempotent delete: %v", ae)
	}
}

func TestJetStreamReadSnapshotConsumerCreate(t *testing.T) {
	_, nc := rsSetupAPITest(t, nil)
	rsCreateAPIStream(t, nc, "RS", "memory", "foo.*")
	for i := 1; i <= 4; i++ {
		_ = nc.Publish("foo.a", []byte(fmt.Sprintf("m%d", i)))
	}
	nc.Flush()
	time.Sleep(200 * time.Millisecond)

	var cresp struct {
		rsAPIEnvelope
		Snapshot *rsAPISnapshot `json:"snapshot"`
	}
	ae := jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotCreateT, "RS"),
		map[string]any{"ttl": int64(time.Minute)}, &cresp)
	if ae != nil {
		t.Fatalf("create: %v", ae)
	}
	id := cresp.Snapshot.SnapshotID

	// Create a consumer mid scan.
	var consumerEnv rsAPIEnvelope
	ae = jsAPIRequest(t, nc, fmt.Sprintf(JSApiDurableCreateT, "RS", "C"),
		map[string]any{
			"stream_name": "RS",
			"config": map[string]any{
				"durable_name":   "C",
				"ack_policy":     "explicit",
				"deliver_policy": "all",
			},
		}, &consumerEnv)
	if ae != nil {
		t.Fatalf("consumer create: %v", ae)
	}

	var rresp struct {
		rsAPIEnvelope
		Messages []rsStoredMsg `json:"messages"`
		Complete bool          `json:"complete"`
	}
	ae = jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotReadT, "RS"),
		map[string]any{"snapshot_id": id, "batch": 100}, &rresp)
	if ae != nil {
		t.Fatalf("read after consumer create: %v", ae)
	}
	if !rresp.Complete || len(rresp.Messages) != 4 {
		t.Fatalf("unexpected read: %+v", rresp)
	}
}

func TestJetStreamReadSnapshotRestart(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := RunServer(&opts)
	nc, err := natsc.Connect(s.ClientURL())
	require_NoError(t, err)

	rsCreateAPIStream(t, nc, "RS", "file", "foo")
	for i := 1; i <= 3; i++ {
		_ = nc.Publish("foo", []byte(fmt.Sprintf("m%d", i)))
	}
	nc.Flush()
	time.Sleep(200 * time.Millisecond)

	var cresp struct {
		rsAPIEnvelope
		Snapshot *rsAPISnapshot `json:"snapshot"`
	}
	ae := jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotCreateT, "RS"),
		map[string]any{"ttl": int64(time.Minute)}, &cresp)
	if ae != nil {
		t.Fatalf("create: %v", ae)
	}
	id := cresp.Snapshot.SnapshotID

	nc.Close()
	s.Shutdown()

	// Restart against the same store directory.
	s2 := RunServer(&opts)
	defer s2.Shutdown()
	nc2, err := natsc.Connect(s2.ClientURL())
	require_NoError(t, err)
	defer nc2.Close()

	var probe rsAPIEnvelope
	ae = jsAPIRequest(t, nc2, fmt.Sprintf(JSApiReadSnapshotInfoT, "RS"),
		map[string]string{"snapshot_id": id}, &probe)
	if ae == nil || ae.Code != 404 {
		t.Fatalf("expected 404 after restart, got %v", ae)
	}
}

func TestJetStreamReadSnapshotServerLimits(t *testing.T) {
	opts := DefaultTestOptions
	opts.JetStreamLimits.MaxReadSnapshots = 1
	s, nc := rsSetupAPITest(t, &opts)
	_ = s
	rsCreateAPIStream(t, nc, "RS", "memory", "foo")
	_ = nc.Publish("foo", []byte("m1"))
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	mk := func() *jsAPIError {
		var env rsAPIEnvelope
		return jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotCreateT, "RS"),
			map[string]any{"ttl": int64(time.Minute)}, &env)
	}
	if ae := mk(); ae != nil {
		t.Fatalf("first create: %v", ae)
	}
	if ae := mk(); ae == nil || ae.Code != 403 {
		t.Fatalf("expected 403 snapshot limit, got %v", ae)
	}
}

func TestJetStreamReadSnapshotLeadershipLost(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, err := natsc.Connect(c.leader().ClientURL())
	require_NoError(t, err)
	defer nc.Close()

	// R3 file stream.
	body := map[string]any{
		"name":         "RS",
		"subjects":     []string{"foo"},
		"storage":      "file",
		"num_replicas": 3,
	}
	var env rsAPIEnvelope
	if ae := jsAPIRequest(t, nc, fmt.Sprintf(JSApiStreamCreateT, "RS"), body, &env); ae != nil {
		t.Fatalf("stream create: %v", ae)
	}

	for i := 1; i <= 5; i++ {
		ack, perr := nc.Request("foo", []byte(fmt.Sprintf("m%d", i)), 5*time.Second)
		require_NoError(t, perr)
		if len(ack.Data) == 0 || string(ack.Data) == "-ERR" {
			t.Fatalf("bad publish ack: %q", ack.Data)
		}
	}

	var cresp struct {
		rsAPIEnvelope
		Snapshot *rsAPISnapshot `json:"snapshot"`
	}
	if ae := jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotCreateT, "RS"),
		map[string]any{"ttl": int64(time.Minute)}, &cresp); ae != nil {
		t.Fatalf("create: %v", ae)
	}
	id := cresp.Snapshot.SnapshotID

	// Force a change of stream leader.
	var oldLeader, newLeader *Server
	for i := 0; i < 5; i++ {
		oldLeader = c.streamLeader("$G", "RS")
		_, err = nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, "RS"), nil, 5*time.Second)
		require_NoError(t, err)
		c.waitOnStreamLeader("$G", "RS")
		newLeader = c.streamLeader("$G", "RS")
		if newLeader != oldLeader {
			break
		}
	}
	if newLeader == nil || newLeader == oldLeader {
		t.Fatalf("leader did not change")
	}

	// On the old leader the snapshot must be observably invalidated.
	checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
		acc, aerr := oldLeader.LookupAccount("$G")
		if aerr != nil {
			return aerr
		}
		mset, lerr := acc.lookupStream("RS")
		if lerr != nil {
			return lerr
		}
		rs := mset.lookupReadSnapshot(id)
		if rs == nil {
			return fmt.Errorf("snapshot missing on old leader")
		}
		if rs.State().Reason != ReadSnapshotReasonLeadershipLost {
			return fmt.Errorf("reason=%q", rs.State().Reason)
		}
		return nil
	})

	// Through the API the request is answered by the new leader, which never
	// hosted this snapshot: explicit 404.
	var probe rsAPIEnvelope
	ae := jsAPIRequest(t, nc, fmt.Sprintf(JSApiReadSnapshotReadT, "RS"),
		map[string]any{"snapshot_id": id, "batch": 10}, &probe)
	if ae == nil || ae.Code != 404 {
		t.Fatalf("expected 404 on new leader, got %v", ae)
	}
}
