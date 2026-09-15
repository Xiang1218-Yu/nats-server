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
	"testing"
	"time"
)

type cuFakeClock struct{ t time.Time }

func newCUFakeClock() *cuFakeClock {
	return &cuFakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *cuFakeClock) now() time.Time          { return c.t }
func (c *cuFakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestCatchUpGate(t *testing.T, cb *CatchUpBudget) (*catchupGate, *cuFakeClock) {
	t.Helper()
	clock := newCUFakeClock()
	g := newCatchUpGate(cb)
	if g == nil {
		t.Fatalf("expected gate for enabled budget")
	}
	g.nowFunc = clock.now
	return g, clock
}

func wakeSignaled(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestCatchUpGateNilBudget(t *testing.T) {
	if g := newCatchUpGate(nil); g != nil {
		t.Fatalf("expected nil gate for nil budget")
	}
	cb, apiErr := (&CatchUpBudget{}).normalize()
	if apiErr != nil || cb != nil {
		t.Fatalf("expected all-zero budget to normalize to nil, got %v err=%v", cb, apiErr)
	}
}

func TestCatchUpGateNormalize(t *testing.T) {
	// Defaults.
	cb, apiErr := (&CatchUpBudget{MaxMsgs: 100}).normalize()
	if apiErr != nil {
		t.Fatalf("unexpected error: %v", apiErr)
	}
	if cb.Window != time.Second || cb.PauseDuration != time.Second || cb.LagThreshold != dgetCaughtUpThresh {
		t.Fatalf("bad defaults: %+v", cb)
	}
	// Explicit values preserved.
	cb, _ = (&CatchUpBudget{
		Window: 2 * time.Second, PauseDuration: 3 * time.Second,
		MaxMsgs: 1, LagThreshold: 5,
	}).normalize()
	if cb.Window != 2*time.Second || cb.PauseDuration != 3*time.Second || cb.LagThreshold != 5 {
		t.Fatalf("values not preserved: %+v", cb)
	}
	// Negative values rejected.
	for _, bad := range []*CatchUpBudget{
		{Window: -1, MaxMsgs: 1},
		{PauseDuration: -1, MaxMsgs: 1},
		{MaxDuration: -1},
		{MaxMsgs: -1},
		{MaxBytes: -1},
		{MaxConcurrent: -1},
	} {
		if _, apiErr := bad.normalize(); apiErr == nil {
			t.Fatalf("expected error for %+v", bad)
		}
	}
}

func TestCatchUpGateRealtimeCatchupTransition(t *testing.T) {
	// Default threshold 10.
	g, clock := newTestCatchUpGate(t, &CatchUpBudget{MaxMsgs: 1000, LagThreshold: 10})
	defer g.stop()

	if st := g.snapshot(); st.Mode != catchUpModeRealtime || st.Enabled != true {
		t.Fatalf("unexpected initial state: %+v", st)
	}

	// pending=11 -> lag=10 -> realtime (lag must be strictly greater).
	if cu := g.delivered(11); cu {
		t.Fatalf("lag=10 should be realtime with threshold 10")
	}
	if st := g.snapshot(); st.Mode != catchUpModeRealtime {
		t.Fatalf("expected realtime, got %+v", st)
	}

	// pending=12 -> lag=11 -> catch-up.
	if cu := g.delivered(12); !cu {
		t.Fatalf("lag=11 should be catch-up with threshold 10")
	}
	st := g.snapshot()
	if st.Mode != catchUpModeCatchingUp || st.InFlight != 1 || st.CatchingUpSince != clock.t {
		t.Fatalf("unexpected catch-up state: %+v", st)
	}

	// A real-time delivery moves back to real-time, signals the wake channel
	// and is not counted inflight.
	if wakeSignaled(g.wakeCh()) {
		t.Fatalf("unexpected wake signal before a pause/resume transition")
	}
	if cu := g.delivered(1); cu {
		t.Fatalf("lag=0 should be real-time")
	}
	if !wakeSignaled(g.wakeCh()) {
		t.Fatalf("expected wake signal on transition back to real-time")
	}
	st = g.snapshot()
	if st.Mode != catchUpModeRealtime || st.InFlight != 1 {
		t.Fatalf("expected realtime with inflight unchanged, got %+v", st)
	}
}

func TestCatchUpGateMaxMsgsWindow(t *testing.T) {
	g, clock := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 5, LagThreshold: 0})
	defer g.stop()

	// Deliver 20 catch-up messages (pending 20..1, lag 19..0 => first 19 are
	// catch-up with threshold 0).
	const totalPending = 20
	for i := 0; i < totalPending; i++ {
		g.delivered(uint64(totalPending - i))
	}
	if st := g.snapshot(); st.InFlight != 19 {
		t.Fatalf("expected 19 inflight catch-up msgs, got %d", st.InFlight)
	}

	// First five admits pass.
	for i := 0; i < 5; i++ {
		lag := lagFromPending(uint64(totalPending - i))
		if ok, reason := g.admit(lag, 1); !ok {
			t.Fatalf("admit %d denied: %s", i, reason)
		}
	}
	st := g.snapshot()
	if st.MsgsInWindow != 5 {
		t.Fatalf("expected 5 msgs in window, got %d", st.MsgsInWindow)
	}

	// Sixth is denied with max_msgs, paused until window end.
	lag := lagFromPending(uint64(totalPending - 5))
	if ok, reason := g.admit(lag, 1); ok || reason != catchUpPauseMaxMsgs {
		t.Fatalf("expected max_msgs denial, got ok=%v reason=%q", ok, reason)
	}
	st = g.snapshot()
	if !st.Paused || st.PauseReason != catchUpPauseMaxMsgs || !st.PauseUntil.Equal(clock.t.Add(time.Second)) {
		t.Fatalf("unexpected paused state: %+v", st)
	}

	// Still denied within the window.
	if ok, _ := g.admit(lag, 1); ok {
		t.Fatalf("admit should remain denied inside window")
	}

	// Applied messages drain inflight even while paused.
	g.applied(true)
	if st := g.snapshot(); st.InFlight != 18 {
		t.Fatalf("expected inflight 18, got %d", st.InFlight)
	}

	// Window recovery: clock advances, next admit succeeds with fresh window.
	clock.advance(time.Second)
	if ok, reason := g.admit(lag, 1); !ok {
		t.Fatalf("expected admit after window recovery, reason=%q", reason)
	}
	st = g.snapshot()
	if st.Paused || st.MsgsInWindow != 1 || st.Mode != catchUpModeCatchingUp {
		t.Fatalf("unexpected recovered state: %+v", st)
	}
	if st.PausesTotal != 1 {
		t.Fatalf("expected 1 recorded pause, got %d", st.PausesTotal)
	}
}

func TestCatchUpGateMaxBytesWindow(t *testing.T) {
	g, clock := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxBytes: 10, LagThreshold: 0})
	defer g.stop()

	g.delivered(5)
	if ok, _ := g.admit(4, 6); !ok {
		t.Fatalf("first admit should pass")
	}
	if ok, _ := g.admit(3, 5); ok {
		t.Fatalf("second admit exceeds byte budget")
	}
	st := g.snapshot()
	if !st.Paused || st.PauseReason != catchUpPauseMaxBytes || st.BytesInWindow != 6 {
		t.Fatalf("unexpected state: %+v", st)
	}
	clock.advance(time.Second)
	if ok, _ := g.admit(3, 5); !ok {
		t.Fatalf("expected admit after window roll")
	}
}

func TestCatchUpGateMaxDuration(t *testing.T) {
	g, clock := newTestCatchUpGate(t, &CatchUpBudget{
		Window:        time.Second,
		MaxMsgs:       1000,
		MaxDuration:   2 * time.Second,
		PauseDuration: time.Second,
		LagThreshold:  0,
	})
	defer g.stop()

	g.delivered(100)
	if ok, _ := g.admit(99, 1); !ok {
		t.Fatalf("first admit should pass")
	}
	clock.advance(2 * time.Second)
	if ok, reason := g.admit(98, 1); ok || reason != catchUpPauseMaxDuration {
		t.Fatalf("expected max_duration denial, got ok=%v reason=%q", ok, reason)
	}
	st := g.snapshot()
	if !st.Paused || !st.PauseUntil.Equal(clock.t.Add(time.Second)) {
		t.Fatalf("unexpected pause: %+v", st)
	}

	// Half the pause: still denied.
	clock.advance(500 * time.Millisecond)
	if ok, _ := g.admit(98, 1); ok {
		t.Fatalf("still within forced pause")
	}
	// Expire the pause via the timer entry point as well.
	clock.advance(500 * time.Millisecond)
	g.onPauseExpired()
	st = g.snapshot()
	if st.Paused || st.Mode != catchUpModeCatchingUp {
		t.Fatalf("expected resumed state, got %+v", st)
	}
	if ok, _ := g.admit(98, 1); !ok {
		t.Fatalf("admit after forced pause should pass")
	}
}

func TestCatchUpGateFlowControlConcurrency(t *testing.T) {
	t.Run("holds until inflight drains", func(t *testing.T) {
		g, _ := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 1000, MaxConcurrent: 10, LagThreshold: 0})
		defer g.stop()

		// 50 catch-up messages in flight, none applied yet.
		for i := 0; i < 50; i++ {
			g.delivered(uint64(100 - i))
		}
		// FC arrives while catching up: must park because inflight > cap.
		if held := g.tryHoldFC("FC.1"); !held {
			t.Fatalf("expected FC to be held")
		}
		if !g.fcHeld() {
			t.Fatalf("expected fcHeld")
		}
		if _, ok := g.takeFCForRelease(); ok {
			t.Fatalf("FC should not release with inflight above cap")
		}
		// Apply 40 messages: inflight 10 == cap -> release allowed.
		for i := 0; i < 40; i++ {
			g.applied(true)
		}
		reply, ok := g.takeFCForRelease()
		if !ok || reply != "FC.1" {
			t.Fatalf("expected FC release at cap, got ok=%v reply=%q", ok, reply)
		}
		if g.fcHeld() {
			t.Fatalf("expected no held FC after release")
		}
	})

	t.Run("immediate when no pressure", func(t *testing.T) {
		g, _ := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 1000, MaxConcurrent: 10, LagThreshold: 0})
		defer g.stop()
		g.delivered(5)
		if g.tryHoldFC("FC.1") {
			t.Fatalf("FC should pass immediately with inflight below cap and room in window")
		}
	})

	t.Run("realtime always immediate", func(t *testing.T) {
		g, _ := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 1, MaxConcurrent: 1, LagThreshold: 0})
		defer g.stop()
		// No catch-up delivery at all: real-time mode.
		if g.tryHoldFC("FC.1") {
			t.Fatalf("FC must be immediate in real-time mode")
		}
	})

	t.Run("held through quota exhaustion and released on resume", func(t *testing.T) {
		g, clock := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 1, LagThreshold: 0})
		defer g.stop()
		g.delivered(10)
		if ok, _ := g.admit(9, 1); !ok {
			t.Fatalf("first admit failed")
		}
		// Window exhausted: FC parked.
		if !g.tryHoldFC("FC.1") {
			t.Fatalf("expected FC parked on exhausted window")
		}
		// Second admit pauses; FC stays parked.
		if ok, reason := g.admit(8, 1); ok || reason != catchUpPauseMaxMsgs {
			t.Fatalf("expected max_msgs pause")
		}
		if _, ok := g.takeFCForRelease(); ok {
			t.Fatalf("FC must stay parked while paused")
		}
		clock.advance(time.Second)
		// The pause timer is the production resume path.
		g.onPauseExpired()
		reply, ok := g.takeFCForRelease()
		if !ok || reply != "FC.1" {
			t.Fatalf("expected parked FC release after resume, ok=%v reply=%q", ok, reply)
		}
	})

	t.Run("realtime transition releases held FC", func(t *testing.T) {
		g, _ := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 1, LagThreshold: 0})
		defer g.stop()
		g.delivered(10)
		g.admit(9, 1)
		if !g.tryHoldFC("FC.1") {
			t.Fatalf("expected FC parked")
		}
		// Backlog drains: real-time tail delivery flips the mode.
		g.delivered(1)
		reply, ok := g.takeFCForRelease()
		if !ok || reply != "FC.1" {
			t.Fatalf("expected immediate FC release in real-time, ok=%v reply=%q", ok, reply)
		}
		if st := g.snapshot(); st.Paused || st.Mode != catchUpModeRealtime {
			t.Fatalf("expected clean real-time state, %+v", st)
		}
	})
}

func TestCatchUpGateStop(t *testing.T) {
	g, clock := newTestCatchUpGate(t, &CatchUpBudget{Window: time.Second, MaxMsgs: 1, LagThreshold: 0})
	g.delivered(10)
	g.admit(9, 1)
	g.tryHoldFC("FC.1")
	g.admit(8, 1) // arms a real timer

	g.stop()
	// Idempotent.
	g.stop()
	st := g.snapshot()
	if !st.Enabled {
		t.Fatalf("snapshot still reports enabled config")
	}
	if g.fcHeld() {
		t.Fatalf("held FC must be cleared on stop")
	}
	// After stop, admits are non-blocking and FC is immediate.
	if ok, _ := g.admit(7, 1); !ok {
		t.Fatalf("admit must pass after stop")
	}
	if g.tryHoldFC("FC.2") {
		t.Fatalf("FC must pass after stop")
	}
	_ = clock
}
