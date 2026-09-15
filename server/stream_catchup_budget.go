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
	"sync"
	"time"
)

// Catch-up synchronization modes.
const (
	catchUpModeRealtime   = "realtime"
	catchUpModeCatchingUp = "catching_up"
	catchUpModePaused     = "paused"
)

// Reasons a catch-up sync can be paused.
const (
	catchUpPauseMaxMsgs     = "max_msgs"
	catchUpPauseMaxBytes    = "max_bytes"
	catchUpPauseMaxDuration = "max_duration"
)

// CatchUpStatus is the observable state of a catch-up budget. It is reported
// per mirror/source as part of StreamSourceInfo. A zero value (Enabled false)
// is reported when the corresponding sync has no budget configured.
type CatchUpStatus struct {
	Enabled         bool          `json:"enabled"`
	Mode            string        `json:"mode"`
	Paused          bool          `json:"paused"`
	PauseReason     string        `json:"pause_reason,omitempty"`
	PauseUntil      time.Time     `json:"pause_until,omitempty"`
	LagThreshold    uint64        `json:"lag_threshold"`
	WindowStart     time.Time     `json:"window_start,omitempty"`
	WindowEnd       time.Time     `json:"window_end,omitempty"`
	MsgsInWindow    int64         `json:"msgs_in_window"`
	BytesInWindow   int64         `json:"bytes_in_window"`
	InFlight        int64         `json:"inflight"`
	CatchingUpSince time.Time     `json:"catching_up_since,omitempty"`
	PausesTotal     uint64        `json:"pauses_total"`
	PausedTotal     time.Duration `json:"paused_total,omitempty"`
}

// catchupGate enforces a CatchUpBudget for a single mirror/source sync. It is
// a pure state machine: it never acquires stream/server/client locks and
// never sends anything itself. The owning message processing loop drives it
// with delivered/admit/applied events, parks flow control replies on its
// advice, and wakes up on the gate's wake channel when a pause expires.
//
// Locking: catchupGate.mu is a leaf lock. Callers must never hold the gate
// lock while acquiring stream/server/client locks (allowed order is
// stream -> catchupGate). The expiry timer only mutates gate state under the
// gate lock and then signals the wake channel.
type catchupGate struct {
	cfg CatchUpBudget

	mu sync.Mutex

	// mode is the current mode of the sync.
	mode string
	// Window accounting, zero valued while in real-time mode.
	winStart time.Time
	winEnd   time.Time
	msgs     int64
	bytes    int64
	// inflight counts catch-up messages delivered from the origin but not
	// yet applied locally.
	inflight int64
	// burst is the start of the current continuous catch-up period.
	burst time.Time

	// Pause state.
	pausedUntil time.Time
	pauseStart  time.Time
	pauseReason string
	pauses      uint64
	pausedTotal time.Duration

	// heldFC is the parked flow control reply subject, empty when none is
	// held. At most one flow control request is outstanding at a time.
	heldFC string

	// Timer firing the transition out of a pause; nil when no pause timer
	// is armed.
	timer *time.Timer

	stopped bool
	wake    chan struct{}
	nowFunc func() time.Time
}

// newCatchUpGate builds a gate from a normalized budget. It returns nil for a
// nil budget, which keeps the legacy sync path untouched.
func newCatchUpGate(cb *CatchUpBudget) *catchupGate {
	if cb == nil {
		return nil
	}
	return &catchupGate{
		cfg:     *cb,
		mode:    catchUpModeRealtime,
		wake:    make(chan struct{}, 1),
		nowFunc: time.Now,
	}
}

// lagFromPending derives the number of messages a delivery is behind the
// origin's stream head from the consumer's pending counter (0 for none).
func lagFromPending(pending uint64) uint64 {
	if pending == 0 {
		return 0
	}
	return pending - 1
}

// logCatchUpPaused emits a log line when a mirror/source catch-up enters a
// budget pause.
func (mset *stream) logCatchUpPaused(kind, name string, st *CatchUpStatus) {
	mset.srv.Warnf("JetStream catch-up for %s %q of stream %q in account %q paused (%s) until %s",
		kind, name, mset.cfg.Name, mset.acc.Name, st.PauseReason, st.PauseUntil.UTC().Format(time.RFC3339Nano))
}

// logCatchUpResumed emits a log line when a mirror/source catch-up leaves a
// pause.
func (mset *stream) logCatchUpResumed(kind, name string) {
	mset.srv.Noticef("JetStream catch-up for %s %q of stream %q in account %q resumed",
		kind, name, mset.cfg.Name, mset.acc.Name)
}

func (g *catchupGate) now() time.Time {
	if g.nowFunc != nil {
		return g.nowFunc()
	}
	return time.Now()
}

// signal wakes the processing loop. Coalesced: the channel is buffered by one
// and sends are non-blocking. Must be called with g.mu held.
func (g *catchupGate) signal() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// wakeCh returns the channel signaled whenever the gate transitions between
// modes or a pause expires.
func (g *catchupGate) wakeCh() <-chan struct{} {
	return g.wake
}

// isCatchup reports whether a delivery with the given lag is catch-up
// traffic according to the configured threshold.
func (g *catchupGate) isCatchup(lag uint64) bool {
	return lag > g.cfg.LagThreshold
}

// enterCatchUpLocked switches from real-time to catch-up mode, opening a
// fresh window and burst. Must be called with g.mu held.
func (g *catchupGate) enterCatchUpLocked(now time.Time) {
	g.mode = catchUpModeCatchingUp
	g.burst = now
	g.winStart = now
	g.winEnd = now.Add(g.cfg.Window)
	g.msgs, g.bytes = 0, 0
}

// toRealtimeLocked clears any throttling state when the backlog drains. A
// parked flow control is released by the processing loop after the wake. Must
// be called with g.mu held.
func (g *catchupGate) toRealtimeLocked(now time.Time) {
	if !g.pauseStart.IsZero() {
		g.pausedTotal += now.Sub(g.pauseStart)
		g.pauseStart = time.Time{}
	}
	g.stopTimerLocked()
	g.mode = catchUpModeRealtime
	g.burst = time.Time{}
	g.winStart, g.winEnd = time.Time{}, time.Time{}
	g.msgs, g.bytes = 0, 0
	g.pausedUntil = time.Time{}
	g.pauseReason = _EMPTY_
	g.signal()
}

// pauseLocked enters paused mode, arming the single expiry timer. Must be
// called with g.mu held.
func (g *catchupGate) pauseLocked(reason string, until time.Time, now time.Time) {
	g.stopTimerLocked()
	g.mode = catchUpModePaused
	g.pauseReason = reason
	g.pausedUntil = until
	g.pauseStart = now
	g.pauses++
	g.timer = time.AfterFunc(until.Sub(now), g.onPauseExpired)
}

// resumeLocked leaves a pause, opening a fresh window and burst. Must be
// called with g.mu held.
func (g *catchupGate) resumeLocked(now time.Time) {
	if !g.pauseStart.IsZero() {
		g.pausedTotal += now.Sub(g.pauseStart)
		g.pauseStart = time.Time{}
	}
	g.stopTimerLocked()
	g.mode = catchUpModeCatchingUp
	g.pauseReason = _EMPTY_
	g.pausedUntil = time.Time{}
	g.burst = now
	g.winStart = now
	g.winEnd = now.Add(g.cfg.Window)
	g.msgs, g.bytes = 0, 0
	g.signal()
}

// stopTimerLocked stops and clears the pause timer. Must be called with g.mu
// held.
func (g *catchupGate) stopTimerLocked() {
	if g.timer != nil {
		g.timer.Stop()
		g.timer = nil
	}
}

// onPauseExpired is the timer callback: it resumes the gate when the pause
// deadline has been reached, re-arming itself on clock skew.
func (g *catchupGate) onPauseExpired() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped || g.mode != catchUpModePaused {
		return
	}
	now := g.now()
	if now.Before(g.pausedUntil) {
		g.timer.Reset(g.pausedUntil.Sub(now))
		return
	}
	g.resumeLocked(now)
}

// delivered records an inbound message at enqueue time. It maintains the mode
// transition and the in-flight counter. The returned value reports whether
// the delivery is catch-up traffic according to its own lag (a static
// property of the delivery, independent of mode transitions).
func (g *catchupGate) delivered(pending uint64) bool {
	lag := lagFromPending(pending)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	now := g.now()
	if !g.isCatchup(lag) {
		if g.mode != catchUpModeRealtime {
			g.toRealtimeLocked(now)
		}
		return false
	}
	g.inflight++
	if g.mode == catchUpModeRealtime {
		g.enterCatchUpLocked(now)
	}
	return true
}

// admit decides whether a queued catch-up message may be applied now. lag is
// the message's own lag and payloadBytes its payload size. A message that is
// no longer catch-up traffic is always admitted (and moves the gate back to
// real-time). On denial the caller parks the remaining batch and waits on
// wakeCh(); the reason describes the active pause.
func (g *catchupGate) admit(lag uint64, payloadBytes int64) (bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return true, _EMPTY_
	}
	now := g.now()

	// Backlog drained (e.g. real-time tail reached while paused): never
	// throttle real-time traffic.
	if !g.isCatchup(lag) {
		if g.mode != catchUpModeRealtime {
			g.toRealtimeLocked(now)
		}
		return true, _EMPTY_
	}

	if g.mode == catchUpModeRealtime {
		g.enterCatchUpLocked(now)
	}

	// An active pause blocks everything until its deadline expires. Serving
	// an expired pause opens a fresh window/burst.
	if g.mode == catchUpModePaused {
		if now.Before(g.pausedUntil) {
			return false, g.pauseReason
		}
		g.resumeLocked(now)
	}

	// Continuous catch-up duration limit.
	if g.mode == catchUpModeCatchingUp && g.cfg.MaxDuration > 0 && now.Sub(g.burst) >= g.cfg.MaxDuration {
		g.pauseLocked(catchUpPauseMaxDuration, now.Add(g.cfg.PauseDuration), now)
		return false, g.pauseReason
	}

	// Roll the fixed window once its deadline passed.
	if !g.winEnd.IsZero() && !now.Before(g.winEnd) {
		g.winStart = now
		g.winEnd = now.Add(g.cfg.Window)
		g.msgs, g.bytes = 0, 0
	}

	// Window quotas.
	if g.cfg.MaxMsgs > 0 && g.msgs+1 > g.cfg.MaxMsgs {
		g.pauseLocked(catchUpPauseMaxMsgs, g.winEnd, now)
		return false, g.pauseReason
	}
	if g.cfg.MaxBytes > 0 && g.bytes+payloadBytes > g.cfg.MaxBytes {
		g.pauseLocked(catchUpPauseMaxBytes, g.winEnd, now)
		return false, g.pauseReason
	}

	g.msgs++
	g.bytes += payloadBytes
	return true, _EMPTY_
}

// applied records that a delivered catch-up message has been processed
// (applied, skipped as duplicate, or dropped), decrementing the in-flight
// counter. catchup is the classification returned by delivered for the same
// message.
func (g *catchupGate) applied(catchup bool) {
	if !catchup {
		return
	}
	g.mu.Lock()
	if g.inflight > 0 {
		g.inflight--
	}
	g.mu.Unlock()
}

// windowExhaustedLocked reports whether the current window quota is fully
// consumed or the burst duration has been reached. Must be called with g.mu
// held.
func (g *catchupGate) windowExhaustedLocked(now time.Time) bool {
	if g.cfg.MaxMsgs > 0 && g.msgs >= g.cfg.MaxMsgs {
		return true
	}
	if g.cfg.MaxBytes > 0 && g.bytes >= g.cfg.MaxBytes {
		return true
	}
	if g.cfg.MaxDuration > 0 && now.Sub(g.burst) >= g.cfg.MaxDuration {
		return true
	}
	return false
}

// tryHoldFC is called when a flow control request reaches the processing
// loop. It returns true when the reply must be withheld (and has been
// parked); false when the request should be answered immediately, preserving
// the legacy behavior.
func (g *catchupGate) tryHoldFC(reply string) bool {
	if reply == _EMPTY_ {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	// Real-time sync is never throttled.
	if g.mode == catchUpModeRealtime {
		return false
	}
	now := g.now()
	// While paused, or once any limit is currently binding, park the FC so
	// the origin stops pushing.
	hold := g.mode == catchUpModePaused || g.windowExhaustedLocked(now)
	if !hold && g.cfg.MaxConcurrent > 0 && g.inflight > int64(g.cfg.MaxConcurrent) {
		hold = true
	}
	if hold && g.heldFC == _EMPTY_ {
		g.heldFC = reply
	}
	return hold
}

// fcHeld reports whether a flow control reply is currently withheld. When
// true, idle heartbeat "consumer stalled" nudges must be ignored so they do
// not resume the origin prematurely.
func (g *catchupGate) fcHeld() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.heldFC != _EMPTY_
}

// takeFCForRelease returns a parked FC reply once releasing it complies with
// the budget: not paused, window/duration not exhausted, and in-flight at or
// below the concurrency limit. Real-time mode always releases.
func (g *catchupGate) takeFCForRelease() (string, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped || g.heldFC == _EMPTY_ {
		return _EMPTY_, false
	}
	if g.mode == catchUpModeRealtime {
		reply := g.heldFC
		g.heldFC = _EMPTY_
		return reply, true
	}
	if g.mode != catchUpModeCatchingUp {
		return _EMPTY_, false
	}
	now := g.now()
	if g.windowExhaustedLocked(now) {
		return _EMPTY_, false
	}
	if g.cfg.MaxConcurrent > 0 && g.inflight > int64(g.cfg.MaxConcurrent) {
		return _EMPTY_, false
	}
	reply := g.heldFC
	g.heldFC = _EMPTY_
	return reply, true
}

// clearFC drops any parked flow control reply (consumer teardown / leader
// loss). The origin's consumer is being recreated and its old FC invalid.
func (g *catchupGate) clearFC() {
	g.mu.Lock()
	g.heldFC = _EMPTY_
	g.mu.Unlock()
}

// stop terminates the gate and its timer. Idempotent.
func (g *catchupGate) stop() {
	g.mu.Lock()
	g.stopped = true
	g.stopTimerLocked()
	g.heldFC = _EMPTY_
	g.signal()
	g.mu.Unlock()
}

// snapshot returns a point-in-time observable copy of the gate state.
func (g *catchupGate) snapshot() *CatchUpStatus {
	g.mu.Lock()
	defer g.mu.Unlock()
	st := &CatchUpStatus{
		Enabled:         true,
		Mode:            g.mode,
		Paused:          g.mode == catchUpModePaused,
		PauseReason:     g.pauseReason,
		PauseUntil:      g.pausedUntil,
		LagThreshold:    g.cfg.LagThreshold,
		WindowStart:     g.winStart,
		WindowEnd:       g.winEnd,
		MsgsInWindow:    g.msgs,
		BytesInWindow:   g.bytes,
		InFlight:        g.inflight,
		CatchingUpSince: g.burst,
		PausesTotal:     g.pauses,
		PausedTotal:     g.pausedTotal,
	}
	return st
}
