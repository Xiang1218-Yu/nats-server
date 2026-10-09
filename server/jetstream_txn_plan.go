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
	"fmt"
	"sort"
)

// txnResourceView is the environment-specific window over current resource
// state used by the platform-independent transaction engine. The clustered
// meta leader implements it over stream/consumer assignments plus inflight
// proposals; standalone JetStream implements it over its live registries.
type txnResourceView interface {
	// currentStream returns the current stream configuration, its account
	// configuration version and whether it exists.
	currentStream(name string) (cfg *StreamConfig, version uint64, exists bool)

	// currentConsumer returns the current consumer configuration, version
	// and whether it exists.
	currentConsumer(stream, name string) (cfg *ConsumerConfig, version uint64, exists bool)

	// consumersOf lists the names of all durable consumers currently on a
	// stream.
	consumersOf(stream string) []string

	// feasibleStream validates replication factor and storage/placement
	// feasibility for a stream configuration in the post-transaction world.
	feasibleStream(cfg *StreamConfig) *ApiError

	// checkLimits validates account quotas against the simulated
	// post-transaction state. postStreams maps every stream name to its
	// resulting configuration (with absent names meaning "stream gone"), and
	// consumerCounts maps surviving streams to their resulting number of
	// durable consumers.
	checkLimits(postStreams map[string]*StreamConfig, consumerCounts map[string]int) *JSApiTxnConflict

	// accountConfigVersion returns the current account-wide configuration
	// version.
	accountConfigVersion() uint64
}

// txnPlan is the normalized, validated and deterministically ordered result of
// preparing a transaction. The plan is what gets replicated and applied.
type txnPlan struct {
	// ops in deterministic application order.
	ops []JSApiTxnOp

	// baseline captures the version of every touched resource at prepare time
	// (0 means the resource did not exist).
	baseline map[string]uint64
}

// txnEngine validates transaction requests against a txnResourceView. It is
// platform-independent and must not touch the network, raft or disk.
type txnEngine struct {
	s    *Server
	acc  *Account
	view txnResourceView
}

func newTxnEngine(s *Server, acc *Account, view txnResourceView) *txnEngine {
	return &txnEngine{s: s, acc: acc, view: view}
}

func txnConflictResource(op *JSApiTxnOp) string { return op.txnResourceKey() }

func (e *txnEngine) conflict(op *JSApiTxnOp, reason, desc string, apiErr *ApiError) JSApiTxnConflict {
	c := JSApiTxnConflict{
		Resource:    txnConflictResource(op),
		Reason:      reason,
		Description: desc,
		ApiError:    apiErr,
	}
	if op != nil {
		c.Op = op.Op
	}
	return c
}

func (e *txnEngine) apiConflict(op *JSApiTxnOp, reason string, apiErr *ApiError) JSApiTxnConflict {
	c := e.conflict(op, reason, apiErr.Description, apiErr)
	return c
}

// prepare normalizes and validates the full operation set. On success the
// returned plan is ready to be replicated and later applied. On failure no
// plan is returned and every conflict is reported.
func (e *txnEngine) prepare(reqOps []JSApiTxnOp) (*txnPlan, []JSApiTxnConflict) {
	var conflicts []JSApiTxnConflict
	addConflict := func(c JSApiTxnConflict) { conflicts = append(conflicts, c) }

	if len(reqOps) == 0 {
		return nil, []JSApiTxnConflict{{Resource: "transaction", Reason: txnReasonValidation, Description: "transaction contains no operations"}}
	}

	plan := &txnPlan{baseline: make(map[string]uint64, len(reqOps))}

	// Index and shape validation.
	index := make(map[string]*JSApiTxnOp, len(reqOps))
	for i := range reqOps {
		op := &reqOps[i]
		if !op.Op.Valid() {
			addConflict(e.conflict(op, txnReasonValidation, fmt.Sprintf("unknown operation %q", op.Op), nil))
			continue
		}
		if op.Stream == _EMPTY_ || !isValidAssetName(op.Stream) {
			addConflict(e.conflict(op, txnReasonValidation, "stream name is required and can not contain '.', '*', '>', '\\', '/'", nil))
			continue
		}
		if op.Op.IsConsumer() && (op.Consumer == _EMPTY_ || !isValidAssetName(op.Consumer)) {
			addConflict(e.conflict(op, txnReasonValidation, "consumer name is required and can not contain '.', '*', '>', '\\', '/'", nil))
			continue
		}
		switch {
		case op.Op.IsCreate() || op.Op.IsUpdate():
			if op.Op.IsStream() && op.StreamConfig == nil {
				addConflict(e.conflict(op, txnReasonValidation, "stream configuration required", nil))
				continue
			}
			if op.Op.IsConsumer() && op.ConsumerConfig == nil {
				addConflict(e.conflict(op, txnReasonValidation, "consumer configuration required", nil))
				continue
			}
		}
		key := op.txnResourceKey()
		if _, dup := index[key]; dup {
			addConflict(e.conflict(op, txnReasonValidation, "multiple operations target the same resource; split them across transactions", nil))
			continue
		}
		index[key] = op
	}
	if conflicts != nil {
		return nil, conflicts
	}

	// Simulated post-transaction world.
	postStreams := make(map[string]*StreamConfig)
	postConsumers := make(map[string]*ConsumerConfig) // key stream/consumer

	// Seed from current state for every referenced resource.
	for _, op := range index {
		name := op.Stream
		if _, seen := postStreams[name]; !seen {
			if cfg, _, exists := e.view.currentStream(name); exists {
				postStreams[name] = cfg
			}
		}
		if op.Op.IsConsumer() {
			key := op.Stream + "/" + op.Consumer
			if _, seen := postConsumers[key]; !seen {
				if cfg, _, exists := e.view.currentConsumer(op.Stream, op.Consumer); exists {
					postConsumers[key] = cfg
				}
			}
		}
	}

	// Apply stream ops to the simulated world, validating each configuration.
	for _, op := range index {
		if !op.Op.IsStream() {
			continue
		}
		name := op.Stream
		_, exists := postStreams[name]
		switch op.Op {
		case JSApiTxnStreamCreate:
			if exists {
				addConflict(e.conflict(op, txnReasonValidation, "stream already exists", NewJSStreamNameExistError()))
				continue
			}
			if op.ExpectedVersion != nil && *op.ExpectedVersion != 0 {
				addConflict(e.conflict(op, txnReasonValidation, "expected_version for create must be 0", nil))
				continue
			}
			cfg := *op.StreamConfig
			cfg.Name = name
			setStaticStreamMetadata(&cfg)
			if ccfg, apiErr := e.s.checkStreamCfg(&cfg, e.acc, false); apiErr != nil {
				addConflict(e.apiConflict(op, txnReasonValidation, apiErr))
				continue
			} else {
				cfg = ccfg
				postStreams[name] = &cfg
				op.StreamConfig = &cfg
			}
		case JSApiTxnStreamUpdate:
			if !exists {
				addConflict(e.conflict(op, txnReasonValidation, "stream does not exist", NewJSStreamNotFoundError()))
				continue
			}
			cfg := *op.StreamConfig
			cfg.Name = name
			setStaticStreamMetadata(&cfg)
			if _, _, jsa := e.acc.getJetStreamFromAccount(); jsa != nil {
				ocfg := *postStreams[name]
				if ucfg, err := jsa.configUpdateCheck(&ocfg, &cfg, e.s, false); err != nil {
					apiErr := NewJSStreamInvalidConfigError(err, Unless(err))
					addConflict(e.apiConflict(op, txnReasonValidation, apiErr))
					continue
				} else {
					cfg = *ucfg
				}
			}
			postStreams[name] = &cfg
			op.StreamConfig = &cfg
		case JSApiTxnStreamDelete:
			if !exists {
				addConflict(e.conflict(op, txnReasonValidation, "stream does not exist", NewJSStreamNotFoundError()))
				continue
			}
			postStreams[name] = nil
		}
	}

	// Apply consumer ops.
	for _, op := range index {
		if !op.Op.IsConsumer() {
			continue
		}
		streamCfg, streamExists := postStreams[op.Stream]
		// Consumer deletes may target a stream that is deleted by the same
		// transaction (the stream-delete coverage rule requires exactly that).
		if (!streamExists || streamCfg == nil) && !op.Op.IsDelete() {
			addConflict(e.conflict(op, txnReasonValidation, "consumer references a stream that does not exist after the transaction", NewJSStreamNotFoundError()))
			continue
		}
		key := op.Stream + "/" + op.Consumer
		// postConsumers is tri-state: absent key = unknown (queried below),
		// nil value = deleted by this transaction, non-nil = resulting config.
		cur, touched := postConsumers[key]
		exists := touched && cur != nil
		switch op.Op {
		case JSApiTxnConsumerCreate:
			if exists {
				addConflict(e.conflict(op, txnReasonValidation, "consumer already exists", NewJSConsumerNameExistError()))
				continue
			}
			if op.ExpectedVersion != nil && *op.ExpectedVersion != 0 {
				addConflict(e.conflict(op, txnReasonValidation, "expected_version for create must be 0", nil))
				continue
			}
			cfg := *op.ConsumerConfig
			cfg.Name = op.Consumer
			setStaticConsumerMetadata(&cfg)
			if apiErr := e.validateConsumerCfg(&cfg, streamCfg); apiErr != nil {
				addConflict(e.apiConflict(op, txnReasonValidation, apiErr))
				continue
			}
			postConsumers[key] = &cfg
			op.ConsumerConfig = &cfg
		case JSApiTxnConsumerUpdate:
			if !exists {
				addConflict(e.conflict(op, txnReasonValidation, "consumer does not exist", NewJSConsumerNotFoundError()))
				continue
			}
			cfg := *op.ConsumerConfig
			cfg.Name = op.Consumer
			setStaticConsumerMetadata(&cfg)
			if apiErr := e.validateConsumerCfg(&cfg, streamCfg); apiErr != nil {
				addConflict(e.apiConflict(op, txnReasonValidation, apiErr))
				continue
			}
			postConsumers[key] = &cfg
			op.ConsumerConfig = &cfg
		case JSApiTxnConsumerDelete:
			if !exists {
				addConflict(e.conflict(op, txnReasonValidation, "consumer does not exist", NewJSConsumerNotFoundError()))
				continue
			}
			postConsumers[key] = nil
		}
	}

	// A stream delete must explicitly delete every consumer that currently
	// exists on it. The explicit consumer deletes have already been applied to
	// the simulated world above.
	for name, cfg := range postStreams {
		if cfg != nil {
			continue
		}
		for _, cname := range e.view.consumersOf(name) {
			key := name + "/" + cname
			if cur, touched := postConsumers[key]; !touched || cur != nil {
				addConflict(e.conflict(&JSApiTxnOp{Op: JSApiTxnStreamDelete, Stream: name}, txnReasonValidation,
					fmt.Sprintf("stream delete requires deleting consumer %q in the same transaction", cname), nil))
			}
		}
	}

	if conflicts != nil {
		return nil, conflicts
	}

	// Cross-stream subject overlap in the post-transaction world.
	names := make([]string, 0, len(postStreams))
	for name, cfg := range postStreams {
		if cfg != nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			a, b := postStreams[names[i]], postStreams[names[j]]
			for _, sa := range a.Subjects {
				for _, sb := range b.Subjects {
					if SubjectsCollide(sa, sb) {
						addConflict(e.conflict(&JSApiTxnOp{Op: JSApiTxnStreamCreate, Stream: names[j]}, txnReasonValidation,
							fmt.Sprintf("subjects overlap between streams %q and %q: %q collides with %q", names[i], names[j], sa, sb),
							NewJSStreamSubjectOverlapError()))
					}
				}
			}
		}
	}

	// Mirror/sources references and source cycle detection.
	for _, name := range names {
		cfg := postStreams[name]
		if cfg.Mirror != nil {
			if tgt := postStreams[cfg.Mirror.Name]; tgt == nil {
				addConflict(e.conflict(&JSApiTxnOp{Op: JSApiTxnStreamUpdate, Stream: name}, txnReasonValidation,
					fmt.Sprintf("mirror target stream %q does not exist after the transaction", cfg.Mirror.Name), nil))
			}
		}
		for _, src := range cfg.Sources {
			if tgt := postStreams[src.Name]; tgt == nil {
				addConflict(e.conflict(&JSApiTxnOp{Op: JSApiTxnStreamUpdate, Stream: name}, txnReasonValidation,
					fmt.Sprintf("sourced stream %q does not exist after the transaction", src.Name), nil))
			}
		}
	}
	if cycle := findStreamSourceCycle(names, postStreams); cycle != _EMPTY_ {
		addConflict(JSApiTxnConflict{Resource: "stream:" + cycle, Reason: txnReasonValidation, Description: "stream sources form a cycle: " + cycle})
	}

	// Replication factor and storage/placement feasibility.
	for _, op := range index {
		if !op.Op.IsStream() {
			continue
		}
		cfg := postStreams[op.Stream]
		if cfg == nil {
			continue
		}
		if apiErr := e.view.feasibleStream(cfg); apiErr != nil {
			addConflict(e.apiConflict(op, txnReasonValidation, apiErr))
		}
	}

	// Account quota on the simulated end state.
	if c := e.view.checkLimits(postStreams, postConsumerCounts(postStreams, postConsumers, e.view)); c != nil {
		addConflict(*c)
	}

	if conflicts != nil {
		return nil, conflicts
	}

	// Capture CAS baselines for every touched resource.
	for _, op := range index {
		key := op.txnResourceKey()
		var ver uint64
		if op.Op.IsStream() {
			_, ver, _ = e.view.currentStream(op.Stream)
		} else {
			_, ver, _ = e.view.currentConsumer(op.Stream, op.Consumer)
		}
		if op.ExpectedVersion != nil && *op.ExpectedVersion != ver {
			addConflict(JSApiTxnConflict{
				Resource:        key,
				Op:              op.Op,
				Reason:          txnReasonConflict,
				ExpectedVersion: *op.ExpectedVersion,
				ActualVersion:   ver,
				Description:     "expected_version does not match current version at prepare",
			})
			continue
		}
		plan.baseline[key] = ver
	}
	if conflicts != nil {
		return nil, conflicts
	}

	plan.ops = orderTxnOps(index)
	return plan, nil
}

// validateConsumerCfg runs the standard consumer configuration checks against
// the post-transaction stream configuration.
func (e *txnEngine) validateConsumerCfg(cfg *ConsumerConfig, streamCfg *StreamConfig) *ApiError {
	srvLim := &e.s.getOpts().JetStreamLimits
	selected, _, _, _ := e.acc.selectLimits(cfg.replicas(streamCfg))
	if selected == nil {
		return NewJSNoLimitsError()
	}
	if err := checkConsumerCfg(cfg, srvLim, streamCfg, e.acc, selected, false); err != nil {
		return NewJSConsumerCreateError(err, Unless(err))
	}
	return nil
}

// postConsumerCounts computes the resulting durable consumer count per
// surviving stream from the simulated consumer state: existing consumers minus
// deleted ones, plus consumers created by the transaction.
func postConsumerCounts(postStreams map[string]*StreamConfig, postConsumers map[string]*ConsumerConfig, view txnResourceView) map[string]int {
	counts := make(map[string]int)
	for name, scfg := range postStreams {
		if scfg == nil {
			continue
		}
		n := 0
		for _, cname := range view.consumersOf(name) {
			if cfg, touched := postConsumers[name+"/"+cname]; touched && cfg == nil {
				continue // deleted in this transaction
			}
			n++
		}
		for key, cfg := range postConsumers {
			if cfg == nil {
				continue
			}
			sname, cname := splitTxnConsumerKey(key)
			if sname != name {
				continue
			}
			if _, _, exists := view.currentConsumer(sname, cname); !exists {
				n++ // created in this transaction
			}
		}
		counts[name] = n
	}
	return counts
}

func splitTxnConsumerKey(key string) (stream, consumer string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return key, _EMPTY_
}

// orderTxnOps produces the deterministic application order: stream upserts by
// name, consumer upserts by stream/name, consumer deletes by stream/name,
// stream deletes by name. This keeps every reference resolvable while the
// operations apply.
func orderTxnOps(index map[string]*JSApiTxnOp) []JSApiTxnOp {
	var streamUpserts, streamDeletes []*JSApiTxnOp
	var consumerUpserts, consumerDeletes []*JSApiTxnOp
	for _, op := range index {
		switch {
		case op.Op.IsStream() && op.Op.IsDelete():
			streamDeletes = append(streamDeletes, op)
		case op.Op.IsStream():
			streamUpserts = append(streamUpserts, op)
		case op.Op.IsDelete():
			consumerDeletes = append(consumerDeletes, op)
		default:
			consumerUpserts = append(consumerUpserts, op)
		}
	}
	sort.Slice(streamUpserts, func(i, j int) bool { return streamUpserts[i].Stream < streamUpserts[j].Stream })
	sort.Slice(streamDeletes, func(i, j int) bool { return streamDeletes[i].Stream < streamDeletes[j].Stream })
	sort.Slice(consumerUpserts, func(i, j int) bool { return consumerLess(consumerUpserts[i], consumerUpserts[j]) })
	sort.Slice(consumerDeletes, func(i, j int) bool { return consumerLess(consumerDeletes[i], consumerDeletes[j]) })

	out := make([]JSApiTxnOp, 0, len(index))
	for _, op := range streamUpserts {
		out = append(out, *op)
	}
	for _, op := range consumerUpserts {
		out = append(out, *op)
	}
	for _, op := range consumerDeletes {
		out = append(out, *op)
	}
	for _, op := range streamDeletes {
		out = append(out, *op)
	}
	return out
}

func consumerLess(a, b *JSApiTxnOp) bool {
	if a.Stream != b.Stream {
		return a.Stream < b.Stream
	}
	return a.Consumer < b.Consumer
}

// findStreamSourceCycle returns a stream name participating in a sources or
// mirror cycle in the post-transaction world, or empty when acyclic.
func findStreamSourceCycle(names []string, cfgs map[string]*StreamConfig) string {
	// State per node: 0 unvisited, 1 in progress, 2 done.
	state := make(map[string]int, len(names))
	var stack []string

	var visit func(name string) bool
	deps := func(cfg *StreamConfig) []string {
		var out []string
		if cfg.Mirror != nil {
			out = append(out, cfg.Mirror.Name)
		}
		for _, s := range cfg.Sources {
			out = append(out, s.Name)
		}
		return out
	}
	visit = func(name string) bool {
		switch state[name] {
		case 1:
			return true
		case 2:
			return false
		}
		state[name] = 1
		stack = append(stack, name)
		cfg := cfgs[name]
		if cfg != nil {
			for _, dep := range deps(cfg) {
				if _, ok := cfgs[dep]; !ok {
					continue // missing refs are reported separately
				}
				if visit(dep) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[name] = 2
		return false
	}
	for _, name := range names {
		if visit(name) {
			return name
		}
	}
	return _EMPTY_
}

// recheckAtCommit verifies the plan against current state. It only performs
// drift/CAS checks and quota/feasibility re-validation; the stored plan's
// configurations were fully validated at prepare time.
func (e *txnEngine) recheckAtCommit(plan *txnPlan) []JSApiTxnConflict {
	var conflicts []JSApiTxnConflict
	addConflict := func(c JSApiTxnConflict) { conflicts = append(conflicts, c) }

	for i := range plan.ops {
		op := &plan.ops[i]
		key := op.txnResourceKey()
		var cur uint64
		if op.Op.IsStream() {
			_, cur, _ = e.view.currentStream(op.Stream)
		} else {
			_, cur, _ = e.view.currentConsumer(op.Stream, op.Consumer)
		}
		if base := plan.baseline[key]; base != cur {
			addConflict(JSApiTxnConflict{
				Resource:        key,
				Op:              op.Op,
				Reason:          txnReasonConflict,
				ExpectedVersion: base,
				ActualVersion:   cur,
				Description:     "resource changed between prepare and commit",
			})
		}
	}
	if conflicts != nil {
		return conflicts
	}

	// Rebuild the simulated world for quota and feasibility rechecks.
	postStreams := make(map[string]*StreamConfig)
	postConsumers := make(map[string]*ConsumerConfig)
	for _, name := range e.allPlanStreams(plan) {
		if cfg, _, exists := e.view.currentStream(name); exists {
			postStreams[name] = cfg
		}
	}
	for i := range plan.ops {
		op := &plan.ops[i]
		switch op.Op {
		case JSApiTxnStreamCreate, JSApiTxnStreamUpdate:
			cfg := *op.StreamConfig
			postStreams[op.Stream] = &cfg
		case JSApiTxnStreamDelete:
			postStreams[op.Stream] = nil
		case JSApiTxnConsumerCreate, JSApiTxnConsumerUpdate:
			cfg := *op.ConsumerConfig
			postConsumers[op.Stream+"/"+op.Consumer] = &cfg
		case JSApiTxnConsumerDelete:
			postConsumers[op.Stream+"/"+op.Consumer] = nil
		}
	}
	for _, op := range plan.ops {
		if !op.Op.IsStream() {
			continue
		}
		if cfg := postStreams[op.Stream]; cfg != nil {
			if apiErr := e.view.feasibleStream(cfg); apiErr != nil {
				op := op
				addConflict(e.apiConflict(&op, txnReasonValidation, apiErr))
			}
		}
	}
	if c := e.view.checkLimits(postStreams, postConsumerCounts(postStreams, postConsumers, e.view)); c != nil {
		addConflict(*c)
	}
	return conflicts
}

func (e *txnEngine) allPlanStreams(plan *txnPlan) []string {
	seen := make(map[string]struct{})
	var out []string
	for i := range plan.ops {
		if _, ok := seen[plan.ops[i].Stream]; !ok {
			seen[plan.ops[i].Stream] = struct{}{}
			out = append(out, plan.ops[i].Stream)
		}
	}
	return out
}
