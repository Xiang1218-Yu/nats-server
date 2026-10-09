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
	"strings"
)

const (
	JSApiTxnCommitSub = "$JS.API.TXN.COMMIT.*"
	JSApiTxnCancelSub = "$JS.API.TXN.CANCEL.*"
	JSApiTxnInfoSub   = "$JS.API.TXN.INFO.*"
)

// Create (prepare) a configuration transaction.
func (s *Server) jsTxnCreateRequest(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	if c == nil || !s.JetStreamEnabled() {
		return
	}
	ci, acc, hdr, msg, err := s.getRequestInfo(c, rmsg)
	if err != nil {
		s.Warnf(badAPIRequestT, msg)
		return
	}
	var resp = JSApiTxnCreateResponse{ApiResponse: ApiResponse{Type: JSApiTxnCreateResponseType}}

	if s.JetStreamIsClustered() {
		js, cc := s.getJetStreamCluster()
		if js == nil || cc == nil {
			return
		}
		if js.isLeaderless() {
			resp.Error = NewJSClusterNotAvailError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
			return
		}
		if !s.JetStreamIsLeader() {
			return
		}
	}

	if errorOnRequiredApiLevel(hdr) {
		resp.Error = NewJSRequiredApiLevelError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if hasJS, doErr := acc.checkJetStream(); !hasJS {
		if doErr {
			resp.Error = NewJSNotEnabledForAccountError()
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		}
		return
	}

	var req JSApiTxnCreateRequest
	if err := s.unmarshalRequest(c, acc, subject, msg, &req); err != nil {
		resp.Error = NewJSInvalidJSONError(err)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if len(req.Ops) == 0 {
		resp.Error = NewJSBadRequestError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	if apiErr := s.txnCheckOpPermissions(c, req.Ops); apiErr != nil {
		resp.Error = apiErr
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}

	if s.JetStreamIsClustered() {
		s.jsClusteredTxnCreate(ci, acc, subject, reply, &req)
		return
	}

	mgr := acc.txnMgrForServer(s)
	if mgr == nil {
		resp.Error = NewJSClusterNotAvailError()
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	rec, conflicts := mgr.create(&req)
	if conflicts != nil {
		resp.Conflicts = conflicts
		resp.Error = conflictsToApiError(conflicts)
		s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(&resp))
		return
	}
	resp.JSApiTxnStatus = rec.status()
	s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(resp))
}

// Common handling for commit/cancel/info subjects.
func (s *Server) jsTxnActionRequest(kind string) func(*subscription, *client, *Account, string, string, []byte) {
	return func(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
		if c == nil || !s.JetStreamEnabled() {
			return
		}
		ci, acc, hdr, msg, err := s.getRequestInfo(c, rmsg)
		if err != nil {
			s.Warnf(badAPIRequestT, msg)
			return
		}
		id := txnIDFromSubject(subject)
		if id == _EMPTY_ {
			return
		}

		respType := JSApiTxnCommitResponseType
		if kind == "cancel" {
			respType = JSApiTxnCancelResponseType
		} else if kind == "info" {
			respType = JSApiTxnInfoResponseType
		}

		if s.JetStreamIsClustered() {
			if kind == "info" {
				info := s.jsClusteredTxnInfo(ci, acc, id)
				s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(info))
				return
			}
			js, cc := s.getJetStreamCluster()
			if js == nil || cc == nil {
				return
			}
			if js.isLeaderless() {
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(txnResponseWithError(kind, NewJSClusterNotAvailError())))
				return
			}
			if !s.JetStreamIsLeader() {
				return
			}
			if kind == "commit" {
				s.jsClusteredTxnCommit(ci, acc, subject, reply, id)
				return
			}
			var req JSApiTxnCancelRequest
			_ = s.unmarshalRequest(c, acc, subject, msg, &req)
			s.jsClusteredTxnCancel(ci, acc, subject, reply, id, req.Reason)
			return
		}

		if errorOnRequiredApiLevel(hdr) {
			s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(txnResponseWithError(kind, NewJSRequiredApiLevelError())))
			return
		}
		if hasJS, doErr := acc.checkJetStream(); !hasJS {
			if doErr {
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(txnResponseWithError(kind, NewJSNotEnabledForAccountError())))
			}
			return
		}

		mgr := acc.txnMgrForServer(s)
		if mgr == nil {
			return
		}

		switch kind {
		case "info":
			rec, apiErr := mgr.info(id)
			r := &JSApiTxnInfoResponse{}
			r.Type = respType
			if apiErr != nil {
				r.Error = apiErr
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
				return
			}
			r.JSApiTxnStatus = rec.status()
			s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
		case "commit":
			rec, conflicts := mgr.commit(id)
			r := &JSApiTxnCommitResponse{}
			r.Type = respType
			if rec == nil {
				r.Error = NewJSTxnNotFoundError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
				return
			}
			r.JSApiTxnStatus = rec.status()
			if conflicts != nil {
				r.Conflicts = conflicts
				r.Error = conflictsToApiError(conflicts)
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
				return
			}
			s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
		case "cancel":
			var req JSApiTxnCancelRequest
			_ = s.unmarshalRequest(c, acc, subject, msg, &req)
			rec, conflicts := mgr.cancel(id, req.Reason)
			r := &JSApiTxnCancelResponse{}
			r.Type = respType
			if rec == nil {
				r.Error = NewJSTxnNotFoundError()
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
				return
			}
			r.JSApiTxnStatus = rec.status()
			if conflicts != nil {
				r.Conflicts = conflicts
				r.Error = conflictsToApiError(conflicts)
				s.sendAPIErrResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
				return
			}
			s.sendAPIResponse(ci, acc, subject, reply, string(msg), s.jsonResponse(r))
		}
	}
}

func txnResponse(kind string) any {
	switch kind {
	case "cancel":
		return &JSApiTxnCancelResponse{ApiResponse: ApiResponse{Type: JSApiTxnCancelResponseType}}
	case "info":
		return &JSApiTxnInfoResponse{ApiResponse: ApiResponse{Type: JSApiTxnInfoResponseType}}
	default:
		return &JSApiTxnCommitResponse{ApiResponse: ApiResponse{Type: JSApiTxnCommitResponseType}}
	}
}

func txnResponseWithError(kind string, apiErr *ApiError) any {
	switch r := txnResponse(kind).(type) {
	case *JSApiTxnCancelResponse:
		r.Error = apiErr
		return r
	case *JSApiTxnInfoResponse:
		r.Error = apiErr
		return r
	default:
		cr := txnResponse("commit").(*JSApiTxnCommitResponse)
		cr.Error = apiErr
		return cr
	}
}

func txnIDFromSubject(subject string) string {
	toks := strings.Split(subject, ".")
	if len(toks) == 0 {
		return _EMPTY_
	}
	return toks[len(toks)-1]
}

// txnCheckOpPermissions verifies the originating client is authorized for the
// equivalent single-resource management subject of every transaction op.
func (s *Server) txnCheckOpPermissions(c *client, ops []JSApiTxnOp) *ApiError {
	if c == nil {
		return nil
	}
	for _, op := range ops {
		var subj string
		switch op.Op {
		case JSApiTxnStreamCreate:
			subj = fmt.Sprintf(JSApiStreamCreateT, op.Stream)
		case JSApiTxnStreamUpdate:
			subj = fmt.Sprintf(JSApiStreamUpdateT, op.Stream)
		case JSApiTxnStreamDelete:
			subj = fmt.Sprintf(JSApiStreamDeleteT, op.Stream)
		case JSApiTxnConsumerCreate, JSApiTxnConsumerUpdate:
			subj = fmt.Sprintf(JSApiDurableCreateT, op.Stream, op.Consumer)
		case JSApiTxnConsumerDelete:
			subj = fmt.Sprintf(JSApiConsumerDeleteT, op.Stream, op.Consumer)
		default:
			return NewJSTxnValidationError(fmt.Errorf("unknown transaction op %q", op.Op))
		}
		if !c.pubAllowedFullCheck(subj, true, false) {
			return &ApiError{Code: 403, ErrCode: uint16(JSTxnConflictErrF), Description: "transaction operation not permitted: " + subj}
		}
	}
	return nil
}
