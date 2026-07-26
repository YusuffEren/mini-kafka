// Package server implements the TCP transport and request dispatch layer for
// mini-kafka. It accepts raw TCP connections, decodes protocol frames and
// routes them to registered API handlers via a Mux.
package server

import (
	"errors"
	"runtime/debug"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

// codedError is the contract for errors that carry a specific protocol error
// code. When a handler returns an error implementing this interface, Dispatch
// uses ErrorCode() to populate the response frame instead of collapsing every
// failure to ErrUnknown.
//
// The interface is intentionally minimal (a single method) so handler authors
// can wrap any existing error type with a code-carrying shim without depending
// on the server package. It is kept unexported here because the canonical
// CodedError interface is already declared in the package's test contract
// (handler_test.go); the anonymous match performed via errors.As accepts any
// type exposing ErrorCode() int16, so the two definitions are compatible.
type codedError interface {
	error
	ErrorCode() int16
}

// Protocol error codes used by the dispatch layer. These mirror the Kafka
// error codes referenced in docs/PROTOCOL.md Section 5.
const (
	ErrNone                    int16 = 0
	ErrUnknown                 int16 = 1
	ErrOffsetOutOfRange        int16 = 2
	ErrCorruptMessage          int16 = 3
	ErrUnknownTopicOrPartition int16 = 4
	ErrNotLeaderForPartition   int16 = 5
	ErrRequestTimedOut         int16 = 6
	ErrMessageTooLarge         int16 = 7
	ErrNotEnoughReplicas       int16 = 8
	ErrUnknownMemberID         int16 = 9
	ErrRebalanceInProgress     int16 = 10
	ErrIllegalGeneration       int16 = 11
	ErrInvalidGroupID          int16 = 12
	ErrUnsupportedVersion      int16 = 13
	ErrTopicAlreadyExists      int16 = 14
	ErrInvalidPartitionCount   int16 = 15
	// ErrInvalidTopicException is returned when a topic name fails validation
	// (illegal characters, reserved names, or path-escape attempts).
	ErrInvalidTopicException int16 = 17
)

// HandlerFunc is the signature for API request handlers. It receives the
// request frame and returns a response frame. Returning an error results in a
// generic error response being sent.
type HandlerFunc func(req *protocol.RequestFrame) (*protocol.ResponseFrame, error)

// Logger is the minimal logging surface Mux can be wired with. It is
// intentionally small so handlers and tests can inject a capturing logger
// without pulling in a concrete logging dependency. The interface is satisfied
// by the testLogger in handler_test.go.
type Logger interface {
	Error(msg string, args ...interface{})
}

// Mux dispatches request frames to registered handlers based on API key.
type Mux struct {
	handlers map[int16]HandlerFunc
	logger   Logger
}

// NewMux creates a new handler multiplexer. An optional logger may be passed
// as the first variadic argument; when non-nil it is stored on the Mux for use
// by future handler-error logging wiring (T-21). Passing no arguments keeps
// the historical behaviour exactly.
func NewMux(opts ...Logger) *Mux {
	m := &Mux{
		handlers: make(map[int16]HandlerFunc),
	}
	if len(opts) > 0 && opts[0] != nil {
		m.logger = opts[0]
	}
	return m
}

// Handle registers a handler for the given API key. If a handler is already
// registered for apiKey, it is replaced.
func (m *Mux) Handle(apiKey int16, h HandlerFunc) {
	m.handlers[apiKey] = h
}

// Dispatch routes a request frame to the appropriate handler. If no handler is
// registered for the API key, it returns a response with errorCode
// UnsupportedVersion (13) and an empty payload. If the handler panics, the
// panic is recovered and a response with errorCode UnknownError (1) is
// returned. If the handler returns an error, a response with errorCode 1 is
// returned; the error message is not serialized into the payload.
//
// When the returned error implements the CodedError contract (an ErrorCode()
// int16 method), its code is used verbatim as the response ErrorCode; otherwise
// the error is collapsed to ErrUnknown (1). Handler errors and recovered
// panics are forwarded to the configured logger (if any) together with the
// apiKey, correlationID and, for panics, the goroutine stack trace so
// operators can diagnose failures without a debugger attached.
//
// The returned response always mirrors the CorrelationID of the request so the
// client can match it.
func (m *Mux) Dispatch(req *protocol.RequestFrame) (resp *protocol.ResponseFrame) {
	resp = &protocol.ResponseFrame{
		CorrelationID: req.CorrelationID,
		ErrorCode:     ErrNone,
	}

	h, ok := m.handlers[req.ApiKey]
	if !ok {
		resp.ErrorCode = ErrUnsupportedVersion
		return resp
	}

	// A panicking handler must never tear down the connection goroutine.
	defer func() {
		if r := recover(); r != nil {
			if m.logger != nil {
				m.logger.Error("handler panic",
					"apiKey", req.ApiKey,
					"correlationID", req.CorrelationID,
					"panic", r,
					"stack", debug.Stack(),
				)
			}
			resp = &protocol.ResponseFrame{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ErrUnknown,
			}
		}
	}()

	out, err := h(req)
	if err != nil {
		if m.logger != nil {
			m.logger.Error("handler failed",
				"apiKey", req.ApiKey,
				"correlationID", req.CorrelationID,
				"err", err,
			)
		}
		// If the error carries a protocol code, surface it; otherwise collapse
		// to the generic ErrUnknown so the wire contract stays stable.
		var coded codedError
		if errors.As(err, &coded) {
			resp.ErrorCode = coded.ErrorCode()
		} else {
			resp.ErrorCode = ErrUnknown
		}
		return resp
	}
	if out != nil {
		// Ensure the correlation id is consistent with the request.
		out.CorrelationID = req.CorrelationID
		return out
	}
	return resp
}
