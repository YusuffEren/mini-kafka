// Package server implements the TCP transport and request dispatch layer for
// mini-kafka. It accepts raw TCP connections, decodes protocol frames and
// routes them to registered API handlers via a Mux.
package server

import (
	"github.com/yusuf/mini-kafka/internal/protocol"
)

// Protocol error codes used by the dispatch layer. These mirror the Kafka
// error codes referenced in MINI_KAFKA_SPEC.md.
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

// Mux dispatches request frames to registered handlers based on API key.
type Mux struct {
	handlers map[int16]HandlerFunc
}

// NewMux creates a new handler multiplexer.
func NewMux() *Mux {
	return &Mux{
		handlers: make(map[int16]HandlerFunc),
	}
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
			resp = &protocol.ResponseFrame{
				CorrelationID: req.CorrelationID,
				ErrorCode:     ErrUnknown,
			}
		}
	}()

	out, err := h(req)
	if err != nil {
		resp.ErrorCode = ErrUnknown
		return resp
	}
	if out != nil {
		// Ensure the correlation id is consistent with the request.
		out.CorrelationID = req.CorrelationID
		return out
	}
	return resp
}
