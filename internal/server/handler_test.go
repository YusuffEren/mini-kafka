package server

import (
	"bytes"
	"errors"
	"testing"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

// ---------------------------------------------------------------------------
// Mutlu yol
// ---------------------------------------------------------------------------

func TestMux_bilinen_api_key_handler_cagrilir_response_doner(t *testing.T) {
	mux := NewMux()
	called := false
	mux.Handle(7, func(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		called = true
		return &protocol.ResponseFrame{
			CorrelationID: 999,
			ErrorCode:     0,
			Payload:       req.Payload,
		}, nil
	})

	req := &protocol.RequestFrame{
		ApiKey:        7,
		ApiVersion:    1,
		CorrelationID: 42,
		ClientID:      "test-client",
		Payload:       []byte("hello"),
	}

	resp := mux.Dispatch(req)

	if !called {
		t.Fatal("registered handler was not called")
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("ErrorCode = %d, want 0", resp.ErrorCode)
	}
	if !bytes.Equal(resp.Payload, []byte("hello")) {
		t.Fatalf("Payload = %q, want %q", resp.Payload, "hello")
	}
	if resp.CorrelationID != 42 {
		t.Fatalf("CorrelationID = %d, want 42", resp.CorrelationID)
	}
}

func TestMux_birden_fazla_handler_kaydi(t *testing.T) {
	mux := NewMux()
	mux.Handle(1, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{Payload: []byte("one")}, nil
	})
	mux.Handle(2, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{Payload: []byte("two")}, nil
	})
	mux.Handle(3, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{Payload: []byte("three")}, nil
	})

	cases := []struct {
		apiKey int16
		want   string
	}{
		{1, "one"},
		{2, "two"},
		{3, "three"},
	}

	for _, tc := range cases {
		req := &protocol.RequestFrame{ApiKey: tc.apiKey, CorrelationID: int32(tc.apiKey)}
		resp := mux.Dispatch(req)
		if resp.ErrorCode != 0 {
			t.Fatalf("apiKey=%d ErrorCode = %d, want 0", tc.apiKey, resp.ErrorCode)
		}
		if !bytes.Equal(resp.Payload, []byte(tc.want)) {
			t.Fatalf("apiKey=%d Payload = %q, want %q", tc.apiKey, resp.Payload, tc.want)
		}
	}
}

func TestMux_correlationID_korunur(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{CorrelationID: 9999}, nil
	})

	req := &protocol.RequestFrame{
		ApiKey:        7,
		ApiVersion:    1,
		CorrelationID: -12345,
		ClientID:      "corr",
		Payload:       []byte{},
	}

	resp := mux.Dispatch(req)
	if resp.CorrelationID != -12345 {
		t.Fatalf("CorrelationID = %d, want -12345", resp.CorrelationID)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("ErrorCode = %d, want 0", resp.ErrorCode)
	}
}

// ---------------------------------------------------------------------------
// Sınır durumları
// ---------------------------------------------------------------------------

func TestMux_handler_nil_response_donerse_default_basarili(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return nil, nil
	})

	req := &protocol.RequestFrame{
		ApiKey:        7,
		CorrelationID: 77,
		Payload:       []byte("ignored"),
	}

	resp := mux.Dispatch(req)
	if resp.ErrorCode != 0 {
		t.Fatalf("ErrorCode = %d, want 0", resp.ErrorCode)
	}
	if resp.CorrelationID != 77 {
		t.Fatalf("CorrelationID = %d, want 77", resp.CorrelationID)
	}
	if len(resp.Payload) != 0 {
		t.Fatalf("Payload = %x, want empty", resp.Payload)
	}
}

func TestMux_ayni_api_key_ustune_yazar(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{Payload: []byte("first")}, nil
	})
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{Payload: []byte("second")}, nil
	})

	req := &protocol.RequestFrame{ApiKey: 7}
	resp := mux.Dispatch(req)
	if !bytes.Equal(resp.Payload, []byte("second")) {
		t.Fatalf("Payload = %q, want %q", resp.Payload, "second")
	}
}

// ---------------------------------------------------------------------------
// Hata durumları
// ---------------------------------------------------------------------------

func TestMux_bilinmeyen_api_key_UnsupportedVersion(t *testing.T) {
	mux := NewMux()
	mux.Handle(0, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{}, nil
	})

	req := &protocol.RequestFrame{
		ApiKey:        99,
		ApiVersion:    1,
		CorrelationID: 5,
		ClientID:      "unknown",
		Payload:       []byte("payload"),
	}

	resp := mux.Dispatch(req)
	if resp.ErrorCode != ErrUnsupportedVersion {
		t.Fatalf("ErrorCode = %d, want %d (UnsupportedVersion)", resp.ErrorCode, ErrUnsupportedVersion)
	}
	if resp.CorrelationID != 5 {
		t.Fatalf("CorrelationID = %d, want 5", resp.CorrelationID)
	}
}

func TestMux_handler_hata_donerse_UnknownError(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return nil, errors.New("handler error")
	})

	req := &protocol.RequestFrame{
		ApiKey:        7,
		CorrelationID: 11,
		Payload:       []byte("data"),
	}

	resp := mux.Dispatch(req)
	if resp.ErrorCode != ErrUnknown {
		t.Fatalf("ErrorCode = %d, want %d (UnknownError)", resp.ErrorCode, ErrUnknown)
	}
	if resp.CorrelationID != 11 {
		t.Fatalf("CorrelationID = %d, want 11", resp.CorrelationID)
	}
}

func TestMux_handler_panic_yaparsa_recover_UnknownError(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		panic("boom")
	})

	req := &protocol.RequestFrame{
		ApiKey:        7,
		CorrelationID: 22,
		Payload:       []byte("panic"),
	}

	resp := mux.Dispatch(req)
	if resp.ErrorCode != ErrUnknown {
		t.Fatalf("ErrorCode = %d, want %d (UnknownError)", resp.ErrorCode, ErrUnknown)
	}
	if resp.CorrelationID != 22 {
		t.Fatalf("CorrelationID = %d, want 22", resp.CorrelationID)
	}
}
