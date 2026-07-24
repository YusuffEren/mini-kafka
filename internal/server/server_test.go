package server

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/yusuf/mini-kafka/internal/protocol"
)

// startServer runs a server on a random port and returns the server and the
// address it is listening on. The caller is responsible for shutting it down.
func startServer(t *testing.T, mux *Mux) (*Server, string) {
	t.Helper()

	srv := NewServer(":0", mux)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	var addr string
	for i := 0; i < 100; i++ {
		if srv.listener != nil {
			addr = srv.listener.Addr().String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		select {
		case err := <-errCh:
			t.Fatalf("server start failed: %v", err)
		default:
			t.Fatal("server did not create listener in time")
		}
	}
	return srv, addr
}

func sendRequest(t *testing.T, conn net.Conn, req *protocol.RequestFrame) {
	t.Helper()
	if _, err := req.Write(conn); err != nil {
		t.Fatalf("RequestFrame.Write error: %v", err)
	}
}

func readResponse(t *testing.T, conn net.Conn) *protocol.ResponseFrame {
	t.Helper()
	resp, err := protocol.ReadResponseFrame(conn)
	if err != nil {
		t.Fatalf("ReadResponseFrame error: %v", err)
	}
	return resp
}

// ---------------------------------------------------------------------------
// Mutlu yol
// ---------------------------------------------------------------------------

func TestServer_roundtrip_echo(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(req *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{
			ErrorCode: 0,
			Payload:   req.Payload,
		}, nil
	})

	srv, addr := startServer(t, mux)
	defer srv.Shutdown(context.Background())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{
		ApiKey:        7,
		ApiVersion:    1,
		CorrelationID: 123,
		ClientID:      "roundtrip",
		Payload:       []byte("echo-payload"),
	}
	sendRequest(t, conn, req)
	resp := readResponse(t, conn)

	if resp.ErrorCode != 0 {
		t.Fatalf("ErrorCode = %d, want 0", resp.ErrorCode)
	}
	if resp.CorrelationID != 123 {
		t.Fatalf("CorrelationID = %d, want 123", resp.CorrelationID)
	}
	if !bytes.Equal(resp.Payload, []byte("echo-payload")) {
		t.Fatalf("Payload = %q, want %q", resp.Payload, "echo-payload")
	}
}

func TestServer_ApiVersions_cevap_kontrol(t *testing.T) {
	want := &protocol.ApiVersionsResponse{
		ApiKeys: []protocol.ApiVersion{
			{ApiKey: 0, MinVersion: 1, MaxVersion: 1},
			{ApiKey: 1, MinVersion: 1, MaxVersion: 1},
			{ApiKey: 12, MinVersion: 1, MaxVersion: 1},
		},
	}

	mux := NewMux()
	mux.Handle(12, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		var body bytes.Buffer
		if err := want.Encode(&body); err != nil {
			return nil, err
		}
		return &protocol.ResponseFrame{ErrorCode: 0, Payload: body.Bytes()}, nil
	})

	srv, addr := startServer(t, mux)
	defer srv.Shutdown(context.Background())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{
		ApiKey:        12,
		ApiVersion:    1,
		CorrelationID: 55,
		ClientID:      "api-versions",
		Payload:       []byte{},
	}
	sendRequest(t, conn, req)
	resp := readResponse(t, conn)

	if resp.ErrorCode != 0 {
		t.Fatalf("ErrorCode = %d, want 0", resp.ErrorCode)
	}
	if resp.CorrelationID != 55 {
		t.Fatalf("CorrelationID = %d, want 55", resp.CorrelationID)
	}

	var got protocol.ApiVersionsResponse
	if err := got.Decode(bytes.NewReader(resp.Payload)); err != nil {
		t.Fatalf("ApiVersionsResponse.Decode error: %v", err)
	}
	if len(got.ApiKeys) != len(want.ApiKeys) {
		t.Fatalf("len(ApiKeys) = %d, want %d", len(got.ApiKeys), len(want.ApiKeys))
	}
	for i, v := range want.ApiKeys {
		if got.ApiKeys[i] != v {
			t.Fatalf("ApiKeys[%d] = %+v, want %+v", i, got.ApiKeys[i], v)
		}
	}
}

// ---------------------------------------------------------------------------
// Sınır durumları
// ---------------------------------------------------------------------------

func TestServer_shutdown_aktif_baglanti_drain(t *testing.T) {
	handlerEntered := make(chan struct{})
	handlerDone := make(chan struct{})
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		close(handlerEntered)
		time.Sleep(200 * time.Millisecond)
		close(handlerDone)
		return &protocol.ResponseFrame{}, nil
	})

	srv, addr := startServer(t, mux)
	defer srv.Shutdown(context.Background())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{ApiKey: 7, CorrelationID: 1, ClientID: "drain", Payload: []byte{}}
	sendRequest(t, conn, req)

	select {
	case <-handlerEntered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- srv.Shutdown(ctx)
	}()

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish")
	}

	// Close the active connection so the server's handleConn loop can exit
	// and Shutdown can drain the worker goroutine.
	conn.Close()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete after handler finished")
	}
}

func TestServer_iki_kez_shutdown_guvenli(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{}, nil
	})

	srv, addr := startServer(t, mux)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	conn.Close()

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown error: %v", err)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Hata durumları
// ---------------------------------------------------------------------------

func TestServer_shutdown_yeni_baglanti_kabul_edilmez(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{}, nil
	})

	srv, addr := startServer(t, mux)

	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	_, err := net.Dial("tcp", addr)
	if err == nil {
		t.Fatal("net.Dial succeeded after shutdown, want error")
	}
}

func TestServer_bilinmeyen_api_key_UnsupportedVersion(t *testing.T) {
	mux := NewMux()
	mux.Handle(0, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{}, nil
	})

	srv, addr := startServer(t, mux)
	defer srv.Shutdown(context.Background())

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{
		ApiKey:        99,
		ApiVersion:    1,
		CorrelationID: 88,
		ClientID:      "unknown",
		Payload:       []byte("x"),
	}
	sendRequest(t, conn, req)
	resp := readResponse(t, conn)

	if resp.ErrorCode != ErrUnsupportedVersion {
		t.Fatalf("ErrorCode = %d, want %d (UnsupportedVersion)", resp.ErrorCode, ErrUnsupportedVersion)
	}
	if resp.CorrelationID != 88 {
		t.Fatalf("CorrelationID = %d, want 88", resp.CorrelationID)
	}
}

func TestServer_gecersiz_adres_Start_hata(t *testing.T) {
	mux := NewMux()
	srv := NewServer("not-a-valid-tcp-address!!", mux)
	if err := srv.Start(); err == nil {
		t.Fatal("Start succeeded with invalid address, want error")
	}
}
