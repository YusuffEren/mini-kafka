package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

// startServer runs a server on a random port and returns the server and the
// address it is listening on. The caller is responsible for shutting it down.
func startServer(t *testing.T, mux *Mux) (*Server, string) {
	t.Helper()

	srv := NewServer(ServerConfig{
		Addr:            ":0",
		MaxConnections:  1024,
		IdleTimeout:     100 * time.Millisecond,
		MaxRequestBytes: 1024 * 1024, // 1 MiB
	}, mux)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	var addr string
	for i := 0; i < 100; i++ {
		if a := srv.Addr(); a != nil {
			addr = a.String()
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
	defer func() { _ = srv.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = srv.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = srv.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

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
	_ = conn.Close()

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
	_ = conn.Close()

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
	defer func() { _ = srv.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

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

func TestServer_gecersiz_address_Start_hata(t *testing.T) {
	mux := NewMux()
	srv := NewServer(ServerConfig{Addr: "not-a-valid-tcp-address!!", MaxConnections: 1024}, mux)
	if err := srv.Start(); err == nil {
		t.Fatal("Start succeeded with invalid address, want error")
	}
}

// ---------------------------------------------------------------------------
// T-20: connection deadline / resource exhaustion
// ---------------------------------------------------------------------------

// TestIdleConnectionClosed shows that a client which connects but never sends
// data must be disconnected after the configured idle timeout. The current
// handleConn implementation never calls SetReadDeadline, so the Read below
// blocks until the client-side deadline expires.
func TestIdleConnectionClosed(t *testing.T) {
	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{}, nil
	})

	srv, addr := startServer(t, mux)
	defer func() { _ = srv.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// After the idle timeout the server should have closed the connection.
	// The client-side deadline is only a safety net so the test does not hang.
	if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline error: %v", err)
	}

	var buf [1]byte
	_, err = conn.Read(buf[:])
	if err == nil {
		t.Fatal("Read returned data without client sending anything; expected EOF/closed")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected EOF/closed after idle timeout, got %v", err)
	}
}

// TestOversizedFrameRejected shows that a frame whose declared size exceeds the
// server-level MaxRequestBytes limit must be rejected and the connection closed,
// without the server allocating the full payload. The current server has no such
// limit, so the frame is accepted and the connection stays open.
func TestOversizedFrameRejected(t *testing.T) {
	const maxRequestBytes = 1024 * 1024 // 1 MB

	mux := NewMux()
	mux.Handle(7, func(_ *protocol.RequestFrame) (*protocol.ResponseFrame, error) {
		return &protocol.ResponseFrame{}, nil
	})

	srv, addr := startServer(t, mux)
	defer func() { _ = srv.Shutdown(context.Background()) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Build a frame that is larger than the intended server limit but still
	// below protocol.MaxFrameSize, so the protocol layer does not reject it.
	size := int32(maxRequestBytes + 1)
	if _, err := protocol.PutInt32(conn, size); err != nil {
		t.Fatalf("write size error: %v", err)
	}
	if _, err := protocol.PutInt16(conn, 7); err != nil {
		t.Fatalf("write apiKey error: %v", err)
	}
	if _, err := protocol.PutInt16(conn, 1); err != nil {
		t.Fatalf("write apiVersion error: %v", err)
	}
	if _, err := protocol.PutInt32(conn, 1); err != nil {
		t.Fatalf("write correlationID error: %v", err)
	}
	if _, err := protocol.PutString(conn, "oversized"); err != nil {
		t.Fatalf("write clientID error: %v", err)
	}

	const headerLen = 2 + 2 + 4 + (2 + len("oversized")) + 4 // apiKey + apiVersion + correlationID + clientID + payload length prefix
	payloadLen := int(size) - headerLen
	if payloadLen < 0 {
		t.Fatalf("declared size %d is too small for frame header", size)
	}
	if _, err := protocol.PutInt32(conn, int32(payloadLen)); err != nil {
		t.Fatalf("write payload length error: %v", err)
	}
	if _, err := conn.Write(make([]byte, payloadLen)); err != nil {
		t.Fatalf("write payload error: %v", err)
	}

	// The server should close the connection after seeing an oversized request.
	// The client-side deadline is only a safety net so the test does not hang.
	if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline error: %v", err)
	}

	var buf [1]byte
	_, err = conn.Read(buf[:])
	if err == nil {
		t.Fatal("Read returned data after oversized frame; expected connection close")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected EOF/closed after oversized frame, got %v", err)
	}
}
