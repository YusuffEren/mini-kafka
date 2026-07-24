package broker

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/yusuf/mini-kafka/internal/config"
	"github.com/yusuf/mini-kafka/internal/protocol"
	"github.com/yusuf/mini-kafka/internal/server"
)

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Broker: config.BrokerConfig{
			ID:               1,
			Host:             "127.0.0.1",
			Port:             0,
			DataDir:          t.TempDir(),
			MaxConnections:   1024,
			RequestTimeoutMs: 30000,
		},
		Topic: config.TopicConfig{
			AutoCreate:        true,
			DefaultPartitions: 1,
		},
	}
}

// tryBrokerAddr returns the actual listening address of a broker if it has
// already created its listener.
func tryBrokerAddr(b *Broker) (string, bool) {
	if b == nil {
		return "", false
	}
	addr := b.Addr()
	if addr == nil {
		return "", false
	}
	return addr.String(), true
}

// brokerAddr returns the listening address of a started broker. Use only when
// the broker is known to be listening.
func brokerAddr(t *testing.T, b *Broker) string {
	t.Helper()
	addr, ok := tryBrokerAddr(b)
	if !ok {
		t.Fatal("broker is not listening")
	}
	return addr
}

func startBroker(t *testing.T, cfg *config.Config) (*Broker, string) {
	t.Helper()

	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	startErr := make(chan error, 1)
	go func() {
		startErr <- b.Start()
	}()

	var addr string
	for i := 0; i < 100; i++ {
		if a, ok := tryBrokerAddr(b); ok {
			addr = a
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		select {
		case err := <-startErr:
			t.Fatalf("broker start failed: %v", err)
		default:
			t.Fatal("broker did not create listener in time")
		}
	}

	return b, addr
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

func TestNew_gecerli_config_ile_broker_olusturur(t *testing.T) {
	cfg := testConfig(t)

	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if b == nil {
		t.Fatal("New returned nil broker")
	}
	defer b.Shutdown(context.Background())

	if b.config != cfg {
		t.Error("broker config mismatch")
	}
	if b.server == nil {
		t.Error("broker server is nil")
	}
	if b.mux == nil {
		t.Error("broker mux is nil")
	}
}

func TestNew_nil_config_icin_hata_doner(t *testing.T) {
	b, err := New(nil)
	if err == nil {
		t.Fatalf("New succeeded with nil config, want error; broker=%v", b)
	}
}

func TestStart_Shutdown_broker_baslatilip_durdurulabilir(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)

	if addr == "" {
		t.Fatal("broker address is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestBroker_ApiVersions_handler_cevabi_dogru(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{
		ApiKey:        12,
		ApiVersion:    0,
		CorrelationID: 42,
		ClientID:      "api-versions",
		Payload:       []byte{},
	}
	sendRequest(t, conn, req)
	resp := readResponse(t, conn)

	if resp.ErrorCode != 0 {
		t.Fatalf("ErrorCode = %d, want 0", resp.ErrorCode)
	}
	if resp.CorrelationID != 42 {
		t.Fatalf("CorrelationID = %d, want 42", resp.CorrelationID)
	}

	var got protocol.ApiVersionsResponse
	if err := got.Decode(bytes.NewReader(resp.Payload)); err != nil {
		t.Fatalf("ApiVersionsResponse.Decode error: %v", err)
	}

	if len(got.ApiKeys) < 3 {
		t.Fatalf("len(ApiKeys) = %d, want at least 3", len(got.ApiKeys))
	}

	keys := make(map[int16]protocol.ApiVersion)
	for _, v := range got.ApiKeys {
		keys[v.ApiKey] = v
	}

	if _, ok := keys[0]; !ok {
		t.Errorf("ApiKeys does not contain Produce (0)")
	}
	if _, ok := keys[1]; !ok {
		t.Errorf("ApiKeys does not contain Fetch (1)")
	}
	if _, ok := keys[12]; !ok {
		t.Errorf("ApiKeys does not contain ApiVersions (12)")
	}
}

func TestBroker_bilinmeyen_api_key_UnsupportedVersion_doner(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{
		ApiKey:        99,
		ApiVersion:    1,
		CorrelationID: 77,
		ClientID:      "unknown",
		Payload:       []byte("payload"),
	}
	sendRequest(t, conn, req)
	resp := readResponse(t, conn)

	if resp.ErrorCode != server.ErrUnsupportedVersion {
		t.Fatalf("ErrorCode = %d, want %d (UnsupportedVersion)", resp.ErrorCode, server.ErrUnsupportedVersion)
	}
	if resp.CorrelationID != 77 {
		t.Fatalf("CorrelationID = %d, want 77", resp.CorrelationID)
	}
}

func TestBroker_Produce_stub_UnknownError_doner(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	req := &protocol.RequestFrame{
		ApiKey:        0,
		ApiVersion:    1,
		CorrelationID: 33,
		ClientID:      "produce",
		Payload:       []byte{},
	}
	sendRequest(t, conn, req)
	resp := readResponse(t, conn)

	if resp.ErrorCode != server.ErrUnknown {
		t.Fatalf("ErrorCode = %d, want %d (UnknownError)", resp.ErrorCode, server.ErrUnknown)
	}
	if resp.CorrelationID != 33 {
		t.Fatalf("CorrelationID = %d, want 33", resp.CorrelationID)
	}
}

func TestBroker_ApiVersions_birden_fazlaki_cevap_donebilir(t *testing.T) {
	cfg := testConfig(t)
	b, addr := startBroker(t, cfg)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial error: %v", err)
	}
	defer conn.Close()

	for i := int32(1); i <= 3; i++ {
		req := &protocol.RequestFrame{
			ApiKey:        12,
			ApiVersion:    0,
			CorrelationID: i,
			ClientID:      "multi",
			Payload:       []byte{},
		}
		sendRequest(t, conn, req)
		resp := readResponse(t, conn)
		if resp.ErrorCode != 0 {
			t.Fatalf("request %d: ErrorCode = %d, want 0", i, resp.ErrorCode)
		}
		if resp.CorrelationID != i {
			t.Fatalf("request %d: CorrelationID = %d, want %d", i, resp.CorrelationID, i)
		}
	}
}
