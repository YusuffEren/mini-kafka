package server

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

// DefaultMaxRequestBytes is the server-level cap on a single request frame
// body (excluding the leading int32 size field) applied when ServerConfig
// leaves MaxRequestBytes at zero. It is intentionally smaller than the
// protocol MaxFrameSize so the server rejects oversized frames before
// allocating the full payload.
const DefaultMaxRequestBytes int32 = 16 * 1024 * 1024 // 16 MiB

// ServerConfig configures a Server. Zero-valued duration fields disable the
// corresponding deadline; a zero MaxRequestBytes falls back to
// DefaultMaxRequestBytes.
type ServerConfig struct {
	// Addr is the TCP address the server listens on (e.g. ":9092").
	Addr string
	// MaxConnections limits the number of concurrently active client
	// connections. Connections accepted beyond that limit are immediately
	// closed. A value of zero or less disables the limit.
	MaxConnections int
	// IdleTimeout is the maximum duration the server will wait for a client
	// to send a complete request frame before closing the connection. It is
	// reset before every read. A value of zero disables the read deadline
	// (the connection can stay idle indefinitely). It must be greater than
	// the maximum long-poll MaxWaitMs honored by the broker, otherwise
	// long-polling fetch requests would be aborted by the idle deadline
	// before they complete.
	IdleTimeout time.Duration
	// WriteTimeout is the maximum duration the server will allow for
	// writing a response frame. It is reset before every write. A value of
	// zero disables the write deadline.
	WriteTimeout time.Duration
	// MaxRequestBytes is the server-level cap on a single request frame
	// body size. Frames whose declared size exceeds this limit are rejected
	// without reading the body, and the connection is closed. A value of
	// zero falls back to DefaultMaxRequestBytes; a negative value disables
	// the server-level limit (only the protocol MaxFrameSize cap applies).
	MaxRequestBytes int32
}

// Server accepts TCP connections and processes mini-kafka protocol frames.
type Server struct {
	addr        string
	listener    net.Listener
	mu          sync.RWMutex
	mux         *Mux
	done        chan struct{}
	wg          sync.WaitGroup
	maxConns    int64
	activeConns int64
	cfg         ServerConfig
}

// NewServer creates a server from cfg that will listen on cfg.Addr. The
// provided mux is used to dispatch request frames to registered handlers.
// cfg.MaxConnections limits the number of concurrently active client
// connections; connections accepted beyond that limit are immediately closed.
// A value of zero or less disables the limit (unlimited connections).
func NewServer(cfg ServerConfig, mux *Mux) *Server {
	return &Server{
		addr:     cfg.Addr,
		mux:      mux,
		done:     make(chan struct{}),
		maxConns: int64(cfg.MaxConnections),
		cfg:      cfg,
	}
}

// Start begins accepting connections. It blocks until the server is shut down
// (via Shutdown) or the underlying listener fails. It returns nil when the
// server has been shut down cleanly, or the error from net.Listen /
// listener.Accept otherwise.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener returns a usable error; check whether we
			// were shut down to distinguish graceful stop from a real error.
			select {
			case <-s.done:
				return nil
			default:
			}
			return err
		}

		// Reject connections that would exceed the configured concurrency
		// limit. The connection is accepted from the listener (so the kernel
		// backlog drains) but immediately closed without spawning a handler.
		if s.maxConns > 0 && atomic.LoadInt64(&s.activeConns) >= s.maxConns {
			_ = conn.Close()
			continue
		}

		// Account for the connection before spawning the handler goroutine.
		// Incrementing inside handleConn would open a race window in which
		// many goroutines pass the limit check above before any of them gets
		// to increment the counter, allowing the real connection count to
		// exceed maxConns. The matching decrement is performed by handleConn
		// when it returns.
		atomic.AddInt64(&s.activeConns, 1)

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// Addr returns the network address the server is listening on, or nil if not listening.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Shutdown gracefully stops the server. It closes the listener and waits for
// all active connections to finish. The provided context is consulted only to
// honor cancellation: if it is cancelled before in-flight connections have
// drained, Shutdown returns ctx.Err(); otherwise it returns nil.
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.done:
		// Already shut down.
	default:
		close(s.done)
	}

	s.mu.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handleConn processes frames from a single TCP connection until the client
// disconnects or a read/write error occurs. It runs in its own goroutine.
//
// Per-iteration deadlines protect the server from slowloris-style resource
// exhaustion: IdleTimeout bounds the time spent waiting for the next request
// frame, and WriteTimeout bounds the time spent writing a response. Both are
// reset at the start of each loop iteration so a single slow request does not
// poison subsequent ones.
func (s *Server) handleConn(conn net.Conn) {
	// activeConns was incremented by Start before this goroutine was
	// spawned; decrement it when the connection handler exits.
	defer atomic.AddInt64(&s.activeConns, -1)

	defer func() { _ = conn.Close() }()

	maxReqBytes := s.cfg.MaxRequestBytes
	if maxReqBytes == 0 {
		maxReqBytes = DefaultMaxRequestBytes
	}

	// Wrap the connection in buffered readers/writers to coalesce small reads
	// and writes. A request frame is preceded by an int32 size field followed
	// by the body, which without buffering translates to 8+ read syscalls per
	// request; similarly a response is written in multiple small chunks. The
	// 64 KiB buffers match a typical TCP send/receive window and keep per-conn
	// memory bounded.
	//
	// SetReadDeadline/SetWriteDeadline keep operating on the underlying conn,
	// which is what the bufio wrappers read from / write to, so the per-iter
	// deadline semantics are preserved.
	br := bufio.NewReaderSize(conn, 64*1024)
	bw := bufio.NewWriterSize(conn, 64*1024)

	for {
		if s.cfg.IdleTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}

		req, err := protocol.ReadRequestFrameWithLimit(br, maxReqBytes)
		if err != nil {
			// EOF, timeout, frame error or any other read failure ends the
			// connection. There is nothing to report back to the client.
			return
		}

		resp := s.mux.Dispatch(req)

		if s.cfg.WriteTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		}
		if _, err := resp.Write(bw); err != nil {
			return
		}
		// Flush after every response: bufio.Writer buffers writes locally and
		// will not push them to the conn until the buffer fills or Flush is
		// called. Skipping the flush leaves the response stuck in the buffer
		// and the client deadlocks waiting for a reply that never arrives.
		if err := bw.Flush(); err != nil {
			return
		}
	}
}
