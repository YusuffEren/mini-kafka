package server

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/YusuffEren/mini-kafka/internal/protocol"
)

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
}

// NewServer creates a server that will listen on addr (e.g. ":9092"). The
// provided mux is used to dispatch request frames to registered handlers.
// maxConnections limits the number of concurrently active client connections;
// connections accepted beyond that limit are immediately closed. A value of
// zero or less disables the limit (unlimited connections).
func NewServer(addr string, mux *Mux, maxConnections int) *Server {
	return &Server{
		addr:     addr,
		mux:      mux,
		done:     make(chan struct{}),
		maxConns: int64(maxConnections),
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
func (s *Server) handleConn(conn net.Conn) {
	atomic.AddInt64(&s.activeConns, 1)
	defer atomic.AddInt64(&s.activeConns, -1)

	defer conn.Close()

	for {
		req, err := protocol.ReadRequestFrame(conn)
		if err != nil {
			// EOF, timeout, frame error or any other read failure ends the
			// connection. There is nothing to report back to the client.
			return
		}

		resp := s.mux.Dispatch(req)
		if _, err := resp.Write(conn); err != nil {
			return
		}
	}
}
