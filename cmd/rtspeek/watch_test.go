package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	rtpeek "github.com/0x524A/rtspeek/pkg/rtspeek"
	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
)

// cliWatchServer is a minimal healthy RTSP server for the CLI watch test.
type cliWatchServer struct {
	srv    *gortsplib.Server
	stream *gortsplib.ServerStream
}

func startCLIWatchServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	media := &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{&format.H264{PayloadTyp: 96, PacketizationMode: 1}},
	}
	h := &cliWatchServer{}
	h.srv = &gortsplib.Server{RTSPAddress: addr, Handler: h}
	if err := h.srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	h.stream = &gortsplib.ServerStream{Server: h.srv, Desc: &description.Session{Medias: []*description.Media{media}}}
	if err := h.stream.Initialize(); err != nil {
		t.Fatalf("stream: %v", err)
	}
	t.Cleanup(func() {
		h.stream.Close()
		h.srv.Close()
	})
	return "rtsp://" + addr + "/x"
}

func (h *cliWatchServer) OnDescribe(*gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	return &base.Response{StatusCode: base.StatusOK}, h.stream, nil
}

func (h *cliWatchServer) OnSetup(*gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	return &base.Response{StatusCode: base.StatusOK}, h.stream, nil
}

func (h *cliWatchServer) OnPlay(*gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// lockedBuffer is safe for concurrent Write/String use: the Watch callback
// writes while the test polls the output.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunWatchEmitsNDJSON(t *testing.T) {
	url := startCLIWatchServer(t)
	var buf lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())

	opts := rtpeek.WatchOptions{
		Timeout:        2 * time.Second,
		Interval:       100 * time.Millisecond,
		BackoffInitial: 50 * time.Millisecond,
		BackoffMax:     time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- runWatch(ctx, url, opts, &buf, false) }()

	deadline := time.Now().Add(3 * time.Second)
	var lines int
	for time.Now().Before(deadline) {
		lines = strings.Count(strings.TrimRight(buf.String(), "\n"), "\n") + 1
		if lines >= 2 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWatch after cancel must return nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runWatch did not stop")
	}

	dec := json.NewDecoder(strings.NewReader(buf.String()))
	n := 0
	sawOK, sawCancelled := false, false
	for dec.More() {
		var ev rtpeek.WatchEvent
		if err := dec.Decode(&ev); err != nil {
			t.Fatalf("line %d is not JSON: %v", n+1, err)
		}
		if ev.Time.IsZero() {
			t.Fatalf("event %d has no timestamp", n)
		}
		if ev.Type == rtpeek.EventProbeOK {
			sawOK = true
		}
		if ev.Type == rtpeek.EventCancelled {
			sawCancelled = true
		}
		n++
	}
	if !sawOK || !sawCancelled {
		t.Fatalf("missing events (ok=%v cancelled=%v) in %d lines", sawOK, sawCancelled, n)
	}
}

// A listener that accepts but never answers RTSP: every probe times out and
// --fail must turn the streak into a non-nil error.
func TestRunWatchFailFlag(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				buf := make([]byte, 256)
				for {
					if _, err := c.Read(buf); err != nil {
						c.Close()
						return
					}
				}
			}(c)
		}
	}()

	var buf lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	opts := rtpeek.WatchOptions{
		Timeout:        250 * time.Millisecond,
		Interval:       100 * time.Millisecond,
		BackoffInitial: 40 * time.Millisecond,
		BackoffMax:     200 * time.Millisecond,
	}
	done := make(chan error, 1)
	go func() { done <- runWatch(ctx, "rtsp://"+ln.Addr().String()+"/x", opts, &buf, true) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("--fail must return an error once a probe failed")
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("runWatch with --fail did not return on probe failure")
	}
	cancel()
}

func TestRunWatchBadOptions(t *testing.T) {
	var buf lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := runWatch(ctx, "rtsp://127.0.0.1:1/x", rtpeek.WatchOptions{
		BackoffInitial: time.Second,
		BackoffMax:     10 * time.Millisecond,
	}, &buf, false)
	if err == nil {
		t.Fatal("invalid watch options must be rejected")
	}
}
