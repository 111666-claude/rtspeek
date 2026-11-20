package rtspeek

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
)

// watchServer is a scriptable RTSP server for Watch tests. It binds a fixed
// address so Close + start again on the same address models a camera
// rebooting: clients reconnect to the same URL after an outage.
type watchServer struct {
	addr string

	mu sync.RWMutex
	// toggles, changeable while the server is running.
	auth        bool   // challenge DESCRIBE with 401
	redirectTo  string // when set, DESCRIBE answers 302 here
	lossPct     int    // 0-100 probability of skipping an RTP packet
	jitter      time.Duration
	packetGap   time.Duration // nominal spacing, default 10ms
	playReject  bool          // reject PLAY with 400 to break the media path

	srv       *gortsplib.Server
	stream    *gortsplib.ServerStream
	media     *description.Media
	sendersWg sync.WaitGroup
	stopCh    chan struct{} // closed when the current server generation stops

	// observations
	describeCalls atomic.Int64
	setupCalls    atomic.Int64
	authOK        atomic.Int64
}

func newWatchServer(t *testing.T) *watchServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	h := &watchServer{addr: addr, packetGap: 10 * time.Millisecond}
	h.media = &description.Media{
		Type: description.MediaTypeVideo,
		Formats: []format.Format{
			&format.H264{PayloadTyp: 96, PacketizationMode: 1},
		},
	}
	h.start(t)
	t.Cleanup(func() { h.stop() })
	return h
}

func (h *watchServer) url() string { return "rtsp://" + h.addr + "/watch" }

func (h *watchServer) start(t *testing.T) {
	t.Helper()
	stopCh := make(chan struct{})
	srv := &gortsplib.Server{RTSPAddress: h.addr, Handler: h}
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	stream := &gortsplib.ServerStream{Server: srv, Desc: &description.Session{Medias: []*description.Media{h.media}}}
	if err := stream.Initialize(); err != nil {
		t.Fatalf("stream init: %v", err)
	}
	h.mu.Lock()
	h.srv = srv
	h.stream = stream
	h.stopCh = stopCh
	h.mu.Unlock()
}

func (h *watchServer) stop() {
	h.mu.Lock()
	srv := h.srv
	stream := h.stream
	stopCh := h.stopCh
	h.srv = nil
	h.mu.Unlock()
	if srv == nil {
		return
	}
	close(stopCh)
	stream.Close()
	srv.Close()
	h.sendersWg.Wait()
}

// restart models a camera reboot: same address, fresh server, fresh RTP
// sequence space.
func (h *watchServer) restart(t *testing.T) {
	t.Helper()
	h.stop()
	h.start(t)
}

func (h *watchServer) setAuth(v bool) {
	h.mu.Lock()
	h.auth = v
	h.mu.Unlock()
}

func (h *watchServer) setRedirect(target string) {
	h.mu.Lock()
	h.redirectTo = target
	h.mu.Unlock()
}

func (h *watchServer) setLossPct(pct int) {
	h.mu.Lock()
	h.lossPct = pct
	h.mu.Unlock()
}

func (h *watchServer) setJitter(d time.Duration) {
	h.mu.Lock()
	h.jitter = d
	h.mu.Unlock()
}

func (h *watchServer) setPlayReject(v bool) {
	h.mu.Lock()
	h.playReject = v
	h.mu.Unlock()
}

func (h *watchServer) snapshot() (lossPct int, jitter time.Duration, gap time.Duration) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lossPct, h.jitter, h.packetGap
}

func (h *watchServer) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	h.describeCalls.Add(1)
	h.mu.RLock()
	auth := h.auth
	redir := h.redirectTo
	stream := h.stream
	h.mu.RUnlock()
	if auth {
		if len(ctx.Request.Header["Authorization"]) == 0 {
			return &base.Response{
				StatusCode: base.StatusUnauthorized,
				Header:     base.Header{"Www-Authenticate": base.HeaderValue{"Digest realm=\"watch\", nonce=\"n1\""}},
			}, nil, nil
		}
		h.authOK.Add(1)
	}
	if redir != "" {
		return &base.Response{
			StatusCode: base.StatusFound,
			Header:     base.Header{"Location": base.HeaderValue{redir}},
		}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

func (h *watchServer) OnSetup(*gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	h.setupCalls.Add(1)
	h.mu.RLock()
	stream := h.stream
	h.mu.RUnlock()
	return &base.Response{StatusCode: base.StatusOK}, stream, nil
}

func (h *watchServer) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	h.mu.RLock()
	reject := h.playReject
	stream := h.stream
	stopCh := h.stopCh
	h.mu.RUnlock()
	if reject {
		return &base.Response{StatusCode: base.StatusBadRequest}, nil
	}
	h.sendersWg.Add(1)
	go h.sendUntilSessionEnds(ctx, stream, stopCh)
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// sendUntilSessionEnds streams packets for one PLAY. Each new session starts
// its sequence numbers fresh at a random base, modelling a real camera after
// reconnect; the client must not treat the reset as loss.
func (h *watchServer) sendUntilSessionEnds(ctx *gortsplib.ServerHandlerOnPlayCtx, stream *gortsplib.ServerStream, stopCh chan struct{}) {
	defer h.sendersWg.Done()
	deadline := time.Now().Add(2 * time.Second)
	for ctx.Session.State() != gortsplib.ServerSessionStatePlay {
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-stopCh:
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
	seq := uint16(watchRand())
	ts := uint32(0)
	for {
		select {
		case <-stopCh:
			return
		default:
		}
		lossPct, jitter, gap := h.snapshot()
		time.Sleep(gap)
		if lossPct > 0 && int(watchRand()%100) < lossPct {
			seq++
			continue
		}
		ts += 3600
		if err := stream.WritePacketRTP(h.media, &rtp.Packet{
			Header: rtp.Header{
				Version: 2, PayloadType: 96, SequenceNumber: seq,
				Timestamp: ts, SSRC: 4242, Marker: true,
			},
			Payload: []byte{0x41, 0x9a, 0x00, 0x01, 0x02, 0x03},
		}); err != nil {
			return
		}
		seq++
		if jitter > 0 {
			time.Sleep(time.Duration(watchRand() % uint32(jitter)))
		}
	}
}

// watchRand is a small xorshift PRNG shared by the test servers, so tests do
// not depend on global seeding.
var watchRandState atomic.Uint32

func init() {
	watchRandState.Store(2463534242)
}

func watchRand() uint32 {
	for {
		v := watchRandState.Add(0)
		next := v ^ (v << 13)
		next ^= next >> 17
		next ^= next << 5
		if watchRandState.CompareAndSwap(v, next) && next != 0 {
			return next % 100000
		}
	}
}
