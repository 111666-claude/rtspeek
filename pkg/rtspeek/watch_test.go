package rtspeek

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// watchRun collects the events a Watch invocation emits.
type watchRun struct {
	mu     sync.Mutex
	events []WatchEvent
}

func (r *watchRun) collect(ev WatchEvent) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *watchRun) snapshot() []WatchEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]WatchEvent, len(r.events))
	copy(out, r.events)
	return out
}

func (r *watchRun) types() []string {
	evs := r.snapshot()
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

func (r *watchRun) waitFor(t *testing.T, typ string, timeout time.Duration) WatchEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range r.snapshot() {
			if ev.Type == typ {
				return ev
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for event %q; got %v", typ, r.types())
	return WatchEvent{}
}

// waitForFailures waits for an event of typ whose consecutive-failure counter
// is at least n. waitFor alone returns the first event of a type, which still
// carries the old counter during a streak.
func (r *watchRun) waitForFailures(t *testing.T, typ string, n int, timeout time.Duration) WatchEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range r.snapshot() {
			if ev.Type == typ && ev.ConsecutiveFailures >= n {
				return ev
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q with failures>=%d; got %v", typ, n, r.types())
	return WatchEvent{}
}

func (r *watchRun) count(typ string) int {
	n := 0
	for _, ev := range r.snapshot() {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

func fastWatch() WatchOptions {
	return WatchOptions{
		Timeout:        2 * time.Second,
		Interval:       120 * time.Millisecond,
		BackoffInitial: 60 * time.Millisecond,
		BackoffMax:     time.Second,
	}
}

func fastAnalyzeWatch() WatchOptions {
	o := fastWatch()
	o.Analyze = &AnalyzeOptions{
		Duration:           300 * time.Millisecond,
		Transport:          TransportTCP,
		FirstPacketTimeout: 500 * time.Millisecond,
	}
	return o
}

func TestWatchOptionsValidate(t *testing.T) {
	if err := fastWatch().Validate(); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	bad := WatchOptions{BackoffInitial: 10 * time.Second, BackoffMax: time.Second}
	if err := bad.Validate(); err == nil {
		t.Fatal("backoff initial above max must be rejected")
	}
	bad2 := fastAnalyzeWatch()
	bad2.Analyze.Duration = 10 * time.Minute
	if err := bad2.Validate(); err == nil {
		t.Fatal("analyze validation must propagate")
	}
	o := WatchOptions{}
	n, err := o.normalize()
	if err != nil {
		t.Fatal(err)
	}
	if n.Interval != DefaultWatchInterval || n.BackoffInitial != DefaultWatchBackoffInitial ||
		n.BackoffMax != DefaultWatchBackoffMax || n.Timeout != 5*time.Second {
		t.Fatalf("defaults not applied: %+v", n)
	}
}

func TestWatchRequiresCallbackAndURL(t *testing.T) {
	if err := Watch(context.Background(), "rtsp://127.0.0.1:1/x", fastWatch(), nil); err == nil {
		t.Fatal("nil callback must error")
	}
	if err := Watch(context.Background(), "http://127.0.0.1/x", fastWatch(), func(WatchEvent) {}); !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("bad URL must return ErrInvalidURL, got %v", err)
	}
}

func TestWatchHealthyDescribeProbes(t *testing.T) {
	srv := newWatchServer(t)
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())

	go func() { _ = Watch(ctx, srv.url(), fastWatch(), run.collect) }()
	ok := run.waitFor(t, EventProbeOK, 3*time.Second)
	if ok.State != WatchStateUp || ok.Metrics == nil || ok.Metrics.LatencyMs <= 0 || ok.Metrics.MediaCount != 1 {
		t.Fatalf("bad probe_ok event: %+v", ok)
	}
	if ev := run.waitFor(t, EventStateUp, time.Second); ev.State != WatchStateUp {
		t.Fatalf("bad state_up: %+v", ev)
	}
	// Two further probes at the interval.
	second := run.waitFor(t, EventProbeOK, 3*time.Second)
	_ = run.waitFor(t, EventProbeOK, 3*time.Second)
	if second.ConsecutiveFailures != 0 {
		t.Fatalf("failures should stay 0 while up, got %d", second.ConsecutiveFailures)
	}
	cancel()
	run.waitFor(t, EventCancelled, time.Second)
}

func TestWatchOutageAndRecovery(t *testing.T) {
	srv := newWatchServer(t)
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = Watch(ctx, srv.url(), fastWatch(), run.collect) }()
	run.waitFor(t, EventProbeOK, 3*time.Second)

	// Outage: close the server; probes must fail with backoff and a counted
	// streak of failures.
	srv.stop()
	down := run.waitFor(t, EventStateDown, 3*time.Second)
	if down.State != WatchStateDown || down.ConsecutiveFailures != 1 || down.Reason == "" {
		t.Fatalf("bad state_down: %+v", down)
	}
	fail := run.waitFor(t, EventProbeFailed, time.Second)
	if fail.Reason != ReasonConnectionRefused && fail.Reason != ReasonTimeout &&
		fail.Reason != ReasonConnectionClosed {
		t.Fatalf("unexpected failure reason %q", fail.Reason)
	}
	bo := run.waitFor(t, EventBackoff, time.Second)
	if bo.BackoffMs <= 0 {
		t.Fatalf("backoff must carry a delay, got %+v", bo)
	}
	secondFail := run.waitForFailures(t, EventProbeFailed, 2, 3*time.Second)
	if secondFail.ConsecutiveFailures < 2 {
		t.Fatalf("failure streak not counted, got %d", secondFail.ConsecutiveFailures)
	}

	// Recovery: same URL, fresh server.
	srv.start(t)
	up := run.waitFor(t, EventStateUp, 4*time.Second)
	if up.ConsecutiveFailures != 0 {
		t.Fatalf("recovery must reset the failure counter, got %d", up.ConsecutiveFailures)
	}
	ok := run.waitFor(t, EventProbeOK, time.Second)
	if ok.Metrics == nil || ok.Metrics.MediaCount != 1 {
		t.Fatalf("post-recovery probe missing metrics: %+v", ok)
	}
	// No state_down after recovery.
	if run.count(EventStateDown) != 1 {
		t.Fatalf("expected exactly one state_down, got %d", run.count(EventStateDown))
	}
}

func TestWatchTimeoutProbe(t *testing.T) {
	// A listener that accepts and stays silent: the dial succeeds but
	// OPTIONS/DESCRIBE never answer, so the probe must hit its timeout.
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
			go func(conn net.Conn) {
				buf := make([]byte, 1024)
				for {
					if _, err := conn.Read(buf); err != nil {
						conn.Close()
						return
					}
				}
			}(c)
		}
	}()

	o := fastWatch()
	o.Timeout = 300 * time.Millisecond
	o.BackoffInitial = 40 * time.Millisecond
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = Watch(ctx, "rtsp://"+ln.Addr().String()+"/x", o, run.collect) }()
	fail := run.waitFor(t, EventProbeFailed, 3*time.Second)
	if fail.Reason != ReasonTimeout {
		t.Fatalf("reason = %q, want %q", fail.Reason, ReasonTimeout)
	}
}

func TestWatchAuthRejectedThenAccepted(t *testing.T) {
	srv := newWatchServer(t)
	srv.setAuth(true)

	o := fastWatch()
	o.Timeout = 2 * time.Second
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No credentials in the URL: the 401 challenge is a hard failure.
	go func() { _ = Watch(ctx, srv.url(), o, run.collect) }()
	fail := run.waitFor(t, EventProbeFailed, 3*time.Second)
	if fail.Reason != ReasonAuthRequired || fail.StatusCode != 401 {
		t.Fatalf("expected auth_required/401, got %+v", fail)
	}

	// Credentials supplied: the library retries with an Authorization header
	// and the same URL keeps working across every reconnect.
	cancel()
	run.waitFor(t, EventCancelled, time.Second)

	authedURL := "rtsp://viewer:secret@" + srv.addr + "/watch"
	run2 := &watchRun{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() { _ = Watch(ctx2, authedURL, o, run2.collect) }()
	ok := run2.waitFor(t, EventProbeOK, 3*time.Second)
	if ok.Metrics == nil {
		t.Fatal("authenticated probe should succeed")
	}
	if srv.authOK.Load() == 0 {
		t.Fatal("server never saw an Authorization header")
	}
	// Passwords must never appear in event text.
	for _, ev := range run2.snapshot() {
		if strings.Contains(ev.Message, "secret") || strings.Contains(ev.RedirectLocation, "secret") {
			t.Fatalf("credential leaked into event: %+v", ev)
		}
	}
}

func TestWatchRedirectObserved(t *testing.T) {
	target := newWatchServer(t)
	front := newWatchServer(t)
	front.setRedirect(target.url() + "?via=redirect")

	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, front.url(), fastWatch(), run.collect) }()

	redir := run.waitFor(t, EventRedirect, 3*time.Second)
	if redir.StatusCode != 302 || redir.RedirectLocation == "" {
		t.Fatalf("bad redirect event: %+v", redir)
	}
	// gortsplib follows the redirect within the same probe, so it still
	// succeeds.
	run.waitFor(t, EventProbeOK, 3*time.Second)
}

func TestWatchCancellationEndsLoop(t *testing.T) {
	srv := newWatchServer(t)
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- Watch(ctx, srv.url(), fastWatch(), run.collect) }()
	run.waitFor(t, EventProbeOK, 3*time.Second)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Watch should return the context error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after cancellation")
	}
	c := run.waitFor(t, EventCancelled, time.Second)
	if c.Type != EventCancelled {
		t.Fatalf("final event = %q, want cancelled", c.Type)
	}
}

func TestBackoffIsCappedAndExponential(t *testing.T) {
	const initial, max = 100 * time.Millisecond, 800 * time.Millisecond
	// Deterministic component only; jitter is checked separately.
	got := make([]time.Duration, 0, 6)
	for n := 1; n <= 6; n++ {
		// Average out the jitter by sampling.
		var sum time.Duration
		const samples = 200
		for range samples {
			sum += backoffDelay(initial, max, n)
		}
		got = append(got, sum/samples)
	}
	// Exponential growth then a cap; the cap includes +/-20% jitter, so
	// allow headroom.
	if got[0] < 80*time.Millisecond || got[0] > 120*time.Millisecond {
		t.Fatalf("first backoff = %v, want ~%v", got[0], initial)
	}
	for i := 1; i < 4; i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("backoff not growing: %v", got)
		}
	}
	for i := 4; i < len(got); i++ {
		if got[i] > max+100*time.Millisecond {
			t.Fatalf("backoff above cap: %v", got[i])
		}
	}

	// Every concrete delay stays within the documented bounds.
	for n := 1; n < 20; n++ {
		d := backoffDelay(initial, max, n)
		// Cap may reach max*(1+jitter); initial never goes negative.
		if d <= 0 || d > time.Duration(float64(max)*1.25) {
			t.Fatalf("delay out of bounds at n=%d: %v", n, d)
		}
	}
}

func TestWatchEmptyWindowEvent(t *testing.T) {
	srv := newWatchServer(t)
	// Stop the stream's packet sender by restarting with a handler whose
	// OnPlay sends nothing: swap the stream out via a silent server on the
	// same address.
	srv.stop()
	silent := startSilentServer(t, srv.addr)
	defer silent.Close()

	o := fastAnalyzeWatch()
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, "rtsp://"+srv.addr+"/watch", o, run.collect) }()

	run.waitFor(t, EventEmptyWindow, 4*time.Second)
}

// A DESCRIBE that succeeds but whose PLAY is rejected must move the watch to
// a down state with reason analysis_failed, then recover after the media path
// is repaired without losing credentials or transport settings.
func TestWatchAnalysisFailureThenRecovery(t *testing.T) {
	srv := newWatchServer(t)
	srv.setPlayReject(true)

	o := fastAnalyzeWatch()
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, srv.url(), o, run.collect) }()

	down := run.waitFor(t, EventStateDown, 4*time.Second)
	if down.Reason != ReasonAnalysisFailed {
		t.Fatalf("reason = %q, want %q; events: %v", down.Reason, ReasonAnalysisFailed, run.types())
	}
	fail := run.waitFor(t, EventProbeFailed, time.Second)
	if fail.Reason != ReasonAnalysisFailed || fail.ConsecutiveFailures != 1 {
		t.Fatalf("bad analysis failure event: %+v", fail)
	}
	run.waitFor(t, EventBackoff, time.Second)

	// Repair: PLAY succeeds again; the failure streak resets and the next
	// window carrying traffic reports metrics over the same TCP transport.
	srv.setPlayReject(false)
	run.waitFor(t, EventStateUp, 5*time.Second)
	deadline := time.Now().Add(6 * time.Second)
	var ok WatchEvent
	for time.Now().Before(deadline) {
		for _, ev := range run.snapshot() {
			if ev.Type == EventProbeOK && ev.Metrics != nil && ev.Metrics.Packets > 0 {
				ok = ev
			}
		}
		if ok.Type != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ok.Type == "" {
		t.Fatalf("no recovered window with packets; events: %v", run.types())
	}
	if ok.Metrics.Transport != "tcp" {
		t.Fatalf("recovered window missing TCP metrics: %+v", ok.Metrics)
	}
}

func TestWatchAnalyzeLossAndJitter(t *testing.T) {
	srv := newWatchServer(t)
	srv.setLossPct(40)

	o := fastAnalyzeWatch()
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, srv.url(), o, run.collect) }()

	ok := run.waitFor(t, EventProbeOK, 4*time.Second)
	if ok.Metrics == nil || ok.Metrics.Packets == 0 {
		t.Fatalf("expected packets in metrics: %+v", ok.Metrics)
	}
	if ok.Metrics.Lost == 0 && ok.Metrics.LossPercent == 0 {
		t.Fatalf("40%% loss should show in metrics: %+v", ok.Metrics)
	}
	if ok.Metrics.Transport != "tcp" {
		t.Fatalf("transport choice must persist across probes, got %q", ok.Metrics.Transport)
	}
}

func TestWatchJitterEvent(t *testing.T) {
	srv := newWatchServer(t)
	// Sleeps up to ~40ms on top of the 10ms cadence, well above the 5ms
	// warn threshold.
	srv.setJitter(40 * time.Millisecond)

	o := fastAnalyzeWatch()
	o.Analyze.Thresholds.JitterWarnMs = 5
	o.Analyze.Thresholds.JitterErrorMs = 100000
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, srv.url(), o, run.collect) }()

	jev := run.waitFor(t, EventJitter, 4*time.Second)
	if jev.Metrics == nil || jev.Metrics.MaxJitterMs < 5 {
		t.Fatalf("bad jitter event: %+v", jev)
	}
}

// TestWatchFreshSessionSequenceNumbers proves the requirement that a
// reconnect does not reuse the old RTP sequence space: the test server
// restarts with a different random sequence base, and the new probe reports
// zero loss rather than a huge gap.
func TestWatchFreshSessionSequenceNumbers(t *testing.T) {
	srv := newWatchServer(t)
	o := fastAnalyzeWatch()
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, srv.url(), o, run.collect) }()

	first := run.waitFor(t, EventProbeOK, 4*time.Second)
	if first.Metrics == nil || first.Metrics.Lost != 0 {
		t.Fatalf("first window should be lossless: %+v", first.Metrics)
	}
	srv.restart(t)
	// Wait until at least one post-restart probe completes.
	deadline := time.Now().Add(6 * time.Second)
	var second WatchEvent
	for time.Now().Before(deadline) {
		// The restart itself should not produce a state_down storm beyond
		// possibly one outage event; focus on the recovered window.
		for _, ev := range run.snapshot() {
			if ev.Type == EventProbeOK && ev.Time.After(first.Time) && ev.Metrics != nil && ev.Metrics.Packets > 0 {
				second = ev
			}
		}
		if second.Type != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if second.Type == "" {
		t.Fatalf("no post-restart probe_ok; events: %v", run.types())
	}
	if second.Metrics.Lost != 0 || second.Metrics.LossPercent != 0 {
		t.Fatalf("new session must not inherit old sequence numbers: %+v", second.Metrics)
	}
}

func TestWatchEventOrderingOnFlap(t *testing.T) {
	srv := newWatchServer(t)
	o := fastAnalyzeWatch()
	o.Interval = 150 * time.Millisecond
	o.BackoffInitial = 50 * time.Millisecond
	run := &watchRun{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Watch(ctx, srv.url(), o, run.collect) }()

	run.waitFor(t, EventProbeOK, 4*time.Second)

	// Flap: stop, wait, restart. Expect, in order, state_down ->
	// probe_failed -> backoff -> ... -> state_up -> probe_ok.
	srv.stop()
	run.waitFor(t, EventStateDown, 3*time.Second)
	srv.start(t)
	run.waitFor(t, EventStateUp, 5*time.Second)

	evs := run.snapshot()
	idx := map[string]int{}
	for i, ev := range evs {
		if _, ok := idx[ev.Type]; !ok {
			idx[ev.Type] = i
		}
	}
	for _, pair := range [][2]string{
		{EventStateDown, EventProbeFailed},
		{EventProbeFailed, EventBackoff},
		{EventStateUp, EventProbeOK},
	} {
		if idx[pair[0]] >= idx[pair[1]] {
			t.Fatalf("event %s must precede %s; order was %v", pair[0], pair[1], eventTypes(evs))
		}
	}
	// Timestamps must be monotonic.
	for i := 1; i < len(evs); i++ {
		if evs[i].Time.Before(evs[i-1].Time) {
			t.Fatalf("timestamps not monotonic at %d", i)
		}
	}
}

func eventTypes(evs []WatchEvent) []string {
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = fmt.Sprintf("%s(%d)", ev.Type, ev.ConsecutiveFailures)
	}
	return out
}
