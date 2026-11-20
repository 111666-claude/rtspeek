package rtspeek

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
)

// Watch state values carried on WatchEvent.State. They describe the probe
// stream's state machine, not one probe's outcome.
const (
	// WatchStateStarting is the state before the first probe completes.
	WatchStateStarting = "starting"
	// WatchStateUp means the last probe succeeded.
	WatchStateUp = "up"
	// WatchStateDown means the last probe failed; consecutive failures are
	// counted on WatchEvent.ConsecutiveFailures.
	WatchStateDown = "down"
)

// Watch event types. Once released these strings are stable.
const (
	// EventProbeOK: one probe succeeded. Metrics holds the window summary.
	EventProbeOK = "probe_ok"
	// EventProbeFailed: one probe failed. Reason/StatusCode carry the details.
	EventProbeFailed = "probe_failed"
	// EventStateUp: the stream recovered, i.e. a probe succeeded after a failure.
	EventStateUp = "state_up"
	// EventStateDown: the first failure after one or more successes.
	EventStateDown = "state_down"
	// EventBackoff: a probe failed and the next attempt is delayed BackoffMs.
	EventBackoff = "backoff"
	// EventRedirect: the server answered with a 3xx redirect. gortsplib
	// follows it within the same probe, so this is observational.
	EventRedirect = "redirect"
	// EventEmptyWindow: an analyze window received no RTP packets at all.
	EventEmptyWindow = "empty_window"
	// EventJitter: an analyze window measured jitter at or above the
	// configured warning threshold.
	EventJitter = "jitter"
	// EventCancelled: the caller's context ended; this is the final event.
	EventCancelled = "cancelled"
)

// Watch defaults and limits.
const (
	// DefaultWatchInterval is used when WatchOptions.Interval is zero.
	DefaultWatchInterval = 10 * time.Second
	// DefaultWatchBackoffInitial is the first delay after a failed probe.
	DefaultWatchBackoffInitial = time.Second
	// DefaultWatchBackoffMax caps the exponential backoff, per requirement
	// that failure handling have an explicit upper bound.
	DefaultWatchBackoffMax = 60 * time.Second
	// watchBackoffFactor doubles the delay on each consecutive failure.
	watchBackoffFactor = 2
	// watchBackoffJitterFraction randomizes the delay by +/-20% so many
	// clients do not retry in lockstep after a shared outage.
	watchBackoffJitterFraction = 0.2
)

// ReasonAnalysisFailed classifies a probe whose DESCRIBE succeeded but whose
// analyze window failed (SETUP/PLAY rejection, interruption, cancellation
// inside the window).
const ReasonAnalysisFailed = "analysis_failed"

// WatchOptions configures Watch. The probe semantics reuse Options: Timeout
// bounds dial/OPTIONS/DESCRIBE, and Analyze selects short-window analysis
// instead of a DESCRIBE-only probe.
type WatchOptions struct {
	// Timeout bounds dial, OPTIONS and DESCRIBE of every probe. Zero selects
	// the single-probe default of 5s.
	Timeout time.Duration
	// Analyze, when non-nil, makes every probe a short receive window instead
	// of a DESCRIBE. The same AnalyzeOptions (transport, duration, thresholds)
	// are reused for every reconnect: credentials and the TCP/UDP choice
	// survive outages.
	Analyze *AnalyzeOptions
	// Interval between successful probes (and the first probe fires
	// immediately). Zero selects DefaultWatchInterval.
	Interval time.Duration
	// BackoffInitial is the delay after the first consecutive failure. Zero
	// selects DefaultWatchBackoffInitial.
	BackoffInitial time.Duration
	// BackoffMax caps the exponential backoff. Zero selects
	// DefaultWatchBackoffMax.
	BackoffMax time.Duration
}

// Validate reports whether the options are acceptable, without running
// anything.
func (o WatchOptions) Validate() error {
	_, err := o.normalize()
	return err
}

func (o WatchOptions) normalize() (WatchOptions, error) {
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.Analyze != nil {
		if err := o.Analyze.Validate(); err != nil {
			return o, err
		}
	}
	if o.Interval <= 0 {
		o.Interval = DefaultWatchInterval
	}
	if o.BackoffInitial <= 0 {
		o.BackoffInitial = DefaultWatchBackoffInitial
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = DefaultWatchBackoffMax
	}
	if o.BackoffInitial > o.BackoffMax {
		return o, fmt.Errorf("watch backoff initial %v exceeds maximum %v", o.BackoffInitial, o.BackoffMax)
	}
	return o, nil
}

func (o WatchOptions) probeOptions() Options {
	return Options{Timeout: o.Timeout, Analyze: o.Analyze}
}

// WatchMetrics summarizes one successful probe window. In DESCRIBE-only mode
// only LatencyMs and MediaCount are meaningful.
type WatchMetrics struct {
	// LatencyMs is the wall-clock duration of dial + OPTIONS + DESCRIBE.
	LatencyMs float64 `json:"latency_ms"`
	// MediaCount is the number of media tracks in the SDP.
	MediaCount int `json:"media_count"`
	// WindowMs is the analyze receive window that actually elapsed; zero in
	// DESCRIBE-only mode.
	WindowMs float64 `json:"window_ms,omitempty"`
	// Packets is the total RTP packets received across tracks.
	Packets uint64 `json:"packets,omitempty"`
	// Lost is the total RTP packets reported lost across tracks.
	Lost uint64 `json:"lost,omitempty"`
	// LossPercent is the aggregate loss percentage across tracks.
	LossPercent float64 `json:"loss_percent,omitempty"`
	// MaxJitterMs is the largest per-track jitter in the window.
	MaxJitterMs float64 `json:"max_jitter_ms,omitempty"`
	// BitrateKbps is the aggregate measured bitrate across tracks.
	BitrateKbps float64 `json:"bitrate_kbps,omitempty"`
	// Transport is "udp", "tcp" etc.; empty in DESCRIBE-only mode.
	Transport string `json:"transport,omitempty"`
}

// WatchEvent is one timestamped observation from Watch.
type WatchEvent struct {
	// Time is when the event was produced (UTC, RFC3339 in JSON).
	Time time.Time `json:"time"`
	// Type is one of the Event* constants.
	Type string `json:"type"`
	// State is one of the WatchState* constants.
	State string `json:"state"`
	// Reason is the machine-readable failure reason (FailureReason values,
	// plus ReasonAnalysisFailed); empty on success events.
	Reason string `json:"reason,omitempty"`
	// Message is a human-readable, credential-redacted detail.
	Message string `json:"message,omitempty"`
	// StatusCode is the RTSP status code when the failure was an RTSP
	// response (e.g. 401, 404); zero otherwise.
	StatusCode int `json:"status_code,omitempty"`
	// RedirectLocation is the Location header of a 3xx response, redacted.
	RedirectLocation string `json:"redirect_location,omitempty"`
	// ConsecutiveFailures counts failures since the last successful probe.
	ConsecutiveFailures int `json:"consecutive_failures"`
	// BackoffMs is the delay before the next probe, on EventBackoff.
	BackoffMs int64 `json:"backoff_ms,omitempty"`
	// Metrics is the window summary on EventProbeOK; nil otherwise.
	Metrics *WatchMetrics `json:"metrics,omitempty"`
}

// watchResponseHookKey is the context key under which Watch installs its
// per-probe response observer.
type watchResponseHookKey struct{}

var responseHookKey = watchResponseHookKey{}

// withResponseHook returns ctx carrying fn, which NewRTSPSession picks up when
// it builds the probe's client.
func withResponseHook(ctx context.Context, fn func(*base.Response)) context.Context {
	return context.WithValue(ctx, responseHookKey, fn)
}

// Watch repeatedly probes url until ctx is cancelled. Every probe builds a
// fresh RTSP client through DescribeStreamWithOptions, so after a reconnect
// the new session starts with its own RTP sequence space instead of reusing
// the old one. Credentials come from url itself and the transport choice from
// opts.Analyze, so both are preserved across reconnects. The single-probe API
// (DescribeStream / DescribeStreamWithOptions) is unchanged.
//
// Each observation is delivered to onEvent; calls are serialized and happen
// from the calling goroutine. The final event is always EventCancelled, after
// which Watch returns ctx.Err() (nil only if ctx ended without an error, which
// practically does not happen).
func Watch(ctx context.Context, url string, opts WatchOptions, onEvent func(WatchEvent)) error {
	if onEvent == nil {
		return errors.New("watch requires an event callback")
	}
	o, err := opts.normalize()
	if err != nil {
		return err
	}
	if !ValidateURL(url) {
		return ErrInvalidURL
	}

	emit := func(ev WatchEvent) {
		ev.Time = time.Now().UTC()
		onEvent(ev)
	}

	failures := 0
	state := WatchStateStarting

	for {
		// Per-probe redirect observations. A 3xx is followed inside
		// gortsplib, so the probe itself may still succeed; the event is
		// emitted before the probe result so ordering stays chronological.
		var redirects []WatchEvent
		hook := func(res *base.Response) {
			if res.StatusCode >= base.StatusMovedPermanently &&
				res.StatusCode <= base.StatusUseProxy &&
				len(res.Header["Location"]) == 1 {
				redirects = append(redirects, WatchEvent{
					Type:             EventRedirect,
					State:            state,
					StatusCode:       int(res.StatusCode),
					RedirectLocation: RedactURL(res.Header["Location"][0]),
					Message:          fmt.Sprintf("RTSP %d redirect", res.StatusCode),
				})
			}
		}
		pctx := withResponseHook(ctx, hook)

		info, perr := DescribeStreamWithOptions(pctx, url, o.probeOptions())

		// A cancelled context wins over whatever the probe returned: its
		// timeout shares the same parent and a shutdown must look like a
		// cancellation, not a stream failure.
		if ctxErr := ctx.Err(); ctxErr != nil {
			emit(WatchEvent{
				Type:                EventCancelled,
				State:               state,
				ConsecutiveFailures: failures,
				Message:             "watch cancelled: " + ctxErr.Error(),
			})
			return ctxErr
		}

		for _, ev := range redirects {
			ev.ConsecutiveFailures = failures
			emit(ev)
		}

		if perr == nil {
			failures = 0
			metrics := buildWatchMetrics(info)
			if state != WatchStateUp {
				emit(WatchEvent{Type: EventStateUp, State: WatchStateUp, Message: "stream is reachable"})
				state = WatchStateUp
			}
			emit(WatchEvent{Type: EventProbeOK, State: WatchStateUp, Metrics: &metrics})

			// Analyze-level problems do not fail DESCRIBE; surface them as
			// explicit events. A window error (SETUP/PLAY rejected, stream
			// interrupted) means the media path is down even though
			// DESCRIBE answered, so it takes precedence over the softer
			// empty-window/jitter observations.
			if a := info.GetAnalysis(); a != nil {
				if a.Error != "" {
					failures++
					reason := ReasonAnalysisFailed
					if state == WatchStateUp {
						emit(WatchEvent{
							Type:                EventStateDown,
							State:               WatchStateDown,
							Reason:              reason,
							Message:             RedactText(a.Error),
							ConsecutiveFailures: failures,
						})
						state = WatchStateDown
					}
					emit(WatchEvent{
						Type:                EventProbeFailed,
						State:               WatchStateDown,
						Reason:              reason,
						Message:             RedactText(a.Error),
						ConsecutiveFailures: failures,
					})
					if waitErr := emitBackoffAndWait(ctx, emit, &state, failures, o); waitErr != nil {
						emit(WatchEvent{
							Type:                EventCancelled,
							State:               state,
							ConsecutiveFailures: failures,
							Message:             "watch cancelled: " + waitErr.Error(),
						})
						return waitErr
					}
					continue
				}
				if windowEmpty(a) {
					emit(WatchEvent{
						Type:    EventEmptyWindow,
						State:   WatchStateUp,
						Message: "analyze window received no RTP packets",
					})
				} else if code, msg, jitter := jitterFinding(a); code != "" {
					emit(WatchEvent{
						Type:    EventJitter,
						State:   WatchStateUp,
						Message: msg,
						Metrics: &WatchMetrics{MaxJitterMs: jitter},
					})
				}
			}

			if waitErr := waitWatch(ctx, o.Interval); waitErr != nil {
				emit(WatchEvent{
					Type:                EventCancelled,
					State:               state,
					ConsecutiveFailures: failures,
					Message:             "watch cancelled: " + waitErr.Error(),
				})
				return waitErr
			}
			continue
		}

		// Probe failure.
		failures++
		reason := FailureReason(perr)
		status := StatusCode(perr)
		message := RedactText(perr.Error())
		if state != WatchStateDown {
			emit(WatchEvent{
				Type:                EventStateDown,
				State:               WatchStateDown,
				Reason:              reason,
				StatusCode:          status,
				Message:             message,
				ConsecutiveFailures: failures,
			})
			state = WatchStateDown
		}
		emit(WatchEvent{
			Type:                EventProbeFailed,
			State:               WatchStateDown,
			Reason:              reason,
			StatusCode:          status,
			Message:             message,
			ConsecutiveFailures: failures,
		})
		if waitErr := emitBackoffAndWait(ctx, emit, &state, failures, o); waitErr != nil {
			emit(WatchEvent{
				Type:                EventCancelled,
				State:               state,
				ConsecutiveFailures: failures,
				Message:             "watch cancelled: " + waitErr.Error(),
			})
			return waitErr
		}
	}
}

// emitBackoffAndWait computes the capped exponential backoff, emits the
// EventBackoff and waits (interruptibly).
func emitBackoffAndWait(ctx context.Context, emit func(WatchEvent), state *string, failures int, o WatchOptions) error {
	delay := backoffDelay(o.BackoffInitial, o.BackoffMax, failures)
	emit(WatchEvent{
		Type:                EventBackoff,
		State:               *state,
		ConsecutiveFailures: failures,
		BackoffMs:           delay.Milliseconds(),
		Message:             fmt.Sprintf("waiting %v before retry", delay.Round(time.Millisecond)),
	})
	return waitWatch(ctx, delay)
}

// backoffDelay is the deterministic exponential component:
// initial * 2^(failures-1), capped at max.
func backoffDelay(initial, max time.Duration, failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	d := initial
	for range failures - 1 {
		if d >= max {
			break
		}
		d *= watchBackoffFactor
		if d > max || d <= 0 {
			d = max
			break
		}
	}
	if d > max {
		d = max
	}
	return jitterDelay(d)
}

// jitterDelay randomizes d by +/-watchBackoffJitterFraction while keeping the
// result non-negative and never above d's cap.
func jitterDelay(d time.Duration) time.Duration {
	span := float64(d) * watchBackoffJitterFraction
	out := float64(d) + (rand.Float64()*2-1)*span
	if out < float64(time.Millisecond) {
		out = float64(time.Millisecond)
	}
	return time.Duration(out)
}

// waitWait blocks for d or until ctx ends, returning ctx.Err() in the latter
// case.
func waitWatch(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// buildWatchMetrics condenses a probe result into a WatchMetrics window.
func buildWatchMetrics(info StreamInfo) WatchMetrics {
	m := WatchMetrics{
		LatencyMs:  info.LatencyMs(),
		MediaCount: info.GetMediaCount(),
	}
	a := info.GetAnalysis()
	if a == nil {
		return m
	}
	m.WindowMs = a.DurationMs
	m.Transport = a.Transport
	var packets, lost, bytes uint64
	var maxJitter float64
	for _, t := range a.Tracks {
		packets += t.Packets
		lost += t.Lost
		bytes += t.Bytes
		if t.JitterMs > maxJitter {
			maxJitter = t.JitterMs
		}
	}
	m.Packets = packets
	m.Lost = lost
	m.MaxJitterMs = maxJitter
	if total := packets + lost; total > 0 {
		m.LossPercent = round2(float64(lost) / float64(total) * 100)
	}
	if a.DurationMs > 0 {
		m.BitrateKbps = round2(float64(bytes) * 8 / a.DurationMs)
	}
	return m
}

// windowEmpty reports whether an analyze window saw no RTP traffic on any
// track. A window without tracks at all is also empty.
func windowEmpty(a *Analysis) bool {
	if a == nil || len(a.Tracks) == 0 {
		return true
	}
	for _, t := range a.Tracks {
		if t.Packets > 0 {
			return false
		}
	}
	return true
}

// jitterFinding returns the high_jitter finding's message and measured
// jitter when the window produced one.
func jitterFinding(a *Analysis) (code, msg string, jitterMs float64) {
	for _, f := range a.Findings {
		if f.Code == FindingHighJitter {
			if f.Value != nil {
				jitterMs = *f.Value
			}
			return f.Code, f.Message, jitterMs
		}
	}
	return "", "", 0
}
