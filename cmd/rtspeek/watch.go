package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	rtpeek "github.com/0x524A/rtspeek/pkg/rtspeek"
)

// runWatch drives continuous probing and writes one JSON object per probe
// event (NDJSON) to out. It returns when ctx is cancelled, or immediately when
// fail is set and the first failed probe has been emitted.
func runWatch(ctx context.Context, url string, opts rtpeek.WatchOptions, out io.Writer, fail bool) error {
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)

	wctx, stopOnFailure := context.WithCancel(ctx)
	defer stopOnFailure()

	var firstErr error
	err := rtpeek.Watch(wctx, url, opts, func(ev rtpeek.WatchEvent) {
		if ev.Type == rtpeek.EventProbeFailed && firstErr == nil {
			firstErr = fmt.Errorf("probe failed: %s", ev.Reason)
			if fail {
				stopOnFailure()
			}
		}
		_ = enc.Encode(ev)
	})
	if fail && firstErr != nil {
		return firstErr
	}
	// A cancelled context is the normal way watch mode ends, not a CLI error.
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		return fmt.Errorf("watch stopped: %w", err)
	}
	return nil
}
