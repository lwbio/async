package async

import "sync/atomic"

// Stats is a point-in-time snapshot. Counters include internal result/retry publishes.
type Stats struct {
	Published, Unroutable, Unknown, Reconnects, Retries, DeadLetters, InFlight int64
}

type runtimeStats struct {
	published, unroutable, unknown, reconnects, retries, deadLetters, inFlight atomic.Int64
}

func (r *Runtime) Stats() Stats {
	s := &r.stats
	return Stats{Published: s.published.Load(), Unroutable: s.unroutable.Load(), Unknown: s.unknown.Load(), Reconnects: s.reconnects.Load(), Retries: s.retries.Load(), DeadLetters: s.deadLetters.Load(), InFlight: s.inFlight.Load()}
}
