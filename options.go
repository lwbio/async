package async

import "time"

type SubscriptionOption func(*Subscription)

func WithSubscription(name string) SubscriptionOption {
	return func(s *Subscription) { s.Identity = name }
}
func WithConcurrency(workers, prefetch int) SubscriptionOption {
	return func(s *Subscription) { s.Concurrency = workers; s.Prefetch = prefetch }
}
func WithRetries(delays ...time.Duration) SubscriptionOption {
	return func(s *Subscription) { s.Retries = append([]time.Duration{}, delays...) }
}
