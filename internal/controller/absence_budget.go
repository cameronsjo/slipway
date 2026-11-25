package controller

import (
	"context"
	"time"
)

// maintainAbsenceBudget bounds the TOTAL time one maintain pass spends waiting
// for torn-down workers to prove absence. Each wait is already bounded (20 s in
// the Kubernetes backend, #951), but maintain destroys workers one after
// another, so twelve slots that never prove absence would spend about 240 s
// before PollSweep. With the budget spent, each later worker gets a single
// absence read, the behavior before #995. A worker short of proof keeps its
// reservation either way, so this only trades waiting for one more cycle.
const maintainAbsenceBudget = 30 * time.Second

type absenceBudgetKey struct{}

// WithAbsenceBudget returns ctx carrying the instant after which absence waits
// stop polling and return their latest reading.
func WithAbsenceBudget(ctx context.Context, deadline time.Time) context.Context {
	return context.WithValue(ctx, absenceBudgetKey{}, deadline)
}

// AbsenceBudget reports the shared absence-wait deadline carried by ctx, if any.
func AbsenceBudget(ctx context.Context) (time.Time, bool) {
	deadline, ok := ctx.Value(absenceBudgetKey{}).(time.Time)
	return deadline, ok && !deadline.IsZero()
}
