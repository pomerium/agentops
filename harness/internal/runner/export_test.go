package runner

func WithOutboxLimit(n int) Option { return func(o *options) { o.outboxMax = n } }
