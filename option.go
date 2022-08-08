package kafetrain

import "time"

type consumerOptionConfig struct {
	StartFrom time.Time
	EndsAt    time.Time
	Offset    int64
	Limit     int64
	Silent    bool

	partitionsMap map[int32]int64
}

func newConsumerOptionConfig() *consumerOptionConfig {
	return &consumerOptionConfig{
		Offset: OffsetNewest,
	}
}

// Option sets a parameter for the logger.
type Option interface {
	Apply(cfg *consumerOptionConfig)
}

type optionFn func(cfg *consumerOptionConfig)

func (fn optionFn) Apply(cfg *consumerOptionConfig) {
	fn(cfg)
}

func WithStartFrom(from time.Time) Option {
	return optionFn(func(cfg *consumerOptionConfig) {
		cfg.StartFrom = from
	})
}

func WithEndsAt(endsAt time.Time) Option {
	return optionFn(func(cfg *consumerOptionConfig) {
		cfg.EndsAt = endsAt
	})
}

func WithOffset(offset int64) Option {
	return optionFn(func(cfg *consumerOptionConfig) {
		cfg.Offset = offset
	})
}

func WithLimit(limit int64) Option {
	return optionFn(func(cfg *consumerOptionConfig) {
		cfg.Limit = limit
	})
}

func WithoutCommit() Option {
	return optionFn(func(cfg *consumerOptionConfig) {
		cfg.Silent = true
	})
}
