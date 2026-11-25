package platform

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Fetch, handle and commit one record at a time. Never advance past a transient failure.
func Consume(ctx context.Context, brokers []string, topic, group string, handle func(context.Context, kafka.Message) error) {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 500 * time.Millisecond, StartOffset: kafka.FirstOffset})
	defer r.Close()
	for ctx.Err() == nil {
		m, e := r.FetchMessage(ctx)
		if e != nil {
			if ctx.Err() == nil {
				slog.Warn("fetch", "topic", topic, "err", e)
				Pause(ctx, time.Second)
			}
			continue
		}
		for ctx.Err() == nil {
			if e = handle(ctx, m); e == nil {
				break
			}
			slog.Warn("retry record", "topic", topic, "partition", m.Partition, "offset", m.Offset, "err", e)
			Pause(ctx, time.Second)
		}
		for ctx.Err() == nil {
			if e = r.CommitMessages(ctx, m); e == nil {
				break
			}
			slog.Warn("retry commit", "err", e)
			Pause(ctx, time.Second)
		}
	}
}
