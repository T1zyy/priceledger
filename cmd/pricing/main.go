package main

import (
	"context"
	"net/http"
	"time"

	pb "github.com/T1zyy/priceledger/api"
	"github.com/T1zyy/priceledger/internal/platform"
	"github.com/T1zyy/priceledger/internal/pricing"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func main() {
	platform.Main(func(rt *platform.Runtime) (*http.ServeMux, func(), error) {
		if e := pricing.Migrate(rt.Context, rt.DB); e != nil {
			return nil, nil, e
		}
		cache := redis.NewClient(&redis.Options{Addr: platform.Env("REDIS_ADDR", "localhost:6379"), MaxRetries: -1, ContextTimeoutEnabled: true, DialTimeout: 200 * time.Millisecond})
		svc := pricing.Service{DB: rt.DB, Cache: cache}
		writer := &kafka.Writer{Addr: kafka.TCP(rt.Brokers...), Topic: pricing.Topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 10 * time.Millisecond}
		pb.RegisterPricingServer(rt.RPC, pricing.RPC{Service: svc})
		rt.Worker(func(ctx context.Context) {
			platform.Poll(ctx, "outbox", func(ctx context.Context) (bool, error) { return svc.PublishOne(ctx, writer) })
		})
		rt.Worker(func(ctx context.Context) {
			platform.Consume(ctx, rt.Brokers, pricing.Topic, "pricing-cache-v1", svc.Project)
		})
		return svc.Routes(), func() { writer.Close(); cache.Close() }, nil
	})
}
