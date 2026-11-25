package pricing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/T1zyy/priceledger/internal/platform"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const Topic = "prices.changed"

func cacheKey(sku string) string { return "pricing:v1:" + sku }

// Fixed-width version strings compare exactly even beyond Lua's integer precision.
var setNewer = redis.NewScript(`local old=redis.call('HGET',KEYS[1],'version')
if not old or ARGV[1]>=old then
 redis.call('HSET',KEYS[1],'version',ARGV[1],'data',ARGV[2])
 redis.call('EXPIRE',KEYS[1],60)
end
return 1`)

func (s Service) cachePrice(ctx context.Context, p Price) error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	c, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	return setNewer.Run(c, s.Cache, []string{cacheKey(p.SKU)}, fmt.Sprintf("%020d", p.Version), b).Err()
}

func (s Service) Get(ctx context.Context, sku string) (Price, error) {
	if !platform.ID(sku) {
		return Price{}, platform.Invalid
	}
	c, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	b, e := s.Cache.HGet(c, cacheKey(sku), "data").Bytes()
	cancel()
	if e == nil {
		var p Price
		if json.Unmarshal(b, &p) == nil && p.SKU == sku && p.Version > 0 {
			return p, nil
		}
	}
	p, e := scan(s.DB.QueryRow(ctx, "SELECT "+columns+" FROM pricing.products WHERE sku=$1", sku))
	if e == nil {
		_ = s.cachePrice(ctx, p)
	}
	return p, e
}

type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

func (s Service) PublishOne(ctx context.Context, w Publisher) (bool, error) {
	worked := false
	e := platform.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		var id int64
		var key string
		var body []byte
		err := tx.QueryRow(ctx, "SELECT id,sku,payload FROM pricing.outbox WHERE published_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED").Scan(&id, &key, &body)
		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err = w.WriteMessages(c, kafka.Message{Key: []byte(key), Value: body}); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE pricing.outbox SET published_at=now() WHERE id=$1", id)
		worked = true
		return err
	})
	return worked, e
}

func (s Service) Project(ctx context.Context, m kafka.Message) error {
	var ev event
	if json.Unmarshal(m.Value, &ev) != nil || ev.Type != "price.changed" || !platform.ID(ev.Price.SKU) || ev.Price.Version < 1 || ev.ID != fmt.Sprintf("%s:%d", ev.Price.SKU, ev.Price.Version) || string(m.Key) != ev.Price.SKU {
		_, e := s.DB.Exec(ctx, "INSERT INTO pricing.rejected VALUES($1,$2,'invalid envelope') ON CONFLICT DO NOTHING", m.Partition, m.Offset)
		return e
	}
	return platform.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		tag, e := tx.Exec(ctx, "INSERT INTO pricing.processed(event_id) VALUES($1) ON CONFLICT DO NOTHING", ev.ID)
		if e != nil || tag.RowsAffected() == 0 {
			return e
		}
		// A replay after cache eviction must also project the latest database version.
		p, e := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM pricing.products WHERE sku=$1", ev.Price.SKU))
		if e != nil {
			return e
		}
		return s.cachePrice(ctx, p)
	})
}
