// Package integration exercises the public protocols of the running Compose stack.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	pb "github.com/T1zyy/priceledger/api"
	"github.com/T1zyy/priceledger/internal/pricing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDemo(t *testing.T) {
	if os.Getenv("LIVE_DEMO") != "1" {
		t.Skip("run make demo against the Compose stack")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	id := fmt.Sprintf("demo-%d", time.Now().UnixNano())
	client := http.Client{Timeout: 4 * time.Second}
	call := func(method, url, key string, in, out any) {
		t.Helper()
		body, e := json.Marshal(in)
		if e != nil {
			t.Fatal(e)
		}
		req, e := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			msg, _ := io.ReadAll(resp.Body)
			t.Fatalf("%s %s: %s %s", method, url, resp.Status, msg)
		}
		if out != nil {
			if e = json.NewDecoder(resp.Body).Decode(out); e != nil {
				t.Fatal(e)
			}
		}
	}
	wait := func(step string, done func() bool) {
		t.Helper()
		for !done() {
			select {
			case <-ctx.Done():
				t.Fatalf("%s: %v", step, ctx.Err())
			case <-time.After(150 * time.Millisecond):
			}
		}
	}
	const priceURL = "http://pricing:8080/v1"
	wait("pricing ready", func() bool {
		r, e := client.Get("http://pricing:8080/healthz")
		if e != nil {
			return false
		}
		r.Body.Close()
		return r.StatusCode == 200
	})
	call("PUT", priceURL+"/rules/"+id, "", map[string]int{"markup_bps": 2000}, nil)
	var first pricing.Price
	call("PUT", priceURL+"/products/"+id, "", map[string]any{"category": id, "cost_kopecks": 10000}, &first)
	if first.Amount != 12000 || first.Version != 1 {
		t.Fatal(first)
	}
	cache := redis.NewClient(&redis.Options{Addr: "redis:6379"})
	defer cache.Close()
	cached := func(amount int64) bool {
		raw, e := cache.HGet(ctx, "pricing:v1:"+id, "data").Bytes()
		var p pricing.Price
		return e == nil && json.Unmarshal(raw, &p) == nil && p.Amount == amount
	}
	wait("outbox -> Kafka -> Redis, version 1", func() bool { return cached(12000) })
	call("PUT", priceURL+"/rules/"+id, "", map[string]int{"markup_bps": 2500}, nil)
	wait("outbox -> Kafka -> Redis, version 2", func() bool { return cached(12500) })
	var history []pricing.Revision
	call("GET", priceURL+"/prices/"+id+"/history", "", nil, &history)
	if len(history) != 2 || history[1].Price.Version != 2 {
		t.Fatal(history)
	}
	conn, e := grpc.NewClient("pricing:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	price, e := pb.NewPricingClient(conn).GetPrice(ctx, &pb.PriceQuery{Sku: id})
	if e != nil || price.GetPriceKopecks() != 12500 {
		t.Fatal(price, e)
	}
	t.Logf("%s: 10000 -> 12000 -> 12500 kopecks, 2 revisions; Kafka projection, Redis and gRPC agree", id)
}
