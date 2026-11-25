package pricing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/T1zyy/priceledger/internal/testdb"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func TestCalculate(t *testing.T) {
	for _, tc := range []struct{ cost, markup, want int64 }{{12345, 0, 12345}, {101, 5000, 152}, {1, 4999, 1}, {1, 5000, 2}, {1000000000000, 100000, 11000000000000}} {
		got, e := Calculate(tc.cost, tc.markup)
		if e != nil || got != tc.want {
			t.Errorf("Calculate(%d,%d)=%d,%v want %d", tc.cost, tc.markup, got, e, tc.want)
		}
	}
	for _, bad := range [][2]int64{{0, 20}, {1, -1}, {1000000000001, 0}, {20, 100001}} {
		if _, e := Calculate(bad[0], bad[1]); e == nil {
			t.Error("accepted", bad)
		}
	}
}

type recorder struct {
	messages []kafka.Message
	err      error
}

func (r *recorder) WriteMessages(_ context.Context, m ...kafka.Message) error {
	if r.err != nil {
		return r.err
	}
	r.messages = append(r.messages, m...)
	return nil
}

func TestPricingLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testdb.Open(t)
	if e := Migrate(ctx, db); e != nil {
		t.Fatal(e)
	}
	cache := redis.NewClient(&redis.Options{Addr: os.Getenv("TEST_REDIS_ADDR")})
	defer cache.Close()
	s := Service{db, cache}
	api := s.Routes()
	sku := "sku-" + db.Config().ConnConfig.Database
	request := func(method, path, body string, want int) []byte {
		t.Helper()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
		if w.Code != want {
			t.Fatalf("%s %s -> %d %s, want %d", method, path, w.Code, w.Body, want)
		}
		return w.Body.Bytes()
	}
	request("PUT", "/v1/rules/dairy", `{"mark_up":12}`, 400)
	request("PUT", "/v1/rules/dairy", `{"markup_bps":2500}`, 204)
	request("PUT", "/v1/products/"+sku, `{"category":"dairy","cost_kopecks":8000}`, 200)
	request("PUT", "/v1/products/"+sku, `{"category":"dairy","cost_kopecks":8000}`, 200)
	check := func(want int64) {
		t.Helper()
		var p Price
		if e := json.Unmarshal(request("GET", "/v1/prices/"+sku, "", 200), &p); e != nil || p.Amount != want {
			t.Fatalf("price=%+v error=%v", p, e)
		}
	}
	check(10000)
	request("PUT", "/v1/rules/dairy", `{"markup_bps":5000}`, 204)
	request("PUT", "/v1/rules/dairy", `{"markup_bps":5000}`, 204)
	var history []Revision
	if e := json.Unmarshal(request("GET", "/v1/prices/"+sku+"/history", "", 200), &history); e != nil || len(history) != 2 || history[1].Price.Amount != 12000 || history[1].Price.Version != 2 {
		t.Fatalf("history=%v %v", history, e)
	}
	w := &recorder{err: errors.New("broker offline")}
	if _, e := s.PublishOne(ctx, w); e == nil {
		t.Fatal("expected broker error")
	}
	w.err = nil
	for i := 0; i < 3; i++ {
		if _, e := s.PublishOne(ctx, w); e != nil {
			t.Fatal(e)
		}
	}
	if len(w.messages) != 2 {
		t.Fatalf("lost or extra outbox rows: %d", len(w.messages))
	}
	for _, i := range []int{1, 0, 1} {
		if e := s.Project(ctx, w.messages[i]); e != nil {
			t.Fatal(e)
		}
	}
	check(12000)
	var count int
	if e := db.QueryRow(ctx, "SELECT count(*) FROM pricing.processed").Scan(&count); e != nil || count != 2 {
		t.Fatalf("dedup count=%d, %v", count, e)
	}
	if e := cache.Del(ctx, cacheKey(sku)).Err(); e != nil {
		t.Fatal(e)
	}
	check(12000)
}
