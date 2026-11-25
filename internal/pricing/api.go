package pricing

import (
	"context"
	"net/http"
	"strconv"

	pb "github.com/T1zyy/priceledger/api"
	"github.com/T1zyy/priceledger/internal/platform"
)

type RPC struct {
	pb.UnimplementedPricingServer
	Service Service
}

func (r RPC) GetPrice(ctx context.Context, q *pb.PriceQuery) (*pb.PriceSnapshot, error) {
	p, e := r.Service.Get(ctx, q.Sku)
	if e != nil {
		return nil, platform.RPCError(e)
	}
	return &pb.PriceSnapshot{Sku: p.SKU, Category: p.Category, CostKopecks: p.Cost, MarkupBps: p.Markup, PriceKopecks: p.Amount, Version: p.Version}, nil
}

func (s Service) Routes() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("PUT /v1/rules/{category}", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Markup *int64 `json:"markup_bps"`
		}
		if e := platform.Decode(w, r, &v); e != nil {
			platform.Fail(w, e)
			return
		}
		if v.Markup == nil {
			platform.Fail(w, platform.Invalid)
			return
		}
		if e := s.SetRule(r.Context(), r.PathValue("category"), *v.Markup); e != nil {
			platform.Fail(w, e)
			return
		}
		w.WriteHeader(204)
	})
	m.HandleFunc("PUT /v1/products/{sku}", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Category string `json:"category"`
			Cost     int64  `json:"cost_kopecks"`
		}
		if e := platform.Decode(w, r, &v); e != nil {
			platform.Fail(w, e)
			return
		}
		p, e := s.SetProduct(r.Context(), r.PathValue("sku"), v.Category, v.Cost)
		if e != nil {
			platform.Fail(w, e)
			return
		}
		platform.JSON(w, 200, p)
	})
	m.HandleFunc("GET /v1/prices/{sku}", func(w http.ResponseWriter, r *http.Request) {
		p, e := s.Get(r.Context(), r.PathValue("sku"))
		if e != nil {
			platform.Fail(w, e)
			return
		}
		platform.JSON(w, 200, p)
	})
	m.HandleFunc("GET /v1/prices/{sku}/history", func(w http.ResponseWriter, r *http.Request) {
		var after int64
		if raw := r.URL.Query().Get("after"); raw != "" {
			var e error
			after, e = strconv.ParseInt(raw, 10, 64)
			if e != nil {
				platform.Fail(w, platform.Invalid)
				return
			}
		}
		h, e := s.History(r.Context(), r.PathValue("sku"), after)
		if e != nil {
			platform.Fail(w, e)
			return
		}
		platform.JSON(w, 200, h)
	})
	return m
}
