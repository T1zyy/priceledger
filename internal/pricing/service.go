package pricing

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/T1zyy/priceledger/internal/platform"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

//go:embed schema.sql
var schema string

func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	return platform.Migrate(ctx, db, 54001, schema)
}

type Price struct {
	SKU      string `json:"sku"`
	Category string `json:"category"`
	Cost     int64  `json:"cost_kopecks"`
	Markup   int64  `json:"markup_bps"`
	Amount   int64  `json:"price_kopecks"`
	Version  int64  `json:"version"`
}

type Service struct {
	DB    *pgxpool.Pool
	Cache *redis.Client
}

const columns = "sku,category,cost_kopecks,markup_bps,price_kopecks,version"

type scanner interface{ Scan(...any) error }

func scan(row scanner) (p Price, e error) {
	e = row.Scan(&p.SKU, &p.Category, &p.Cost, &p.Markup, &p.Amount, &p.Version)
	return
}

func Calculate(cost, markup int64) (int64, error) {
	if cost < 1 || cost > 1_000_000_000_000 || markup < 0 || markup > 100_000 {
		return 0, fmt.Errorf("%w: cost or markup out of range", platform.Invalid)
	}
	return (cost*(10_000+markup) + 5_000) / 10_000, nil
}

type event struct {
	ID    string `json:"event_id"`
	Type  string `json:"type"`
	Price Price  `json:"price"`
}

func save(ctx context.Context, tx pgx.Tx, sku, category string, cost, markup int64) (Price, error) {
	amount, e := Calculate(cost, markup)
	if e != nil {
		return Price{}, e
	}
	p, e := scan(tx.QueryRow(ctx, `INSERT INTO pricing.products AS p VALUES ($1,$2,$3,$4,$5,1)
 ON CONFLICT(sku) DO UPDATE SET category=excluded.category,cost_kopecks=excluded.cost_kopecks,markup_bps=excluded.markup_bps,price_kopecks=excluded.price_kopecks,version=p.version+1
 WHERE (p.category,p.cost_kopecks,p.markup_bps) IS DISTINCT FROM (excluded.category,excluded.cost_kopecks,excluded.markup_bps) RETURNING `+columns, sku, category, cost, markup, amount))
	if e == pgx.ErrNoRows {
		return scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM pricing.products WHERE sku=$1", sku))
	}
	if e != nil {
		return p, e
	}
	body, e := json.Marshal(p)
	if e != nil {
		return p, e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO pricing.history(sku,version,snapshot) VALUES ($1,$2,$3)", sku, p.Version, body); e != nil {
		return p, e
	}
	body, e = json.Marshal(event{fmt.Sprintf("%s:%d", p.SKU, p.Version), "price.changed", p})
	if e != nil {
		return p, e
	}
	_, e = tx.Exec(ctx, "INSERT INTO pricing.outbox(sku,payload) VALUES ($1,$2)", sku, body)
	return p, e
}

func (s Service) SetProduct(ctx context.Context, sku, category string, cost int64) (p Price, e error) {
	if !platform.ID(sku) || !platform.ID(category) {
		return p, platform.Invalid
	}
	if _, e = Calculate(cost, 0); e != nil {
		return
	}
	e = platform.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		var markup int64
		if err := tx.QueryRow(ctx, "SELECT markup_bps FROM pricing.rules WHERE category=$1 FOR SHARE", category).Scan(&markup); err != nil {
			return err
		}
		var err error
		p, err = save(ctx, tx, sku, category, cost, markup)
		return err
	})
	return
}

func (s Service) SetRule(ctx context.Context, category string, markup int64) error {
	if !platform.ID(category) {
		return platform.Invalid
	}
	if _, e := Calculate(1, markup); e != nil {
		return e
	}
	return platform.WithTx(ctx, s.DB, func(tx pgx.Tx) error {
		tag, e := tx.Exec(ctx, `INSERT INTO pricing.rules VALUES($1,$2) ON CONFLICT(category) DO UPDATE SET markup_bps=excluded.markup_bps WHERE pricing.rules.markup_bps<>excluded.markup_bps`, category, markup)
		if e != nil || tag.RowsAffected() == 0 {
			return e
		}
		rows, e := tx.Query(ctx, "SELECT "+columns+" FROM pricing.products WHERE category=$1 ORDER BY sku FOR UPDATE", category)
		if e != nil {
			return e
		}
		var products []Price
		for rows.Next() {
			p, e := scan(rows)
			if e != nil {
				rows.Close()
				return e
			}
			products = append(products, p)
		}
		rows.Close()
		if e = rows.Err(); e != nil {
			return e
		}
		for _, p := range products {
			if _, e = save(ctx, tx, p.SKU, category, p.Cost, markup); e != nil {
				return e
			}
		}
		return nil
	})
}

type Revision struct {
	Price     Price     `json:"price"`
	ChangedAt time.Time `json:"changed_at"`
}

func (s Service) History(ctx context.Context, sku string, after int64) ([]Revision, error) {
	if !platform.ID(sku) || after < 0 {
		return nil, platform.Invalid
	}
	rows, e := s.DB.Query(ctx, "SELECT snapshot,changed_at FROM pricing.history WHERE sku=$1 AND version>$2 ORDER BY version LIMIT 100", sku, after)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	result := []Revision{}
	for rows.Next() {
		var r Revision
		if e = rows.Scan(&r.Price, &r.ChangedAt); e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
