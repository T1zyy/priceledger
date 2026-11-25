CREATE SCHEMA IF NOT EXISTS pricing;
CREATE TABLE IF NOT EXISTS pricing.rules(category text PRIMARY KEY, markup_bps bigint NOT NULL CHECK(markup_bps BETWEEN 0 AND 100000));
CREATE TABLE IF NOT EXISTS pricing.products(sku text PRIMARY KEY,category text NOT NULL REFERENCES pricing.rules, cost_kopecks bigint NOT NULL CHECK(cost_kopecks BETWEEN 1 AND 1000000000000),markup_bps bigint NOT NULL,price_kopecks bigint NOT NULL,version bigint NOT NULL CHECK(version>0));
CREATE INDEX IF NOT EXISTS products_category ON pricing.products(category,sku);
CREATE TABLE IF NOT EXISTS pricing.history(sku text NOT NULL,version bigint NOT NULL,snapshot jsonb NOT NULL,changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(sku,version));
CREATE TABLE IF NOT EXISTS pricing.outbox(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,sku text NOT NULL,payload jsonb NOT NULL,published_at timestamptz);
CREATE INDEX IF NOT EXISTS unpublished ON pricing.outbox(id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS pricing.processed(event_id text PRIMARY KEY,processed_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS pricing.rejected(partition integer NOT NULL, off bigint NOT NULL,reason text NOT NULL,PRIMARY KEY(partition,off));
