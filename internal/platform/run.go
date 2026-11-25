package platform

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type Runtime struct {
	Context context.Context
	DB      *pgxpool.Pool
	Brokers []string
	RPC     *grpc.Server
	workers sync.WaitGroup
}

func (r *Runtime) Worker(f func(context.Context)) {
	r.workers.Add(1)
	go func() { defer r.workers.Done(); f(r.Context) }()
}

func run(setup func(*Runtime) (*http.ServeMux, func(), error)) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, e := pgxpool.New(ctx, Env("DATABASE_URL", "postgres://commerce:localdev@localhost/commerce?sslmode=disable"))
	if e != nil {
		return e
	}
	defer db.Close()
	rt := &Runtime{Context: ctx, DB: db, Brokers: strings.Split(Env("KAFKA_BROKERS", "localhost:9092"), ","), RPC: grpc.NewServer()}
	mux, closeClients, e := setup(rt)
	if e != nil {
		return e
	}
	e = Serve(ctx, db, mux, rt.RPC)
	stop()
	rt.workers.Wait()
	closeClients()
	return e
}

func Main(setup func(*Runtime) (*http.ServeMux, func(), error)) {
	if e := run(setup); e != nil {
		slog.Error("service stopped", "err", e)
		os.Exit(1)
	}
}
