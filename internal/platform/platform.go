package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	Invalid    = errors.New("invalid input")
	Missing    = errors.New("not found")
	Conflict   = errors.New("conflict")
	identifier = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9._-]{0,95}$")
)

func ID(s string) bool { return identifier.MatchString(s) }

func Env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func WithTx(ctx context.Context, db *pgxpool.Pool, f func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, db, f)
}

func Migrate(ctx context.Context, db *pgxpool.Pool, key int64, schema string) error {
	return WithTx(ctx, db, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "select pg_advisory_xact_lock($1)", key); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, schema)
		return e
	})
}

func Decode(w http.ResponseWriter, r *http.Request, v any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("%w: malformed request body", Invalid)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: expected one JSON object", Invalid)
	}
	return nil
}

func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func classify(err error) (int, codes.Code) {
	switch {
	case errors.Is(err, Invalid):
		return 400, codes.InvalidArgument
	case errors.Is(err, Missing), errors.Is(err, pgx.ErrNoRows):
		return 404, codes.NotFound
	case errors.Is(err, Conflict):
		return 409, codes.FailedPrecondition
	case errors.Is(err, context.DeadlineExceeded):
		return 504, codes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		return 408, codes.Canceled
	default:
		return 500, codes.Internal
	}
}

func message(code int, err error) string {
	if code >= 500 {
		slog.Error("operation failed", "err", err)
		return http.StatusText(code)
	}
	if code == 404 {
		return "not found"
	}
	return err.Error()
}

func Fail(w http.ResponseWriter, err error) {
	code, _ := classify(err)
	JSON(w, code, map[string]string{"error": message(code, err)})
}

func RPCError(err error) error {
	code, rpc := classify(err)
	return status.Error(rpc, message(code, err))
}

func Serve(ctx context.Context, db *pgxpool.Pool, mux *http.ServeMux, rpc *grpc.Server) error {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if db.Ping(c) != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		w.WriteHeader(200)
	})
	httpLn, e := net.Listen("tcp", Env("HTTP_ADDR", ":8080"))
	if e != nil {
		return e
	}
	defer httpLn.Close()
	grpcLn, e := net.Listen("tcp", Env("GRPC_ADDR", ":9090"))
	if e != nil {
		return e
	}
	defer grpcLn.Close()
	srv := &http.Server{Handler: http.TimeoutHandler(mux, 15*time.Second, "request timed out"), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- srv.Serve(httpLn) }()
	go func() { errs <- rpc.Serve(grpcLn) }()
	slog.Info("listening", "http", httpLn.Addr(), "grpc", grpcLn.Addr())
	select {
	case <-ctx.Done():
	case e = <-errs:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { rpc.GracefulStop(); close(done) }()
	_ = srv.Shutdown(shutdown)
	select {
	case <-done:
	case <-shutdown.Done():
		rpc.Stop()
	}
	return e
}

func Pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func Poll(ctx context.Context, name string, job func(context.Context) (bool, error)) {
	for ctx.Err() == nil {
		worked, e := job(ctx)
		if e != nil && ctx.Err() == nil {
			slog.Error(name, "err", e)
		}
		if e != nil || !worked {
			Pause(ctx, time.Second)
		}
	}
}
