// Command print-queue is the service half of the Prusa Connect print queue module.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/prusa3d/prusa-connect/sdk/extension"
	"github.com/prusa3d/prusa-connect/sdk/routesapi"
	"github.com/prusa3d/prusa-connect/sdk/wire"
	"github.com/prusa3d/prusa-connect/sdk/wirerpc"
)

const (
	name    = "print-queue"
	release = extension.Version("0.1.0")
)

//go:embed openapi.json
var openAPI []byte

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	line, err := (wire.Handshake{
		Protocol: wire.ProtocolMajor,
		MinMinor: wire.MinMinor,
		MaxMinor: wire.MaxMinor,
		Address:  wire.StdioAddress,
	}).Line()
	if err != nil {
		return fmt.Errorf("render handshake: %w", err)
	}

	if _, err = os.Stdout.WriteString(line); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}

	methods := wirerpc.Methods{
		wirerpc.MethodInfo: func(context.Context, wirerpc.Call) (any, error) {
			return routesapi.Describe(
				wirerpc.Info{Name: name, Version: release, Contract: wirerpc.Contract},
				openAPI,
			)
		},
		wirerpc.MethodShutdown: func(context.Context, wirerpc.Call) (any, error) {
			return nil, nil
		},
		routesapi.MethodServe: routesapi.Serve(routes()),
	}

	conn, err := wirerpc.NewConn(stdio{}, wirerpc.Options{
		Role:    wirerpc.RoleChild,
		Methods: methods,
	})
	if err != nil {
		return fmt.Errorf("build connection: %w", err)
	}

	if err = conn.Serve(ctx); err != nil {
		return fmt.Errorf("serve: %w", err)
	}

	return nil
}

func routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	return mux
}

type stdio struct{}

func (stdio) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdio) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdio) Close() error                { return os.Stdin.Close() }
