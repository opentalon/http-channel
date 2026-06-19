package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"time"

	httpchannel "github.com/opentalon/http-channel/channel"
	"github.com/opentalon/opentalon/pkg/channel"
)

func main() {
	os.Exit(run())
}

func run() int {
	addr := flag.String("addr", "0.0.0.0:9100", "HTTP server address (host:port)")
	path := flag.String("path", "/chat", "HTTP endpoint path")
	origins := flag.String("origins", "", "Comma-separated allowed CORS origins (empty = allow all)")
	timeout := flag.Duration("timeout", 120*time.Second, "Max time to wait for the LLM response per request")
	flag.Parse()

	cfg := httpchannel.Config{
		Addr:    *addr,
		Path:    *path,
		Timeout: *timeout,
	}
	if *origins != "" {
		for _, o := range strings.Split(*origins, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				cfg.CORSOrigins = append(cfg.CORSOrigins, o)
			}
		}
	}

	ch := httpchannel.New(cfg)
	defer func() { _ = ch.Stop() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := channel.Serve(ctx, ch); err != nil && ctx.Err() == nil {
		_, _ = os.Stderr.WriteString("channel serve: " + err.Error() + "\n")
		return 1
	}
	return 0
}
