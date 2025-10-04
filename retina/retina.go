package retina

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/bluesky-social/indigo/util"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	slogecho "github.com/samber/slog-echo"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/semaphore"
)

var tracer = otel.Tracer("retina")

type Retina struct {
	httpd             *http.Server
	metricsHttpd      *http.Server
	client            *http.Client
	echo              *echo.Echo
	logger            *slog.Logger
	downloadSemaphore *semaphore.Weighted
	ocrSemaphore      *semaphore.Weighted
	pdqPath           string
}

type Args struct {
	APIListenAddr         string
	MetricsAddr           string
	Debug                 bool
	Logger                *slog.Logger
	MaxConcurrentOCRExecs int64
	PdqPath               string
}

func New(args *Args) (*Retina, error) {
	level := slog.LevelInfo
	if args.Debug {
		level = slog.LevelDebug
	}

	if args.Logger == nil {
		args.Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		}))
	}

	client := util.RobustHTTPClient()
	client.Timeout = 5 * time.Second

	e := echo.New()

	e.Use(middleware.Recover())
	e.Use(middleware.RemoveTrailingSlash())
	e.Use(echoprometheus.NewMiddleware(""))

	slogEchoCfg := slogecho.Config{
		DefaultLevel:     level,
		ServerErrorLevel: slog.LevelError,
		WithResponseBody: true,
		Filters: []slogecho.Filter{
			func(ctx echo.Context) bool {
				return ctx.Request().URL.Path != "/_health"
			},
		},
	}

	e.Use(slogecho.NewWithConfig(args.Logger, slogEchoCfg))

	httpd := &http.Server{
		Addr:    args.APIListenAddr,
		Handler: e,
	}

	// PProf should auto-register with the default mux
	metricsMux := http.DefaultServeMux
	metricsMux.Handle("/metrics", promhttp.Handler())

	metricsHttpd := &http.Server{
		Addr:    args.MetricsAddr,
		Handler: metricsMux,
	}

	downloadSem := semaphore.NewWeighted(args.MaxConcurrentOCRExecs * 4)
	ocrSem := semaphore.NewWeighted(args.MaxConcurrentOCRExecs)

	return &Retina{
		httpd:             httpd,
		metricsHttpd:      metricsHttpd,
		client:            client,
		echo:              e,
		logger:            args.Logger,
		downloadSemaphore: downloadSem,
		ocrSemaphore:      ocrSem,
		pdqPath:           args.PdqPath,
	}, nil
}

func (r *Retina) Run(ctx context.Context) error {
	go func() {
		if err := r.metricsHttpd.ListenAndServe(); err != nil {
			r.logger.Error("failed to start retina metrics server", "error", err)
		}
	}()

	r.addRoutes()

	wg := sync.WaitGroup{}

	shutdownEcho := make(chan struct{})
	go func() {
		defer wg.Done()
		log := r.logger.With("component", "retina_echo")
		log.Info("retina api server listening", "addr", r.httpd.Addr)

		go func() {
			if err := r.httpd.ListenAndServe(); err != http.ErrServerClosed {
				log.Error("failed to start retina api server", "error", err)
			}
		}()

		<-shutdownEcho

		log.Info("shutting down retina api server")
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := r.httpd.Shutdown(ctx); err != nil {
			log.Error("failed to shut down retina api server", "error", err)
			return
		}

		log.Info("retina api server shut down")
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals

	r.logger.Info("shutting down on signal")

	close(shutdownEcho)
	wg.Wait()

	r.logger.Info("shut down successfuly")

	return nil
}

func (r *Retina) addRoutes() {
	r.echo.GET("/_health", func(e echo.Context) error {
		return e.String(http.StatusOK, "healthy")
	})

	g := r.echo.Group("/api")
	g.POST("/analyze", r.handleAnalyze)
	g.POST("/analyze_blob", r.handleAnalyzeBlob)
	g.POST("/hash", r.handlePdq)
	g.POST("/hash_blob", r.handlePdqBlob)
}
