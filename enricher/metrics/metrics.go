package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var APIDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "enricher_api_duration_sec",
	Help: "Duration API calls",
}, []string{"service", "status"})

var CacheResults = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "enricher_api_cache_result",
	Help: "Cache results (hit, miss, etc) for a given cache",
}, []string{"service", "result"})

var CacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "enricher_api_cache_size",
	Help: "Current size of the cache",
}, []string{"service"})
