package retina

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	imagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "retina_images_processed",
		Help: "total number of images processed by status",
	}, []string{"status", "job"})
	imageDownloadHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "retina_image_download_time",
		Help:    "histogram of image download times",
		Buckets: prometheus.ExponentialBucketsRange(0.01, 60, 20),
	}, []string{"status"})
	requestTimeHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "retina_request_time",
		Help:    "histogram of request times",
		Buckets: prometheus.ExponentialBucketsRange(0.01, 60, 20),
	}, []string{"status", "job"})
	hashHist = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "retina_pdq_hash_hist",
		Help:    "histogram of pdq hashing times",
		Buckets: prometheus.ExponentialBucketsRange(0.01, 60, 20),
	})
)
