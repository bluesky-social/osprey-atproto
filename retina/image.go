package retina

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

var (
	ErrImageNotFound      = errors.New("image not found")
	ErrImageBadStatusCode = errors.New("received bad status code when downloading image")
)

func (r *Retina) downloadImage(ctx context.Context, imageUrl string) ([]byte, error) {
	start := time.Now()
	status := "error"
	defer func() {
		imageDownloadHist.WithLabelValues(status).Observe(float64(time.Since(start).Seconds()))
	}()

	if err := r.downloadSemaphore.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("error acquiring semaphore lock: %w", err)
	}
	defer r.downloadSemaphore.Release(1)

	req, err := http.NewRequestWithContext(ctx, "GET", imageUrl, nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrImageNotFound
		}
		return nil, fmt.Errorf("%w: status code was %d", ErrImageBadStatusCode, resp.StatusCode)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	status = "ok"
	return b, nil
}

func saveBytes(bytes []byte) (string, error) {
	file, err := os.CreateTemp("", "*.jpg")
	if err != nil {
		return "", err
	}
	defer file.Close()

	if _, err := file.Write(bytes); err != nil {
		return "", err
	}

	return file.Name(), nil
}

func makeCdnUrl(did, cid string) string {
	return fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", did, cid)
}
