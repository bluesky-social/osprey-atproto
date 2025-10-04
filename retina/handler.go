package retina

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
)

type ImageRequest struct {
	Did string `json:"did"`
	Cid string `json:"cid"`
}

type AnalyzeResult struct {
	Text  string `json:"text"`
	Error string `json:"error,omitempty"`
}

type PdqResult struct {
	Hash          *string `json:"hash,omitempty"`
	Binary        *string `json:"binary,omitempty"`
	QualityTooLow bool    `json:"qualityTooLow"`
}

func makeErrorJson(error string) AnalyzeResult {
	return AnalyzeResult{
		Error: error,
	}
}

func (r *Retina) handleAnalyze(e echo.Context) error {
	ctx := e.Request().Context()

	start := time.Now()

	status := "error"
	defer func() {
		imagesProcessed.WithLabelValues(status, "ocr").Inc()
		requestTimeHist.WithLabelValues(status, "ocr").Observe(float64(time.Since(start).Seconds()))
	}()

	var req ImageRequest
	if err := e.Bind(&req); err != nil {
		return e.JSON(http.StatusBadRequest, makeErrorJson("could not bind request"))
	}

	cdnUrl := makeCdnUrl(req.Did, req.Cid)
	imageBytes, err := r.downloadImage(ctx, cdnUrl)
	if err != nil {
		if errors.Is(err, ErrImageNotFound) {
			return e.JSON(http.StatusBadRequest, makeErrorJson("image not found"))
		}

		r.logger.Error("error downloading image", "url", cdnUrl, "error", err)
		// not really an internal error?
		return e.JSON(http.StatusInternalServerError, makeErrorJson("could not download image"))
	}

	imageText, err := r.getImageTextStream(ctx, bytes.NewReader(imageBytes))
	if err != nil {
		r.logger.Error("error getting text from image", "error", err)
		return e.JSON(http.StatusInternalServerError, makeErrorJson("could not get text from image"))
	}

	status = "ok"

	return e.JSON(http.StatusOK, AnalyzeResult{
		Text: imageText,
	})
}

// handleAnalyzeBlob handles analyze requests that include the image blob as the request body
// DID and CID are provided in the request query parameters
func (r *Retina) handleAnalyzeBlob(e echo.Context) error {
	ctx, span := tracer.Start(e.Request().Context(), "handleAnalyzeBlob")
	defer span.End()

	span.SetAttributes(
		attribute.String("did", e.QueryParam("did")),
		attribute.String("cid", e.QueryParam("cid")),
	)

	req := e.Request()

	start := time.Now()
	status := "error"
	defer func() {
		imagesProcessed.WithLabelValues(status, "ocr-blob").Inc()
		requestTimeHist.WithLabelValues(status, "ocr-blob").Observe(float64(time.Since(start).Seconds()))
	}()

	contentType := req.Header.Get("Content-Type")
	if !supportedMimeType(contentType) {
		return e.JSON(http.StatusUnsupportedMediaType, makeErrorJson("unsupported media type"))
	}

	imageText, err := r.getImageTextStream(ctx, req.Body)
	req.Body.Close()
	if err != nil {
		r.logger.Error("error getting text from request body stream", "error", err)
		return e.JSON(http.StatusInternalServerError, makeErrorJson("could not get text from image"))
	}

	status = "ok"

	return e.JSON(http.StatusOK, AnalyzeResult{
		Text: imageText,
	})
}

func (r *Retina) handlePdq(e echo.Context) error {
	ctx := e.Request().Context()

	start := time.Now()

	status := "error"
	defer func() {
		imagesProcessed.WithLabelValues(status, "pdq").Inc()
		requestTimeHist.WithLabelValues(status, "pdq").Observe(float64(time.Since(start).Seconds()))
	}()

	var req ImageRequest
	if err := e.Bind(&req); err != nil {
		return e.JSON(http.StatusBadRequest, makeErrorJson("could not bind request"))
	}

	cdnUrl := makeCdnUrl(req.Did, req.Cid)
	imageBytes, err := r.downloadImage(ctx, cdnUrl)
	if err != nil {
		if errors.Is(err, ErrImageNotFound) {
			return e.JSON(http.StatusBadRequest, makeErrorJson("image not found"))
		}

		r.logger.Error("error downloading image", "url", cdnUrl, "error", err)
		return e.JSON(http.StatusInternalServerError, makeErrorJson("could not download image"))
	}

	filePath, err := saveBytes(imageBytes)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, makeErrorJson("could not save image bytes to disk"))
	}
	defer func() {
		if err := os.Remove(filePath); err != nil {
			r.logger.Error("unable to delete image file", "error", err)
		}
	}()

	hashRes, err := r.GetImageHash(ctx, filePath)
	if err != nil {
		if errors.Is(err, ErrQualityTooLow) {
			return e.JSON(http.StatusOK, PdqResult{
				QualityTooLow: true,
			})
		}
		return e.JSON(http.StatusInternalServerError, makeErrorJson(fmt.Sprintf("error getting image hash: %v", err)))
	}

	binary, err := strToBinary(hashRes)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, makeErrorJson("unable to convert pdq hash to binary"))
	}

	status = "ok"

	return e.JSON(http.StatusOK, PdqResult{
		Hash:          &hashRes,
		Binary:        &binary,
		QualityTooLow: false,
	})
}

func (r *Retina) handlePdqBlob(e echo.Context) error {
	req := e.Request()
	ctx := req.Context()

	start := time.Now()

	status := "error"
	defer func() {
		imagesProcessed.WithLabelValues(status, "pdq").Inc()
		requestTimeHist.WithLabelValues(status, "pdq").Observe(float64(time.Since(start).Seconds()))
	}()

	b, err := io.ReadAll(req.Body)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, makeErrorJson(fmt.Sprintf("error reading image bytes from request: %v", err)))
	}

	filePath, err := saveBytes(b)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, makeErrorJson("could not save image bytes to disk"))
	}
	defer func() {
		if err := os.Remove(filePath); err != nil {
			r.logger.Error("unable to delete image file", "error", err)
		}
	}()

	hashRes, err := r.GetImageHash(ctx, filePath)
	if err != nil {
		if errors.Is(err, ErrQualityTooLow) {
			return e.JSON(http.StatusOK, PdqResult{
				QualityTooLow: true,
			})
		}
		return e.JSON(http.StatusInternalServerError, makeErrorJson(fmt.Sprintf("error getting image hash: %v", err)))
	}

	binary, err := strToBinary(hashRes)
	if err != nil {
		return e.JSON(http.StatusInternalServerError, makeErrorJson("unable to convert pdq hash to binary"))
	}

	status = "ok"

	return e.JSON(http.StatusOK, PdqResult{
		Hash:          &hashRes,
		Binary:        &binary,
		QualityTooLow: false,
	})
}

var SupportedMimeTypes = map[string]bool{
	"image/jpeg": true,
}

func supportedMimeType(contentType string) bool {
	if contentType == "" {
		return false
	}
	_, ok := SupportedMimeTypes[contentType]
	return ok
}
