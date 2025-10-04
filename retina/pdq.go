package retina

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var (
	ErrEmptyPdqResponse = errors.New("pdq response was empty")
	ErrBadPdqResponse   = errors.New("pdq response was bad")
	ErrInvalidPdqFloat  = errors.New("pdq response float was invalid")
	ErrQualityTooLow    = errors.New("pdq hash quality was too low")
)

func (r *Retina) GetImageHash(ctx context.Context, filepath string) (string, error) {
	start := time.Now()
	status := "error"
	defer func() {
		if status == "ok" {
			hashHist.Observe(time.Since(start).Seconds())
		}
	}()

	cmd := exec.CommandContext(ctx, r.pdqPath, filepath)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		return "", err
	}

	output := stdout.String()
	if len(output) == 0 {
		return "", ErrEmptyPdqResponse
	}

	respParts := strings.Split(output, ",")
	if len(respParts) != 3 {
		return "", ErrBadPdqResponse
	}

	qualitystr := respParts[1]
	quality, err := strconv.ParseInt(qualitystr, 10, 64)
	if err != nil {
		return "", ErrInvalidPdqFloat
	}

	if quality < 50 { // recommended minimum quality of pdq hashes
		status = "quality_too_low"
		return "", fmt.Errorf("%w. quality was %d", ErrQualityTooLow, quality)
	}

	status = "ok"

	return respParts[0], nil
}

func strToBinary(input string) (string, error) {
	hashb, err := hex.DecodeString(input)
	if err != nil {
		return "", err
	}

	result := make([]byte, len(hashb)*8)
	for i, b := range hashb {
		for j := 7; j >= 0; j-- {
			if (b>>j)&1 == 1 {
				result[i*8+(7-j)] = '1'
			} else {
				result[i*8+(7-j)] = '0'
			}
		}
	}

	return string(result), nil
}
