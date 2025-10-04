package retina

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

func (r *Retina) getImageTextStream(ctx context.Context, img io.Reader) (string, error) {
	if err := r.ocrSemaphore.Acquire(ctx, 1); err != nil {
		return "", fmt.Errorf("error acquiring semaphore lock: %w", err)
	}
	defer r.ocrSemaphore.Release(1)

	cmd := exec.CommandContext(ctx, "tesseract", "stdin", "stdout")
	cmd.Stdin = img

	out := &bytes.Buffer{}
	cmd.Stdout = out

	errOut := &bytes.Buffer{}
	cmd.Stderr = errOut

	err := cmd.Run()
	if err != nil {
		r.logger.Error("error running tesseract command", "error", err, "stderr", errOut.String())
		return "", err
	}

	return strings.TrimSpace(out.String()), nil
}
