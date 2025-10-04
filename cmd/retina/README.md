# Retina - Image OCR and Perceptual Hash Generator

Retina is a small service that accepts image bytes and returns both OCR content (using Tesseract) and a perceptual hash using Facebook's PDQ algorithim.

## Usage

### Environment Variables

| Name                            | Default                   | Description                                                                                                |
|---------------------------------|---------------------------|------------------------------------------------------------------------------------------------------------|
| RETINA_API_LISTEN_ADDR          | :8080                     | Listen address for the Retina API                                                                          |
| RETINA_METRICS_ADDR             | :8081                     | Listen address for Retina Prometheus metrics                                                               |
| RETINA_DEBUG                    | false                     | Optional flag for enabling debug logging                                                                   |
| RETINA_MAX_CONCURRENT_OCR_EXECS | 5                         | Number of OCR (Tesseract) execs that can run in parallel. Uses a semaphore to enqueue execs.               |
| RETINA_PDQ_PATH                 | /usr/bin/pdq-photo-hasher | Path to PDQ photo hasher binary. If using included Dockerfile, you should not need to change this default. |


### Running

The included `compose.yaml` should 
```bash
docker compose up -d
```
