# Retina - Image OCR and Perceptual Hash Generator

Retina is a small service that accepts image bytes and returns both OCR content (using Tesseract) and a perceptual hash using Facebook's PDQ algorithm.


### Tesseract OCR

[Tesseract](https://github.com/tesseract-ocr/tesseract) is an open-source OCR engine originally developed by HP and now maintained by Google. It can extract text from images in over 100 languages.

**Documentation:** https://tesseract-ocr.github.io/

### PDQ (Perceptual Distance Quality)

[PDQ](https://github.com/facebook/ThreatExchange/tree/main/pdq) is a perceptual hashing algorithm developed by Facebook for image similarity detection. It generates a compact hash that remains similar even when images are slightly modified (resized, compressed, color-adjusted, etc.), making it useful for detecting duplicate or near-duplicate images.

**Documentation:** https://github.com/facebook/ThreatExchange/blob/main/pdq/README.md

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

The included `compose.yaml` will run the service with the default environment variables.

```bash
docker compose up -d
```

If you wish to run this without Docker, you will need to manage cloning Facebook's ThreatExchange repository, building the PDQ image hasher, and update the `RETINA_PDQ_PATH`
environment variable when you run the service.

### API

The Retina API provides endpoints for extracting text from images using OCR and generating perceptual hashes using PDQ.

#### Endpoints

##### `POST /api/analyze`

Extract text from an image using OCR (Tesseract).

**Request Body:**
```json
{
  "did": "did:plc:...",
  "cid": "bafyrei..."
}
```

**Response:**
```json
{
  "text": "extracted text from image",
  "error": "error message if any"
}
```

**Status Codes:**
- `200 OK`: Success
- `400 Bad Request`: Invalid request or image not found
- `500 Internal Server Error`: Processing error

---

##### `POST /api/analyze_blob`

Extract text from an image blob in the request body using OCR (Tesseract).

**Query Parameters:**
- `did` (optional): DID of the image owner
- `cid` (optional): CID of the image

**Headers:**
- `Content-Type`: Must be `image/jpeg` (only supported MIME type)

**Request Body:** Raw image bytes

**Response:**
```json
{
  "text": "extracted text from image",
  "error": "error message if any"
}
```

**Status Codes:**
- `200 OK`: Success
- `415 Unsupported Media Type`: Invalid Content-Type
- `500 Internal Server Error`: Processing error

---

##### `POST /api/hash`

Generate a PDQ perceptual hash for an image.

**Request Body:**
```json
{
  "did": "did:plc:...",
  "cid": "bafyrei..."
}
```

**Response:**
```json
{
  "hash": "hexadecimal PDQ hash",
  "binary": "binary representation of hash",
  "qualityTooLow": false
}
```

If image quality is too low for hashing:
```json
{
  "qualityTooLow": true
}
```

**Status Codes:**
- `200 OK`: Success (even when quality is too low)
- `400 Bad Request`: Invalid request or image not found
- `500 Internal Server Error`: Processing error

---

##### `POST /api/hash_blob`

Generate a PDQ perceptual hash for an image blob in the request body.

**Query Parameters:**
- `did` (optional): DID of the image owner
- `cid` (optional): CID of the image

**Request Body:** Raw image bytes

**Response:**
```json
{
  "hash": "hexadecimal PDQ hash",
  "binary": "binary representation of hash",
  "qualityTooLow": false
}
```

If image quality is too low for hashing:
```json
{
  "qualityTooLow": true
}
```

**Status Codes:**
- `200 OK`: Success (even when quality is too low)
- `500 Internal Server Error`: Processing error

---

##### `GET /_health`

Health check endpoint.

**Response:** `healthy` (200 OK)


