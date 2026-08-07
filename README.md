# claims-status-service

claims-status-service — domain: claims

- **Port:** 8805
- **Language:** Python 3.11 + Flask
- **Database:** `claims` (Postgres, table `claims_status`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/claims_status/`          |
| POST      | `/api/claims_status/`          |
| GET       | `/api/claims_status/<id>`      |
| PUT/PATCH | `/api/claims_status/<id>`      |
| DELETE    | `/api/claims_status/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** claim.submitted, claim.adjudicated

## HTTP peer dependencies

- `claims-submission-service`
- `payer-edi-connect`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
