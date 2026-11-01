# SmartPark İzmir

A microservices-based smart parking management system that monitors real-time parking occupancy in İzmir, provides dynamic pricing, and recommends the best parking spots.

## Screenshots

**Finding parking near a destination.** The user searches for Konak Meydanı; the five best lots are ranked by distance, dynamic price and availability, and map pins are colored by live occupancy.

![Parking recommendations for Konak Meydanı with ranked lots, dynamic prices and occupancy-colored map pins](docs/screenshots/recommendations.png)

**Analytics.** System-wide averages and per-lot occupancy statistics, served from the TimescaleDB continuous aggregate.

![Analytics tab showing total lots, average occupancy, reading count and per-lot statistics](docs/screenshots/analytics.png)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         User (Browser)                               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTP / WebSocket
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      API Gateway (Traefik)                           │
│   /occupancy  /forecast  /recommend  /pricing  /analytics  /ws      │
└───┬────────────┬──────────────────┬──────────────────┬──────────────┘
    │            │                  │                  │
    ▼            ▼                  ▼                  ▼
┌───────┐ ┌──────────────┐ ┌──────────────────┐ ┌─────────────┐
│Occup. │ │ Forecasting  │ │Pricing & Routing │ │Notification │
│State  │ │ + Analytics  │ │    Service       │ │  Service    │
│       │ │              │ │ ┌──────────────┐ │ │             │
│ Redis │ │ TimescaleDB  │ │ │Circuit Break.│ │ │  WebSocket  │
└───┬───┘ └──────┬───────┘ │ │  + Retry    │ │ └──────┬──────┘
    │            │          │ └──────────────┘ │       │
    │            │          └──────────────────┘       │
    └────────────┴─────────────────────────────────────┘
                               ▲ RabbitMQ (Topic Exchange)
                               │ parking.occupancy.changed
                    ┌──────────┴──────────┐
                    │   Data Ingestion    │
                    │  (every 30 seconds) │
                    └──────────┬──────────┘
                               │ polling
                    ┌──────────┴──────────┐
                    │  İzmir Municipality │
                    │      Open API       │
                    └─────────────────────┘
```

## Microservices Patterns Applied

| Pattern | Where | Description |
|---------|-------|-------------|
| **API Gateway** | Traefik | Single entry point for all external traffic |
| **Event-Driven Architecture** | RabbitMQ Topic Exchange | Services communicate via messages, fully decoupled |
| **Database per Service** | 1× PostgreSQL, 1× TimescaleDB, 1× Redis | Each service owns its data, no shared databases |
| **Circuit Breaker** | pricing-routing | Fail-fast when a downstream service is unavailable, prevents cascade failures |
| **Retry + Exponential Backoff** | pricing-routing | Retries transient failures with 1s → 2s → 4s delay |
| **Health Check** | All services | `/health` endpoint reports dependency status |
| **Correlation ID** | All services | `X-Correlation-ID` header traces a request across the service chain |
| **Service Discovery** | Docker DNS | Services find each other by container name |

## Services

| Service | Port (internal) | Technology | Responsibility |
|---------|----------------|------------|----------------|
| data-ingestion | — | Python + APScheduler | Polls İzmir API every 30s, publishes changes to RabbitMQ |
| occupancy-state | 8000 | FastAPI + Redis | Maintains live occupancy state, serves geo queries |
| forecasting | 8000 | FastAPI + TimescaleDB | Produces occupancy forecasts by blending an EWMA of the live signal with a per-lot day-of-week × hour profile; also serves the analytics endpoints from the `lot_hourly` continuous aggregate |
| pricing-routing | 8000 | FastAPI + PostgreSQL | Calculates dynamic prices, recommends best parking spots |
| notification | 8000 | FastAPI + WebSocket | Pushes live updates to connected browsers |
| frontend | 80 | Nginx + Leaflet.js | Map, search, and analytics UI |

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows / macOS)  
  or Docker Engine + Docker Compose (Linux)

Nothing else needs to be installed. Python, PostgreSQL, Redis, etc. all run inside Docker.

## Getting Started

```bash
git clone https://github.com/<username>/smartpark-izmir.git
cd smartpark-izmir
docker compose up --build
```

First run may take 2–3 minutes to pull images.

Open in browser: **http://localhost**

### Other Interfaces

| URL | What |
|-----|------|
| http://localhost | SmartPark application |
| http://localhost:8080 | Traefik dashboard (routing status) |
| http://localhost:15672 | RabbitMQ management UI (`guest` / `guest`) |

### Stopping

```bash
docker compose down        # stop services
docker compose down -v     # stop services and delete database volumes
```

## API Endpoints

### Occupancy State
```
GET /occupancy                          → all parking lots
GET /occupancy/{lot_id}                 → single lot
GET /occupancy/near?lat=&lng=&radius=   → nearby lots (geo query)
GET /health                             → service health status
```

### Forecasting
```
GET /forecast/{lot_id}?horizon=30m      → occupancy forecast (horizon: 30m, 90min, 2h …)
GET /health                             → service health status
```

### Pricing & Routing
```
GET /recommend?userLat=&userLng=&destLat=&destLng=&duration_hours=2
GET /pricing/{lot_id}?duration_hours=1
GET /health                             → includes circuit breaker states
```

### Analytics

Served by the forecasting service. Statistics are read from the `lot_hourly`
continuous aggregate, which TimescaleDB keeps up to date from the raw
`occupancy_readings` hypertable — there is no separate analytics database.

```
GET /analytics/summary                  → system-wide statistics
GET /analytics/lots                     → per-lot statistics
GET /analytics/hourly/{lot_id}          → hourly occupancy profile
GET /analytics/health
```

### Notification
```
WS  /ws                                 → real-time occupancy updates
GET /notifications/status
GET /notifications/health
```

## Circuit Breaker Behavior

The `pricing-routing` service uses a circuit breaker for calls to `occupancy-state` and `forecasting`:

```
Normal →  CLOSED    calls go through as usual
5 failures → OPEN   calls are rejected immediately with 503 (fail-fast)
After 30s → HALF_OPEN  one probe call is allowed
Success →  CLOSED   normal operation resumes
```

Current circuit state is visible at `GET /health`.

## Tech Stack

- **Backend:** Python 3.11, FastAPI, Uvicorn
- **Message Broker:** RabbitMQ 3.13
- **Databases:** PostgreSQL 16, TimescaleDB (time-series), Redis 7
- **API Gateway:** Traefik v3
- **Frontend:** Vanilla JS, Leaflet.js, OpenStreetMap
- **Infrastructure:** Docker Compose
