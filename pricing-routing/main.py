import asyncio
import enum
import logging
import math
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://smartpark:smartpark@localhost:5432/pricing")
OCCUPANCY_URL = os.getenv("OCCUPANCY_SERVICE_URL", "http://occupancy-state:8000")
FORECASTING_URL = os.getenv("FORECASTING_SERVICE_URL", "http://forecasting:8000")

IZMIR_TZ = ZoneInfo("Europe/Istanbul")   # zirve saatleri yerel saate gore

SEARCH_RADIUS_M = 3000.0
MAX_MULTIPLIER = 1.8
W_DIST = 0.4
W_PRICE = 0.3
W_AVAIL = 0.3

# ─── Circuit Breaker ──────────────────────────────────────────────────────────


class _State(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Three-state circuit breaker for downstream service calls.

    CLOSED  → normal operation, failures accumulate
    OPEN    → calls are rejected immediately (fail-fast)
    HALF_OPEN → one probe call allowed; success resets, failure re-opens
    """

    def __init__(self, name: str, failure_threshold: int = 5, reset_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._failures = 0
        self._state = _State.CLOSED
        self._last_failure_time = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        return self._state.value

    def is_call_allowed(self) -> bool:
        with self._lock:
            if self._state == _State.CLOSED:
                return True
            if self._state == _State.OPEN:
                if time.monotonic() - self._last_failure_time >= self.reset_timeout:
                    self._state = _State.HALF_OPEN
                    log.info("Circuit '%s' → HALF_OPEN (probing)", self.name)
                    return True
                return False
            return True  # HALF_OPEN: allow one probe

    def record_success(self):
        with self._lock:
            if self._state != _State.CLOSED:
                log.info("Circuit '%s' → CLOSED (service recovered)", self.name)
            self._failures = 0
            self._state = _State.CLOSED

    def record_failure(self):
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.monotonic()
            if self._state == _State.HALF_OPEN or self._failures >= self.failure_threshold:
                self._state = _State.OPEN
                log.warning("Circuit '%s' → OPEN (failures=%d)", self.name, self._failures)


class CircuitOpenError(Exception):
    pass


# One circuit breaker per downstream service
occupancy_cb = CircuitBreaker("occupancy-state", failure_threshold=5, reset_timeout=30.0)
forecasting_cb = CircuitBreaker("forecasting", failure_threshold=5, reset_timeout=30.0)

# ─── Retry helper ─────────────────────────────────────────────────────────────


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    cb: CircuitBreaker,
    *,
    params: dict | None = None,
    max_retries: int = 3,
):
    """
    GET with exponential-backoff retry and circuit-breaker integration.
    Raises CircuitOpenError when the circuit is OPEN.
    Raises the last httpx exception after exhausting retries.
    """
    if not cb.is_call_allowed():
        raise CircuitOpenError(f"Circuit '{cb.name}' is OPEN — downstream unavailable")

    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(max_retries):
        try:
            r = await client.get(url, params=params)
            r.raise_for_status()
            cb.record_success()
            return r
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            cb.record_failure()
            if attempt < max_retries - 1:
                backoff = 2 ** attempt  # 1s, 2s, 4s …
                log.warning("GET %s attempt %d/%d failed (%s) — retry in %ds", url, attempt + 1, max_retries, exc, backoff)
                await asyncio.sleep(backoff)
            else:
                log.error("GET %s failed after %d attempts: %s", url, max_retries, exc)

    raise last_exc


# ─── Middleware ───────────────────────────────────────────────────────────────


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """Propagates X-Correlation-ID header for distributed request tracing."""

    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


# ─── DB helpers ───────────────────────────────────────────────────────────────


def _db():
    return psycopg2.connect(DATABASE_URL)


def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def compute_multiplier(occ_pct: float, forecast_pct: float, hour: int) -> float:
    if occ_pct < 50:
        base = 0.8
    elif occ_pct < 70:
        base = 1.0
    elif occ_pct < 85:
        base = 1.2
    elif occ_pct < 95:
        base = 1.5
    else:
        base = MAX_MULTIPLIER

    trend = (forecast_pct - occ_pct) / 100.0
    trend_adj = 1.0 + trend * 0.3

    # hour = İzmir yerel saati; sabah ve akşam trafik zirveleri
    peak = hour in range(8, 10) or hour in range(17, 20)
    tod_adj = 1.1 if peak else 1.0

    return round(min(base * trend_adj * tod_adj, MAX_MULTIPLIER), 3)


def _all_prices() -> dict[str, float]:
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT lot_id, price_per_hour FROM lot_prices")
        return {r["lot_id"]: float(r["price_per_hour"]) for r in cur.fetchall()}


def _ensure_prices(lots: list[dict], prices: dict) -> dict:
    unknown = [l for l in lots if l["lot_id"] not in prices]
    if not unknown:
        return prices
    try:
        with _db() as conn, conn.cursor() as cur:
            for lot in unknown:
                cur.execute(
                    "INSERT INTO lot_prices (lot_id, lot_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (lot["lot_id"], lot.get("name", "")),
                )
            conn.commit()
    except Exception as exc:
        log.warning("Auto-insert lot_prices failed: %s", exc)
    return _all_prices()


async def _get_forecast(client: httpx.AsyncClient, lot_id: str, fallback: float) -> float:
    """Fetch forecast with circuit breaker; silently falls back to current occupancy."""
    try:
        r = await _get_with_retry(
            client,
            f"{FORECASTING_URL}/forecast/{lot_id}",
            forecasting_cb,
            params={"horizon": "30m"},
            max_retries=2,
        )
        return r.json()["forecast_occupancy_pct"]
    except (CircuitOpenError, Exception):
        return fallback


# ─── App ─────────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="Dynamic Pricing & Routing Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(CorrelationIDMiddleware)


@app.get("/health")
async def health():
    """Liveness + dependency health check including downstream circuit-breaker states."""
    checks: dict = {}
    overall = "ok"

    # Pricing DB check
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM lot_prices")
            count = cur.fetchone()[0]
        checks["pricingdb"] = {"status": "ok", "lot_prices": count}
    except Exception as exc:
        checks["pricingdb"] = {"status": "error", "detail": str(exc)}
        overall = "degraded"

    # Downstream service circuit-breaker states
    checks["occupancy_circuit"] = {"state": occupancy_cb.state}
    checks["forecasting_circuit"] = {"state": forecasting_cb.state}

    # Lightweight probe of downstream services (skip if circuit is OPEN)
    async with httpx.AsyncClient(timeout=3.0) as client:
        for name, url, cb in [
            ("occupancy-state", OCCUPANCY_URL, occupancy_cb),
            ("forecasting", FORECASTING_URL, forecasting_cb),
        ]:
            if not cb.is_call_allowed():
                checks[name] = {"status": "circuit_open"}
                overall = "degraded"
                continue
            try:
                r = await client.get(f"{url}/health")
                checks[name] = {"status": "ok" if r.status_code == 200 else "degraded"}
            except Exception as exc:
                checks[name] = {"status": "error", "detail": str(exc)}
                overall = "degraded"

    return JSONResponse(
        {"status": overall, "service": "pricing-routing", "checks": checks},
        status_code=200 if overall == "ok" else 207,
    )


@app.get("/recommend")
async def recommend(
    userLat: float,
    userLng: float,
    destLat: float,
    destLng: float,
    duration_hours: int = 2,
    top_k: int = 5,
):
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r = await _get_with_retry(
                client,
                f"{OCCUPANCY_URL}/occupancy/near",
                occupancy_cb,
                params={"lat": destLat, "lng": destLng, "radius": SEARCH_RADIUS_M},
            )
            lots = r.json()
        except CircuitOpenError as exc:
            raise HTTPException(503, detail=f"Occupancy service unavailable: {exc}")
        except Exception as exc:
            raise HTTPException(502, detail=f"Occupancy service error: {exc}")

    if not lots:
        raise HTTPException(404, detail="Hedefe yakın otopark bulunamadı")

    prices = _ensure_prices(lots, _all_prices())
    now_hour = datetime.now(IZMIR_TZ).hour

    async with httpx.AsyncClient(timeout=5.0) as client:
        forecasts = await asyncio.gather(
            *[_get_forecast(client, lot["lot_id"], lot["occupancy_pct"]) for lot in lots]
        )

    results = []
    for lot, forecast_pct in zip(lots, forecasts):
        lid = lot["lot_id"]
        occ = lot["occupancy_pct"]
        base_price = prices.get(lid, 10.0)
        multiplier = compute_multiplier(occ, forecast_pct, now_hour)
        dynamic_total = round(base_price * multiplier * duration_hours, 2)

        dist_dest = haversine(destLat, destLng, lot["lat"], lot["lng"])
        dist_user = haversine(userLat, userLng, lot["lat"], lot["lng"])

        avail_score = lot["free"] / max(lot["capacity"], 1)
        dist_score = max(0.0, 1.0 - dist_dest / SEARCH_RADIUS_M)
        price_score = max(0.0, 1.0 - dynamic_total / 300.0)

        score = W_DIST * dist_score + W_PRICE * price_score + W_AVAIL * avail_score

        results.append({
            "lot_id": lid,
            "name": lot.get("name", lid),
            "lat": lot["lat"],
            "lng": lot["lng"],
            "capacity": lot["capacity"],
            "free": lot["free"],
            "occupancy_pct": occ,
            "forecast_occupancy_pct": round(forecast_pct, 1),
            "base_price_per_hour": base_price,
            "multiplier": multiplier,
            "dynamic_price_total": dynamic_total,
            "duration_hours": duration_hours,
            "distance_to_dest_m": round(dist_dest),
            "distance_to_user_m": round(dist_user),
            "score": round(score, 4),
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return {
        "recommendations": results[:top_k],
        "searched_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/pricing/{lot_id}")
async def get_pricing(lot_id: str, duration_hours: int = 1):
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            r = await _get_with_retry(
                client,
                f"{OCCUPANCY_URL}/occupancy/{lot_id}",
                occupancy_cb,
            )
            lot = r.json()
        except CircuitOpenError as exc:
            raise HTTPException(503, detail=f"Occupancy service unavailable: {exc}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(404, detail="Lot not found")
            raise HTTPException(502, detail=str(exc))

        forecast_pct = await _get_forecast(client, lot_id, lot["occupancy_pct"])

    prices = _all_prices()
    base = prices.get(lot_id, 10.0)
    mult = compute_multiplier(lot["occupancy_pct"], forecast_pct, datetime.now(IZMIR_TZ).hour)
    return {
        "lot_id": lot_id,
        "base_price_per_hour": base,
        "multiplier": mult,
        "dynamic_price_total": round(base * mult * duration_hours, 2),
        "occupancy_pct": lot["occupancy_pct"],
        "forecast_occupancy_pct": round(forecast_pct, 1),
    }
