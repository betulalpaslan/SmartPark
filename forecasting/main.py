import os
import re
import json
import threading
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pika
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://smartpark:smartpark@localhost:5432/forecasting")
EXCHANGE = "parking"
ROUTING_KEY = "parking.occupancy.changed"
ALPHA = 0.3  # EWM smoothing factor
TZ_NAME = "Europe/Istanbul"   # saatlik profil yerel saate gore tutulur
IZMIR_TZ = ZoneInfo(TZ_NAME)

# Saatlik profilde bir kovanin tasiyabilecegi azami agirlik. Sinirsiz aritmetik
# ortalamada yeni okumanin etkisi 1/n ile sifira gider; birkac gun sonra profil
# degisime (yeni AVM, tarife, yol kapanmasi) kor kalir. Doyma noktasindan sonra
# kova 1/PROFILE_MAX_WEIGHT adimli bir EWMA gibi davranir ve guncel kalir.
PROFILE_MAX_WEIGHT = 200

# Bir kovayi tek basina kullanmak icin gereken en az okuma sayisi; altinda
# kalirsa _profile_for daha genis kovaya dusup soguk baslangici yumusatir.
MIN_PROFILE_SAMPLES = 5

# Profilin azami agirligi: uzak vadede bile mevcut duruma pay kalir, cunku
# doluluk yavas degisir ve su anki deger 1-2 saat ileride de bilgi tasir.
BLEND_MAX = 0.8

HORIZON_MAX_MINUTES = 24 * 60

# Per-lot state: {lot_id: {"smoothed": float,
#                          "profile": [7][24] floats,   # [haftanin gunu][saat]
#                          "counts":  [7][24] ints}}
# Gun ayrimi olmadan Pazar 14:00 ile Sali 14:00 ayni kovada erir ve iki gunun
# ikisinde de yanlis tahmin uretir; bu yuzden profil gun x saat tutulur.
_state: dict[str, dict] = {}
_lock = threading.Lock()


def _new_state(occ: float) -> dict:
    return {
        "smoothed": occ,
        "profile": [[0.0] * 24 for _ in range(7)],
        "counts": [[0] * 24 for _ in range(7)],
    }


def _profile_for(s: dict, dow: int, hour: int) -> tuple[float, str]:
    """Saatlik profili en dar kovadan en genise dogru arar.

    dow x hour -> ayni gun tipinde (hafta ici / hafta sonu) ayni saat ->
    gunden bagimsiz saat -> hic veri yoksa smoothed. Her adim okuma sayisiyla
    agirliklanir, boylece az ornekli kova ortalamayi bozmaz.
    """
    if s["counts"][dow][hour] >= MIN_PROFILE_SAMPLES:
        return s["profile"][dow][hour], "day_hour"

    for days, label in ((range(5) if dow < 5 else range(5, 7), "daytype_hour"),
                        (range(7), "hour")):
        total = sum(s["counts"][d][hour] for d in days)
        if total >= MIN_PROFILE_SAMPLES:
            weighted = sum(s["profile"][d][hour] * s["counts"][d][hour] for d in days)
            return weighted / total, label

    return s["smoothed"], "smoothed"


def _parse_horizon(horizon: str) -> int:
    """'30m' / '90min' / '2h' -> dakika. Gecersiz girdi sessizce 30'a dusmez."""
    match = re.fullmatch(r"(\d+)\s*(m|min|h)?", horizon.strip().lower())
    if not match:
        raise HTTPException(400, detail=f"Gecersiz horizon: {horizon!r} (ornek: '30m', '2h')")
    minutes = int(match.group(1)) * (60 if match.group(2) == "h" else 1)
    if not 1 <= minutes <= HORIZON_MAX_MINUTES:
        raise HTTPException(
            400, detail=f"horizon 1-{HORIZON_MAX_MINUTES} dakika araliginda olmali"
        )
    return minutes


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """Propagates X-Correlation-ID header for distributed request tracing."""

    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


def _db():
    return psycopg2.connect(DATABASE_URL)


def _wait_db(retries=20, delay=5):
    for i in range(retries):
        try:
            _db().close()
            log.info("TimescaleDB ready")
            return
        except Exception:
            log.info("Waiting for DB (%d/%d)…", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("DB unavailable")


def _bootstrap():
    try:
        with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT lot_id,
                       EXTRACT(ISODOW FROM ts AT TIME ZONE '{TZ_NAME}')::int - 1 AS dow,
                       EXTRACT(HOUR   FROM ts AT TIME ZONE '{TZ_NAME}')::int     AS hour,
                       AVG(occupancy_pct) AS avg_occ,
                       COUNT(*)           AS n
                FROM occupancy_readings
                GROUP BY lot_id, dow, hour
            """)
            rows = cur.fetchall()
            cur.execute("""
                SELECT DISTINCT ON (lot_id) lot_id, occupancy_pct
                FROM occupancy_readings
                ORDER BY lot_id, ts DESC
            """)
            latest = {r["lot_id"]: float(r["occupancy_pct"]) for r in cur.fetchall()}
        with _lock:
            for row in rows:
                lid = row["lot_id"]
                s = _state.setdefault(lid, _new_state(latest.get(lid, float(row["avg_occ"]))))
                d, h = int(row["dow"]), int(row["hour"])
                # Gerçek okuma sayısı yüklenir: aksi halde restart sonrası ilk
                # ölçüm, öğrenilmiş saatlik profili tek başına ezer. Üst sınır
                # ise profilin zamanla donmasını engeller.
                s["profile"][d][h] = float(row["avg_occ"])
                s["counts"][d][h] = min(int(row["n"]), PROFILE_MAX_WEIGHT)
        log.info("Bootstrap: loaded %d lot profiles", len(_state))
    except Exception as exc:
        log.warning("Bootstrap skipped: %s", exc)


def _update(lot: dict):
    lid = lot["lot_id"]
    occ = float(lot["occupancy_pct"])
    ts = datetime.fromisoformat(lot["timestamp"])
    local = ts.astimezone(IZMIR_TZ)
    d, h = local.weekday(), local.hour

    with _lock:
        s = _state.setdefault(lid, _new_state(occ))
        s["smoothed"] = ALPHA * occ + (1 - ALPHA) * s["smoothed"]
        # Agirlik PROFILE_MAX_WEIGHT'te doyar: altinda birikimli ortalama gibi,
        # ustunde sabit adimli EWMA gibi davranir, boylece profil guncel kalir.
        w = min(s["counts"][d][h], PROFILE_MAX_WEIGHT - 1)
        s["profile"][d][h] = (s["profile"][d][h] * w + occ) / (w + 1)
        s["counts"][d][h] = w + 1

    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO occupancy_readings (lot_id, ts, occupancy_pct, free, capacity)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                (lid, ts, occ, lot.get("free"), lot.get("capacity")),
            )
            conn.commit()
    except Exception as exc:
        log.error("DB insert error: %s", exc)


def _start_consumer():
    def run():
        while True:
            try:
                conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
                ch = conn.channel()
                ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
                q = ch.queue_declare("forecasting_q", durable=True)
                ch.queue_bind(exchange=EXCHANGE, queue=q.method.queue, routing_key=ROUTING_KEY)

                def on_message(ch, method, props, body):
                    # Bozuk mesaj ack'lenmezse yeniden teslim edilir, tuketici
                    # tekrar patlar ve arkasindaki saglam mesajlar da islenmez.
                    # DB hatasi _update icinde yutuldugu icin buraya dusen hata
                    # kalici (bicimsiz payload) sayilir ve mesaj dusurulur.
                    try:
                        _update(json.loads(body))
                    except Exception as exc:
                        log.error("Bozuk mesaj dusuruldu: %s - body=%.200s", exc, body)
                    ch.basic_ack(method.delivery_tag)

                ch.basic_qos(prefetch_count=10)
                ch.basic_consume(queue=q.method.queue, on_message_callback=on_message, auto_ack=False)
                log.info("Forecasting consumer ready")
                ch.start_consuming()
            except Exception as exc:
                log.warning("Consumer error: %s — retry in 5s", exc)
                time.sleep(5)

    threading.Thread(target=run, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _wait_db()
    _bootstrap()
    _start_consumer()
    yield


app = FastAPI(title="Forecasting Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(CorrelationIDMiddleware)


@app.get("/health")
def health():
    """Liveness + dependency health check for orchestrators and circuit breakers."""
    checks: dict = {}
    overall = "ok"
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM occupancy_readings")
            reading_count = cur.fetchone()[0]
        checks["timescaledb"] = {"status": "ok", "total_readings": reading_count}
    except Exception as exc:
        checks["timescaledb"] = {"status": "error", "detail": str(exc)}
        overall = "unhealthy"
    with _lock:
        checks["in_memory_state"] = {"tracked_lots": len(_state)}
    return JSONResponse(
        {"status": overall, "service": "forecasting", "checks": checks},
        status_code=200 if overall == "ok" else 503,
    )


@app.get("/forecast/{lot_id}")
def forecast(lot_id: str, horizon: str = "30m"):
    minutes = _parse_horizon(horizon)

    now = datetime.now(timezone.utc)
    target = (now + timedelta(minutes=minutes)).astimezone(IZMIR_TZ)

    with _lock:
        s = _state.get(lot_id)
        if s is None:
            raise HTTPException(404, detail="No data for this lot yet")
        smoothed = s["smoothed"]
        profile_pct, profile_source = _profile_for(s, target.weekday(), target.hour)

    blend = min(minutes / 60.0, BLEND_MAX)
    predicted = (1 - blend) * smoothed + blend * profile_pct

    return {
        "lot_id": lot_id,
        "horizon_minutes": minutes,
        "forecast_occupancy_pct": round(predicted, 1),
        "current_smoothed_pct": round(smoothed, 1),
        "profile_pct": round(profile_pct, 1),
        "profile_source": profile_source,
        "profile_weight": round(blend, 3),
        "forecast_at": (now + timedelta(minutes=minutes)).isoformat(),
    }
# ─── Analytics ────────────────────────────────────────────────────────────────
# Ayrı analytics servisi ve kendi PostgreSQL'i kaldırıldı; istatistikler artık
# lot_hourly continuous aggregate'inden okunuyor.

_WEIGHTED_AVG = """ROUND((SUM(avg_occupancy_pct * reading_count)
                          / NULLIF(SUM(reading_count), 0))::numeric, 1)"""


@app.get("/analytics/summary")
def analytics_summary():
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT COUNT(DISTINCT lot_id) AS total_lots,
                   {_WEIGHTED_AVG}        AS avg_occupancy_pct,
                   SUM(reading_count)     AS total_readings,
                   MIN(bucket)            AS first_reading,
                   MAX(bucket)            AS last_reading
            FROM lot_hourly
        """)
        return dict(cur.fetchone())


@app.get("/analytics/lots")
def analytics_lots():
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT lot_id,
                   {_WEIGHTED_AVG}                           AS avg_occupancy_pct,
                   ROUND(MAX(max_occupancy_pct)::numeric, 1) AS max_occupancy_pct,
                   ROUND(MIN(min_occupancy_pct)::numeric, 1) AS min_occupancy_pct,
                   SUM(reading_count)                        AS reading_count,
                   MAX(bucket)                               AS last_seen
            FROM lot_hourly
            GROUP BY lot_id
            ORDER BY avg_occupancy_pct DESC
        """)
        return [dict(r) for r in cur.fetchall()]


@app.get("/analytics/hourly/{lot_id}")
def analytics_hourly(lot_id: str):
    with _db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT EXTRACT(HOUR FROM bucket AT TIME ZONE '{TZ_NAME}')::int AS hour,
                   {_WEIGHTED_AVG}                AS avg_occupancy_pct,
                   SUM(reading_count)             AS reading_count
            FROM lot_hourly
            WHERE lot_id = %s
            GROUP BY hour
            ORDER BY hour
        """, (lot_id,))
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(404, detail="Bu lot için veri yok")
    return [dict(r) for r in rows]


@app.get("/analytics/health")
def analytics_health():
    try:
        with _db() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT lot_id) FROM lot_hourly")
            count = cur.fetchone()[0]
        return {"status": "ok", "tracked_lots": count}
    except Exception as exc:
        raise HTTPException(503, detail=str(exc))
