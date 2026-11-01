import unittest
from datetime import datetime, timedelta, timezone

from tests.helpers import install_fastapi_service_stubs, load_module


class _FakeCursor:
    def execute(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeConn:
    """_update icindeki INSERT yolunu hatasiz gecirir."""

    def cursor(self, *args, **kwargs):
        return _FakeCursor()

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class ForecastingLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_fastapi_service_stubs()
        cls.fc = load_module("forecasting_main", "forecasting/main.py")
        cls.fc._db = lambda: _FakeConn()

    def setUp(self):
        self.fc._state.clear()

    def _feed(self, lot_id, moment, occupancy, times=1):
        for _ in range(times):
            self.fc._update({
                "lot_id": lot_id,
                "occupancy_pct": occupancy,
                "timestamp": moment.isoformat(),
            })
        return moment.weekday(), moment.hour

    # --- 1. profil donmuyor --------------------------------------------------

    def test_profile_keeps_adapting_after_many_readings(self):
        """Sinirsiz ortalamada 300 okumadan sonra profil yeni gercege kor kalirdi."""
        moment = datetime(2026, 11, 3, 14, 0, tzinfo=self.fc.IZMIR_TZ)
        d, h = self._feed("lot-a", moment, 50.0, times=300)
        self._feed("lot-a", moment, 90.0, times=600)

        profile = self.fc._state["lot-a"]["profile"][d][h]
        unbounded = (50.0 * 300 + 90.0 * 600) / 900  # agirlik sinirlanmasaydi: 76.7
        self.assertGreater(profile, 85.0)
        self.assertGreater(profile, unbounded)

    def test_saturated_profile_adapts_at_one_over_cap(self):
        """Doyma sonrasi kova 1/W adimli EWMA: W okuma bir zaman sabiti (~%63 yol)."""
        moment = datetime(2026, 11, 3, 14, 0, tzinfo=self.fc.IZMIR_TZ)
        cap = self.fc.PROFILE_MAX_WEIGHT
        d, h = self._feed("lot-a", moment, 50.0, times=cap)
        self._feed("lot-a", moment, 90.0, times=cap)

        expected = 90.0 - 40.0 * (1 - 1 / cap) ** cap
        self.assertAlmostEqual(self.fc._state["lot-a"]["profile"][d][h], expected, places=6)

    def test_profile_weight_saturates_at_cap(self):
        moment = datetime(2026, 11, 3, 14, 0, tzinfo=self.fc.IZMIR_TZ)
        d, h = self._feed("lot-a", moment, 50.0, times=self.fc.PROFILE_MAX_WEIGHT + 150)
        self.assertEqual(self.fc._state["lot-a"]["counts"][d][h], self.fc.PROFILE_MAX_WEIGHT)

    def test_single_reading_still_moves_saturated_profile(self):
        moment = datetime(2026, 11, 3, 14, 0, tzinfo=self.fc.IZMIR_TZ)
        d, h = self._feed("lot-a", moment, 50.0, times=self.fc.PROFILE_MAX_WEIGHT)
        self._feed("lot-a", moment, 90.0)

        moved = self.fc._state["lot-a"]["profile"][d][h] - 50.0
        self.assertAlmostEqual(moved, 40.0 / self.fc.PROFILE_MAX_WEIGHT, places=6)

    # --- 2. gun ayrimi -------------------------------------------------------

    def test_weekday_and_weekend_profiles_stay_separate(self):
        tuesday = datetime(2026, 11, 3, 14, 0, tzinfo=self.fc.IZMIR_TZ)
        sunday = datetime(2026, 11, 8, 14, 0, tzinfo=self.fc.IZMIR_TZ)
        self.assertEqual((tuesday.weekday(), sunday.weekday()), (1, 6))

        self._feed("lot-a", tuesday, 85.0, times=20)
        self._feed("lot-a", sunday, 30.0, times=20)

        s = self.fc._state["lot-a"]
        # Tek boyutlu profilde ikisi de ~57.5 olurdu.
        self.assertAlmostEqual(self.fc._profile_for(s, 1, 14)[0], 85.0, places=6)
        self.assertAlmostEqual(self.fc._profile_for(s, 6, 14)[0], 30.0, places=6)

    # --- 3. soguk baslangic geri cekilmesi -----------------------------------

    def test_sparse_bucket_falls_back_to_same_day_type(self):
        monday = datetime(2026, 11, 2, 9, 0, tzinfo=self.fc.IZMIR_TZ)
        self._feed("lot-a", monday, 70.0, times=20)
        s = self.fc._state["lot-a"]

        # Sali 09:00 kovasi bos; hafta ici ayni saate dusmeli.
        value, source = self.fc._profile_for(s, 1, 9)
        self.assertEqual(source, "daytype_hour")
        self.assertAlmostEqual(value, 70.0, places=6)

    def test_weekend_query_falls_back_to_hour_across_all_days(self):
        monday = datetime(2026, 11, 2, 9, 0, tzinfo=self.fc.IZMIR_TZ)
        self._feed("lot-a", monday, 70.0, times=20)

        value, source = self.fc._profile_for(self.fc._state["lot-a"], 6, 9)
        self.assertEqual(source, "hour")
        self.assertAlmostEqual(value, 70.0, places=6)

    def test_empty_profile_falls_back_to_smoothed_not_magic_constant(self):
        s = self.fc._new_state(42.0)
        value, source = self.fc._profile_for(s, 2, 3)
        self.assertEqual(source, "smoothed")
        self.assertEqual(value, 42.0)

    # --- 4. blend tavani -----------------------------------------------------

    def _seed_uniform(self, lot_id, smoothed, profile_pct):
        """Tum kovalari ayni degere doldurur: hedef kova saat sinirina takilmaz."""
        s = self.fc._new_state(smoothed)
        for d in range(7):
            for h in range(24):
                s["profile"][d][h] = profile_pct
                s["counts"][d][h] = 50
        self.fc._state[lot_id] = s

    def test_long_horizon_still_accounts_for_current_state(self):
        self._seed_uniform("lot-a", smoothed=95.0, profile_pct=40.0)
        result = self.fc.forecast("lot-a", horizon="2h")

        self.assertEqual(result["profile_weight"], self.fc.BLEND_MAX)
        # blend=1.0 iken sonuc duz 40.0 olur, agzina kadar dolu otopark yok sayilirdi.
        self.assertAlmostEqual(result["forecast_occupancy_pct"], 51.0, places=6)

    def test_short_horizon_blend_unchanged(self):
        self._seed_uniform("lot-a", smoothed=95.0, profile_pct=40.0)
        result = self.fc.forecast("lot-a", horizon="30m")

        self.assertEqual(result["profile_weight"], 0.5)
        self.assertAlmostEqual(result["forecast_occupancy_pct"], 67.5, places=6)

    def test_forecast_reports_profile_source(self):
        self._seed_uniform("lot-a", smoothed=95.0, profile_pct=40.0)
        self.assertEqual(self.fc.forecast("lot-a")["profile_source"], "day_hour")

    def test_unknown_lot_returns_404(self):
        with self.assertRaises(self.fc.HTTPException) as ctx:
            self.fc.forecast("yok")
        self.assertEqual(ctx.exception.status_code, 404)

    # --- 5. horizon ayristirmasi --------------------------------------------

    def test_horizon_accepts_minutes_and_hours(self):
        for raw, expected in [("30m", 30), ("90min", 90), ("2h", 120), ("45", 45), (" 15M ", 15)]:
            with self.subTest(raw=raw):
                self.assertEqual(self.fc._parse_horizon(raw), expected)

    def test_invalid_horizon_raises_400_instead_of_silent_default(self):
        for raw in ["abc", "", "-5m", "30x", "0m", "2000m"]:
            with self.subTest(raw=raw):
                with self.assertRaises(self.fc.HTTPException) as ctx:
                    self.fc._parse_horizon(raw)
                self.assertEqual(ctx.exception.status_code, 400)

    # --- 6. bozuk mesaj ------------------------------------------------------

    def test_update_raises_on_malformed_payload(self):
        """on_message icindeki try/except'in neden gerekli oldugunu sabitler."""
        with self.assertRaises(Exception):
            self.fc._update({"occupancy_pct": 50.0, "timestamp": "2026-11-03T14:00:00+03:00"})
        with self.assertRaises(Exception):
            self.fc._update({"lot_id": "lot-a", "occupancy_pct": "abc", "timestamp": "x"})


if __name__ == "__main__":
    unittest.main()
