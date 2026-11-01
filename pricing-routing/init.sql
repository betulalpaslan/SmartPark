CREATE TABLE IF NOT EXISTS lot_prices (
    lot_id           TEXT PRIMARY KEY,
    lot_name         TEXT,
    price_per_hour   NUMERIC(6, 2) NOT NULL DEFAULT 10.00,
    rules            JSONB,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- İZELMAN tarifesine yakın örnek fiyatlar (TRY/saat).
-- lot_id'ler data-ingestion'daki gerçek İzmir otopark kimlikleriyle birebir
-- aynı olmalı; eşleşmeyen kayıt pricing-routing tarafından 10.00 TL varsayılanı
-- ile yeniden eklenir ve buradaki tarife hiç uygulanmaz.
INSERT INTO lot_prices (lot_id, lot_name, price_per_hour) VALUES
    ('NEDAP-TR-IZM-014', '14 Plevne Bul. Yol Kenarı Otopark',     12.00),
    ('NEDAP-TR-IZM-033', '33 Hürriyet Bulvarı',                   12.00),
    ('NEDAP-TR-IZM-005', '05 Ali Çetinkaya Yol Kenarı Otopark',   12.00),
    ('NEDAP-TR-IZM-004', '04 Ziya Gökalp Yol Kenarı Otopark',     12.00),
    ('NEDAP-TR-IZM-007', '07 26. Ağustos Yol Kenarı Otopark',     10.00),
    ('NEDAP-TR-IZM-008', '08 Vasıf Çınar Yol Kenarı Otopark',     12.00),
    ('NEDAP-TR-IZM-031', '31 Şair Eşref Bulvarı',                 12.00),
    ('NEDAP-TR-IZM-034', '34 Sabancı Kültür Merkezi',             10.00),
    ('NEDAP-TR-IZM-006', '06 1393 Sk. Yol Kenarı Otopark',        10.00),
    ('CPS-TR-IZM-M2-04', 'Tam Otomatik Alsancak Otopark',         18.00),
    ('CPS-TR-IZM-B2-01', 'Hatay Katlı Pazaryeri',                 14.00),
    ('CPS-TR-IZM-M2-02', 'Alsancak Yeraltı Otopark',              16.00),
    ('CPS-TR-IZM-K3-01', 'Bahriye Üçok Yeraltı Otoparkı',         14.00),
    ('CPS-TR-IZM-N2-01', 'Bostanlı Katlı Otopark',                14.00),
    ('CPS-TR-IZM-M1-01', 'Konak Katlı Otopark',                   15.00)
ON CONFLICT (lot_id) DO NOTHING;
