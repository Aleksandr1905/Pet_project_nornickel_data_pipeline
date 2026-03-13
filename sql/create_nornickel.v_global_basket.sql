DROP VIEW IF EXISTS nornickel.v_global_basket;

CREATE OR REPLACE VIEW nornickel.v_global_basket AS
WITH hourly_data AS (
    SELECT
        date_trunc('hour', market_data."timestamp") as msk_hour,
        max(CASE WHEN market_data.ticker = 'GMKN' THEN market_data.price END) as gmkn,
        max(CASE WHEN market_data.ticker = 'USDRUB' THEN market_data.price END) as usd,
        max(CASE WHEN market_data.ticker = 'NICKEL' THEN market_data.price END) as ni_usd,
        max(CASE WHEN market_data.ticker = 'COPPER' THEN market_data.price END) as cu_usd,
        max(CASE WHEN market_data.ticker = 'PALLADIUM' THEN market_data.price END) as pd_usd,
        max(CASE WHEN market_data.ticker = 'PLATINUM' THEN market_data.price END) as pt_usd
    FROM nornickel.market_data
    GROUP BY date_trunc('hour', market_data."timestamp")
)
SELECT
    msk_hour as msk_time,
    round(gmkn, 2) as real_price,
    round(
        (
            COALESCE(pd_usd, LAG(pd_usd) OVER (ORDER BY msk_hour)) / 31.1 * 1000 * 0.40 +
            COALESCE(pt_usd, LAG(pt_usd) OVER (ORDER BY msk_hour)) / 31.1 * 1000 * 0.10 +
            COALESCE(ni_usd, LAG(ni_usd) OVER (ORDER BY msk_hour)) / 1000 * 0.25 +
            COALESCE(cu_usd, LAG(cu_usd) OVER (ORDER BY msk_hour)) / 1000 * 0.20
        ) * COALESCE(usd, LAG(usd) OVER (ORDER BY msk_hour)) / 14000,
    2) as basket_price
FROM hourly_data
WHERE gmkn IS NOT NULL;

COMMENT ON VIEW nornickel.v_global_basket IS
'Корзина металлов: палладий (40%), платина (10%), никель (25%), медь (20%)
Пересчет: унции → тонны (/31.1*1000), USD → RUB, деление на 14000 для масштаба';