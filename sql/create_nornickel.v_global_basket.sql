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
),
filled_data AS (
    SELECT
        msk_hour,
        gmkn,
        COALESCE(usd,
            LAG(usd, 1) OVER (ORDER BY msk_hour),
            LAG(usd, 2) OVER (ORDER BY msk_hour),
            LAG(usd, 48) OVER (ORDER BY msk_hour)
        ) as usd,
        COALESCE(ni_usd,
            LAG(ni_usd, 1) OVER (ORDER BY msk_hour),
            LAG(ni_usd, 2) OVER (ORDER BY msk_hour),
            LAG(ni_usd, 48) OVER (ORDER BY msk_hour)
        ) as ni_usd,
        COALESCE(cu_usd,
            LAG(cu_usd, 1) OVER (ORDER BY msk_hour),
            LAG(cu_usd, 2) OVER (ORDER BY msk_hour),
            LAG(cu_usd, 48) OVER (ORDER BY msk_hour)
        ) as cu_usd,
        COALESCE(pd_usd,
            LAG(pd_usd, 1) OVER (ORDER BY msk_hour),
            LAG(pd_usd, 2) OVER (ORDER BY msk_hour),
            LAG(pd_usd, 48) OVER (ORDER BY msk_hour)
        ) as pd_usd,
        COALESCE(pt_usd,
            LAG(pt_usd, 1) OVER (ORDER BY msk_hour),
            LAG(pt_usd, 2) OVER (ORDER BY msk_hour),
            LAG(pt_usd, 48) OVER (ORDER BY msk_hour)
        ) as pt_usd
    FROM hourly_data
)
SELECT
    msk_hour as msk_time,
    round(gmkn, 2) as real_price,
    round(
        (
            pd_usd / 31.1 * 1000 * 0.40 +
            pt_usd / 31.1 * 1000 * 0.10 +
            ni_usd / 1000 * 0.25 +
            cu_usd / 1000 * 0.20
        ) * usd / 14000,
    2) as basket_price
FROM filled_data
WHERE gmkn IS NOT NULL
ORDER BY msk_hour DESC;