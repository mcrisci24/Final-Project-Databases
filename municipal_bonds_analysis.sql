-- --Diag query
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'bond_purposes'
-- ORDER BY ordinal_position;


-- --Diag query
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'credit_ratings'
-- ORDER BY ordinal_position;

-- --Diag query
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'bonds'
-- ORDER BY ordinal_position;


-- --Diag query
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'issuers'
-- ORDER BY ordinal_position;

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'bonds';

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'bond_purposes';

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'credit_ratings';

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'audit_logs';

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'issuers';

-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'macro_economic_data';



-- --Diag query
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'macro_economic_data'
-- ORDER BY ordinal_position;



-- --DIAG queryu
-- -- needed to get the definitive, case-sensitive column names for the 'trades' table.
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'trades'
-- ORDER BY ordinal_position;



-- ----------------------------------------------------------
-- Analysis for the Municipal Bonds Dashboard


-- ----------------------------------------------------------------------------------
-- QUERY 1: Average Coupon Rate by Bond Purpose
-- ----------------------------------------------------------------------------------
SELECT
    bp.purpose_category,
    -- CORRECTED: Changed 'coupon_rate' to 'coupon_rate_pct'
    ROUND(AVG(b.coupon_rate_pct), 3) AS average_coupon_rate_pct,
    COUNT(b.bond_id) AS total_bonds_issued
FROM
    bonds b
JOIN
    bond_purposes bp ON b.purpose_id = bp.purpose_id
GROUP BY
    bp.purpose_category
ORDER BY
    average_coupon_rate_pct DESC;



-- QUERY 2: High-Volume Issuers by Type and State
SELECT
    i.state_code,
    i.issuer_type,
    COUNT(b.bond_id) AS total_bonds_issued,
    ROUND(AVG(i.population_num), 0) AS avg_population
FROM
    bonds b
JOIN
    issuers i ON b.issuer_id = i.issuer_id
GROUP BY
    i.state_code, i.issuer_type
HAVING
    COUNT(b.bond_id) > 10
ORDER BY
    total_bonds_issued DESC;


-- QUERY 3: Credit Sentiment Trend
SELECT
    EXTRACT(YEAR FROM rating_date) AS rating_year,
    outlook,
    COUNT(rating_id) AS total_ratings_in_year,
    ROUND(AVG(
        CASE WHEN outlook = 'Positive' THEN 100
             WHEN outlook = 'Negative' THEN -100
             ELSE 0 END
    ), 2) AS average_sentiment_score
FROM
    credit_ratings
WHERE
    outlook != 'Stable'
GROUP BY
    rating_year,
    outlook
ORDER BY
    rating_year DESC,
    outlook DESC;



-- QUERY 4: Long-Duration Liquidity
SELECT
    b.bond_id,
    i.issuer_name,
    bp.purpose_category,
    -- CORRECTED: Changed 'duration' to 'duration_years'
    b.duration_years AS duration_num, 
    -- CORRECTED: Changed 'quantity' to 'quantity_num'
    SUM(t.quantity_num) AS total_quantity_traded_since_issue,
    COUNT(t.trade_id) AS total_trade_count
FROM
    bonds b
JOIN
    issuers i ON b.issuer_id = i.issuer_id
JOIN
    bond_purposes bp ON b.purpose_id = bp.purpose_id
JOIN
    trades t ON b.bond_id = t.bond_id
WHERE
    -- CORRECTED: Changed 'duration' to 'duration_years'
    b.duration_years > 6
    AND bp.purpose_category IN ('Education', 'Transportation', 'Utilities')
GROUP BY
    b.bond_id, i.issuer_name, bp.purpose_category, b.duration_years
HAVING
    COUNT(t.trade_id) > 5
ORDER BY
    total_trade_count DESC
LIMIT 20;


-- QUERY 5: Undervalued Bonds
WITH BondAvgPrice AS (
    SELECT
        bond_id,
        -- CORRECTED: Changed 'trade_price' to 'trade_price_usd'
        AVG(trade_price_usd) AS avg_trade_price
    FROM trades
    GROUP BY bond_id
),
BondLastTrade AS (
    SELECT
        t1.bond_id,
        t1.trade_date,
        -- CORRECTED: Changed 'trade_price' to 'trade_price_usd'
        t1.trade_price_usd AS last_trade_price,
        ROW_NUMBER() OVER(PARTITION BY t1.bond_id ORDER BY t1.trade_date DESC) as rn
    FROM trades t1
)
SELECT
    blt.bond_id,
    i.issuer_name,
    blt.trade_date AS last_trade_date,
    blt.last_trade_price,
    bap.avg_trade_price,
    ROUND(blt.last_trade_price - bap.avg_trade_price, 2) AS price_difference
FROM
    BondLastTrade blt
JOIN
    BondAvgPrice bap ON blt.bond_id = bap.bond_id
JOIN
    bonds b ON blt.bond_id = b.bond_id
JOIN
    issuers i ON b.issuer_id = i.issuer_id
WHERE
    blt.rn = 1
    AND blt.last_trade_price < bap.avg_trade_price
ORDER BY
    price_difference ASC
LIMIT 15;


-- QUERY 6: Yield Spread Risk
SELECT
    t.trade_id,
    i.issuer_name,
    t.trade_date,
    -- CORRECTED: Changed 'yield' to 'yield_pct'
    t.yield_pct AS bond_yield,
    m.treasury_10yr_rate_pct AS treasury_rate,
    -- CORRECTED: Changed 'yield' to 'yield_pct'
    ROUND((t.yield_pct - m.treasury_10yr_rate_pct), 3) AS yield_spread_bps
FROM
    trades t
JOIN
    bonds b ON t.bond_id = b.bond_id
JOIN
    issuers i ON b.issuer_id = i.issuer_id
JOIN
    macro_economic_data m ON i.state_code = m.state_code
        AND DATE_TRUNC('month', t.trade_date) = DATE_TRUNC('month', m.date)
ORDER BY
    yield_spread_bps DESC
LIMIT 50;