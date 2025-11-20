-- QUERY 1: Average Coupon Rate by Bond Purpose
-- Goal: See which bond categories tend to have the highest coupon (interest) rates.
SELECT
    bp.purpose_category,
    ROUND(AVG(b.coupon_rate_pct), 3) AS average_coupon_rate_pct
FROM
    bonds b
JOIN
    bond_purposes bp ON b.purpose_id = bp.purpose_id
GROUP BY
    bp.purpose_category
ORDER BY
    average_coupon_rate_pct DESC;




-- QUERY 2: Most Recent Credit Rating for a Specific Bond
-- Goal: Find the latest rating given to a bond (e.g., BOND0001)
SELECT
    bond_id,
    rating_agency_name,
    rating_code,
    rating_date,
    outlook
FROM
    credit_ratings
WHERE
    bond_id = 'BOND0001' -- Replace with any bond_id you want to check
ORDER BY
    rating_date DESC
LIMIT 1;



-- QUERY 3: Trade Volume Breakdown by Buyer Type
-- Goal: Understand who is buying the most municipal bonds (institutional vs. retail).
SELECT
    buyer_type,
    COUNT(trade_id) AS total_trades,
    SUM(quantity_num) AS total_bonds_traded
FROM
    trades
GROUP BY
    buyer_type
ORDER BY
    total_bonds_traded DESC;



--Diag query
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'bond_purposes'
ORDER BY ordinal_position;


--Diag query
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'credit_ratings'
ORDER BY ordinal_position;

--Diag query
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'bonds'
ORDER BY ordinal_position;


--Diag query
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'issuers'
ORDER BY ordinal_position;



--Diag query
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'macro_economic_data'
ORDER BY ordinal_position;



--DIAG queryu
-- needed to get the definitive, case-sensitive column names for the 'trades' table.
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'trades'
ORDER BY ordinal_position;


-- query 4
-- Goal: Calculate the average bond yield for bonds categorized as 'Utilities' or 'Healthcare',
-- grouped by the bond's latest credit rating.

SELECT
    bp.purpose_category,
    cr.outlook, -- Confirmed correct column name from diagnostic query
    ROUND(AVG(t.yield_pct), 3) AS average_yield_pct, -- Confirmed correct column name from diagnostic query (unquoted lowercase)
    COUNT(DISTINCT b.bond_id) AS bond_count -- Counting distinct bonds in each group
FROM
    trades t
JOIN
    bonds b ON t.bond_id = b.bond_id
JOIN
    bond_purposes bp ON b.purpose_id = bp.purpose_id
JOIN
    credit_ratings cr ON b.bond_id = cr.bond_id
WHERE
    bp.purpose_category IN ('Utilities', 'Healthcare')
    -- Subquery to ensure we only consider the LATEST rating for each bond
    AND cr.rating_date = (
        SELECT MAX(rating_date)
        FROM credit_ratings
        WHERE bond_id = b.bond_id
    )
GROUP BY
    bp.purpose_category,
    cr.outlook
ORDER BY
    bp.purpose_category,
    average_yield_pct DESC;


-- Goal: Calculate the average trade price and total trade volume (quantity) for all trades
-- made in 2023, grouped by the issuer's state.
SELECT
    i.state_code, -- Confirmed: Schema name required for Issuers table
    ROUND(AVG(t.trade_price_usd), 2) AS average_trade_price, -- DEDUCED: Correcting from 'trade_price' to 'price'
    SUM(t.quantity_num) AS total_volume
FROM
    trades t
JOIN
    bonds b ON t.bond_id = b.bond_id
JOIN
    issuers i ON b.issuer_id = b.issuer_id
WHERE
    -- Filter trades to include only those in the year 2023
    t.trade_date >= '2023-01-01' AND t.trade_date <= '2023-12-31'
GROUP BY
    i.state_code
ORDER BY
    total_volume DESC;


-- Goal: Calculate the average 10-year Treasury rate for all states where the average
-- Filter groups where the average unemployment rate for between 2012 and 2022 was
--above 3.5

SELECT
    m.state_code AS state_code, -- Using 'state' as the column name from CSV
    ROUND(AVG(m.treasury_10yr_rate_pct), 3) AS average_10yr_treasury_rate
FROM
    macro_economic_data m
WHERE
    -- Filter data to include only the year 2022
    m.date >= '2012-01-01' AND m.date <= '2022-12-31'
GROUP BY
    m.state_code
HAVING
    AVG(m.unemployment_rate_pct) > 3.5
ORDER BY
    average_10yr_treasury_rate DESC;




-- Below are the 5 required queries
-- ANALYSIS queries
-- Multi-table JOIN, aggregation with HAVING, Subquery, Date-based analysis, and a financial metric calculation



-- Multi table JOIN joins 4 tables: bonds, issuers, bond_purposes, and trades to identify the most actively traded, long duration--risky--bonds
-- specifically issued by county governments for education purposes. Looking for liquidity in long term sector 9 years
--Tried to do it on more than 20 years and found out that anything greater than 10 years make the criteria
-- too strict so we REDUCED it to 9 years

SELECT
    i.issuer_name,    -- County or Issue names
    bp.purpose_category,  -- Specified use/purpose of bond
    b.duration_years AS bond_duration, -- Bonds duration/remaining life (yrs)
    COUNT(t.trade_id) AS total_trades,  --Total number of transcations for these bonds
    ROUND(AVG(t.trade_price_usd), 2) AS average_trade_price  --avg price these bonds trade
FROM
    bonds b
JOIN
    issuers i ON b.issuer_id = i.issuer_id     -- Links bond to issueing identity. table 2
JOIN
    bond_purposes bp ON b.purpose_id = bp.purpose_id  -- link bond to what it funded. table 3
JOIN
    trades t ON b.bond_id = t.bond_id   --Link trade activity to bond
WHERE
    i.issuer_type = 'County'  --filter county level issuers only
    AND bp.purpose_category = 'Education'  -- education bonds
    AND b.duration_years > 9  -- Filter for long-duration bonds more than 9 years
GROUP BY
    i.issuer_name,
    bp.purpose_category,
    b.duration_years
ORDER BY
    total_trades DESC
LIMIT 20;



-- Aggregate with GROUP BY and HAVING
-- GOAL: Find specific combination of issuer type such as city or authority and state that have issued
-- a larger or signifcant volume--here greater than 50 bonds-- with high average interest rates greater than 5%
-- To identify aggressive issuers in specific geographical areas
-- We dropped the interest rate to 3% because 5% was too strict and did not find anything

SELECT
    i.state_code,
    i.issuer_type,      --- Type whether its city,county, authority
    COUNT(b.bond_id) AS total_bonds_issued,  -- Total # of bonds issued in this group
    ROUND(AVG(b.coupon_rate_pct), 3) AS avg_coupon_rate   -- AVG interest rate paid
FROM
    bonds b
JOIN
    issuers i on b.issuer_id = i.issuer_id
GROUP BY
    i.state_code,
    i.issuer_type
HAVING
    COUNT(b.bond_id) > 50
    AND AVG(b.coupon_rate_pct) > 3.0
ORDER BY
    avg_coupon_rate DESC,
    total_bonds_issued DESC
LIMIT 30;




-- Correlated subquery to list all bonds where the most recent trade price was below the avg trade price
-- recorded for all ttrades with that specific bond. Highlights currently undervalued bonds.

SELECT
    b.bond_id,
    i.issuer_name,
    t.trade_price_usd AS latest_trade_price,     -- price of trade being examined
    (
        SELECT                                  --Subquery runs for every bond in outer query
            ROUND(AVG(t_inner.trade_price_usd), 2)
        FROM
            trades t_inner
        WHERE
            t_inner.bond_id = b.bond_id           -- links the inner query to the current bond in the outer query
    ) AS bond_historical_avg
FROM
    bonds b
JOIN
    issuers i ON b.issuer_id = i.issuer_id
JOIN
    trades t ON b.bond_id = t.bond_id
WHERE                                           -- Need to only look at most recent trade for comparison
    t.trade_date = (SELECT MAX(trade_date) FROM trades WHERE bond_id = b.bond_id)
    AND t.trade_price_usd < (
        SELECT AVG(t_inner.trade_price_usd)
        FROM trades t_inner
        WHERE t_inner.bond_id = b.bond_id

    )
ORDER BY
    t.trade_price_usd DESC
LIMIT 30;



-- Data based analysis using date functions for time trends
-- THe idea is to analyze the avg credit rating outlook across the market over time, grouping results by
-- year of the credit rating and filter out stable outlooks to show market risk perception-- year over year changes.

SELECT
    EXTRACT(YEAR FROM rating_date) AS rating_year,  --Date function that extracts the year from the rating date
    outlook,                                        -- risk outlook choices are positive or negative
    COUNT(rating_id) AS total_ratings_in_year,     -- How many ratings were issued with this outlook
    ROUND(AVG(
        CASE WHEN outlook = 'Positive' THEN 100
             WHEN outlook = 'Negative' THEN -100
             ELSE 0 END

    ),2) AS average_sentiment_score
FROM
    credit_ratings
WHERE
    outlook != 'Stable'
GROUP BY
    rating_year,
    outlook
ORDER BY
    rating_year DESC,
    average_sentiment_score DESC
LIMIT 30;



-- Financial metric calculation
-- Calculate the yield spread between the trade yield and avg 10-year treasury rate for the corresponding
-- month and state. A higher spread indicates higher risk/return vs the risk free rate
-- Calculation is:
-- Yield Spread = bond yield - 10 year treasury rate

SELECT
    t.trade_id,
    i.issuer_name,
    t.trade_date,
    t.yield_pct AS bond_yield,
    m.treasury_10yr_rate_pct AS treasury_rate,
    ROUND((t.yield_pct - m.treasury_10yr_rate_pct), 3) AS yield_spread_bps  -- Calculation
FROM
    trades t
JOIN
    bonds b ON t.bond_id = b.bond_id
JOIN
    issuers i ON b.issuer_id = i.issuer_id
JOIN
    macro_economic_data m ON i.state_code = m.state_code
        AND DATE_TRUNC('month', t.trade_date) = DATE_TRUNC('month',m.date)    -- joins only on the specific month state combination
WHERE
    t.yield_pct IS NOT NULL AND m.treasury_10yr_rate_pct IS NOT NULL
ORDER BY
    yield_spread_bps DESC     --will highlight trades with most risk
LIMIT 30;

