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