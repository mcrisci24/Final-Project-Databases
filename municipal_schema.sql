-- -- Database: municipal_bonds
-- -- -- PostgreSQL schema for a municipal bond market dataset.
-- DROP TABLE IF EXISTS audit_logs;
-- DROP TABLE IF EXISTS trades;
-- DROP TABLE IF EXISTS credit_ratings;
-- DROP TABLE IF EXISTS bonds;
-- DROP TABLE IF EXISTS macro_economic_data;
-- DROP TABLE IF EXISTS issuers CASCADE; -- Cascade handles the dependent bonds table foreign key
-- DROP TABLE IF EXISTS bond_purposes;

-- ===================================
-- 1. ENTITY/DIMENSION TABLES
-- ===================================

-- First: Issuers (Entity/Dimension Table)
-- Contains static and demographic data about the bond issuer (city, county, state, authority)
CREATE TABLE issuers (
    issuer_id SERIAL PRIMARY KEY,
    issuer_name VARCHAR(255) NOT NULL,
    state_code CHAR(2) NOT NULL, -- Two character state abbreviation
    issuer_type VARCHAR(50),
    population_num NUMERIC,
    tax_base_millions_num NUMERIC,

    -- Data Validation Checks
    CONSTRAINT issuers_state_code_check CHECK (state_code ~ '^[A-Z]{2}$'),
    CONSTRAINT issuers_population_positive_check CHECK (population_num >= 0)
);

-- Bond Purpose (Lookup Table)
-- Categorizes what the bond proceeds will fund (Education, Transportation, etc.).
CREATE TABLE bond_purposes (
    purpose_id INTEGER PRIMARY KEY,
    purpose_category VARCHAR(100) NOT NULL,
    purpose_description VARCHAR(255),

    CONSTRAINT bond_purposes_category_unique UNIQUE (purpose_category)
);


-- ===================================
-- 2. FACT TABLES
-- ===================================

-- Bonds (Central Fact Table)
-- Contains the static terms and structure of each bond issuance.
CREATE TABLE bonds (
    bond_id VARCHAR(10) PRIMARY KEY, -- Internal bond identifier
    issuer_id INTEGER NOT NULL,
    purpose_id INTEGER NOT NULL,
    cusip VARCHAR(9) UNIQUE NOT NULL, -- Committee on Uniform Security Identification Procedures number
    bond_type VARCHAR(50) NOT NULL, -- e.g., General Obligation, Revenue
    coupon_rate_pct NUMERIC(5, 3) NOT NULL, -- Annual interest rate percentage
    issue_date DATE NOT NULL,
    maturity_date DATE NOT NULL,
    face_value_usd NUMERIC(10, 2) NOT NULL, -- The par value of the bond (usually $1,000)
    duration_years NUMERIC(4, 2) NOT NULL, -- Modified Duration in years
    tax_status VARCHAR(20) NOT NULL, -- e.g., 'Tax-Exempt', 'Taxable'

    -- Foreign Keys
    CONSTRAINT bonds_issuer_id_fkey FOREIGN KEY (issuer_id) REFERENCES issuers (issuer_id),
    CONSTRAINT bonds_purpose_id_fkey FOREIGN KEY (purpose_id) REFERENCES bond_purposes (purpose_id),

    -- Data Validation Checks
    CONSTRAINT bonds_dates_order_check CHECK (issue_date < maturity_date),
    CONSTRAINT bonds_coupon_positive_check CHECK (coupon_rate_pct > 0)
);

-- Trades (Transaction Fact Table)
-- Contains transactional data for each bond sale/purchase.
CREATE TABLE trades (
    trade_id SERIAL PRIMARY KEY,
    bond_id VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    trade_price_usd NUMERIC(6, 2) NOT NULL, -- Price as percentage of face value (e.g., 95.50 for $955.00)
    yield_pct NUMERIC(5, 3), -- Yield to maturity percentage
    quantity_num INTEGER NOT NULL, -- Number of bonds traded
    buyer_type VARCHAR(30), -- e.g., 'Retail', 'Institutional', 'Municipal'

    -- Foreign Key
    CONSTRAINT trades_bond_id_fkey FOREIGN KEY (bond_id) REFERENCES bonds (bond_id),

    -- Data Validation Check
    CONSTRAINT trades_quantity_positive_check CHECK (quantity_num > 0)
);

-- Credit Ratings (Fact Table)
-- Captures time-series changes in credit ratings for each bond.
CREATE TABLE credit_ratings (
    rating_id SERIAL PRIMARY KEY,
    bond_id VARCHAR(10) NOT NULL,
    rating_agency_name VARCHAR(20) NOT NULL, -- e.g., 'Moodys', 'S&P', 'Fitch'
    rating_code VARCHAR(10) NOT NULL, -- e.g., 'AAA', 'A+', 'BBB-'
    rating_date DATE NOT NULL,
    outlook VARCHAR(20), -- e.g., 'Stable', 'Negative', 'Positive'

    CONSTRAINT credit_ratings_bond_id_fkey FOREIGN KEY (bond_id) REFERENCES bonds (bond_id)
);


-- Macro Economic Data (Time-Series)
-- Key economic indicators by state and time, used for risk modeling.
CREATE TABLE macro_economic_data (
    state_code CHAR(2) NOT NULL,
    date DATE NOT NULL,
    unemployment_rate_pct NUMERIC(4, 2) NOT NULL,
    treasury_10yr_rate_pct NUMERIC(4, 2),
    treasury_20yr_rate_pct NUMERIC(4, 2),
    vix_index_num NUMERIC(5, 2),

    PRIMARY KEY (state_code, date),

    CONSTRAINT macro_unemployment_range_check CHECK (unemployment_rate_pct BETWEEN 0 AND 100)
);

-- ===================================
-- 3. AUDIT LOGS (NEW)
-- ===================================

-- Audit Logs (System Table)
-- Tracks all data manipulation events on key tables.
CREATE TABLE audit_logs (
    log_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id VARCHAR(50) NOT NULL, -- The PK value of the record that was changed
    operation_type CHAR(1) NOT NULL, -- 'I' (Insert), 'U' (Update), 'D' (Delete)
    change_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    changed_by_user VARCHAR(100) DEFAULT CURRENT_USER
);


-- ===================================
-- 4. INDEXES
-- ===================================

-- Optimize common lookups and joins

-- Index for efficient joining of issuers to macro data
CREATE INDEX idx_issuers_state_code ON issuers (state_code);

-- Indices for fast time-series filtering
CREATE INDEX idx_trades_trade_date ON trades (trade_date);
CREATE INDEX idx_bonds_issue_date ON bonds (issue_date);
CREATE INDEX idx_ratings_bond_id ON credit_ratings (bond_id);


-- Index for finding all bonds issued by an issuer
CREATE INDEX idx_bonds_issuer_id ON bonds (issuer_id);