-- ===============================================
-- Database Triggers for Audit Logging
-- Run this AFTER creating your tables in municipal_schema.sql
-- ===============================================

-- 1. Audit Function for BONDS (PK is VARCHAR)
CREATE OR REPLACE FUNCTION log_bond_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_record_id VARCHAR;
    v_operation CHAR(1);
BEGIN
    -- Determine operation type and record ID
    IF (TG_OP = 'INSERT') THEN
        v_operation := 'I';
        v_record_id := NEW.bond_id;
    ELSIF (TG_OP = 'UPDATE') THEN
        v_operation := 'U';
        v_record_id := NEW.bond_id;
    ELSIF (TG_OP = 'DELETE') THEN
        v_operation := 'D';
        v_record_id := OLD.bond_id;
    ELSE
        RETURN NULL;
    END IF;

    -- Insert into audit_logs
    INSERT INTO audit_logs (table_name, record_id, operation_type)
    VALUES (TG_TABLE_NAME, v_record_id, v_operation);

    -- Return appropriate record for the trigger type
    IF (TG_OP = 'DELETE') THEN 
        RETURN OLD; 
    ELSE 
        RETURN NEW; 
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 2. Trigger definition for Bonds
DROP TRIGGER IF EXISTS trg_audit_bonds ON bonds;
CREATE TRIGGER trg_audit_bonds
AFTER INSERT OR UPDATE OR DELETE ON bonds
FOR EACH ROW
EXECUTE FUNCTION log_bond_changes();


-- 3. Audit Function for TRADES (PK is SERIAL/INT)
CREATE OR REPLACE FUNCTION log_trade_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_record_id VARCHAR;
    v_operation CHAR(1);
BEGIN
    -- Determine operation type and record ID
    -- NOTE: We cast trade_id to VARCHAR (::VARCHAR) to match the audit_logs schema
    IF (TG_OP = 'INSERT') THEN
        v_operation := 'I';
        v_record_id := NEW.trade_id::VARCHAR;
    ELSIF (TG_OP = 'UPDATE') THEN
        v_operation := 'U';
        v_record_id := NEW.trade_id::VARCHAR;
    ELSIF (TG_OP = 'DELETE') THEN
        v_operation := 'D';
        v_record_id := OLD.trade_id::VARCHAR;
    ELSE
        RETURN NULL;
    END IF;

    -- Insert into audit_logs
    INSERT INTO audit_logs (table_name, record_id, operation_type)
    VALUES (TG_TABLE_NAME, v_record_id, v_operation);

    -- Return appropriate record
    IF (TG_OP = 'DELETE') THEN 
        RETURN OLD; 
    ELSE 
        RETURN NEW; 
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 4. Trigger definition for Trades
DROP TRIGGER IF EXISTS trg_audit_trades ON trades;
CREATE TRIGGER trg_audit_trades
AFTER INSERT OR UPDATE OR DELETE ON trades
FOR EACH ROW
EXECUTE FUNCTION log_trade_changes();