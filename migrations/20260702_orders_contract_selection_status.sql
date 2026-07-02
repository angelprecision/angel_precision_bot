ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS contract_selection_status TEXT;
