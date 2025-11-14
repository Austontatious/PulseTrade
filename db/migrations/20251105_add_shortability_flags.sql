ALTER TABLE dim_company_profile
    ADD COLUMN IF NOT EXISTS shortable BOOLEAN;

ALTER TABLE dim_company_profile
    ADD COLUMN IF NOT EXISTS easy_to_borrow BOOLEAN;
