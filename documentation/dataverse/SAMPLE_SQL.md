# Sample PSQL Queries

## Create Table
> CREATE TABLE IF NOT EXISTS staging.dv_new_servloc_raw (
    payload jsonb NOT NULL,
    pulled_at timestamptz DEFAULT now()
);

> CREATE OR REPLACE TABLE dataverse.new_servloc AS
SELECT
    payload->>'new_servlocid' AS new_servlocid,
    payload->>'new_name'      AS name,
    payload->>'statecodename' AS state,
    payload->>'statuscodename' AS status
FROM staging.dv_new_servloc_raw;
