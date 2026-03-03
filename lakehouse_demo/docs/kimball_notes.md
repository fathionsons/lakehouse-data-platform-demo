# Kimball Notes

## Fact vs Dimension

- Dimensions store descriptive attributes used for slicing and filtering analytics.
- Facts store numeric measurements and foreign keys that join to dimensions.
- In this demo:
  - Dimensions: `dim_date`, `dim_company`, `dim_channel`, `dim_policy_status`
  - Facts: `fact_review`, `fact_policy_premium`

## Surrogate Keys

- Surrogate keys (`company_key`, `channel_key`, `status_key`) are integer technical keys.
- Natural keys (`company_id`) remain in dimensions for lineage and business traceability.
- Facts use surrogate keys for consistent joins and future-proofing if natural keys change.

## Grain

- Grain is the atomic level of a fact table and must be explicit before modeling.
- `fact_review` grain: one row per single review event (`event_id`).
- `fact_policy_premium` grain: one row per policy per start date.
- Correct grain avoids double counting and ambiguous metrics.

## Slowly Changing Dimensions (SCD) Concept

- This demo implements SCD Type 2 for `dim_company`.
- Implemented columns:
  - `valid_from`: start date when the version became active
  - `valid_to`: end date of the version (null for current row)
  - `is_current`: boolean flag for the active record
- Facts resolve `company_key` using both `company_id` and event date, so historical
  reporting uses the correct company version at that point in time.
