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

- This demo implements a Type 1 style dimension overwrite for simplicity.
- In production:
  - Type 1 updates overwrite attribute history.
  - Type 2 keeps history with effective dates and current flags.
- `dim_company` is the most likely candidate for future Type 2 handling if company attributes evolve.
