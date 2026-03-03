# Star Schema

The Gold layer follows Kimball dimensional modeling with conformed dimensions and two facts.

```mermaid
erDiagram
    DIM_DATE {
      INT date_key PK
      DATE date
      INT year
      INT month
      INT day
      INT week
      STRING weekday_name
    }

    DIM_COMPANY {
      INT company_key PK
      STRING company_id UK
      STRING name
      STRING city
      STRING industry
    }

    DIM_CHANNEL {
      INT channel_key PK
      STRING channel_name
    }

    DIM_POLICY_STATUS {
      INT status_key PK
      STRING status
    }

    FACT_REVIEW {
      BIGINT review_key PK
      STRING event_id
      INT date_key FK
      INT company_key FK
      INT channel_key FK
      INT rating
    }

    FACT_POLICY_PREMIUM {
      STRING policy_id
      INT date_key FK
      INT company_key FK
      INT status_key FK
      DOUBLE premium_amount
    }

    DIM_DATE ||--o{ FACT_REVIEW : date_key
    DIM_COMPANY ||--o{ FACT_REVIEW : company_key
    DIM_CHANNEL ||--o{ FACT_REVIEW : channel_key

    DIM_DATE ||--o{ FACT_POLICY_PREMIUM : date_key
    DIM_COMPANY ||--o{ FACT_POLICY_PREMIUM : company_key
    DIM_POLICY_STATUS ||--o{ FACT_POLICY_PREMIUM : status_key
```

## Fact Grains

- `fact_review`: one row per review event.
- `fact_policy_premium`: one row per policy per `start_date`.
