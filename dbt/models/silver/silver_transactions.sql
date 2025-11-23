{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

{% set lookback_minutes = 20 %}
{% set silver_relation = adapter.get_relation(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier
) %}
{% set silver_columns = adapter.get_columns_in_relation(silver_relation) if silver_relation else [] %}
{% set silver_column_names = silver_columns | map(attribute='name') | list %}
{% set silver_has_window = silver_relation is not none and 'transaction_window_start' in silver_column_names %}

WITH bounds AS (
    {% if is_incremental() and silver_has_window %}
    SELECT
        COALESCE(MAX(transaction_window_start), TIMESTAMP '1970-01-01') AS max_window,
        COALESCE(MAX(transaction_window_start), TIMESTAMP '1970-01-01') - INTERVAL '{{ lookback_minutes }}' MINUTE AS lower_bound
    FROM {{ this }}
    {% else %}
    SELECT
        TIMESTAMP '1970-01-01' AS max_window,
        TIMESTAMP '1970-01-01' AS lower_bound
    {% endif %}
),

bronze_transactions AS (
    SELECT *
    FROM {{ ref('bronze_transaction_events') }}
    WHERE transaction_window_start >= (SELECT lower_bound FROM bounds)
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY loaded_at DESC
            ) AS row_num
        FROM bronze_transactions
    )
    WHERE row_num = 1
),

validated_transactions AS (
    SELECT
        transaction_id,
        transaction_timestamp,
        user_id,
        platform,
        product_id,
        product_category,
        quantity,
        unit_price,
        total_amount,
        currency,
        payment_method,
        shipping_country,
        is_first_purchase,
        transaction_date,
        transaction_window_start,
        loaded_at,
        CURRENT_TIMESTAMP AS processed_at
    FROM deduplicated
    WHERE
        transaction_id IS NOT NULL
        AND transaction_timestamp IS NOT NULL
        AND user_id IS NOT NULL
        AND total_amount >= 0
        AND quantity > 0
        AND transaction_timestamp <= CURRENT_TIMESTAMP
        AND transaction_timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAY
)

SELECT * FROM validated_transactions

{% if is_incremental() and silver_has_window %}
    WHERE transaction_window_start > (SELECT max_window FROM bounds) - INTERVAL '{{ lookback_minutes }}' MINUTE
{% endif %}
