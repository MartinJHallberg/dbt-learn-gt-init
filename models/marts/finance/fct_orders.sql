with orders as (
    SELECT
        order_id,
        customer_id,
    from {{ ref('stg_jaffle_shop__orders') }}
),
payment_summed as (
    SELECT
        order_id,
        SUM(amount) as amount
    FROM {{ ref('stg_stripe__payments') }}
    GROUP BY order_id
),
final as (
    SELECT
        o.order_id,
        o.customer_id,
        p.amount
    FROM orders o
    LEFT JOIN payment_summed p
    ON o.order_id = p.order_id
)

select * from final