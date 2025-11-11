with customers as (

SELECT * FROM {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

SELECT * FROM {{ ref('stg_jaffle_shop__orders') }}

),

payment_sum as (
    select 
    order_id,
        sum(amount) as total_payment_amount
    from {{ ref('stg_stripe__payments') }}
    where status = 'success'
    group by 1
),

orders_with_payments as (
    select 
    o.*,
    coalesce(ps.total_payment_amount, 0) as total_payment_amount
    from orders o
    left join payment_sum ps
    on o.order_id = ps.order_id
),

customer_payments as (
    select 
    customer_id,
        sum(total_payment_amount) as total_spent
    from orders_with_payments
    group by 1
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders


    from orders

    group by 1

),




final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_payments.total_spent, 0) as total_spent

    from customers

    left join customer_orders using (customer_id)
    left join customer_payments using (customer_id)
)

select * from final
