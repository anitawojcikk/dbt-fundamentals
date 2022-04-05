with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

customer_payments as (

    select
        order_id,
        SUM(amount) as amount
    FROM payments
    group by 1
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(customer_payments.amount, 0) as amount
    from orders
    left join customer_payments using(order_id)
)
select * from final

