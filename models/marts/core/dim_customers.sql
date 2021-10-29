with customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value
    from {{ ref('fct_orders') }}
    group by 1
),
final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.first_order_date,
        o.most_recent_order_date,
        coalesce(o.number_of_orders, 0) as number_of_orders,
        o.lifetime_value
    from {{ ref('stg_customers')}} c
    left join customer_orders o on c.customer_id = o.customer_id
)
select * from final