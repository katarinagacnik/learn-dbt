with order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount

    from {{ ref('stg_payments') }}
    group by 1
),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        coalesce(p.amount, 0) as amount

    from {{ ref('stg_orders') }} o
    left join order_payments p on o.order_id = p.order_id
)

select * from final