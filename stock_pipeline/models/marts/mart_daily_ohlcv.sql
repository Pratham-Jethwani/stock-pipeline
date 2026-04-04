with base as (
    select * from {{ ref('stg_stock_ticks') }}
)

select
    trade_date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_return,
    case
        when daily_return > 0 then 'positive'
        when daily_return < 0 then 'negative'
        else 'neutral'
    end as return_direction
from base