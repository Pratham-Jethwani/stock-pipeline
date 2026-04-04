with base as (
    select * from {{ ref('stg_stock_ticks') }}
)

select
    trade_date,
    ticker,
    volume,
    volume_anomaly,
    close_price,
    daily_return
from base
where volume_anomaly = true