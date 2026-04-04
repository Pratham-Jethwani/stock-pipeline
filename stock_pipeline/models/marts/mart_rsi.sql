with base as (
    select * from {{ ref('stg_stock_ticks') }}
)

select
    trade_date,
    ticker,
    close_price,
    rsi_14,
    case
        when rsi_14 >= 70 then 'overbought'
        when rsi_14 <= 30 then 'oversold'
        else 'neutral'
    end as rsi_signal
from base