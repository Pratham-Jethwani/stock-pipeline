with base as (
    select * from {{ ref('stg_stock_ticks') }}
)

select
    trade_date,
    ticker,
    close_price,
    ma_7,
    ma_20,
    case
        when ma_7 > ma_20 then 'bullish'
        when ma_7 < ma_20 then 'bearish'
        else 'neutral'
    end as trend_signal
from base