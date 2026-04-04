with source as (
    select * from {{ source('stock_pipeline', 'staging_stock_ticks') }}
),

cleaned as (
    select
        trade_date::date   as trade_date,
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        ma_7,
        ma_20,
        rsi_14,
        daily_return,
        volume_anomaly
    from source
    where
        close_price > 0
        and volume > 0
        and ticker is not null
)

select * from cleaned