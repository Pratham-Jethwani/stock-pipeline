import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import redshift_connector
import os
from dotenv import dotenv_values
from datetime import date, timedelta

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')

st.set_page_config(
    page_title="NSE Stock Pipeline Dashboard",
    page_icon="📈",
    layout="wide"
)

@st.cache_resource
def get_connection():
    try:
        # production — streamlit cloud secrets
        return redshift_connector.connect(
            host=st.secrets['REDSHIFT_HOST'],
            database=st.secrets['REDSHIFT_DB'],
            port=int(st.secrets['REDSHIFT_PORT']),
            user=st.secrets['REDSHIFT_USER'],
            password=st.secrets['REDSHIFT_PASSWORD']
        )
    except Exception:
        # local development — .env file
        v = dotenv_values(ENV_PATH)
        return redshift_connector.connect(
            host=v.get('REDSHIFT_HOST'),
            database=v.get('REDSHIFT_DB'),
            port=int(v.get('REDSHIFT_PORT')),
            user=v.get('REDSHIFT_USER'),
            password=v.get('REDSHIFT_PASSWORD')
        )

@st.cache_data(ttl=300)
def run_query(query):
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

# sidebar
st.sidebar.title("NSE Stock Pipeline")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["Market Overview", "Stock Analysis", "RSI Signals", "Volume Anomalies", "Pipeline Health"]
)

st.sidebar.markdown("---")

# ticker selector
@st.cache_data(ttl=3600)
def get_tickers():
    df = run_query("SELECT DISTINCT ticker FROM mart_daily_ohlcv ORDER BY ticker")
    return df['ticker'].tolist()

try:
    tickers = get_tickers()
except:
    tickers = ["TCS.NS", "INFY.NS", "RELIANCE.NS"]

selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)

# date range
end_date   = date.today()
start_date = end_date - timedelta(days=30)
date_range = st.sidebar.date_input(
    "Date Range",
    value=(start_date, end_date)
)

st.sidebar.markdown("---")
st.sidebar.caption("Data refreshes daily after NSE market close (3:30 PM IST)")

# ── page 1: market overview ──────────────────────────────────────────
if page == "Market Overview":
    st.title("Market Overview")
    st.markdown("Latest trading day summary across all NSE tickers")

    try:
        df = run_query("""
            SELECT
                t.ticker,
                c.company_name,
                c.sector,
                t.close_price,
                t.daily_return,
                t.volume,
                t.return_direction
            FROM mart_daily_ohlcv t
            LEFT JOIN dim_companies c ON t.ticker = c.ticker
            WHERE t.trade_date = (SELECT MAX(trade_date) FROM mart_daily_ohlcv)
            ORDER BY t.daily_return DESC
        """)

        if df.empty:
            st.warning("No data available yet. Run the pipeline first.")
        else:
            # metrics row
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Tickers", len(df))
            col2.metric("Gainers", len(df[df['return_direction'] == 'positive']))
            col3.metric("Losers",  len(df[df['return_direction'] == 'negative']))
            col4.metric("Latest Date", str(df['trade_date'].iloc[0]) if 'trade_date' in df.columns else "Today")

            st.markdown("---")

            # top gainers and losers
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Top 5 Gainers")
                gainers = df[df['return_direction'] == 'positive'].head(5)[
                    ['ticker', 'company_name', 'close_price', 'daily_return']
                ].reset_index(drop=True)
                gainers['daily_return'] = gainers['daily_return'].apply(lambda x: f"+{x:.2f}%")
                st.dataframe(gainers, use_container_width=True)

            with col2:
                st.subheader("Top 5 Losers")
                losers = df[df['return_direction'] == 'negative'].tail(5)[
                    ['ticker', 'company_name', 'close_price', 'daily_return']
                ].reset_index(drop=True)
                losers['daily_return'] = losers['daily_return'].apply(lambda x: f"{x:.2f}%")
                st.dataframe(losers, use_container_width=True)

            st.markdown("---")

            # sector performance
            st.subheader("Sector Performance")
            sector_df = df.groupby('sector')['daily_return'].mean().reset_index()
            sector_df.columns = ['sector', 'avg_return']
            sector_df = sector_df.sort_values('avg_return', ascending=True)

            fig = px.bar(
                sector_df,
                x='avg_return',
                y='sector',
                orientation='h',
                color='avg_return',
                color_continuous_scale=['red', 'lightgray', 'green'],
                title='Average Daily Return by Sector'
            )
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

            # full table
            st.subheader("All Tickers")
            st.dataframe(
                df[['ticker', 'company_name', 'sector', 'close_price', 'daily_return', 'volume']],
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Error loading data: {e}")

# ── page 2: stock analysis ───────────────────────────────────────────
elif page == "Stock Analysis":
    st.title(f"Stock Analysis — {selected_ticker}")

    try:
        start = date_range[0] if len(date_range) == 2 else start_date
        end   = date_range[1] if len(date_range) == 2 else end_date

        # ohlcv data
        ohlcv_df = run_query(f"""
            SELECT trade_date, open_price, high_price, low_price, close_price, volume
            FROM mart_daily_ohlcv
            WHERE ticker = '{selected_ticker}'
            AND trade_date BETWEEN '{start}' AND '{end}'
            ORDER BY trade_date
        """)

        # moving averages
        ma_df = run_query(f"""
            SELECT trade_date, close_price, ma_7, ma_20, trend_signal
            FROM mart_moving_averages
            WHERE ticker = '{selected_ticker}'
            AND trade_date BETWEEN '{start}' AND '{end}'
            ORDER BY trade_date
        """)

        if ohlcv_df.empty:
            st.warning(f"No data found for {selected_ticker} in selected date range.")
        else:
            # company info
            company_df = run_query(f"""
                SELECT company_name, sector, industry, market_cap
                FROM dim_companies
                WHERE ticker = '{selected_ticker}'
            """)

            if not company_df.empty:
                col1, col2, col3 = st.columns(3)
                col1.metric("Company",    company_df['company_name'].iloc[0])
                col2.metric("Sector",     company_df['sector'].iloc[0])
                col3.metric("Market Cap", f"₹{company_df['market_cap'].iloc[0]:,.0f}")

            st.markdown("---")

            # candlestick chart
            st.subheader("Price Chart with Moving Averages")
            fig = go.Figure()

            fig.add_trace(go.Candlestick(
                x=ohlcv_df['trade_date'],
                open=ohlcv_df['open_price'],
                high=ohlcv_df['high_price'],
                low=ohlcv_df['low_price'],
                close=ohlcv_df['close_price'],
                name='OHLCV'
            ))

            if not ma_df.empty:
                fig.add_trace(go.Scatter(
                    x=ma_df['trade_date'],
                    y=ma_df['ma_7'],
                    name='MA 7',
                    line=dict(color='orange', width=1.5)
                ))
                fig.add_trace(go.Scatter(
                    x=ma_df['trade_date'],
                    y=ma_df['ma_20'],
                    name='MA 20',
                    line=dict(color='blue', width=1.5)
                ))

            fig.update_layout(
                xaxis_rangeslider_visible=False,
                height=500,
                title=f"{selected_ticker} Price Chart"
            )
            st.plotly_chart(fig, use_container_width=True)

            # volume chart
            st.subheader("Volume")
            fig2 = px.bar(
                ohlcv_df,
                x='trade_date',
                y='volume',
                title=f"{selected_ticker} Daily Volume"
            )
            fig2.update_layout(height=300)
            st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {e}")

# ── page 3: rsi signals ──────────────────────────────────────────────
elif page == "RSI Signals":
    st.title("RSI Signals")
    st.markdown("Relative Strength Index — overbought (≥70) and oversold (≤30) signals")

    try:
        rsi_df = run_query("""
            SELECT
                r.trade_date,
                r.ticker,
                c.company_name,
                r.close_price,
                r.rsi_14,
                r.rsi_signal
            FROM mart_rsi r
            LEFT JOIN dim_companies c ON r.ticker = c.ticker
            WHERE r.trade_date = (SELECT MAX(trade_date) FROM mart_rsi)
            ORDER BY r.rsi_14 DESC
        """)

        if rsi_df.empty:
            st.warning("No RSI data available.")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("Overbought", len(rsi_df[rsi_df['rsi_signal'] == 'overbought']))
            col2.metric("Neutral",    len(rsi_df[rsi_df['rsi_signal'] == 'neutral']))
            col3.metric("Oversold",   len(rsi_df[rsi_df['rsi_signal'] == 'oversold']))

            st.markdown("---")

            # rsi chart
            fig = px.bar(
                rsi_df,
                x='ticker',
                y='rsi_14',
                color='rsi_signal',
                color_discrete_map={
                    'overbought': 'red',
                    'neutral':    'gray',
                    'oversold':   'green'
                },
                title='RSI 14 by Ticker'
            )
            fig.add_hline(y=70, line_dash='dash', line_color='red',   annotation_text='Overbought (70)')
            fig.add_hline(y=30, line_dash='dash', line_color='green', annotation_text='Oversold (30)')
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")

            # rsi for selected ticker over time
            st.subheader(f"RSI Trend — {selected_ticker}")
            ticker_rsi = run_query(f"""
                SELECT trade_date, rsi_14, rsi_signal
                FROM mart_rsi
                WHERE ticker = '{selected_ticker}'
                ORDER BY trade_date
            """)

            if not ticker_rsi.empty:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=ticker_rsi['trade_date'],
                    y=ticker_rsi['rsi_14'],
                    name='RSI 14',
                    line=dict(color='purple', width=2)
                ))
                fig2.add_hline(y=70, line_dash='dash', line_color='red',   annotation_text='Overbought')
                fig2.add_hline(y=30, line_dash='dash', line_color='green', annotation_text='Oversold')
                fig2.update_layout(height=350, title=f"{selected_ticker} RSI over time")
                st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {e}")

# ── page 4: volume anomalies ─────────────────────────────────────────
elif page == "Volume Anomalies":
    st.title("Volume Anomalies")
    st.markdown("Tickers where volume exceeded 2× the average — signals unusual activity")

    try:
        vol_df = run_query("""
            SELECT
                v.trade_date,
                v.ticker,
                c.company_name,
                c.sector,
                v.volume,
                v.close_price,
                v.daily_return
            FROM mart_volume_anomaly v
            LEFT JOIN dim_companies c ON v.ticker = c.ticker
            ORDER BY v.trade_date DESC, v.volume DESC
        """)

        if vol_df.empty:
            st.info("No volume anomalies detected in the current dataset.")
        else:
            st.metric("Total Anomalies Detected", len(vol_df))
            st.markdown("---")
            st.dataframe(vol_df, use_container_width=True)

            fig = px.scatter(
                vol_df,
                x='daily_return',
                y='volume',
                color='sector',
                hover_data=['ticker', 'company_name'],
                title='Volume Anomalies — Return vs Volume'
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {e}")

# ── page 5: pipeline health ──────────────────────────────────────────
elif page == "Pipeline Health":
    st.title("Pipeline Health")
    st.markdown("Monitor data freshness and quality across all pipeline layers")

    try:
        col1, col2, col3 = st.columns(3)

        # row counts per table
        staging_count = run_query("SELECT COUNT(*) as cnt FROM staging_stock_ticks")
        ohlcv_count   = run_query("SELECT COUNT(*) as cnt FROM mart_daily_ohlcv")
        company_count = run_query("SELECT COUNT(*) as cnt FROM dim_companies")

        col1.metric("Staging rows",   int(staging_count['cnt'].iloc[0]))
        col2.metric("OHLCV mart rows", int(ohlcv_count['cnt'].iloc[0]))
        col3.metric("Companies",       int(company_count['cnt'].iloc[0]))

        st.markdown("---")

        # latest dates per table
        st.subheader("Data Freshness")
        dates_df = run_query("""
            SELECT
                'staging_stock_ticks' as table_name,
                MAX(trade_date) as latest_date,
                COUNT(DISTINCT trade_date) as trading_days,
                COUNT(DISTINCT ticker) as tickers
            FROM staging_stock_ticks
            UNION ALL
            SELECT
                'mart_daily_ohlcv',
                MAX(trade_date),
                COUNT(DISTINCT trade_date),
                COUNT(DISTINCT ticker)
            FROM mart_daily_ohlcv
        """)
        st.dataframe(dates_df, use_container_width=True)

        st.markdown("---")

        # daily row counts over time
        st.subheader("Daily Row Counts")
        daily_df = run_query("""
            SELECT trade_date, COUNT(*) as row_count
            FROM mart_daily_ohlcv
            GROUP BY trade_date
            ORDER BY trade_date
        """)

        if not daily_df.empty:
            fig = px.bar(
                daily_df,
                x='trade_date',
                y='row_count',
                title='Rows loaded per trading day'
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # sector distribution
        st.subheader("Ticker Distribution by Sector")
        sector_count = run_query("""
            SELECT sector, COUNT(*) as ticker_count
            FROM dim_companies
            GROUP BY sector
            ORDER BY ticker_count DESC
        """)
        fig2 = px.pie(
            sector_count,
            values='ticker_count',
            names='sector',
            title='Tickers by Sector'
        )
        st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {e}")