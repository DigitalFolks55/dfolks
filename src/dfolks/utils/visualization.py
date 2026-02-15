"""Data visualization utilities for DFolks."""

import plotly.graph_objects as go
from plotly.subplots import make_subplots


def plot_candlestick_stockprice(
    df,
    open_price_col,
    high_price_col,
    low_price_col,
    close_price_col,
    volume_col,
    date_col,
    code_col="local_code",
    code=None,
    ui_setting="browser",
):

    if code is not None:
        df = df[df[code_col] == code]

    if ui_setting is not None:
        import plotly.io as pio

        pio.renderers.default = ui_setting

    # Figure
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.6, 0.4],
        vertical_spacing=0.03,
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df[date_col],
            open=df[open_price_col],
            high=df[high_price_col],
            low=df[low_price_col],
            close=df[close_price_col],
            name="Stock Price",
        ),
        row=1,
        col=1,
    )

    # Volume bars
    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df[volume_col],
            name="Transaction Volume",
            opacity=1.0,
        ),
        row=2,
        col=1,
    )

    # Y-axis formatting for volume
    fig.update_yaxes(
        row=2, col=1, nticks=4, tickformat="~s", range=[0, df[volume_col].quantile(1.0)]
    )

    # Layout tweaks
    fig.update_layout(
        title=f"Code {code} Stock Price with Volume",
        xaxis_rangeslider_visible=False,
        yaxis_title="Stock Price",
        yaxis2_title="Transaction Volume",
        hovermode="x unified",
    )

    fig.show()
