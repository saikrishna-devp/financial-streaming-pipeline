import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import yaml


def load_config():
    with open('config/config.yaml') as f:
        return yaml.safe_load(f)


def get_data(db_file):
    try:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query('SELECT * FROM stock_quotes ORDER BY timestamp DESC LIMIT 500', conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def create_app(db_file):
    app = Dash(__name__)

    app.layout = html.Div([
        html.Div([
            html.H1('Real-Time Stock Market Monitor',
                   style={'color': 'white', 'margin': '0', 'fontSize': '24px'}),
            html.P('Live Alpha Vantage API - Kafka - PostgreSQL',
                  style={'color': '#aaa', 'margin': '5px 0 0 0'}),
        ], style={'background': '#1a1a2e', 'padding': '20px 30px', 'borderBottom': '2px solid #0f3460'}),

        html.Div(id='metric-cards', style={
            'display': 'flex', 'gap': '15px',
            'padding': '20px 30px', 'background': '#16213e',
        }),

        html.Div([
            html.Div([dcc.Graph(id='price-chart')], style={'flex': '2'}),
            html.Div([dcc.Graph(id='change-chart')], style={'flex': '1'}),
        ], style={'display': 'flex', 'gap': '15px', 'padding': '0 30px', 'background': '#16213e'}),

        html.Div([dcc.Graph(id='volume-chart')],
                style={'padding': '0 30px 20px', 'background': '#16213e'}),

        dcc.Interval(id='interval', interval=30000, n_intervals=0),

    ], style={'background': '#16213e', 'minHeight': '100vh', 'fontFamily': 'Arial'})


    @app.callback(
        [Output('metric-cards', 'children'),
         Output('price-chart', 'figure'),
         Output('change-chart', 'figure'),
         Output('volume-chart', 'figure')],
        [Input('interval', 'n_intervals')]
    )
    def update_dashboard(n):
        df = get_data(db_file)

        empty = go.Figure()
        empty.update_layout(paper_bgcolor='#1a1a2e', plot_bgcolor='#1a1a2e',
                           font_color='white', title='Waiting for data...')

        if df.empty:
            return [], empty, empty, empty

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['price'] = pd.to_numeric(df['price'])
        df['change'] = pd.to_numeric(df['change'])
        df['volume'] = pd.to_numeric(df['volume'])

        latest = df.groupby('symbol').first().reset_index()

        def card(title, value, color):
            return html.Div([
                html.P(title, style={'color': '#aaa', 'margin': '0', 'fontSize': '13px'}),
                html.H2(value, style={'color': color, 'margin': '5px 0 0', 'fontSize': '24px'}),
            ], style={
                'background': '#1a1a2e', 'padding': '15px 20px',
                'borderRadius': '8px', 'flex': '1',
                'border': f'1px solid {color}33',
            })

        cards = []
        for _, row in latest.iterrows():
            color = '#4ade80' if row['change'] >= 0 else '#f87171'
            cards.append(card(
                row['symbol'],
                f"  ({row['change']:+.2f})",
                color
            ))

        price_fig = px.line(
            df, x='timestamp', y='price', color='symbol',
            title='Stock Price Over Time',
            color_discrete_sequence=['#4cc9f0','#4ade80','#f97316','#a78bfa','#f87171'],
        )
        price_fig.update_layout(
            paper_bgcolor='#1a1a2e', plot_bgcolor='#0f3460',
            font_color='white', title_font_color='white',
            xaxis=dict(gridcolor='#1a1a2e'),
            yaxis=dict(gridcolor='#1a1a2e'),
        )

        change_fig = px.bar(
            latest, x='symbol', y='change',
            title='Price Change',
            color='change',
            color_continuous_scale=['#f87171','#4ade80'],
        )
        change_fig.update_layout(
            paper_bgcolor='#1a1a2e', plot_bgcolor='#0f3460',
            font_color='white', title_font_color='white',
        )

        volume_fig = px.bar(
            latest, x='symbol', y='volume',
            title='Trading Volume',
            color_discrete_sequence=['#4cc9f0'],
        )
        volume_fig.update_layout(
            paper_bgcolor='#1a1a2e', plot_bgcolor='#0f3460',
            font_color='white', title_font_color='white',
        )

        return cards, price_fig, change_fig, volume_fig

    return app


if __name__ == '__main__':
    config = load_config()
    db_file = config['pipeline']['db_file']
    app = create_app(db_file)
    print('Dashboard running at http://localhost:8050')
    app.run(debug=False, port=8050)
