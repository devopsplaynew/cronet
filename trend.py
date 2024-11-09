import pandas as pd
import plotly.graph_objects as go

data = pd.read_csv('data.csv')

data.columns = ['bussiness_dt', 'client_cd', 'processing_region_cd', 'snapshot_type_cd', 'records']
data['bussiness_dt'] = pd.to_datetime(data['bussiness_dt'])

fig = go.Figure()

for (client, region, snapshot), group in data.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd']):
    fig.add_trace(go.Scatter(
        x=group['bussiness_dt'],
        y=group['records'],
        mode='lines',
        name=f"{client} - {region} - {snapshot}",
        fill='tozeroy',   # Area chart style
        hoverinfo='x+y+name',
        line=dict(width=0.5)
    ))

fig.update_layout(
    title="Volume Trends",
    xaxis_title="Business Date",
    yaxis_title="Records",
    template="plotly_white",
    legend_title="Client, Region, Snapshot",
    xaxis=dict(tickformat="%Y-%m-%d"),
    showlegend=True,
    margin=dict(l=50, r=50, t=80, b=50)
)

fig.write_html("filtered_records_trend.html", include_plotlyjs="cdn", config={'displayModeBar': False})

print("Chart saved as filtered_records_trend.html")
