import pandas as pd
import plotly.graph_objects as go

# Load the CSV file
data = pd.read_csv('data.csv')

# Clean up column names and ensure date consistency
data.columns = ['bussiness_dt', 'client_cd', 'processing_region_cd', 'snapshot_type_cd', 'records']
data['bussiness_dt'] = pd.to_datetime(data['bussiness_dt'])

# Initialize figure for area chart
fig = go.Figure()

# Create traces for each combination of client, region, and snapshot type
traces = []
for (client, region, snapshot), group in data.groupby(['client_cd', 'processing_region_cd', 'snapshot_type_cd']):
    traces.append(go.Scatter(
        x=group['bussiness_dt'],
        y=group['records'],
        mode='lines',
        fill='tozeroy',
        name=f"{client} - {region} - {snapshot}",
        visible=True  # Initially set traces to visible
    ))

# Add traces to the figure
for trace in traces:
    fig.add_trace(trace)

# Create dropdown filters dynamically based on available values in the data
client_options = data['client_cd'].unique().tolist()
region_options = data['processing_region_cd'].unique().tolist()
snapshot_options = data['snapshot_type_cd'].unique().tolist()

# Function to update visibility of traces based on selected filters
def update_visibility(client, region, snapshot):
    visibility = []
    for trace in fig.data:
        trace_client, trace_region, trace_snapshot = trace.name.split(" - ")
        visible = (
            (client == "All Clients" or trace_client == client) and
            (region == "All Regions" or trace_region == region) and
            (snapshot == "All Snapshots" or trace_snapshot == snapshot)
        )
        visibility.append(visible)
    return visibility

# Create dropdowns for filters with improved layout and formatting
fig.update_layout(
    title="Records Trend by Business Date, Client, Region, and Snapshot Type",
    xaxis_title="Business Date",
    yaxis_title="Records",
    template="plotly_white",
    legend_title="Client, Region, Snapshot",
    xaxis=dict(tickformat="%Y-%m-%d"),
    showlegend=True,
    margin=dict(l=50, r=50, t=80, b=50),
    updatemenus=[
        # Client dropdown filter (no "All Clients" option)
        dict(
            buttons=[dict(label=client, method="update", 
                          args=[{"visible": update_visibility(client=client, region="All Regions", snapshot="All Snapshots")}])
                     for client in client_options],
            direction="down",
            showactive=True,
            x=0.17,
            xanchor="left",
            y=1.15,
            yanchor="top",
            font=dict(size=12),
            pad={"r": 10, "t": 10},
            bgcolor="lightgray",
            bordercolor="black",
            borderwidth=1,
        ),
        # Region dropdown filter (no "All Regions" option)
        dict(
            buttons=[dict(label=region, method="update", 
                          args=[{"visible": update_visibility(client="All Clients", region=region, snapshot="All Snapshots")}])
                     for region in region_options],
            direction="down",
            showactive=True,
            x=0.25,
            xanchor="left",
            y=1.15,
            yanchor="top",
            font=dict(size=12),
            pad={"r": 10, "t": 10},
            bgcolor="lightgray",
            bordercolor="black",
            borderwidth=1,
        ),
        # Snapshot type dropdown filter (no "All Snapshots" option)
        dict(
            buttons=[dict(label=snapshot, method="update", 
                          args=[{"visible": update_visibility(client="All Clients", region="All Regions", snapshot=snapshot)}])
                     for snapshot in snapshot_options],
            direction="down",
            showactive=True,
            x=0.33,
            xanchor="left",
            y=1.15,
            yanchor="top",
            font=dict(size=12),
            pad={"r": 10, "t": 10},
            bgcolor="lightgray",
            bordercolor="black",
            borderwidth=1,
        )
    ]
)

# Save the plot as HTML without Plotly toolbar
fig.write_html("filtered_records_trend.html", include_plotlyjs="cdn", config={'displayModeBar': False})

print("Chart saved as filtered_records_trend.html")
