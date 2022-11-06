# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 10:38:50 2022

@author: s14085
"""

import pandas as pd
import dash
import pathlib
from dash import dcc, html
from dash.dependencies import Input, Output, State, ClientsideFunction
from dateutil.relativedelta import *
from datetime import datetime
import plotly.graph_objects as go
import h3
import numpy as np
import boto3
import pyarrow.parquet as pq
import s3fs
from controls import FLEET,COLORS
from choropleth_trade_map import count_by_hexagon, choropleth_map

AWS_KEY= os.environ.get('AWS_KEY', None)
AWS_SECRET  = os.environ.get('AWS_SECRET', None)

s3 = s3fs.S3FileSystem(key=AWS_KEY,secret=AWS_SECRET)
# # ###Dataframes

base=pq.ParquetDataset(os.environ.get('BASE_U', None), filesystem=s3).read_pandas().to_pandas()

port_visits=pq.ParquetDataset(os.environ.get('PORTS_U', None), filesystem=s3).read_pandas().to_pandas()

berth_time=pq.ParquetDataset(os.environ.get('BERTH_U', None), filesystem=s3).read_pandas().to_pandas()


dates_ports=pq.ParquetDataset(os.environ.get('DATES_U', None), filesystem=s3).read_pandas().to_pandas()

# get relative data folder
PATH = pathlib.Path(__file__).parent
# DATA_PATH = PATH.joinpath("data").resolve()

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
 
app.title = 'Utkilen'

server = app.server

####Global map templates
MAPBOX_TOKEN = os.environ.get('MAPBOX_KEY', None)

layout_map = dict(
    autosize=True,
    paper_bgcolor='#ffffff',
    plot_bgcolor='#ffffff',
    margin=dict(l=10, r=10, b=10, t=10),
    hovermode="closest",
    font=dict(family="Open Sans",size=17,color="#313131"),
    legend=dict(font=dict(size=10), orientation="h"),
    mapbox=dict(
        accesstoken=MAPBOX_TOKEN,
        style='mapbox://styles/gabrielfuenmar/ckkmdsqrm4ngq17o21tnno1ce',
        center=dict(lon=6.5, lat=59.2),
        zoom=9,
    ),
    showlegend=False,
) 

###Global Graphs templates
layout= dict(
    legend=dict(bgcolor='rgba(0,0,0,0)',font=dict(size=14,family="HelveticaNeue")),
    font_family="Open Sans",
    font_color="#313131",
    title_font_family="Open Sans",
    title_font_color="#313131",
    title_font_size=20,
    paper_bgcolor='#ffffff',
    plot_bgcolor='#ffffff',
    margin=dict(l=0, r=0, t=0, b=0),
    xaxis=dict(gridcolor="rgba(178, 178, 178, 0.1)",title_font_size=15,
                tickfont_size=14,title_font_family="HelveticaNeue",tickfont_family="HelveticaNeue"),
    yaxis=dict(gridcolor="rgba(178, 178, 178, 0.1)",title_font_size=15,tickfont_size=14,
                title_font_family="HelveticaNeue",tickfont_family="HelveticaNeue")
    )

##------Modebar on graphs
config={"displaylogo":False, 'modeBarButtonsToRemove': ['autoScale2d']} 
config_fix={'displayModeBar': False,"displaylogo":False, 'modeBarButtonsToRemove': ['autoScale2d']}
##------Annotation on graphs
annotation_layout=dict(
    xref="x domain",
    yref="y domain",
    x=0.25,
    y=-0.35)


app.layout=html.Div(
                    [    #1
                    dcc.Store(id="aggregate_data"),
                    # empty Div to trigger javascript file for graph resizing
                    html.Div(id="output-clientside"),
                    html.Div(## 1.1
                        [
                        html.Div(## 1.1.1
                            [##Empty
                            ],
                            className="one-third column",
                        ),
                        html.Div( ##1.1.2
                            [
                            html.H3(
                                  "ShipTrack",
                                  style={"margin-bottom": "0px"},
                              ),
                              html.H5(
                                  "Utkilen statistics", style={"margin-top": "0px"}
                              ),
                            ],
                            className="one-half column",
                            id="title",
                                ),
                        html.Div(   ## 1.1.3
                            [
                                html.Button("Refresh", id="refresh-button"), 
                                html.A(
                                    html.Button("Developer", id="home-button"),
                                    href="https://gabrielfuentes.org",
                                        )                  
                            ],
                            className="one-third column",
                            id="button",
                            style={
                                "text-align": "center"},
                                ),
                        ],
                            id="header",
                            className="row flex-display",
                            style={"margin-bottom": "15px"},
                            ),
                    html.Div( ###1.2
                        [ 
                        html.Div([                           
                            html.Div(### 1.2.1 filters
                                  [
                                  html.P("Date Filter",
                                                className="control_label",
                                            ),
                                  html.Div([html.P(id="date_from"),
                                  html.P(id="date_to")],className="datecontainer"),
                                  dcc.RangeSlider(
                                    id="year_slider",
                                    min=0,
                                    max=33,
                                    value=[0, 33],
                                    marks={
                                        0:"Jan 2020",  6:"Jul 2020", 
                                        12:"Jan 2021", 18:"Jul 2021",
                                        24:"Jan 2022", 30:"Jul 2022", 
                                        33:"Oct 2022"},
                                    step=1,
                                    allowCross=False,
                                    className="dcc_control",
                                ),
                                  html.P("Vessel:", className="control_label"),
                                  dcc.Dropdown(
                                    id='vessels_dropdown',
                                    options=[{'label': row,'value': FLEET[row]} \
                                              for row in sorted(FLEET)],
                                    placeholder="All",multi=True,
                                    className="dcc_control")
                                  ],
                        id="cross-filter-options",
                        className="pretty_container"
                            ),       
                        html.Div([html.Div([html.H5("Top visited ports")]),
                                  dcc.Loading([dcc.Graph(animate=False,config=config,id="top_ports_in")],id="loading_icon_ports",type="circle")
                                  ],className="pretty_container")
                                  ],id="box_filter_graph",className="four columns",
                                style={"display": "flex", "flex-direction":"column"}),
                        html.Div(### 1.2.2 map
                                    [
                                        html.Div([ html.H5("Trade patterns"),
                                        html.H6(id="month_map",style={"color":"#313131"})],
                                        style={"display": "flex", "flex-direction": "row",
                                                "justify-content":"space-between"}),
                                        
                                        dcc.Loading([dcc.Graph(animate=False,config=config,id="map_in")],id="loading_icon",type="circle"),                                     
                                        html.P(["Grid size"],id="grid_size",className="control_label"),
                                          dcc.Slider(
                                          id="zoom_slider",
                                          min=2,
                                          max=6,
                                          value=6,
                                          marks={2:{'label': '1'},3:{'label': '2'},4:{'label': '3'},
                                            5:{'label': '4'},6:{'label': '5'}},
                                          className="dcc_control",
                                          included=False), 
                                ],   
                            id="MapContainer",
                            style={"display": "flex", "flex-direction":"column", "justify-content":"start"},
                            className="pretty_container five columns",                    
                            ),
                        html.Div(#### 1.2.3 Top ports visited
                            [   html.Div([html.H5("Most visited area")]),
                                dcc.Loading([dcc.Graph(animate=False,config=config_fix,id="map_hex")],id="loading_icon_hex",type="circle")
                                ],
                            id="top_ports",
                            className="pretty_container three columns",
                            style={"display": "flex", "flex-direction":"column", 
                                    "justify-content":"start"}
                            )                                 
                        ],
                    className="row flex-display twelve columns",
                        ),
                    html.Div([html.Div( ###1.3
                                        [html.Div([html.H5("Time at berth")]),
                                                  dcc.Loading([dcc.Graph(animate=False,config=config,id="berth_time")],id="loading_icon_btime",type="circle")
                                                  ],id="berth_time_box",className="pretty_container twelve columns"),
                              # html.Div( ###1.4
                              #          [html.H4("Port congestion"),
                              #           html.H5("Under construction")],id="congestion",
                              #          className="pretty_container six columns",
                              #          style={"display":"flex","flex-direction":"column",
                              #                 "justify-content":"center","align-items":"center"})
                              ],
                              id="third_row",style={"display": "flex", "flex-direction": "row"})
                    ],
                    id="mainContainer",
                    style={"display": "flex", "flex-direction": "column"},
                    )

def trade_map(res,fr="01-01-2020",to="30-09-2022",lat=None,lon=None,zoom=None,vessels_imo=[]):
    
    base_in=base.copy()
    date_fr=pd.to_datetime(fr,dayfirst=True)
    date_to=pd.to_datetime(to,dayfirst=True)
    
    df_aggreg=count_by_hexagon(base_in,res,date_fr,date_to,vessels=vessels_imo)
    
    ##Update layout
    if lat is not None:
        layout_map["mapbox"]["center"]["lon"]=lon
        layout_map["mapbox"]["center"]["lat"]=lat
        layout_map["mapbox"]["zoom"]=zoom
        
    if df_aggreg.shape[0]>0:
        heatmap=choropleth_map(df_aggreg,layout_map)
    else:
        heatmap=go.Figure(data=go.Scattermapbox(lat=[0],lon=[0]),layout=layout_map)

    return heatmap

def port_visits_graph(fr="01-01-2020",to="30-09-2022",vessels_imo=[]):
    
    port_in=port_visits.copy()
    date_fr=pd.to_datetime(fr,dayfirst=True)
    date_to=pd.to_datetime(to,dayfirst=True)
    
    if vessels_imo:
        port_in=port_in[((port_in.arrival_time.between(date_fr,date_to))&(port_in.imo.isin(vessels_imo)))]
    else:
        port_in=port_in[port_in.arrival_time.between(date_fr,date_to)]
    
    port_in=port_in.groupby("port_name_short").agg({"imo":"count"}).rename(columns={"imo":"count"}).reset_index()
    
    port_in=port_in.sort_values(by=["count"],ascending=False).reset_index()
    
    port_in=port_in.loc[0:10].sort_values(by=["count"]).reset_index()
    
    plot_port=go.Figure(go.Bar(x=port_in["count"],y=port_in["port_name_short"],orientation="h"))
    
    plot_port.update_layout(layout)
    
    plot_port.update_traces(marker_color='#01B8AA', marker_line_color='#5f6B6D',
                  marker_line_width=1.5, opacity=0.6)
    
    return plot_port
 

def most_visited_map(fr="01-01-2020",to="30-09-2022",vessels_imo=[]):
    
    port_in=port_visits.copy()
    base_in=base.copy()
    date_fr=pd.to_datetime(fr,dayfirst=True)
    date_to=pd.to_datetime(to,dayfirst=True)
    
    if vessels_imo:
        port_in=port_in[((port_in.arrival_time.between(date_fr,date_to))&(port_in.imo.isin(vessels_imo)))]
        pos_in=base_in[((base_in.nav_status=="Moored")&(base.imo.isin(vessels_imo))&(base_in.dt_pos_utc.between(date_fr,date_to)))]
    else:
        port_in=port_in[port_in.arrival_time.between(date_fr,date_to)]
        pos_in=base_in[((base_in.nav_status=="Moored")&(base_in.dt_pos_utc.between(date_fr,date_to)))]
    
    port_c=port_in.groupby("port_name_short").agg({"imo":"count"}).rename(columns={"imo":"count"}).reset_index()
    
    port_c=port_c.sort_values(by=["count"],ascending=False).reset_index()
    
    port_in=port_in[port_in.port_name_short==port_c.port_name_short.iloc[0]]
    
    port_in=port_in.groupby("res_8_destination").agg({"imo":"count"}).rename(columns={"imo":"count"}).reset_index()
    
    port_in=port_in.sort_values(by=["count"],ascending=False).reset_index()
    
    pos_in=pos_in[pos_in.res_8==port_in.res_8_destination.iloc[0]]
    
    coord=h3.h3_to_geo_boundary(port_in.res_8_destination.iloc[0],
                                geo_json=True)
    center=h3.h3_to_geo(port_in.res_8_destination.iloc[0])
    
    zoom_fig = go.Figure(go.Scattermapbox(lat=pos_in.latitude.tolist(),
    lon=pos_in.longitude.tolist(),
    mode = "markers",marker = {'size': 3, 'color': "#ffffff" ,"opacity":0.6}),
                          layout=layout_map)
    
    zoom_fig.update_layout(
    mapbox = {'center': { 'lon': center[1], 
                          'lat': center[0]},
        'zoom': 13, "style":"mapbox://styles/gabrielfuenmar/cla32e7tw00f414nxrhtsurkl",
        'layers': [{
            'source': {
                'type': "FeatureCollection",
                'features': [{
                    'type': "Feature",
                    'geometry': {
                        'type': "Polygon",
                        'coordinates': [coord]
                    }
                }]
            },
            'type': "line", 'below': "traces", 'color': "#FD625E"}]},
    margin = {'l':0, 'r':0, 'b':0, 't':0})
    
    return zoom_fig

def berth_time_graph(fr="01-01-2020",to="30-09-2022",vessels_imo=[]):
    
    port_in=port_visits.copy()
    berth_in=berth_time.copy()
    dates_ports_in=dates_ports.copy()
    date_fr=pd.to_datetime(fr,dayfirst=True)
    date_to=pd.to_datetime(to,dayfirst=True)
    
    if vessels_imo:
        port_in=port_in[((port_in.arrival_time.between(date_fr,date_to))&(port_in.imo.isin(vessels_imo)))]
        berth_in=berth_in[((berth_in.first_dt_pos_utc.between(date_fr,date_to))&(berth_in.imo.isin(vessels_imo)))]
    
    else:
        port_in=port_in[port_in.arrival_time.between(date_fr,date_to)]
        berth_in=berth_in[berth_in.first_dt_pos_utc.between(date_fr,date_to)]
    
    port_in=port_in.groupby("port_name_short").agg({"imo":"count"}).rename(columns={"imo":"count"}).reset_index()
    
    port_in=port_in.sort_values(by=["count"],ascending=False).reset_index()
    
    port_in=port_in.loc[0:4].sort_values(by=["count"]).reset_index()
    
    port_ls=port_in.port_name_short.unique().tolist()
    
    berth_in=berth_in[berth_in.port_name_short.isin(port_ls)]
    
    berth_in=berth_in.groupby(["week_year","port_name_short","week","year"])\
                .agg({"berth_time":np.mean}).reset_index()
               
    dates_ports_in=dates_ports_in[dates_ports_in.port_name_short.isin(port_ls)]
    berth_in=pd.merge(dates_ports_in,berth_in,on=["week_year","port_name_short","week","year"],how="left")
    
    berth_in=berth_in.sort_values(by=["year","week"]).reset_index(drop=True)
    
    plot_times=go.Figure()
    plot_times.update_layout(layout)
    plot_times.update_layout(xaxis_title='Week-Year',
                              yaxis_title='Hours')
    val=0
    for ind,group in berth_in.groupby("port_name_short"):
        
        plot_times.add_trace(go.Scatter(
            x=group.week_year,
            y=group.berth_time,
            name=ind,mode='lines+markers',
            line=dict(color=COLORS[val], width=2)))
        val+=1
    
    return plot_times

###Callbacks
##Month_map
@app.callback(
      Output("month_map", "children"),
    [ Input('year_slider', 'value') 
      ],
)

def month_map(date):
    fr=(pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[0]))
    to=(pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[1]))
    
    m_fr=datetime.strptime(str(fr.month), "%m").strftime("%b")
    m_to=datetime.strptime(str(to.month), "%m").strftime("%b")

    
    m_e="{} {} to {} {}".format(m_fr,fr.year,m_to,to.year)
    
    return m_e

##Update map
@app.callback(
    Output("map_in", "figure"),
    [Input("zoom_slider","value"),
      Input('year_slider', 'value'),
      Input("vessels_dropdown","value"),
      ],
    [State("map_in","relayoutData")]
)

def update_trade_map(resol,date,vessels_imo,relay):
    
    date_fr=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[0])
    date_to=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[1])
    date_to=date_to+ relativedelta(day=31)
     
    if relay is not None:   
        if "mapbox.center" in relay.keys():
            lat=relay["mapbox.center"]["lat"]
            lon=relay["mapbox.center"]["lon"]
            zoom=relay["mapbox.zoom"]
        else:
            lat=59.2
            lon=6.5
            zoom=3
    else:
        lat=59.2
        lon=6.5
        zoom=3
    
    if vessels_imo is None: 
        vessels_imo=[]
    
    ####Size deactived for the time being.
    trade_fig=trade_map(resol,fr=date_fr,to=date_to,lat=lat,lon=lon,zoom=zoom,vessels_imo=vessels_imo)
        
    return trade_fig 

##Update port graph
@app.callback(
    Output("top_ports_in", "figure"),
    [Input('year_slider', 'value'),
      Input("vessels_dropdown","value"),
      ],
)

def update_port_visits_graph(date,vessels_imo):
    
    date_fr=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[0])
    date_to=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[1])
    date_to=date_to+ relativedelta(day=31)
    
    port_fig_in=port_visits_graph(fr=date_fr,to=date_to,vessels_imo=vessels_imo)
    
    return port_fig_in

@app.callback(
    Output("map_hex", "figure"),
    [Input('year_slider', 'value'),
      Input("vessels_dropdown","value"),
      ])

def update_hex_map(date,vessels_imo):
    
    date_fr=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[0])
    date_to=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[1])
    date_to=date_to+ relativedelta(day=31)
        
    if vessels_imo is None: 
        vessels_imo=[]
    
    ####Size deactived for the time being.
    zoom_fig=most_visited_map(fr=date_fr,to=date_to,vessels_imo=vessels_imo)
        
    return zoom_fig

@app.callback(
    Output("berth_time", "figure"),
    [Input('year_slider', 'value'),
      Input("vessels_dropdown","value"),
      ],
)

def update_port_visits_graph(date,vessels_imo):
    
    date_fr=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[0])
    date_to=pd.to_datetime("01-01-2020 00:00")+relativedelta(months=+date[1])
    date_to=date_to+ relativedelta(day=31)
    
    port_time_fig_in=berth_time_graph(fr=date_fr,to=date_to,vessels_imo=vessels_imo)
    
    return port_time_fig_in
##Refresh button
@app.callback([Output("vessels_dropdown","value"),
                Output('year_slider', 'value')],
              [Input('refresh-button', 'n_clicks')])     

def clearMap(n_clicks):
    if n_clicks !=0:
        pdd=[]
        ysld=[0,33]

        return pdd,ysld
    
if __name__ == "__main__":
    app.run_server(debug=True,use_reloader=True)
