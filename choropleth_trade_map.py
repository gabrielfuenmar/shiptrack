# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 12:34:25 2022

@author: s14085
"""

import pandas as pd
import h3
import orjson
from geojson.feature import FeatureCollection, Feature
import plotly.graph_objs as go
import geopandas as gpd
from shapely import wkt


def count_by_hexagon(df,resolution,fr,to,vessels):
    """
    Use h3.geo_to_h3 to index each data point into the spatial index of the specified resolution.
    Use h3.h3_to_geo_boundary to obtain the geometries of these hexagons
    
    Ex counts_by_hexagon(data, 8)
    """
       
    if vessels:
        df_aggreg=df[((df.dt_pos_utc.between(fr,to))&(df.imo.isin(vessels)))]
    else:
        df_aggreg=df[df.dt_pos_utc.between(fr,to)]
    if df_aggreg.shape[0]>0:        
        if resolution==8:
            df_aggreg = df_aggreg.groupby(by = "res_8").agg({"vessel_name":"count"}).reset_index()\
                        .rename(columns={"res_8":"hex_id","vessel_name":"count"})
        else:
            df_aggreg = df_aggreg.assign(new_res=df_aggreg.res_8.apply(lambda x: h3.h3_to_parent(x,resolution)))
            df_aggreg = df_aggreg.groupby(by = "new_res").agg({"vessel_name":"count"}).reset_index()\
                        .rename(columns={"new_res":"hex_id","vessel_name":"count"})
                        
        df_aggreg["geometry"] =  df_aggreg.hex_id.apply(lambda x: 
                                                                {    "type" : "Polygon",
                                                                      "coordinates": 
                                                                    [h3.h3_to_geo_boundary(x,geo_json=True)]
                                                                }
                                                            )
        return df_aggreg
    else:
        return df_aggreg
    
def hexagons_dataframe_to_geojson(df_hex, file_output = None):
    """
    Produce the GeoJSON for a dataframe that has a geometry column in geojson 
    format already, along with the columns hex_id and value
    
    Ex counts_by_hexagon(data)
    """    
   
    list_features = []
    
    for i,row in df_hex.iterrows():
        feature = Feature(geometry = row["geometry"] , id=row["hex_id"], properties = {"value" : row["value"]})
        list_features.append(feature)
        
    feat_collection = FeatureCollection(list_features)
    
    geojson_result = orjson.dumps(feat_collection)
    
    #optionally write to file
    if file_output is not None:
        with open(file_output,"w") as f:
            orjson.dump(feat_collection,f)
    
    return geojson_result 
    

def choropleth_map(df_aggreg,layout_in,fill_opacity = 0.5):
    
    """
    Creates choropleth maps given the aggregated data.
    """    
    
    
    df_aggreg.rename(columns={"count":"value"},inplace=True)   
    #colormap
    min_value = df_aggreg["value"].min()
    max_value = df_aggreg["value"].max()
    m = round ((min_value + max_value ) / 2 , 0)
    
    #take resolution from the first row
    res = h3.h3_get_resolution(df_aggreg.loc[0,'hex_id'])
    
    #create geojson data from dataframe
    geojson_data = orjson.loads(hexagons_dataframe_to_geojson(df_hex = df_aggreg))
    
    ##plot on map
    initial_map=go.Choroplethmapbox(geojson=geojson_data,
                                    locations=df_aggreg.hex_id.tolist(),
                                    z=df_aggreg["value"].round(2).tolist(),
                                    colorscale="YlOrBr",
                                    marker_opacity=fill_opacity,
                                    marker_line_width=1,
                                    colorbar = dict(thickness=20, ticklen=3,title="density"),
                                    hovertemplate = '%{z:,.2f}<extra></extra>')
    
    initial_map=go.Figure(data=initial_map,layout=layout_in)
    
    return initial_map



