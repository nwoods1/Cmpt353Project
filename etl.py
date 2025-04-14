"""
Perform ETL on the dataset found in ~/data/raw/data.

CMPT 353: Spring 2025 Final project.
"""

from __future__ import annotations

import json
import os
import re

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, Point


RAW_DATA_PATH = "data/raw_data"
CLEANED_DATA_PATH = "data/cleaned_data"


def read_csv(file_name: str, sep:str =";") -> pd.DataFrame:
    """
    Read from a csv and output a pandas dataframe

    Args:
        file_name (str): File path to csv.
        sep (str, optional): Separator in the csv. Defaults to ";".

    Returns:
        pd.DataFrame: Parsed csv file as a dataframe.
    """
    return pd.read_csv(file_name, sep=sep, engine="python", on_bad_lines="skip")


def to_csv(df: pd.DataFrame, name: str) -> None:
    """
    Write dataframe out to a csv file.

    Args:
        df (pd.DataFrame): Dataframe to be written out.
        name (str): Name of the file to write out to.
    """
    df.to_csv(f"{CLEANED_DATA_PATH}/{name}", index=False)


def df_to_gdf(df: pd.DataFrame, geometry_key: str) -> gpd.GeoDataFrame:
    """
    Convert a pandas dataframe to a geopandas geodataframe.

    Args:
        df (pd.DataFrame): pandas dataframe to be converted.
        geometry_key (str): Column name of the geometry data.

    Returns:
        gpd.GeoDataFrame: Converted pandas dataframe.
    """
    return gpd.GeoDataFrame(df, geometry=geometry_key)


def tickets_preproc(tickets: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Transform the raw ticket with the following:
    * Remove unnecessary columns
    * Extract date and hours values from EntryData
    * Add Point objects that represent lat & lon given the Block and Street
    
    Args:
        tickets (pd.DataFrame): Raw parking ticket dat

    Returns:
        gpd.GeoDataFrame: Cleaned and transformed tickets data.
    """
    print("Performing pre-processing on tickets")
   
    target_bylaws = [2952] # parking-meter related bylaw
    target_sections = [
        # PARK IN A METERED SPACE IF THE TIME RECORDED BY THE OPERATOR UNDER 
        # THE PAY BY PHONE OR PAY BY LICENCE PLATE OPTION HAS EXPIRED
        "5(4)(A)(ii)",
        "5(4)(a)(ii)",
        
        # PARK IN A METERED SPACE IF THE PARKING METER HEAD DISPLAYS FOUR 
        # FLASHING ZEROS IN A WINDOW
        "5(4)(B)",
        "5(4)(b)"
    ]
    target_status = ["IS"] # ticket issued
   
    tickets = tickets[
        (tickets["Bylaw"].isin(target_bylaws)) 
        & (tickets["Section"].isin(target_sections)) 
        & (tickets["Status"].isin(target_status))
    ]     
    
    tickets = tickets.drop(
        ["Bylaw", "Section", "InfractionText", "Status", "BI_ID"], 
        axis=1
    )

    # clean up the entry date
    tickets['EntryDate'] = pd.to_datetime(tickets['EntryDate'], errors='coerce')
    tickets = tickets[tickets['EntryDate'].notnull()]
    tickets["dayofweek"] = tickets['EntryDate'].dt.dayofweek

    # add latitude and longitude to tickets df
    block_street_with_lat_lon = read_csv(f"{CLEANED_DATA_PATH}/block_street_with_lat_lon.csv", sep=",")
    
    tickets["Block"] = tickets["Block"].astype(int)
    tickets["Street"] = tickets["Street"].astype(str)
    block_street_with_lat_lon["Block"] = block_street_with_lat_lon["Block"].astype(int)
    block_street_with_lat_lon["Street"] = block_street_with_lat_lon["Street"].astype(str)
    
    tickets = pd.merge(tickets, block_street_with_lat_lon, on=["Block", "Street"], how="inner")
    tickets = tickets.dropna(subset=["lat", "lon"])
    tickets["Geometry"] = tickets.apply(lambda row: Point(row["lat"], row["lon"]), axis=1)
    tickets = tickets.drop(["lat", "lon"], axis=1)
    
    return df_to_gdf(tickets, "Geometry")
    

def meters_preproc() -> gpd.GeoDataFrame:
    """
    Transform the raw meter with the following:
    * Remove unnecessary columns
    * Convert raw Geom data into Point objects    

    Returns:
        gpd.GeoDataFrame: Cleaned and transformed meter data
    """
    print("performing pre-processing on meters")

    # read and extract
    with open(
        f"{RAW_DATA_PATH}/parking-meters.csv", 
        "r", 
        encoding="utf-8", 
        errors="ignore"
    ) as f:
        lines = f.readlines()

    rows = [line.strip().split(";") for line in lines]
    meters = pd.DataFrame(rows[1:], columns=rows[0])  

    headers = [
        "METERHEAD", "R_MF_9A_6P", "R_MF_6P_10", "R_SA_9A_6P", "R_SA_6P_10", 
        "R_SU_9A_6P", "R_SU_6P_10", "RATE_MISC", "TIMEINEFFE", "T_MF_9A_6P", 
        "T_MF_6P_10", "T_SA_9A_6P", "T_SA_6P_10", "T_SU_9A_6P", "T_SU_6P_10", 
        "TIME_MISC", "CREDITCARD", "PAY_PHONE", "Geom", "Geo Local Area", 
        "METERID", "geo_point_2d"
    ]

    meters.columns = headers        
    headers = set(headers)
    headers_to_keep = {
        "METERHEAD", "CREDITCARD", "Geom", "Geo Local Area", "METERID"
    }
    meters = meters.drop(headers - headers_to_keep, axis=1)

    # convert credit card values to boolean int
    meters["CREDITCARD"] = meters["CREDITCARD"].replace({"Yes": 1, "No": 0})
    
    
    def extract_coordinates(geom_str):
        match = re.search(r'\[(-?\d+\.\d+),\s*(-?\d+\.\d+)\]', str(geom_str))
        if match:
            return float(match.group(1)), float(match.group(2))
        return None, None


    meters["lon"], meters["lat"] = zip(*meters['Geom'].apply(extract_coordinates))
    meters = meters[meters["lon"].notnull() & meters["lat"].notnull()]    
    meters["Geometry"] = meters.apply(lambda row: Point(row["lat"], row["lon"]), axis=1)
    
    meters = meters.drop(["Geom", "lat", "lon"], axis=1)    
    return df_to_gdf(meters, "Geometry")
    

def boundaries_preproc(boundaries_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Transform the raw boundary with the following:
    * Remove unnecessary columns
    * Convert raw Geom data into LineString objects
    * Add more descriptive names

    Args:
        boundaries_df (pd.DataFrame): Raw boundary data for Vancouver.

    Returns:
        gpd.GeoDataFrame: Cleaned and transformed boundary data.
    """
    print("performing pre-processing on boundaries")
    boundaries_df = boundaries_df.drop("geo_point_2d", axis=1)
    
    # prepare the dataframe to be converted into a geodataframe
    def to_linestring(area) -> Polygon:
        area = json.loads(area)    
        coords = area["coordinates"][0]
        flipped = [(lat, lon) for lon, lat in coords]
        return Polygon(flipped)


    boundaries_df["Geom"] = boundaries_df["Geom"].apply(to_linestring)
    boundaries_df.rename(
        columns={
            "Name": "Neighbourhood",     
            "Geom": "Geometry"
        }, inplace=True)    
    return df_to_gdf(boundaries_df, "Geometry")


def add_geo_local_area_to_tickets(
    tickets_df: gpd.GeoDataFrame,
    boundaries_df: gpd.GeoDataFrame 
) ->gpd.GeoDataFrame:
    """
    Add the neighbourhood (geo local area) from boundaries_df
    to tickets_df.

    Args:
        tickets_df (gpd.GeoDataFrame): Ticket dataframe with Point objects in Vancouver.
        boundaries_df (gpd.GeoDataFrame): Boundary dataframe of Vancouver neighbourhoods.

    Returns:
        gpd.GeoDataFrame: Ticket dataframe with neighbourhood data.
    """

    joined_gdf = gpd.sjoin(tickets_df, boundaries_df, how="left", predicate="within")
    joined_gdf = joined_gdf.drop(columns=["index_right"])
    return joined_gdf


def etl() -> None:
    """
    Perform ETL on raw ticket, meter and Vancouver boundary data.
    """
    # read in the raw data
    chunked_files = sorted([f for f in os.listdir(f"{RAW_DATA_PATH}/parking_tickets") if f.endswith(".json.gz")])
    tickets_df = pd.concat([
        pd.read_json(os.path.join(f"{RAW_DATA_PATH}/parking_tickets", f), orient='records', lines=True)
        for f in chunked_files
    ], ignore_index=True)        
    boundaries_df = read_csv("data/raw_data/local-area-boundary.csv")

    # perform preprocessing and cleaning
    tickets_df = tickets_preproc(tickets_df) 
    meters_df = meters_preproc()
    boundaries_df = boundaries_preproc(boundaries_df)

    # add Geo Local Area to tickets_df from boundaries_df
    tickets_df = add_geo_local_area_to_tickets(tickets_df, boundaries_df) 

    # write the processed csv files     
    to_csv(tickets_df, "parking_tickets.csv")
    to_csv(meters_df, "parking_meters.csv")
    to_csv(boundaries_df, "local_area_boundaries.csv")
    

if __name__ == "__main__":
    etl()    