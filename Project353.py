import json
import os
from typing import Tuple

import geopandas as gpd
import pandas as pd
import re
from shapely.geometry import LineString, Polygon, Point


RAW_DATA_PATH = "data/raw_data"
CLEANED_DATA_PATH = "data/cleaned_data"

def read_csv(file_name: str, sep:str =";") -> pd.DataFrame:
    return pd.read_csv(file_name, sep=sep, engine="python", on_bad_lines="skip")


def to_csv(df: gpd.GeoDataFrame, name: str) -> None:
    df.to_csv(f"{CLEANED_DATA_PATH}/{name}", index=False)


def df_to_gdf(df: pd.DataFrame, geometry_key: str) -> gpd.GeoDataFrame:
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
   
    meter_infraction_text = [
        'PARK IN A METERED SPACE IF THE TIME RECORDED BY THE OPERATOR UNDER THE PAY BY PHONE OR PAY BY LICENCE PLATE OPTION HAS EXPIRED',
        'PARK IN A METERED SPACE IF THE PARKING METER HEAD DISPLAYS FOUR FLASHING ZEROS IN A WINDOW',
        'PARK IN A METERED SPACE IF THE TIME RECORDED BY THE OPERATOR UNDER THE PAY BY PHONE OR PAY BY LICENCE PLATE OPTION HAS EXPIRED..',
        'VEHICLE LEFT IN A METERED SPACE FOR A PERIOD LONGER THAN THE TIME LIMIT IN HOURS THAT IS SHOWN ON THE PARKING METER HEAD OR RECORDED UNDER THE PAY BY PHONE OR PAY BY LICENCE PLATE OPTION',
        'A PERSON MUST PARK A VEHICLE ENTIRELY WITHIN A METERED SPACE AS DEFINED IN SECTION 2(2)',
        'PARK IN A METERED SPACE IF THE PARKING METER HEAD DISPLAYS FOUR FLASHING ZEROS IN A WINDOW..',
        'PARK IN A METERED SPACE IF THE PARKING METER HEAD DISPLAYS A "FAIL" TEXT IN A WINDOW',
        'IN METERED SPACES PARALLEL TO THE CLOSEST CURB OR SIDEWALK, A PERSON MUST PARK A VEHICLE PARALLEL TO THE CURB OR SIDEWALK, EXCEPT MOTORCYCLES OR MOTOR ASSISTED VEHICLES CAN PARK AT AN ANGLE',
        'PARK IN A METERED SPACE IF THE PARKING METER HEAD DISPLAYS FOUR FLASHING ZEROS IN A WINDOW.',
        'PARK IN A METERED SPACE IF THE PARKING METER HEAD DISPLAYS AN "OUT OF ORDER" TEXT IN A WINDOW',
        'PARK IN A METERED SPACE IF THE TIME RECORDED BY THE OPERATOR UNDER THE PAY BY PHONE OR PAY BY LICENCE PLATE OPTION HAS EXPIRED.',
    ]
   
    tickets = tickets[~tickets["InfractionText"].isin(meter_infraction_text)] 
    tickets = tickets.drop(["Bylaw", "Section", "InfractionText"], axis=1)

    tickets['EntryDate'] = pd.to_datetime(tickets['EntryDate'], errors='coerce')
    tickets = tickets[tickets['EntryDate'].notnull()]
    tickets = tickets[(tickets['EntryDate'].dt.year == 2023)] # temporary filter
    tickets["month"] = tickets["EntryDate"].dt.month
    tickets['dayofweek'] = tickets['EntryDate'].dt.dayofweek

    
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
    with open(f"{RAW_DATA_PATH}/parking-meters.csv", "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    rows = [line.strip().split(";") for line in lines]
    meters = pd.DataFrame(rows[1:], columns=rows[0])  

    meters.columns = [
        "METERHEAD", "R_MF_9A_6P", "R_MF_6P_10", "R_SA_9A_6P", "R_SA_6P_10", "R_SU_9A_6P", 
        "R_SU_6P_10", "RATE_MISC", "TIMEINEFFE", "T_MF_9A_6P", "T_MF_6P_10", "T_SA_9A_6P", 
        "T_SA_6P_10", "T_SU_9A_6P", "T_SU_6P_10", "TIME_MISC", "CREDITCARD", "PAY_PHONE", 
        "Geom", "Geo Local Area", "METERID", "geo_point_2d"
    ]

    meters = meters.drop(["CREDITCARD", "PAY_PHONE", "geo_point_2d"], axis=1)
    
    def extract_coordinates(geom_str):
        match = re.search(r'\[(-?\d+\.\d+),\s*(-?\d+\.\d+)\]', str(geom_str))
        if match:
            return float(match.group(1)), float(match.group(2))
        return None, None

    meters['lon'], meters['lat'] = zip(*meters['Geom'].apply(extract_coordinates))
    meters = meters[meters['lon'].notnull() & meters['lat'].notnull()]    
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


def etl() -> Tuple[gpd.GeoDataFrame]:
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
    
    return tickets_df, meters_df, boundaries_df
    

def main() -> None:
    tickets_df, meters_df, boundaries_df = etl()


if __name__ == "__main__":
    main()    