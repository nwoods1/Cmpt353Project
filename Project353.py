import pandas as pd
import re


def read_csv(file_name: str) -> pd.DataFrame:
    return pd.read_csv(file_name, sep=";", engine="python", on_bad_lines="skip")


def to_csv(df: pd.DataFrame, name: str) -> None:
    df.to_csv(f"data/cleaned_data/{name}", index=False)


def tickets_preproc(tickets: pd.DataFrame) -> pd.DataFrame:
    print("Performing pre-processing on tickets")
    tickets['EntryDate'] = pd.to_datetime(tickets['EntryDate'], errors='coerce')
    tickets = tickets[tickets['EntryDate'].notnull()]
    tickets = tickets[(tickets['EntryDate'].dt.month == 7) & (tickets['EntryDate'].dt.year == 2023)]
    tickets['hour'] = tickets['EntryDate'].dt.hour
    tickets['dayofweek'] = tickets['EntryDate'].dt.dayofweek

    return tickets


def meters_preproc() -> pd.DataFrame:
    print("performing pre-processing on meters")
    with open("data/raw_data/parking-meters.csv", "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    rows = [line.strip().split(";") for line in lines]
    meters = pd.DataFrame(rows[1:], columns=rows[0])  

    meters.columns = [
        "METERHEAD", "R_MF_9A_6P", "R_MF_6P_10", "R_SA_9A_6P", "R_SA_6P_10", "R_SU_9A_6P", "R_SU_6P_10",
        "RATE_MISC", "TIMEINEFFE", "T_MF_9A_6P", "T_MF_6P_10", "T_SA_9A_6P", "T_SA_6P_10", "T_SU_9A_6P",
        "T_SU_6P_10", "TIME_MISC", "CREDITCARD", "PAY_PHONE", "Geom", "Geo Local Area", "METERID", "geo_point_2d"
    ]

    def extract_coordinates(geom_str):
        match = re.search(r'\[(-?\d+\.\d+),\s*(-?\d+\.\d+)\]', str(geom_str))
        if match:
            return float(match.group(1)), float(match.group(2))
        return None, None

    meters['lon'], meters['lat'] = zip(*meters['Geom'].apply(extract_coordinates))

    meters = meters[meters['lon'].notnull() & meters['lat'].notnull()]
    return meters


def boundaries_preproc(boundaries_df: pd.DataFrame) -> pd.DataFrame:
    print("performing pre-processing on boundaries")
    boundaries_df = boundaries_df.drop("geo_point_2d", axis=1)
    return boundaries_df


def main():

    # read in the raw data
    tickets_df = read_csv("data/raw_data/parking-tickets.csv")
    boundaries_df = read_csv("data/raw_data/local-area-boundary.csv")

    # perform preprocessing and cleaning
    tickets_df = tickets_preproc(tickets_df) 
    meters_df = meters_preproc()
    boundaries_df = boundaries_preproc(boundaries_df)

    # write the processed csv files     
    to_csv(tickets_df, "cleaned_parking_tickets.csv")
    to_csv(meters_df, "cleaned_parking_meters.csv")
    to_csv(boundaries_df, "cleaned_local_area_boundaries.csv")
    

if __name__ == "__main__":
    main()    