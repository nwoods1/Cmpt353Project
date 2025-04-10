import pandas as pd
import re

tickets = pd.read_csv("parking-tickets.csv", sep=';', engine='python', on_bad_lines='skip')
tickets['EntryDate'] = pd.to_datetime(tickets['EntryDate'], errors='coerce')
tickets = tickets[tickets['EntryDate'].notnull()]
tickets = tickets[(tickets['EntryDate'].dt.month == 7) & (tickets['EntryDate'].dt.year == 2023)]
tickets['hour'] = tickets['EntryDate'].dt.hour
tickets['dayofweek'] = tickets['EntryDate'].dt.dayofweek
with open("parking-meters.csv", "r", encoding="utf-8", errors="ignore") as f:
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

tickets.to_csv("cleaned_parking_tickets.csv", index=False)
meters.to_csv("cleaned_parking_meters.csv", index=False)
