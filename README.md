# Cmpt353Project

Required Libraries: 
pandas	Data manipulation and loading CSV files
geopandas	Working with geographic (geometry) data and spatial joins
shapely	Handling geometric objects (e.g., Point, Polygon)
scikit-learn	Machine learning models (e.g., RandomForestClassifier, train_test_split)
matplotlib	Plotting visualizations
seaborn	Enhanced plotting (e.g., heatmaps, bar charts)
numpy	Array operations and numerical calculations
geopy	Geocoding block and street names into lat/lon coordinates

Input files needed:
parking_tickets.csv

parking_meters.csv

block_street_with_lat_lon.csv

cached_block_street_with_lat_lon.csv

local_area_boundaries.csv

Step 1: Visualizations
File: notebooks/2_visualizations.ipynb

Generates Figures 1–7 in the report

Helps identify Downtown as a high-activity ticket zone

Produces:

Neighborhood outlines

Ticket count maps and heatmaps

Step 2: Modeling
File: notebooks/3_model.ipynb

Trains a RandomForest model using Positive-Unlabeled (PU) Learning

Performs:

Spatial join with meters

Date simulation for unlabeled events

Reliable negative generation via probability thresholding

Final model training and evaluation

Produces:

Classification report

Accuracy score
