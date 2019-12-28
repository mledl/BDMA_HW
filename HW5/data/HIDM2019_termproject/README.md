# Software and Library Versions

Python: 3.7.4

## Python Packages

Pandas: 0.25.1
GDAL: 3.0.2
Fiona: 1.8.13
Rtree: 0.9.1
pyproj: 2.4.2.post1
Shapely: 1.6.4.post2
geojson: 2.5.0
geopandas: 0.6.2
geojsonio: 0.0.3
github3.py: 0.7.0
gmplot: 1.2.0

Installing the should work out of the box on MacOS and Linux systems.
For Windows follow the instructions provided here https://geoffboeing.com/2014/09/using-geopandas-windows/.


# Data Analysis

This section gives an overview over which columns are present in the datasets.

Arrest_Data_from_2010_to_Present.csv:
Total Number of Lines: 1313405

| Column Name | Number of Entries | Datatype | Lines Missing Value |
| --- | --- | --- | --- |
| Report ID                   | 1313405 | int64 | 0 |
| Arrest Date                 | 1313405 | object | 0 |
| Time                        | 1313209 | float64 | 196 |
| Area ID                     | 1313405 | int64 | 0 |
| Area Name                   | 1313405 | object | 0 |
| Reporting District          | 1313405 | int64 | 0 |
| Age                         | 1313405 | int64 | 0 |
| Sex Code                    | 1313405 | object | 0 |
| Descent Code                | 1313405 | object | 0 |
| Charge Group Code           | 1225010 | float64 | 88395 |
| Charge Group Description    | 1224486 | object | 88919 |
| Arrest Type Code            | 1313405 | object | 0 |
| Charge                      | 1313405 | object | 0 |
| Charge Description          | 1225060 | object | 88345 |
| Address                     | 1313405 | object | 0 |
| Cross Street                | 749250  | object | 564155 |
| Location                    | 1313405 | object | 0 |

Metro_Bike_Share_Trip_Data.csv:
Total Number of Lines: 132427

| Column Name | Number of Entries | Datatype |  Lines Missing Value |
| --- | --- | --- | --- |
| Trip ID                       | 132427 |  object | 0 |
| Duration                      | 132427 |  object | 0 |
| Start Time                    | 132427 |  object | 0 |
| End Time                      | 132427 |  object | 0 |
| Starting Station ID           | 132408 |  object | 19 |
| Starting Station Latitude     | 132379 |  float64 | 48 |
| Starting Station Longitude    | 132379 |  float64 | 48 |
| Ending Station ID             | 132331 |  object | 96 |
| Ending Station Latitude       | 131376 |  float64 | 1051 |
| Ending Station Longitude      | 131376 |  float64 | 1051 |
| Bike ID                       | 132417 |  object | 10 |
| Plan Duration                 | 131661 |  float64 | 766 |
| Trip Route Category           | 132427 |  object | 0 |
| Passholder Type               | 132427 |  object | 0 |
| Starting Lat-Long             | 98622 |  object  | 33805 |
| Ending Lat-Long               | 131376 |  object | 1051 |
