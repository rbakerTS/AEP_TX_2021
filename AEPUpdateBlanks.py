import arcpy

# get unique counties
# for each county
# query with geopandas -> item
# intersect poles with found item
# iterate over each pole to see if it has a null county
# if null add county found in tx_counties


import geopandas as gpd
import numpy as np
import pandas as pd

live = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/AEP_RGV_Test.shp"
city_layer = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/TX_Cities.shp"
county_layer = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/TX_Counties.shp"

live_df = gpd.read_file(live)
city_df = gpd.read_file(city_layer)
county_df = gpd.read_file(county_layer)

county_list = list()
for item in county_df.itertuples():
    c = str(item.COUNTY)
    county_list.append(c)

county_list = list(set(county_list))
master_df = pd.DataFrame()
for index, county in enumerate(county_list):
    # print(index, county)
    found_county_df = county_df[county_df["COUNTY"] == county]

    sjoin_temp = gpd.sjoin(live_df, found_county_df)

    if len(sjoin_temp) > 0:
        print(f'found {index, county}')
        sjoin_temp['County'] = sjoin_temp['County'].replace(to_replace=np.nan,
                                                            value=county.replace('County', '').strip())
        master_df = master_df.append(sjoin_temp, ignore_index=True)
    else:
        print('did not successfully spatially join stuff')

master_gdf = gpd.GeoDataFrame(data=master_df, geometry=list(master_df['geometry']))

master_gdf.to_file('master_test.shp')

quit()

arcpy.env.overwriteOutput = True
live = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/AEP_RGV_Test.shp"
# fields = [item.aliasName for item in arcpy.ListFields(dataset=live)]
district = "RIO GRANDE VALLEY"
city_layer = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/TX_Cities.shp"
county_layer = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/TX_Counties.shp"
data_folder = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data"
pole_city = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/pole_city.shp"
pole_county = "C:/Users/TechServPC/PycharmProjects/ArcGeoprocessing/Data/pole_county.shp"
arcpy.SpatialJoin_analysis(live, county_layer, pole_county)
arcpy.SpatialJoin_analysis(live, city_layer, pole_city)
desired_fields = ['location_n', 'Field_Cond', 'Comments', 'District', 'County', 'City', 'Latitude', 'Longitude',
                  'station_na', 'FID']
fields_county = ['location_n', 'Field_Cond', 'Comments', 'District', 'County', 'City', 'Latitude', 'Longitude',
                 'station_na', 'FID', 'COUNTY_1']
fields_city = ['location_n', 'Field_Cond', 'Comments', 'District', 'County', 'City', 'Latitude', 'Longitude',
               'station_na', 'FID', 'CITY_NM']


def set_added_pole(dataset: str, fields: list) -> None:
    with arcpy.da.UpdateCursor(dataset, fields) as cursor:
        for row in cursor:
            if row[1].strip() == "Added Pole":
                continue
            elif row[1].strip() == "":
                row[1] = "Added Pole"
            elif row[1].strip() != "" or row[1].strip() != "Added Pole":
                row[2] = row[1]
                row[1] = "Added Pole"
            cursor.updateRow(row)


def set_district(dataset: str, fields: list, district: str) -> None:
    with arcpy.da.UpdateCursor(dataset, fields) as cursor:
        for row in cursor:
            if row[3].strip() == "":
                row[3] = district
            cursor.updateRow(row)


def set_county(pole_county, fields_county) -> None:
    with arcpy.da.UpdateCursor(pole_county, fields_county) as cursor:
        for row in cursor:
            if row[4].strip() == "":
                row[4] = row[-2]
            cursor.updateRow(row)


def set_city(pole_city, fields_city) -> None:
    with arcpy.da.UpdateCursor(pole_city, fields_city) as cursor:
        for row in cursor:
            if row[5].strip() == "":
                row[5] = row[-1]
            cursor.updateRow(row)


def set_latitude(dataset: str, fields: list) -> None:
    f = fields.copy()
    f.append('SHAPE@Y')
    with arcpy.da.UpdateCursor(dataset, f) as cursor:
        for row in cursor:
            if row[6] == 0:
                row[6] = row[-1]
            cursor.updateRow(row)


def set_longitude(dataset: str, fields: list) -> None:
    f = fields.copy()
    f.append('SHAPE@X')
    with arcpy.da.UpdateCursor(dataset, f) as cursor:
        for row in cursor:
            if row[7] == 0:
                row[7] = row[-1]
            cursor.updateRow(row)


# def set_station(dataset: str, fields: list)-> None:
#     with arcpy.da.UpdateCursor(dataset, fields) as cursor:
#         for row in cursor:
#             if row[8].strip() == "":
#                 row[8] = "Station"
#             cursor.updateRow(row)

def main(dataset: str, fields: list, district: str):
    set_added_pole(dataset, fields)
    set_district(dataset, fields, district)
    set_latitude(dataset, fields)
    set_longitude(dataset, fields)


a = arcpy.GetParameterAsText(0) if arcpy.GetParameterAsText(0) else live
b = arcpy.GetParameterAsText(1) if arcpy.GetParameterAsText(0) else desired_fields
c = arcpy.GetParameterAsText(2) if arcpy.GetParameterAsText(0) else district

main(a, b, c)

# if __name__ == "__main__":
