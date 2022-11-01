from AGO_Manager import AGO_manager
from dataclasses import dataclass
from arcgis.gis import Layer
from arcgis.features import FeatureLayerCollection, SpatialDataFrame
import pandas as pd
import geopandas as gpd
import os
from datetime import datetime, date
import fiona
import numpy as np
import json
from AEP_AGO import AGO


@dataclass
class customLayer:
    raw_layer: Layer

    @property
    def properties(self):
        return self.raw_layer.properties

    @property
    def from_item(self):
        return self.raw_layer.fromitem

    @property
    def name(self):
        return self.raw_layer.properties.name

    @property
    def crs(self):
        return self.raw_layer.url.spatialReference

    @property
    def id(self):
        return self.raw_layer.url.id

    @property
    def url(self):
        return self.raw_layer.url.url


class Layer_Search:
    def __init__(self, poles, lights, station, created_after, created_before, title_search, tag, item_type, status,
                 date, update, district):
        self.district=district
        self.created_after = created_after
        self.created_before = created_before
        self.title_search = title_search
        self.tag = tag
        self.item_type = item_type
        self.poles_title = poles
        self.lights_title = lights
        self.station = station
        self.status = status
        self.date = date
        self.update = update
        with open('secrets.json') as file:
            x = json.load(file)

        username = x['username']
        password = x['password']

        self.manager = AGO_manager(username, password)
        # self.aep_ago = AGO()
        self.tracking_spreadsheet_path = f'C:\\Users\TechServPC\OneDrive - TechServ\AEP StreetLight Project Management\\{self.district}\AEP_Tracking_Field.xlsx'
        # self.tracking_spreadsheet_path = r'C:\Work\Client\AEP\Abilene\AEP_Tracking_Field_20212281539.xlsx'
        self.tracking_spreadsheet = pd.ExcelFile(self.tracking_spreadsheet_path)
        self.tracking_status = pd.read_excel(self.tracking_spreadsheet, 'Status')
        self.tracking_deliverables = pd.read_excel(self.tracking_spreadsheet, 'Deliverables')
        self.tracking_invoicing = pd.read_excel(self.tracking_spreadsheet, 'Invoicing')
        self.data_manager_folder = f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\\{self.district}\Raw_QAQC_Data'.replace(
            '\\', '/')
        os.makedirs(self.data_manager_folder, exist_ok=True)
        self.tracking_folder = f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\\{self.district}\Tracking'.replace(
            '\\', '/')
        os.makedirs(self.tracking_folder, exist_ok=True)
        self.deliverables_folder = '\\'.join(
            [f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\\{self.district}\Deliverables',
             datetime.strftime(datetime.now(), '%Y-%m-%d')]).replace('\\', '/')
        os.makedirs(self.deliverables_folder, exist_ok=True)

    def search_contents(self):
        print('Searching AGO contents')
        self.content_search = self.manager.content_search(self.created_after, self.created_before, self.title_search,
                                                          self.tag, self.item_type)
        return self.content_search

    def search_to_layer_list(self, content_search: list):
        self.search_fl_list = []
        self.search_cl_list = []
        for i, item in enumerate(content_search):
            search_item = item
            search_layer = Layer(search_item)
            search_fl = search_layer.url.layers
            search_cl = customLayer(search_fl)
            self.search_fl_list.append(search_fl)
            self.search_cl_list.append(search_cl)
        return self.search_cl_list

    def search_poles_old(self):
        self.poles_search = self.manager.content_search(title_search=self.poles_title, item_type='Feature Layer')
        # The first item is being selected as the poles layer
        self.poles_list = self.search_to_layer_list(self.poles_search)
        for search in self.poles_list:
            layer = search.raw_layer[0]
            if layer.properties.name == self.poles_title:
                self.poles_custom = search
                break
        # print(f"Poles layer: {self.poles_custom.properties.name}")
        return self.poles_custom

    def search_poles(self):
        self.poles_search = self.manager.content_search(title_search=self.poles_title, item_type='Feature Layer')
        self.poles_list = self.search_to_layer_list(self.poles_search)
        for search in self.poles_list:
            layer = search.raw_layer[0]
            if 'Map' in layer.url:
                self.poles_custom = search
                break
        print(f"Poles layer: {self.poles_custom.raw_layer[0].properties.name}")
        self.poles_sdf = pd.DataFrame.spatial.from_layer(self.poles_custom.raw_layer[0])
        return self.poles_custom

    def search_lights(self):
        self.lights_search = self.manager.content_search(title_search=self.lights_title, item_type='Feature Layer')
        # The first item is being selected as the lights layer
        self.lights_custom = self.search_to_layer_list(self.lights_search)[0]
        print(f"Lights layer: {self.lights_custom.properties.name}")
        return self.lights_custom

    def get_stations_counts(self):
        print(f"/nLocating pole features")
        self.pole_features = self.poles_custom.raw_layer.query()
        self.poles_count = len(self.pole_features)
        print(f"Generated {self.poles_count} pole features")
        print(f"Locating light features")
        self.light_features = self.lights_custom.raw_layer.query()
        self.lights_count = len(self.light_features)
        print(f"Generated {self.lights_count} light features")
        self.stations = self.pole_features.sdf.station_name.unique()
        self.station_count = len(self.stations)
        print(f"Generated station list with {self.station_count} stations")
        self.station_counts = {"Priority": [], "Substation": [], "Station_Name": [], "Pole Count": [],
                               "Light Count": []}
        print(f"Generated blank stations dictionary")
        for i, station in enumerate(self.stations):
            print("----------------------------------------------------------")
            print(f"{i + 1}/{self.station_count} Station: {station}")
            poles = self.poles_custom.raw_layer.query(where=f"station_name='{station}'")
            lights = self.lights_custom.raw_layer.query(where=f"station_name='{station}'")
            self.station_counts["Priority"].append(0)
            self.station_counts["Substation"].append('')
            self.station_counts["Station_Name"].append(station)
            self.station_counts["Pole Count"].append(len(poles))
            self.station_counts["Light Count"].append(len(lights))
            print(f"Poles: {len(poles)} Lights: {len(lights)}")

    def export_station_counts(self):
        print('Exporting station counts as csv')
        self.export_csv = 'Z:/Audits and Inventories/AEP/AEP STREETLIGHT AUDIT 2021/Abilene/station_counts.csv'.replace(
            "//", "/")
        self.station_counts_df = pd.DataFrame(self.station_counts)
        self.station_counts_df.to_csv(self.export_csv, index=False)

    def search_station_features(self):
        self.station_poles = self.poles_custom.raw_layer[0].query(where=f"station_name='{self.station}'")
        print(f'Found {len(self.station_poles)} poles in {self.station}')

    def field_mapping_station(self):
        self.field_map_dict = {
            'OBJECTID': 'OBJECTID',
            'location_number': 'Location Number',
            'Latitude': 'Latitude',
            'Longitude': 'Longitude',
            'Tech': 'Tech',
            'District': 'District',
            'City': 'City',
            'County': 'County',
            'height': 'Height',
            'class': 'Class',
            'pole_year': 'Pole Year',
            'Field_Conditions': 'Field Conditions',
            'Light_Count_1': 'Light Count_1',
            'Fixture_Type_1': 'Fixture Type_1',
            'Light_Type_1': 'Light Type_1',
            'Watts_1': 'Watts_1',
            'Light_Target_1': 'Light Target_1',
            'Power_Source_1': 'Power Source_1',
            'Arm_Length_1': 'Arm Length_1',
            'Comments': 'Comments',
            'circuit_name': 'Circuit Name',
            'station_name': 'Station Name',
            'Pole_Num_Missing': 'Pole Number Missing',
            'Fixture_Type_2': 'Fixture Type_2',
            'Light_Type_2': 'Light Type_2',
            'Watts_2': 'Watts_2',
            'Light_Target_2': 'Light Target_2',
            'Power_Source_2': 'Power Source_2',
            'Arm_Length_2': 'Arm Length_2',
            'Fixture_Type_3': 'Fixture Type_3',
            'Light_Type_3': 'Light Type_3',
            'Watts_3': 'Watts_3',
            'Light_Target_3': 'Light Target_3',
            'Power_Source_3': 'Power Source_3',
            'Arm_Length_3': 'Arm Length_3',
            'Fixture_Type_4': 'Fixture Type_4',
            'Light_Type_4': 'Light Type_4',
            'Watts_4': 'Watts_4',
            'Light_Target_4': 'Light Target_4',
            'Power_Source_4': 'Power Source_4',
            'Arm_Length_4': 'Arm Length_4',
            'pole_type': 'pole_type',
            'Pole_Dir_1': 'Pole_Dir_1',
            'Mount_Dir_1': 'Mount_Dir_1',
            'Bottom_Dir_1': 'Bottom_Dir_1',
            'Pole_Dir_2': 'Pole_Dir_2',
            'Mount_Dir_2': 'Mount_Dir_2',
            'Bottom_Dir_2': 'Bottom_Dir_2',
            'Pole_Dir_3': 'Pole_Dir_3',
            'Mount_Dir_3': 'Mount_Dir_3',
            'Bottom_Dir_3': 'Bottom_Dir_3',
            'Pole_Dir_4': 'Pole_Dir_4',
            'Mount_Dir_4': 'Mount_Dir_4',
            'Bottom_Dir_4': 'Bottom_Dir_4',
            'GlobalID': 'GlobalID',
            'creation_date': 'CreationDate',
            'edit_date': 'EditDate',
            'CreationDate': 'CreationDate_Data',
            'EditDate': 'EditDate_Data',
            'Pwr_Source_Loc_Num': 'Power Source Location Number',
            'ownership': 'ownership',
            'Foreign_Pole_Number': 'Foreign Pole Number',
            'record_timestamp': 'Timestamp',
            'SHAPE': 'x_'
        }

        self.station_poles_sdf = self.station_poles.sdf
        self.station_poles_mapped_sdf = self.station_poles_sdf.rename(columns=self.field_map_dict)
        # self.station_poles_mapped_sdf = self.station_poles_mapped_sdf.drop('Device', axis=1)
        self.station_poles_mapped_sdf['y_'] = self.station_poles_mapped_sdf['x_']
        self.station_poles_mapped_sdf['Creator'] = ''
        self.station_poles_mapped_sdf['Editor'] = ''
        self.station_poles_mapped_sdf['x'] = 0.00
        self.station_poles_mapped_sdf['y'] = 0.00
        for index, row in enumerate(self.station_poles_mapped_sdf.itertuples()):
            self.station_poles_mapped_sdf.at[index, 'x'] = self.station_poles_mapped_sdf.at[index, 'x_']['x']
            self.station_poles_mapped_sdf.at[index, 'y'] = self.station_poles_mapped_sdf.at[index, 'x_']['y']
        self.station_poles_mapped_sdf = self.station_poles_mapped_sdf.drop('x_', axis=1)

    def field_mapping_poles(self):
        self.field_map_dict = {
            'OBJECTID': 'OBJECTID',
            'location_number': 'Location Number',
            'Latitude': 'Latitude',
            'Longitude': 'Longitude',
            'Tech': 'Tech',
            'District': 'District',
            'City': 'City',
            'County': 'County',
            'height': 'Height',
            'class': 'Class',
            'pole_year': 'Pole Year',
            'Field_Conditions': 'Field Conditions',
            'Light_Count_1': 'Light Count',
            'Fixture_Type_1': 'Fixture Type_1',
            'Light_Type_1': 'Light Type_1',
            'Watts_1': 'Watts_1',
            'Light_Target_1': 'Light Target_1',
            'Power_Source_1': 'Power Source_1',
            'Arm_Length_1': 'Arm Length_1',
            'Comments': 'Comments',
            'circuit_name': 'Circuit Name',
            'station_name': 'Station Name',
            'Pole_Num_Missing': 'Pole Number Missing',
            'Fixture_Type_2': 'Fixture Type_2',
            'Light_Type_2': 'Light Type_2',
            'Watts_2': 'Watts_2',
            'Light_Target_2': 'Light Target_2',
            'Power_Source_2': 'Power Source_2',
            'Arm_Length_2': 'Arm Length_2',
            'Fixture_Type_3': 'Fixture Type_3',
            'Light_Type_3': 'Light Type_3',
            'Watts_3': 'Watts_3',
            'Light_Target_3': 'Light Target_3',
            'Power_Source_3': 'Power Source_3',
            'Arm_Length_3': 'Arm Length_3',
            'Fixture_Type_4': 'Fixture Type_4',
            'Light_Type_4': 'Light Type_4',
            'Watts_4': 'Watts_4',
            'Light_Target_4': 'Light Target_4',
            'Power_Source_4': 'Power Source_4',
            'Arm_Length_4': 'Arm Length_4',
            'pole_type': 'Pole Type',
            'Pole_Dir_1': 'Pole_Dir_1',
            'Mount_Dir_1': 'Mount_Dir_1',
            'Bottom_Dir_1': 'Bottom_Dir_1',
            'Pole_Dir_2': 'Pole_Dir_2',
            'Mount_Dir_2': 'Mount_Dir_2',
            'Bottom_Dir_2': 'Bottom_Dir_2',
            'Pole_Dir_3': 'Pole_Dir_3',
            'Mount_Dir_3': 'Mount_Dir_3',
            'Bottom_Dir_3': 'Bottom_Dir_3',
            'Pole_Dir_4': 'Pole_Dir_4',
            'Mount_Dir_4': 'Mount_Dir_4',
            'Bottom_Dir_4': 'Bottom_Dir_4',
            'GlobalID': 'GlobalID',
            'creation_date': 'CreationDate',
            'edit_date': 'EditDate',
            'CreationDate': 'CreationDate_Data',
            'EditDate': 'EditDate_Data',
            'Pwr_Source_Loc_Num': 'Power Source Location Number',
            'ownership': 'ownership',
            'Foreign_Pole_Number': 'Foreign Pole Number',
            'record_timestamp': 'Timestamp',
            'SHAPE': 'x_'
        }
        print("Creating data frame from pole data")
        # self.poles_all = self.poles_custom.raw_layer.query()
        self.poles_sdf = pd.DataFrame.spatial.from_layer(self.poles_custom.raw_layer[0])
        print(f"{self.poles_sdf.shape[0]} poles found")
        print('Renaming fields in dataframe')
        self.poles_mapped_sdf = self.poles_sdf.rename(columns=self.field_map_dict)
        print('Adding and deleting fields from dataframe')
        self.poles_mapped_sdf = self.poles_mapped_sdf.drop('Device', axis=1)
        self.poles_mapped_sdf['y_'] = self.poles_mapped_sdf['x_']
        self.poles_mapped_sdf['Creator'] = ''
        self.poles_mapped_sdf['Editor'] = ''
        self.poles_mapped_sdf['x'] = 0.00
        self.poles_mapped_sdf['y'] = 0.00
        print('Calculating lat and lon of poles in dataframe')
        for index, row in enumerate(self.poles_mapped_sdf.itertuples()):
            self.poles_mapped_sdf.at[index, 'x'] = self.poles_mapped_sdf.at[index, 'x_']['x']
            self.poles_mapped_sdf.at[index, 'y'] = self.poles_mapped_sdf.at[index, 'x_']['y']
            creation_date = self.poles_mapped_sdf.at[index, 'CreationDate']
            edit_date = self.poles_mapped_sdf.at[index, 'EditDate']
            if pd.notna(creation_date):
                self.poles_mapped_sdf.at[index, 'CreationDate'] = datetime.strftime(creation_date,
                                                                                    "%Y-%m-%d %H:%M:%S.000")
            if pd.notna(edit_date):
                self.poles_mapped_sdf.at[index, 'EditDate'] = datetime.strftime(edit_date, "%Y-%m-%d %H:%M:%S.000")
        self.poles_mapped_sdf = self.poles_mapped_sdf.drop('x_', axis=1)

    def configure_headers_station(self):
        # self.station_poles_tracking = self.station_poles_sdf.loc(axis=1)[
        #     'OBJECTID', 'location_number', 'Latitude', 'Longitude', 'Tech', 'District', 'City', 'County', 'Light_Count_1',
        #     'station_name', 'GlobalID', 'edit_date']
        self.station_poles_trimmed = self.station_poles_mapped_sdf.loc(axis=1)[
            'OBJECTID',
            'Location Number',
            'Latitude',
            'Longitude',
            'Tech',
            'District',
            'City',
            'County',
            'Height',
            'Class',
            'Pole Year',
            'Field Conditions',
            'Light Count_1',
            'Fixture Type_1',
            'Light Type_1',
            'Watts_1',
            'Light Target_1',
            'Power Source_1',
            'Arm Length_1',
            'Comments',
            'Circuit Name',
            'Station Name',
            'Pole Number Missing',
            'Fixture Type_2',
            'Light Type_2',
            'Watts_2',
            'Light Target_2',
            'Power Source_2',
            'Arm Length_2',
            'Fixture Type_3',
            'Light Type_3',
            'Watts_3',
            'Light Target_3',
            'Power Source_3',
            'Arm Length_3',
            'Fixture Type_4',
            'Light Type_4',
            'Watts_4',
            'Light Target_4',
            'Power Source_4',
            'Arm Length_4',
            'pole_type',
            'Pole_Dir_1',
            'Mount_Dir_1',
            'Bottom_Dir_1',
            'Pole_Dir_2',
            'Mount_Dir_2',
            'Bottom_Dir_2',
            'Pole_Dir_3',
            'Mount_Dir_3',
            'Bottom_Dir_3',
            'Pole_Dir_4',
            'Mount_Dir_4',
            'Bottom_Dir_4',
            'GlobalID',
            'CreationDate',
            'Creator',
            'EditDate',
            'Editor',
            'Power Source Location Number',
            'ownership',
            'Foreign Pole Number',
            'Timestamp',
            'x',
            'y',
            'CreationDate_Data',
            'EditDate_Data'
        ]

    def configure_headers_poles(self):
        # self.station_poles_tracking = self.station_poles_sdf.loc(axis=1)[
        #     'OBJECTID', 'location_number', 'Latitude', 'Longitude', 'Tech', 'District', 'City', 'County', 'Light_Count_1',
        #     'station_name', 'GlobalID', 'edit_date']
        print('Trimming dataframe to needed fields')
        self.poles_all_trimmed = self.poles_mapped_sdf.loc(axis=1)[
            'OBJECTID',
            'Location Number',
            'Latitude',
            'Longitude',
            'Tech',
            'District',
            'City',
            'County',
            'Height',
            'Class',
            'Pole Year',
            'Field Conditions',
            'Light Count',
            'Fixture Type_1',
            'Light Type_1',
            'Watts_1',
            'Light Target_1',
            'Power Source_1',
            'Arm Length_1',
            'Comments',
            'Circuit Name',
            'Station Name',
            'Pole Number Missing',
            'Fixture Type_2',
            'Light Type_2',
            'Watts_2',
            'Light Target_2',
            'Power Source_2',
            'Arm Length_2',
            'Fixture Type_3',
            'Light Type_3',
            'Watts_3',
            'Light Target_3',
            'Power Source_3',
            'Arm Length_3',
            'Fixture Type_4',
            'Light Type_4',
            'Watts_4',
            'Light Target_4',
            'Power Source_4',
            'Arm Length_4',
            'Pole Type',
            'Pole_Dir_1',
            'Mount_Dir_1',
            'Bottom_Dir_1',
            'Pole_Dir_2',
            'Mount_Dir_2',
            'Bottom_Dir_2',
            'Pole_Dir_3',
            'Mount_Dir_3',
            'Bottom_Dir_3',
            'Pole_Dir_4',
            'Mount_Dir_4',
            'Bottom_Dir_4',
            'GlobalID',
            'CreationDate',
            'Creator',
            'EditDate',
            'Editor',
            'Power Source Location Number',
            'ownership',
            'Foreign Pole Number',
            'Timestamp',
            'x',
            'y'
        ]

    def export_features_station(self):
        self.station_poles_trimmed.to_csv('/'.join(
            [self.data_manager_folder, f'{self.station}_{self.status}.csv']), index=False)
        # self.station_poles_tracking.to_csv('/'.join(
        #     [self.tracking_folder, f'{self.station}_{self.status}_tracking.csv']), index=False)
        print(f'Exported {self.station}_{self.status} as csv')

    def export_features_poles(self):
        print('Creating folders if needed')
        self.data_manager_folder = 'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Abilene\Raw_QAQC_Data'.replace(
            '\\', '/')
        os.makedirs(self.data_manager_folder, exist_ok=True)
        self.tracking_folder = 'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Abilene\Tracking'.replace(
            '\\', '/')
        os.makedirs(self.tracking_folder, exist_ok=True)
        print('Generating csvs')
        for key, value in self.update.items():
            if value == self.status:
                station_name = key.replace(r"*", "")
                # data = self.poles_all_trimmed.query("`Station Name` == @key")
                data = self.poles_all_trimmed.query("`Station Name`.str.contains(@key)")
                data.to_csv('/'.join([self.data_manager_folder, f'{station_name}_{value}.csv']), index=False)
                # data.to_csv('/'.join([self.tracking_folder, f'{self.station}_{self.status}_tracking.csv']), index=False)
                print(f'Exported {station_name}_{value} as csv containing {len(data)} rows')

    def export_features_station_poles(self):
        print('Creating folders if needed')
        self.data_manager_folder = 'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Abilene\Raw_QAQC_Data'.replace(
            '\\', '/')
        os.makedirs(self.data_manager_folder, exist_ok=True)
        self.tracking_folder = 'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Abilene\Tracking'.replace(
            '\\', '/')
        os.makedirs(self.tracking_folder, exist_ok=True)
        print('Generating csvs')
        for key, value in self.update.items():
            if value == self.status:
                station_name = key.replace(r"*", "")
                # data = self.poles_all_trimmed.query("`Station Name` == @key")
                # data = self.poles_all_trimmed.query("`Station Name`.str.contains(@key)")
                data = AGO.search_items(key)
                data.to_csv('/'.join([self.data_manager_folder, f'{station_name}_{value}.csv']), index=False)
                # data.to_csv('/'.join([self.tracking_folder, f'{self.station}_{self.status}_tracking.csv']), index=False)
                print(f'Exported {station_name}_{value} as csv containing {len(data)} rows')

    # def stations_to_sql(self):
    #     engine_str = 'mssql+pyodbc://TS-TYLER/AEP2021StreetlightAudit?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0'
    #     d = BasicDatabaseManager(connection_str=engine_str)
    #     engine = d.engine
    #     columns = d.query(
    #         "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'test'",
    #         return_rows=True)
    #     with engine.begin() as connection:
    #         self.station_poles_trimmed.to_sql('test', con=connection, if_exists='append', index=False)
    #     # d.insert('dbo.tblQAQCData', qaqc_data)
    #     # d.insert('dbo.tblRawData', raw_data)
    #     print()

    def stations_to_counties(self):
        self.counties = []
        for station_row in self.tracking_status.itertuples():
            self.counties.append(station_row[10])
            self.counties.append(station_row[11])
            self.counties.append(station_row[12])
            self.counties.append(station_row[13])
        self.counties = set(self.counties)
        self.counties = {x for x in self.counties if x == x}
        self.counties_stations = {key: [] for key in self.counties}
        for county in self.counties_stations:
            for column in ['County', 'County2', 'County3', 'County4']:
                for x in self.tracking_status[self.tracking_status[column].isin([county])].itertuples():
                    self.counties_stations[county].append(x.Station_Name)

    def stations_to_cities(self):
        self.cities = []
        for station_row in self.tracking_status.itertuples():
            self.cities.append(station_row[14])
            self.cities.append(station_row[15])
            self.cities.append(station_row[16])
            self.cities.append(station_row[17])
            self.cities.append(station_row[18])
        self.cities = set(self.cities)
        self.cities = {x for x in self.cities if x == x}
        self.cities_stations = {key: [] for key in self.cities}
        for city in self.cities_stations:
            for column in ['City', 'City2', 'City3', 'City4', 'City5']:
                for x in self.tracking_status[self.tracking_status[column].isin([city])].itertuples():
                    self.cities_stations[city].append(x.Station_Name)

    def cities_to_counties(self):
        self.cities_gdf = gpd.read_file('data/TX_Cities.shp')
        self.counties_gdf = gpd.read_file('data/TX_Counties.shp')
        self.cities_counties_gdf = gpd.sjoin(self.cities, self.counties)
        self.cities_counties_trimmed_gdf = self.cities_counties_gdf.loc['County', 'CITY_NM']

    def qaqc_stations(self):
        self.stations_qaqc = []
        for station_row in self.tracking_status.itertuples():
            if station_row[24] == 'Complete':
                self.stations_qaqc.append(station_row[2])

    def delivered_counties(self):
        self.counties_delivered = []
        for row in self.tracking_deliverables.itertuples():
            if row[9] != '':
                if row[2] == 0:
                    self.counties_delivered.append(row[1])

    def deliverable_counties(self):
        self.counties_deliverable = []
        for key, value in self.counties_stations.items():
            check = all(item in self.stations_qaqc for item in value)
            if check:
                self.counties_deliverable.append(key)
        self.counties_deliverable_df = pd.DataFrame(self.counties_deliverable)
        if self.counties_delivered != []:
            self.counties_deliverable.remove(self.counties_delivered)

    def delivered_cities(self):
        self.cities_delivered = []
        for row in self.tracking_deliverables.itertuples():
            if row[9] != '':
                if row[2] != 0:
                    self.counties_delivered.append(row[2])

    def deliverable_cities(self):
        self.cities_deliverable = []
        for key, value in self.cities_stations.items():
            check = all(item in self.stations_qaqc for item in value)
            if check:
                self.cities_deliverable.append(key)
        self.cities_deliverable_df = pd.DataFrame(self.cities_deliverable)
        if self.cities_delivered != []:
            self.cities_deliverable.remove(self.cities_delivered)

    def delivered_stations(self):
        self.stations_delivered = []
        for station_row in self.tracking_status.itertuples():
            if station_row[26] == 'Complete':
                self.stations_delivered.append(station_row[2])
        if self.stations_delivered != []:
            self.stations_qaqc.remove(self.stations_delivered)

    def deliverable_stations(self):
        self.stations_deliverable = []
        for county in self.counties_deliverable:
            for column in ['County', 'County2', 'County3', 'County4']:
                for x in self.tracking_status[self.tracking_status[column].isin([county])].itertuples():
                    self.stations_deliverable.append(x.Station_Name)
        for city in self.cities_deliverable:
            for column in ['City', 'City2', 'City3', 'City4', 'City5']:
                for x in self.tracking_status[self.tracking_status[column].isin([city])].itertuples():
                    self.stations_deliverable.append(x.Station_Name)
        self.stations_deliverable = set(self.stations_deliverable)
        self.stations_deliverable_df = pd.DataFrame(self.stations_deliverable)

    def build_deliverable_excel(self):
        writer = pd.ExcelWriter(
            '/'.join(
                [self.deliverables_folder, f"ReadyForDelivery_{datetime.strftime(datetime.now(), '%Y-%m-%d')}.xlsx"]),
            engine='xlsxwriter')
        self.counties_deliverable_df.to_excel(writer, sheet_name='Counties', index=False, header=False)
        self.cities_deliverable_df.to_excel(writer, sheet_name='Cities', index=False, header=False)
        self.stations_deliverable_df.to_excel(writer, sheet_name='Stations', index=False, header=False)
        writer.save()

    def added_poles(self):
        poles_search = self.search_poles()
        poles_layer = poles_search.raw_layer[0]
        poles = poles_layer.query()
        poles_sdf = poles.sdf
        poles_sdf_clean = ''
        # self.added_poles_trimmed = poles.sdf.loc(axis=1)[
        #     'ObjectID',
        #     'Location Number',
        #     'Latitude',
        #     'Longitude',
        #     'Station Name',
        #     'Tech',
        #     'Comments',
        #     'Field Conditions',
        #     'Light Count',
        #     'Pole Type',
        #     'Height',
        #     'Class',
        #     'Pole Year',
        #     'District',
        #     'City',
        #     'County'
        # ]
        field = "`Location Number`"
        query = field + ' != ' + field
        self.added_poles = poles_sdf.query(query)
        print(f'Found {len(self.added_poles)} Added Poles')
        self.added_poles.to_csv('/'.join([self.data_manager_folder, 'Added_Poles.csv']), index=False)
        print('Exported Added_Poles.csv')
        pass

    def make_editable(self):
        station_poles_list = self.manager.content_search(title_search='SanAngelo_', created_after='2021-12-01',
                                                         created_before='2021-12-30')
        for station in station_poles_list:
            if station.type == 'Feature Service':
                print(station)
                station_flc = FeatureLayerCollection.fromitem(station)
                updated = station_flc.manager.update_definition({'capabilities': 'Editing'})
                print(updated)

    def get_itemids(self):
        station_itemids = {}
        station_poles_list = self.manager.content_search(title_search='SanAngelo_', created_after='2021-12-01',
                                                         created_before='2021-12-30')
        for station in station_poles_list:
            if station.type == 'Feature Service':
                station_flc = FeatureLayerCollection.fromitem(station)
                station_itemids.append()

    def run_create_tables(self):
        # self.search_contents()
        self.search_poles()
        self.field_mapping_poles()
        self.configure_headers_poles()
        self.export_features_poles()

    def run_deliverable_table(self):
        self.stations_to_counties()
        self.stations_to_cities()
        self.qaqc_stations()
        self.delivered_counties()
        self.deliverable_counties()
        self.delivered_cities()
        self.deliverable_cities()
        self.delivered_stations()
        self.deliverable_stations()
        self.build_deliverable_excel()


class Update_Blanks:
    def __init__(self, county, city, poles):
        self.county_boundary = gpd.read_file(county)
        self.city_boundary = gpd.read_file(city)
        self.poles_title = poles
        self.manager = AGO_manager()

    def search_to_layer_list(self, content_search: list):
        self.search_fl_list = []
        self.search_cl_list = []
        for i, item in enumerate(content_search):
            search_item = item
            search_layer = Layer(search_item)
            search_fl = search_layer.url.layers[0]
            search_cl = customLayer(search_fl)
            self.search_fl_list.append(search_fl)
            self.search_cl_list.append(search_cl)
        return self.search_cl_list

    def search_poles(self):
        self.poles_search = self.manager.content_search(title_search=self.poles_title, item_type='Feature Layer')
        self.poles_list = self.search_to_layer_list(self.poles_search)
        for search in self.poles_list:
            layer = search.raw_layer[0]
            if 'Map' in layer.url:
                self.poles_custom = search
                break
        print(f"Poles layer: {self.poles_custom.properties.name}")
        self.poles_sdf = pd.DataFrame.spatial.from_layer(self.poles_custom.raw_layer)
        return self.poles_custom

    def join_counties_to_poles(self):
        county_list = list()
        for item in self.county_boundary.itertuples():
            c = str(item.COUNTY)
            county_list.append(c)
        county_list = list(set(county_list))
        master_df = pd.DataFrame()
        for index, county in enumerate(county_list):
            found_county_df = self.county_boundary[self.county_boundary["COUNTY"] == county]

            sjoin_temp = gpd.sjoin(self.poles_sdf, found_county_df)

            if len(sjoin_temp) > 0:
                print(f'found {index, county}')
                sjoin_temp['County'] = sjoin_temp['County'].replace(to_replace=np.nan,
                                                                    value=county.replace('County', '').strip())
                master_df = master_df.append(sjoin_temp, ignore_index=True)
            else:
                print('did not successfully spatially join stuff')


class Invoicing:
    def __init__(self, poles: str, district: str):
        print(f"Loading poles from {poles}")
        self.poles_df = pd.read_csv(poles)
        self.district = district
        self.tracking_spreadsheet_path = f'C:\\Users\TechServPC\OneDrive - TechServ\AEP StreetLight Project Management\\{self.district}\AEP_Tracking_Field.xlsx'
        self.tracking_spreadsheet = pd.ExcelFile(self.tracking_spreadsheet_path)
        self.tracking_invoicing = pd.read_excel(self.tracking_spreadsheet, 'Invoicing')

    def find_unique_county_city_pairs(self):
        print("Finding county, city pairs")
        self.county_city_pairs_df = self.poles_df[['County', 'City']]
        print("Finding unique county, city pairs")
        self.county_city_pairs_unique_df = self.county_city_pairs_df.drop_duplicates()
        print("Exporting as csv")
        self.county_city_pairs_unique_df.to_csv(f'results/{self.district}_county_city.csv', index=False)
        pass

    def count_invoicable(self):
        folder = f'results/{str(date.today())}'
        os.makedirs(folder, exist_ok=True)
        self.invoice_df = self.county_city_pairs_unique_df
        for invoice in self.county_city_pairs_unique_df.itertuples():
            self.invoice_df.loc[
                (self.poles_df['County'] == invoice.County) & (self.poles_df['City'] == invoice.City) & (self.poles_df[
                    'Light_Count_1'] != ''), 'Invoicable'] = 'True'
        self.invoice_df.to_csv(f"{folder}/{self.district}_invoicable_{str(date.today())}.csv", index=False)


if __name__ == '__main__':
    # u = Update_Blanks(
    #     county="data/TX_Counties.shp",
    #     city="data/TX_Cities.shp",
    #     poles='aep abilene poles v2'
    # )
    # u.search_poles()
    # u.join_counties_to_poles()

    # i = Invoicing(
    #     poles='data/0SanAngelo_poles.csv',
    #     district='San Angelo'
    # )
    # i.find_unique_county_city_pairs()
    # i.count_invoicable()
    # quit()

    l = Layer_Search(
        district='Laredo',
        created_after='',
        created_before='',
        title_search='',
        tag='',
        item_type='Feature Layer',
        poles='AEP_Abilene_Poles_v2',
        lights='abilenelights',
        station='ALBANY',
        status='Raw',
        date='20220310',
        update={
            'STERLING CITY': 'Raw',
            'PERKINS PROTHO': 'Raw',
            'SILVER': 'Raw',
            'BRONTE AMBASSADOR': 'Raw',
            'EDITH HUMBLE': 'Raw',
            'ROBERT LEE': 'Raw',
            'BRONTE': 'Raw',
            'FT CHADBOURNE': 'Raw',
            'FT CHADBOURNE_B': 'Raw',
            'WINTERS': 'Raw',
            'WINTERS_B': 'Raw',
            'BALLINGER': 'Raw',
            'BALLINGER_B': '',
            'TALPA ATLANTIC': 'Raw',
            'VALERA HUMBLE': 'Raw',
            'SANTA ANNA': 'Raw',
            'BRADY CITY': 'Raw',
            'MELVIN': 'Raw',
            'EDEN': '',
            'EOLA': '',
            'PAINT ROCK': '',
            'ROWENA': '',
            'MILES': '',
            'SA GRAPE CREEK': '',
            'SA GRAPE CREEK_B': '',
            'SA GRAPE CREEK_C': '',
            'SA GRAPE CREEK_D': '',
            'SA LAKE DR': '',
            'SA LAKE DR_B': '',
            'SA NORTH': '',
            'SA NORTH_B': '',
            'SA NORTH_C': '',
            'PAULANN': '',
            'SA COKE ST': '',
            'SA EMERSON ST': '',
            'SA WALNUT STREET': '',
            'SA WALNUT STREET_B': '',
            'HIGHLAND': '',
            'SA CONCHO': '',
            'SA JACKSON ST': '',
            'SA JACKSON ST_B': '',
            'SA AVENUE N': '',
            'COLLEGE HILLS': '',
            'BLUFFS': '',
            'SA SOUTH': '',
            'SA SOUTH_B': '',
            'BEN FICKLIN': '',
            'BEN FICKLIN_B': '',
            'SA SOUTHLAND HILLS': '',
            'SA MATHIS FIELD': '',
            'TANKERSLY (CVEC)': '',
            'TANKERSLY (CVEC)_B': '',
            'MERTZON': '',
            'MERTZON_B': '',
            'CHRISTOVAL': '',
            'CHRISTOVAL_B': '',
            'BARNHART': '',
            'MIDWAY LANE': '',
            'OZONA': '',
            'OZONA_B': '',
            'CROCKETT HEIGHTS': '',
            'SONORA ATLANTIC (SWTEC)': '',
            'SONORA 138 SUB': '',
            'SONORA': '',
            'FRIESS RANCH': '',
            'ELDORADO': '',
            'YELLOWJACKET': '',
            'YELLOWJACKET_B': '',
            'JUNCTION': '',
            'SHEFFIELD': '',
            'IRAAN': '',
            'MESA VIEW': '',
            'RUSSEK STREET': '',
            'RUSSEK STREET_B': '',
            'HUMBLE KEMPER': '',
            'POWELL FIELD': '',
            'SANTA RITA': '',
            'RANKIN': '',
            'RANKIN_B': '',
            'MCCAMEY': '',
            'MCCAMEY_B': '',
            'MCCAMEY_C': '',
            'NORTH MCCAMEY': '',
            'INDIAN MESA': '',
            'BOBCAT HILLS': '',
            'RIO PECOS': '',
            'SUN VALLEY': '',
            'MASTERSON FIELD': '',
            'PECOS VALLEY': '',
            'SPUDDER FLAT': '',
            'MCELROY': '',
            'DUNEFIELD (N CRANE)': '',
            'HOEFFS ROAD': '',
            'VERHALEN': '',
            'SARAGOSA': '',
            'SARAGOSA_B': '',
            'CHERRY CREEK': '',
            'CRYO': '',
            'FT DAVIS': '',
            'FT DAVIS_B': '',
            'VALENTINE': '',
            'VALENTINE_B': '',
            'MARFA': '',
            'MARFA_B': '',
            'PAISANO': '',
            'ALPINE 12KV': '',
            'ALPINE 12KV_B': '',
            'ALPINE 12KV_C': '',
            'ALPINE 12KV_D': '',
            'ALPINE 12KV_E': '',
            'BRYANTS RANCH': '',
            'SHAFTER': '',
            'GONZALES': '',
            'GONZALES_B': '',
            'GONZALES_C': ''
        }
    )
    # l.run_create_tables()
    l.run_deliverable_table()
    # l.stations_to_counties()
    # l.stations_to_cities()
    # l.qaqc_stations()
    # l.delivered_counties()
    # l.deliverable_counties()
    # l.delivered_cities()
    # l.deliverable_cities()
    # l.delivered_stations()
    # l.deliverable_stations()
    # l.build_deliverable_excel()
    # l.search_contents()
    # l.search_poles()
    # l.search_lights()
    # l.get_stations_counts()
    # l.export_station_counts()
    # l.search_station_features()
    # l.field_mapping_station()
    # l.field_mapping_poles()
    # l.configure_headers_station()
    # l.configure_headers_poles()
    # l.export_features_station()
    # l.export_features_poles()
    # l.stations_to_sql()
    # l.added_poles()
    # l.make_editable()
    print()
