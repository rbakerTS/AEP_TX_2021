from random import random

from arcgis.features import FeatureLayerCollection
# from sql_alchemy_wrapper.basic_db_manager import BasicDatabaseManager
from arcgis.gis.admin import AGOLAdminManager
from AGO_Manager import AGO_manager
import json
import os
import time
import pandas as pd
import geopandas as gpd
import csv
import pytz  # $ pip install pytz
import tzlocal  # $ pip install tzlocal
from datetime import date, datetime, timedelta
from UpdateWebMaps import customWebMap
from UpdateWebMaps import Update_WM
from arcgis.mapping import WebMap
from ArcGIS_python_api.ArcGIS_python_api_wrapper import ArcGISFeatureManager
import Renderer
from arcgis.apps import survey123
import operator
from numpy import random


# import arcpy


class AGO:
    def __init__(self, searches, folder: str, column_name: str, sql_select: str, sql_from: str, sql_where: str,
                 csv_file_path: str, status: str, fgdb_folder_path: str = '', lights_folder_path: str = '',
                 ago_folder: str = '', output_name: str = '', district: str = '', search_type: str = 'Feature Service',
                 lights_name: str = 'Lights'):
        self.fgdb_folder_path = fgdb_folder_path
        os.makedirs(self.fgdb_folder_path, exist_ok=True)
        self.lights_folder_path = lights_folder_path
        os.makedirs(self.lights_folder_path, exist_ok=True)
        self.ago_folder = ago_folder
        os.makedirs(self.ago_folder, exist_ok=True)
        self.poles_layer_name = f'{district}_Poles'
        self.lights_layer_name = f'{district}_Lights'
        self.searches = searches
        self.search_type = search_type
        self.lights_name = lights_name
        self.district = district
        self.sql_select = sql_select
        self.sql_from = sql_from
        self.sql_where = sql_where
        self.output_name = output_name
        self.csv_file_path = csv_file_path
        self.column_name = column_name
        self.sql_engine = 'mssql+pyodbc://TS-TYLER/AEP2021StreetlightAudit?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0'
        self.tracking_spreadsheet = f'C:/Users/TechServPC/OneDrive - TechServ/AEP StreetLight Project Management/{self.district}/AEP_Tracking_Field.xlsx'
        self.tracking_status = pd.read_excel(self.tracking_spreadsheet, 'Status')
        self.status = status
        with open('secrets.json') as file:
            x = json.load(file)

        username = x['username']
        password = x['password']

        self.manager = AGO_manager(username, password)
        self.token = self.manager.token()
        self.content = self.manager.content()
        self.local_timezone = tzlocal.get_localzone()
        self.date = date.today()
        self.now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
        self.fgdb_files = os.listdir(self.fgdb_folder_path)
        self.folder = folder
        os.makedirs(self.folder, exist_ok=True)
        # self.admin = AGOLAdminManager(self.manager)

    def survey123(self):
        survey123.SurveyManager(self.manager, None)

    def raw_stations(self):
        self.stations_raw = []
        for station_row in self.tracking_status.itertuples():
            if station_row[22] == 'Complete':
                self.stations_raw.append(station_row[2])
        return self.stations_raw

    def qaqc_stations(self):
        self.stations_qaqc = []
        for station_row in self.tracking_status.itertuples():
            if station_row[24] == 'Complete':
                self.stations_qaqc.append(station_row[2])
        return self.stations_qaqc

    def search_poles(self):
        poles_search = self.manager.content_search(title_search=self.poles_layer_name, item_type='Feature Service')
        for search in poles_search:
            if search['title'] == self.poles_layer_name:
                self.poles = search
        poles_layer = self.poles.layers[0]
        self.poles_sdf = poles_layer.query().sdf

    def search_lights(self):
        lights_search = self.manager.content_search(title_search=self.lights_layer_name, item_type='Feature Service')
        for search in lights_search:
            if search['title'] == self.lights_layer_name:
                self.lights = search
        self.lights_layer = self.lights.layers[0]
        self.lights_sdf = self.lights_layer.query().sdf

    def list_stations(self):
        self.station_list = list(self.poles_sdf.station_name.unique())
        return self.station_list

    def upload_station_poles(self):
        for file in self.fgdb_files:
            fgdb_name = os.path.splitext(file)[0] + '_FGDB'
            fgdb_search_results = self.manager.content_search(title_search=f'{fgdb_name}')
            if len(fgdb_search_results['items']) == 0:
                self.manager.upload_file(data='/'.join([self.fgdb_folder_path, file]), title=fgdb_name,
                                         type='File Geodatabase',
                                         tags='AEP, Streetlight Audit, Laredo, Poles, FGDB',
                                         ago_folder=self.ago_folder)
                print(f'Uploaded {fgdb_name} to AGO')
            else:
                print(f'{fgdb_name} already exists on AGO')
            print('---------------------------------------------------------------------------------------------------')

    def publish_station_poles(self):
        for file in self.fgdb_files:
            t = 0
            success = False
            while success == False:
                try:
                    time.sleep(t)
                    fgdb_name = os.path.splitext(file)[0] + '_FGDB'
                    fgdb_search_results = self.manager.content_search(title_search=f'{fgdb_name}')
                    fgdb_item = fgdb_search_results['items'][0]
                    layer_name = os.path.splitext(file)[0] + '_Poles'
                    layer_search_results = self.manager.content_search(created_after='2022-01-01',
                                                                       created_before='2022-03-01',
                                                                       title_search=f'{layer_name}')
                    renderer = Renderer.aep_tx_pole_renderer
                    if len(layer_search_results['items']) == 0:
                        self.manager.publish_item(item=fgdb_item, name=layer_name, renderer=renderer)
                        print(f'Published {layer_name} to AGO')
                    else:
                        print(f'{layer_name} already exists on AGO')
                    success = True
                except:
                    t += 1

        print('---------------------------------------------------------------------------------------------------')

    def rename_layers(self):
        renamed_items = []
        multiple_items = []
        no_items = []
        for file in self.fgdb_files:
            layer_name = os.path.splitext(file)[0] + '_Poles'
            new_name = os.path.splitext(file)[0] + '_Poles'
            layer_search = self.manager.content_search(created_after='2021-12-14', created_before='2021-12-25',
                                                       title_search=f'{layer_name}')
            layer_search = layer_search.sort()
            if len(layer_search) == 1:
                station_layer = layer_search[0]
                station_layer.update(item_properties={'title': new_name})
                print(f'Renamed {layer_name} to {new_name}')
                renamed_items.append(layer_name)
            elif len(layer_search) > 1:
                print(f'Found multiple matches for {layer_name}')
                multiple_items.append(layer_name)
            elif len(layer_search) == 0:
                print(f'Found no matches for {layer_name}')
                no_items.append(layer_name)
            print('---------------------------------------------------------------------------------------------------')
            time.sleep(0)

    def split_lights_by_station(self):
        for station in self.station_list:
            station_lights_fs = self.lights_layer.query(where=f"station_name='{station}'")
            station_lights_json = station_lights_fs.to_json
            station_lights_json_path = '/'.join([self.lights_folder_path, f'{station}.json'])
            with open(station_lights_json_path, 'w') as file:
                json.dump(station_lights_json, file)
            print(f'Created JSON for {station}')
            station_lights_fc = self.manager.upload_file(
                data='/'.join([self.lights_folder_path, f'{station}.json']),
                title=f'SanAngelo_{station}_Lights',
                tags='AEP, San Angelo, Streetlight, Lights',
                ago_folder='AEP_Streetlight_San Angelo'
            )
            print(f'Created Layer for {station}')
        print(f'{len(os.listdir(self.lights_folder_path))} json files in json folder')

    def items_search(self, search: str, type: str = 'Feature Layer'):
        self.search_items = []
        self.search = search
        self.search_results = self.manager.content_search(title_search=search, max_items=10000, item_type=type)
        self.search_items = self.search_results['items']
        self.search_count = len(self.search_items)
        return self.search_items

    def items_search_sleep(self, search: str, type: str = 'Feature Layer'):
        # def items_search_sleep(self, search: str):
        s = 300
        self.search_items = []
        while True:
            try:
                self.search = search
                self.search_results = self.manager.content_search(title_search=search, max_items=10000, item_type=type)
                # self.search_results = self.manager.content_search(title_search=search, max_items=10000)
                self.search_items = self.search_results['items']
                self.search_count = len(self.search_items)
                break
            except Exception as e:
                print(e)
                print(f'Sleeping for {s} seconds')
                print(f"Resuming at {datetime.strftime(datetime.now() + timedelta(seconds=s), '%I:%M:%S %p')}")
                time.sleep(s)
                s = 30
        return self.search_items

    def items_search_exact(self, search: str, type: str = 'Feature Service'):
        self.search = search
        self.search_results = self.manager.content_search(title_search=search, max_items=10000, item_type=type)
        self.search_items_trimmed = []
        for item in self.search_results['items']:
            if search == item['title']:
                self.search_items_trimmed.append(item)
        self.search_count_trimmed = len(self.search_items_trimmed)
        self.search_items = self.search_items_trimmed
        self.search_count = len(self.search_items)
        if self.search_count == 0:
            search = input(f'Search parameter {self.search} returned 0 results. Input an alternative search term: ')
            self.search_results = self.manager.content_search(title_search=search, max_items=10000, item_type=type)
            self.search_items_trimmed = []
            for item in self.search_results['items']:
                if search == item['title']:
                    self.search_items_trimmed.append(item)
            self.search_count_trimmed = len(self.search_items_trimmed)
            self.search_items = self.search_items_trimmed
            self.search_count = len(self.search_items)
        print(f"{self.search_count_trimmed} items contain search term")
        return self.search_items

    def item_summary(self):
        items = []
        stations = []
        item_ids = []
        types = []
        tags = []
        owners = []
        groups = []
        modified_dates = []
        urls = []

        for number, item in enumerate(self.search_items, start=1):
            item_title = item['title']
            items.append(item_title)
            station_name = item_title.replace(f'{self.district}_', '').replace('_Poles', '').replace('AEP_',
                                                                                                     '').replace(
                f'{self.district}_', '').replace(f'{self.district.replace(" ", "")}_', '')
            stations.append(station_name)
            item_ids.append(item.id)
            types.append(item.type)
            tags.append(str(item.tags))
            owners.append(item.owner)
            groups.append(str(item.shared_with['groups']))
            # modified_utc = datetime.utcfromtimestamp((item.modified / 1000)).strftime('%Y-%m-%d %H:%M:%S')
            # modified_local = modified_utc.replace(tzinfo=pytz.utc).astimezone(self.local_timezone)
            modified_dates.append(
                datetime.utcfromtimestamp((item.modified / 1000) - 21600).strftime('%Y-%m-%d %H:%M:%S'))
            urls.append(item.url)
            print(f"{number} of {self.search_count}: Added summary row for {item['title']}")
        columns = ['Item_Name', 'Station', 'Item_ID', 'Type', 'Tags', 'Owner', 'Groups', 'Last Modified', 'URL']
        search_name = self.search.replace('_', '').replace(' ', '_')
        self.folder = f'results/{self.date}'
        os.makedirs(self.folder, exist_ok=True)
        csv_name = f'results/{search_name}_{self.date}_items_summary.csv'
        with open(csv_name, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            data = [x for x in zip(items, stations, item_ids, types, tags, owners, groups, modified_dates, urls)]
            for x in data:
                writer.writerow(x)
        print(f"Generated {self.output_name}")

    def merge_csvs(self):
        print(f"Merging files in {self.folder} in one csv")
        self.files = os.listdir(self.folder)
        master_file = self.files[0]
        master_filepath = self.folder + "/" + master_file
        master_df = pd.read_csv(master_filepath)
        for file in self.files[1:]:
            if '.csv' in file:
                filepath = self.folder + "/" + file
                x = pd.read_csv(filepath)
                master_df = pd.concat([master_df, x], ignore_index=True)
        output_name = f'0{self.district}_{self.status}_{self.now}'
        self.merged_csv_name = f'{self.folder}/{output_name}.csv'
        master_df.to_csv(self.merged_csv_name, index=False)
        print(f"Merged files from {self.folder} into {output_name}")

    def get_station_name(self, item):
        item_station_name = 'Unable to determine'
        if self.search_type == 'Web Map':
            wm = WebMap(item)
            item_name = wm.item['title']
            item_name_trimmed = item_name.replace('AEP SA', '').strip().upper()
            item_name_split = item_name_trimmed.split()
            l1 = item_name_split[:-2]
            l1.append('_'.join(item_name_split[-2:]))
            item_station_name = ' '.join(l1)
            # item_station_name = map_station_name.split()
            # l1 = map_station_name[:-2]
            # l1.append('_'.join(map_station_name[-2:]))
            # map_station_name = ' '.join(l1)
            # map_layers = wm.layers
            # for layer in reversed(map_layers):
            #     poles_name = layer['title'].replace('Poles', '').replace('AEP', '').replace('SA', '').replace(
            #         'SanAngelo', '').replace('_', ' ').strip().lower()
            #     if poles_name == '':
            #         poles_name = item_name_trimmed
            #     if 'Lights' not in poles_name and 'QAQC' not in poles_name:
            #         poles_search = self.manager.content_search(title_search=poles_name,
            #                                                    item_type='Feature Layer')
            #         for item in poles_search['items']:
            #             item_name = item['title']
            #             item_name_trimmed = item_name.replace('AEP', '').replace('SA', '').replace('Poles',
            #                                                                                            '').replace(
            #                 'SanAngelo', '').replace('_', ' ')
            #             # if item_name_trimmed.upper() == item_name_trimmed:
            #             # item_name_trimmed = item['title'].replace('AEP', '').replace('SA', '').replace('SanAngelo',
            #             #                                                                                '').replace('_',
            #             #                                                                                            ' ').strip().lower()
            #             if poles_name in item_name_trimmed:
            #                 poles_layer = item.layers[0].query()
            #                 poles_sdf = poles_layer.sdf
            #                 item_station_name = poles_sdf.station_name[0]
            #             break
            #     break

        if self.search_type == 'Feature Layer':
            item_name = item['title']
            item_name_trimmed = item_name.replace('AEP SA', '').strip().upper()
            item_name_split = item_name_trimmed.split()
            l1 = item_name_split[:-2]
            l1.append('_'.join(item_name_split[-2:]))
            item_station_name = ' '.join(l1)

        item_name = item['title']
        item_station_name = item_name.replace('AEP SA', '').strip().upper()
        return item_station_name

    def update_tags(self, tags_to_add: str):
        tags_to_add_list = tags_to_add.split(',')
        self.tags_update_successful = 0
        for number, item in enumerate(self.search_items, start=1):
            tags_full = []
            tags_existing = item.tags
            for x in tags_existing:
                x = x.strip()
                tags_full.append(x)
            for y in tags_to_add_list:
                y = y.strip()
                tags_full.append(y)
            tags_trimmed = list(set(tags_full))
            update_result = item.update(item_properties={'tags': tags_trimmed})
            if update_result:
                self.tags_update_successful += 1
            print(f"{number} of {self.search_count}: Added ({tags_to_add}) tags for {item['title']}")
        print(f"Updated tags successfully for {self.tags_update_successful} of {self.search_count} items")

    def download_items_csv(self):
        download_successful = 0
        self.folder = f'downloads/{self.now}'
        os.makedirs(self.folder, exist_ok=True)
        failure_folder = f'downloads/{self.now}/failures'
        os.makedirs(failure_folder, exist_ok=True)
        failure_df = pd.DataFrame(columns=['item_name', 'exception', 'time'])
        for number, item in enumerate(self.search_items, start=1):
            try:
                item_name = item.title
                csv_name = f'{self.folder}/{item_name}_{self.now}.csv'
                item_df = item.layers[0].query().sdf
                success = item_df.to_csv(csv_name, index=False)
                if success is None:
                    download_successful += 1
                print(f"{number} of {self.search_count}: Downloaded {csv_name}")
            except Exception as e:
                print(f'FGDB download of {item.title} failed with:')
                print(e)
                failure_df.append(
                    {'item_name': item.title, 'exception': e, 'time': datetime.strftime(datetime.now(), '%Y%m%d%H%M')},
                    ignore_index=True)
                failure_df.to_csv(f'{failure_folder}/{item.title}_{self.now}.csv')
                continue
        print(f"Downloaded {download_successful} of {self.search_count} successfully")

    def download_items_fgdb(self):
        folder = f'downloads/{self.district}/deliverable_fgdb/'
        os.makedirs(folder, exist_ok=True)
        failure_folder = f'downloads/{self.district}/deliverable_fgdb/0failures'
        os.makedirs(failure_folder, exist_ok=True)
        failure_df = pd.DataFrame(columns=['item_name', 'exception', 'time'])
        files = os.listdir(folder)
        folder_server = f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\\{self.district}\Deliverables\FGDBs'
        files_server = os.listdir(folder_server)
        os.makedirs(folder_server, exist_ok=True)
        for number, item in enumerate(self.search_items, start=1):
            try:
                item_name = item.title
                item_id = item.id
                # fgdb_name = f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\\{self.district}\Deliverables\FGDB/{item_name}_{self.now}.gdb'
                fgdb_name = f'{folder}/{item_name}'
                download = True
                if item_name in files:
                    print(f'{item_name} found in {folder}')
                    download = False
                if item_name in files_server:
                    print(f'{item_name} found in {folder_server}')
                    download = False
                if download == True:
                    print(f'Downloading {item_name}')
                    item_fgdb = item.export(title=item_name, export_format='File Geodatabase')
                    item_fgdb.download(fgdb_name)
                    print(f"{number} of {self.search_count}: Downloaded {fgdb_name}")
            except Exception as e:
                print(f'FGDB download of {item.title} failed with:')
                print(e)
                failure_df.append(
                    {'item_name': item.title, 'exception': e, 'time': datetime.strftime(datetime.now(), '%Y%m%d%H%M')},
                    ignore_index=True)
                failure_df.to_csv(f'{failure_folder}/{item.title}.csv')
                continue

    def download_items_fgdb_nowait(self):
        self.export_status = []
        for number, item in enumerate(self.search_items, start=1):
            item_name = item.title
            item_id = item.id
            print(f'{number} of {self.search_count}: Exporting {item_name}')
            fgdb_name = f'downloads/{item_name}_{self.now}.gdb'
            item_fgdb_status = item.export(title=item_name, export_format='File Geodatabase', wait=False)
            item_fgdb_status['ItemName'] = item_name
            self.export_status.append(item_fgdb_status)
        keys = self.export_status[0].keys()
        with open('job_statuses.csv', 'w', newline='') as file:
            dict_writer = csv.DictWriter(file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.export_status)

    def job_statuses(self):
        job_statuses = pd.read_csv('job_statuses.csv')
        job_statuses['Status'] = ''
        job_statuses['Message'] = ''
        for i, job in job_statuses.iterrows():
            job_id = job.jobId
            service_id = job.serviceItemId
            export_id = job.exportItemId
            item = self.content.get(export_id)
            job_statuses.at[i, 'Status'] = item.status()['status']
            job_statuses.at[i, 'Message'] = item.status()['statusMessage']
            pass
        pass

    def export_jobs_csv(self):
        pass

    def set_basemap(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Setting basemaps')
        for number, item in enumerate(self.search_items, start=1):
            wm = WebMap(item)
            wm.basemap = 'hybrid'
            wm_name = wm.item['title']
            wm_basemap = wm.basemap.title
            wm.update()
            print(f'{wm_name} basemap set to {wm_basemap}')
        print(f'Set basemaps')

    def remove_all_layers(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Removing all layers from maps')
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_name = wm.item['title']
            x = len(wm.layers)
            if x != 0:
                for i in range(x - 1):
                    layer_to_remove = wm.layers[0]
                    wm.remove_layer(layer_to_remove)
                layer_to_remove = wm.layers[0]
                wm.remove_layer(layer_to_remove)

            wm.update()
            print(f'Removed all layers from {wm_name}')
        print(f'Removed all layers from maps')

    def remove_lights(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Removing lights layers from maps')
        for i, map in enumerate(search_results, start=1):
            wm = WebMap(map)
            wm_name = wm.item['title']
            wm_layers = wm.layers
            for layer in wm_layers:
                if 'Lights' in layer.title:
                    wm.remove_layer(layer)
                    wm.update()
                    print(f'Removed all lights layers from {wm_name}')
                else:
                    print(f'No lights layers in {wm_name}')
        print(f'Removed lights layers from maps')

    def add_layers(self, lights_name):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Adding station lights and poles to maps')
        lights_search = self.manager.content_search(title_search=lights_name, item_type='Feature Layer')
        station_mismatch = []
        for i, item in enumerate(lights_search['items']):
            if lights_name in item['title']:
                lights_layer = item.layers[0]
                break

        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_layers = []
            wm_name = wm.item['title']
            map_station_name = self.get_station_name(map)
            station_name = map_station_name.replace('_', ' ').replace('AEP ', '').replace(f'{self.district.upper()} ',
                                                                                          '')
            poles_search = self.items_search(station_name, 'Feature Layer')
            search_result_num = []
            search_result_created = []
            search_result_match = []
            for i, item in enumerate(poles_search):
                search_result_num.append(i)
                search_result_created.append(item.created)
                if len(station_name.split()) == 1:
                    if station_name == item.title.replace(f'{self.district.upper()} ', '').replace('AEP ', '').replace(
                            ' Poles', '').replace('_', ' '):
                        search_result_match.append('yes')
                if len(station_name.split()) > 1:
                    if (' '.join(station_name.split()[:-1]) in item.title.replace('_', ' ')) and (station_name.split()[
                                                                                                      -1] ==
                                                                                                  item.title.split('_')[
                                                                                                      -1]):
                        search_result_match.append('yes')
                if len(station_name.split()) < 1:
                    search_result_match.append('missing station_name')
                else:
                    search_result_match.append('no')

            results = list(zip(search_result_num, search_result_created, search_result_match))
            results_sorted = sorted(results, key=operator.itemgetter(1), reverse=True)
            # num_match = dict(zip(search_result_num, search_result_match))

            matches = []
            for result in results_sorted:
                if result[2] == 'yes':
                    matches.append(result)
            try:
                match = matches[0][0]
                poles_layer = poles_search[match].layers[0]
            except:
                try:
                    poles_layer = poles_search[0].layers[0]
                except:
                    continue
            # station_poles = poles_layer.query(where=f"station_name='{station_name}'")
            station_poles = poles_layer.query()
            if len(station_poles.features) == 0:
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
            if len(station_poles.features) == 0:
                station_name = station_name.split()
                l1 = station_name[:-2]
                l1.append('_'.join(station_name[-2:]))
                station_name = ' '.join(l1)
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(map_station_name.split('_')[-1])}%'")
            poles_station_name = station_poles.sdf.station_name[0]
            wm.add_layer(poles_layer, {'title': f"{station_name} Poles", 'visibility': True,
                                       'renderer': Renderer.aep_tx_pole_renderer})
            print(f'Added {poles_layer.properties.name} to {wm_name}')

            for layer in wm.layers:
                wm_layers.append(layer["title"])
            wm_layers_str = str(wm_layers)
            if 'Lights' not in wm_layers_str:
                station_lights = lights_layer.query(where=f"station_name='None'")
                if len(station_lights.features) == 0:
                    station_lights = lights_layer.query(
                        where=f"station_name LIKE '%{map_station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
                if len(station_lights.features) == 0:
                    station_lights = lights_layer.query(where=f"station_name = '{map_station_name}'")
                elif len(station_lights.features) > 0:
                    map_station_name = map_station_name.split()
                    l1 = map_station_name[:-2]
                    l1.append('_'.join(map_station_name[-2:]))
                    map_station_name = ' '.join(l1)
                    station_lights = lights_layer.query(
                        where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(station_name.split('_')[-1])}%'")
                if len(station_lights) > 0:
                    lights_station_name = station_lights.sdf.station_name[0]
                    wm.add_layer(station_lights, {'title': f"{map_station_name} Lights", 'visibility': True,
                                                  'renderer': Renderer.aep_tx_light_renderer})
                    print(f'Added {lights_layer.properties.name} to {wm_name}')
                else:
                    print(f'No lights found for {wm_name}')
            # else:
            #     print(f'{wm_name} already has a lights layer')
            wm.update()
            # poles_extent = poles_layer.properties.extent
            # map_extent = wm.item.extent
            # map_sr = wm.item.spatialReference
            # blX = poles_extent.xmin
            # blY = poles_extent.ymin
            # trX = poles_extent.xmax
            # trY = poles_extent.ymax
            # wkid = poles_extent.spatialReference.wkid
            # extent = f"[[{blX}, {blY}], [{trX}, {trY}]]"
            # wm.item.update(item_properties={'extent': extent, 'spatialReference': wkid})
            # pass

        # print(f'Added layers to maps')
        #
        # if poles_station_name != lights_station_name:
        #     station_mismatch.append(wm_name)
        # station_mismatch_series = pd.Series(station_mismatch)
        # now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
        # station_mismatch_series.to_csv(f'station_mismatch_maps_{now}.csv')
        # print(station_mismatch)
        # print('Station names checked')

    def set_extent(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Updating extents of each map based on pole layer extent')
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_name = wm.item['title']
            map_station_name = self.get_station_name(map)
            station_name = map_station_name.replace('_', ' ')
            poles_search = self.items_search(station_name, 'Feature Layer')
            search_result_num = []
            search_result_created = []
            search_result_match = []
            for i, item in enumerate(poles_search):
                search_result_num.append(i)
                search_result_created.append(item.created)
                if ''.join(station_name.split()[:-1]) in item['title'] and station_name.split()[-1] in item[
                    'title']:
                    search_result_match.append('yes')
                    # poles_layer = item.layers[0]
                    # break
                else:
                    search_result_match.append('no')

            results = list(zip(search_result_num, search_result_created, search_result_match))
            results_sorted = sorted(results, key=operator.itemgetter(1), reverse=True)
            # num_match = dict(zip(search_result_num, search_result_match))

            matches = []
            for result in results_sorted:
                if result[2] == 'yes':
                    matches.append(result)
            match = matches[0][0]
            poles_layer = poles_search[match].layers[0]
            poles_extent = poles_layer.properties.extent
            map_extent = wm.item.extent
            map_sr = wm.item.spatialReference
            blX = poles_extent.xmin
            blY = poles_extent.ymin
            trX = poles_extent.xmax
            trY = poles_extent.ymax
            wkid = poles_extent.spatialReference.wkid
            extent = f"[[{blX}, {blY}], [{trX}, {trY}]]"
            wm.item.update(item_properties={'extent': extent, 'spatialReference': wkid})
            print(f"Update extent of {wm_name}")
        print("Updated extent of maps")

    def check_station_names(self, lights_name):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Checking that station names match in poles and lights')
        station_mismatch = []
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_name = wm.item['title']
            map_station_name = self.get_station_name(map)
            station_name = map_station_name.replace('_', ' ')

            poles_search = self.items_search(station_name, 'Feature Layer')
            for item in poles_search:
                if ''.join(station_name.split()[:-1]) in item['title'] and station_name.split()[-1] in item[
                    'title']:
                    poles_layer = item.layers[0]
                    break
            station_poles = poles_layer.query(where=f"station_name='{station_name}'")
            if len(station_poles.features) == 0:
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
            if len(station_poles.features) == 0:
                station_name = station_name.split()
                l1 = station_name[:-2]
                l1.append('_'.join(station_name[-2:]))
                station_name = ' '.join(l1)
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(map_station_name.split('_')[-1])}%'")
            poles_station_name = station_poles.sdf.station_name[0]

            lights_search = self.manager.content_search(title_search=lights_name, item_type='Feature Layer')
            for item in lights_search['items']:
                if lights_name in item['title']:
                    lights_layer = item.layers[0]
                    break
            station_lights = lights_layer.query(where=f"station_name='{station_name}'")
            if len(station_lights.features) == 0:
                station_lights = lights_layer.query(
                    where=f"station_name LIKE '%{station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
            if len(station_lights.features) == 0:
                station_name = station_name.split()
                l1 = map_station_name[:-2]
                l1.append('_'.join(station_name[-2:]))
                station_name = ' '.join(l1)
                station_lights = lights_layer.query(
                    where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(map_station_name.split('_')[-1])}%'")
            lights_station_name = station_lights.sdf.station_name[0]

            if poles_station_name != lights_station_name:
                station_mismatch.append(wm_name)
            time.sleep(30)
        station_mismatch_series = pd.Series(station_mismatch)
        csv_name = f'station_mismatch_maps_{self.now}.csv'
        # station_mismatch_series.to_csv(csv_name)
        with open(csv_name, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(station_mismatch)
        mismatched_str = 'no maps'
        if len(station_mismatch) > 0:
            mismatched_str = ', '.join(station_mismatch)
        print(f'Mismatched stations found on {mismatched_str}')
        print('Station names checked')

    def add_lights(self, lights_name):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Adding station lights and poles to maps')
        lights_search = self.manager.content_search(title_search=lights_name, item_type='Feature Layer')
        station_mismatch = []
        for i, item in enumerate(lights_search['items']):
            if lights_name in item['title']:
                lights_layer = item.layers[0]
                break
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_layers = []
            wm_name = wm.item['title']
            map_station_name = self.get_station_name(map)
            station_name = map_station_name.replace('_', ' ')
            poles_search = self.items_search(station_name, 'Feature Layer')
            search_result_num = []
            search_result_created = []
            search_result_match = []
            for i, item in enumerate(poles_search):
                search_result_num.append(i)
                search_result_created.append(item.created)
                if len(station_name.split()) == 1:
                    if station_name == item.title.replace('SanAngelo_', '').replace('_Poles', '').replace('_', ' '):
                        search_result_match.append('yes')
                if len(station_name.split()) > 1:
                    if ' '.join(station_name.split()[:-1]) in item.title.replace('_', ' ') and station_name.split()[
                        -1] in item.title.replace('_', ' '):
                        search_result_match.append('yes')
                if len(station_name.split()) < 1:
                    search_result_match.append('missing station_name')
                else:
                    search_result_match.append('no')

            results = list(zip(search_result_num, search_result_created, search_result_match))
            results_sorted = sorted(results, key=operator.itemgetter(1), reverse=True)
            # num_match = dict(zip(search_result_num, search_result_match))

            matches = []
            for result in results_sorted:
                if result[2] == 'yes':
                    matches.append(result)
            try:
                match = matches[0][0]
                poles_layer = poles_search[match].layers[0]
            except:
                try:
                    poles_layer = poles_search[0].layers[0]
                except:
                    continue
            # station_poles = poles_layer.query(where=f"station_name='{station_name}'")
            station_poles = poles_layer.query()
            if len(station_poles.features) == 0:
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
            if len(station_poles.features) == 0:
                station_name = station_name.split()
                l1 = station_name[:-2]
                l1.append('_'.join(station_name[-2:]))
                station_name = ' '.join(l1)
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(map_station_name.split('_')[-1])}%'")
            poles_station_name = station_poles.sdf.station_name[0]
            wm.add_layer(poles_layer, {'title': f"{station_name} Poles", 'visibility': True,
                                       'renderer': Renderer.aep_tx_pole_renderer})
            print(f'Added {poles_layer.properties.name} to {wm_name}')

    def add_poles(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Adding station poles to maps')
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_layers = []
            wm_name = wm.item['title']
            map_station_name = self.get_station_name(map)
            station_name = map_station_name.replace('_', ' ').replace('AEP ', '').replace(f'{self.district.upper()} ',
                                                                                          '')
            poles_search = self.items_search(station_name, 'Feature Layer')
            search_result_num = []
            search_result_created = []
            search_result_match = []
            for i, item in enumerate(poles_search):
                search_result_num.append(i)
                search_result_created.append(item.created)
                if len(station_name.split()) == 1:
                    if station_name == item.title.replace(f'{self.district.upper()} ', '').replace('AEP ', '').replace(
                            ' Poles', '').replace('_', ' '):
                        search_result_match.append('yes')
                if len(station_name.split()) > 1:
                    if (' '.join(station_name.split()[:-1]) in item.title.replace('_', ' ')) and (station_name.split()[
                                                                                                      -1] ==
                                                                                                  item.title.split('_')[
                                                                                                      -1]):
                        search_result_match.append('yes')
                if len(station_name.split()) < 1:
                    search_result_match.append('missing station_name')
                else:
                    search_result_match.append('no')

            results = list(zip(search_result_num, search_result_created, search_result_match))
            results_sorted = sorted(results, key=operator.itemgetter(1), reverse=True)
            # num_match = dict(zip(search_result_num, search_result_match))

            matches = []
            for result in results_sorted:
                if result[2] == 'yes':
                    matches.append(result)
            try:
                match = matches[0][0]
                poles_layer = poles_search[match].layers[0]
            except:
                try:
                    poles_layer = poles_search[0].layers[0]
                except:
                    continue
            # station_poles = poles_layer.query(where=f"station_name='{station_name}'")
            try:
                station_poles = poles_layer.query()
            except:
                station_poles = poles_layer
            if len(station_poles.features) == 0:
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
            if len(station_poles.features) == 0:
                station_name = station_name.split()
                l1 = station_name[:-2]
                l1.append('_'.join(station_name[-2:]))
                station_name = ' '.join(l1)
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(map_station_name.split('_')[-1])}%'")
            poles_station_name = station_poles.sdf.station_name[0]
            wm.add_layer(poles_layer, {'title': f"{station_name} Poles", 'visibility': True,
                                       'renderer': Renderer.aep_tx_pole_renderer})
            print(f'Added {poles_layer.properties.name} to {wm_name}')

    def qaqc_layer_symbology(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Updating QAQC layer symbology')
        for i, map in enumerate(search_results, start=1):
            wm = WebMap(map)
            wm_layers = wm.layers
            map_station_name = self.get_station_name(map)
            poles = None
            for i, layer in enumerate(wm_layers):
                if "Comments" in layer['title']:
                    layer.layerDefinition = Renderer.comment_poles
                    continue
                if ("Poles" in layer['title']) and ("Comments" not in layer['title']):
                    layer.layerDefinition = Renderer.light_poles
                    continue
            wm.update()

    def qaqc_layer_order(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Updating QAQC layer order')
        for i, map in enumerate(search_results, start=1):
            wm = WebMap(map)
            wm_item = self.content.get(wm.item.id)
            wm_item_data = wm_item.get_data()

            lights, poles, comments, notes = None, None, None, None

            for i, wm_item_layer in enumerate(wm_item_data['operationalLayers']):
                if "Lights" in wm_item_layer['title']:
                    lights = wm_item_layer
                elif "Notes" in wm_item_layer['title']:
                    notes = wm_item_layer
                elif "Comments" in wm_item_layer['title']:
                    comments = wm_item_layer
                elif ("Comments" not in wm_item_layer['title']) and ("Poles" in wm_item_layer['title']):
                    poles = wm_item_layer

            wm_item_data['operationalLayers'][0] = lights
            wm_item_data['operationalLayers'][1] = poles
            wm_item_data['operationalLayers'][2] = comments
            wm_item_data['operationalLayers'][3] = notes
            item_properties = {"text": wm_item_data}
            wm_item.update(item_properties=item_properties)

    def update_layer_symbology(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Updating pole symbology')
        pole_renderer = Renderer.aep_tx_pole_renderer
        for i, item in enumerate(self.search_items, start=1):
            layer = item.layers[0]
            layer.manager.update_definition(pole_renderer)

    def update_maps(self, basemap: str = 'hybrid', lights_name: str = 'San Angelo Lights'):
        for item in self.search_items:
            wm = WebMap(item)
            wm_custom = customWebMap(wm)
            update_wm = Update_WM(wm)
            update_wm.get_station_name()
            lights_search = self.items_search(lights_name, 'Feature Layer')
            for item in lights_search:
                if item['title'] == lights_name:
                    district_lights = item
            self.add_lights()
            update_wm.set_basemap(basemap)
            update_wm.update_wm()

    def locate_added_poles(self, item):
        item_title = item.title
        station_poles = item.layers[0].query()
        station_poles_sdf = station_poles.sdf
        station_poles_trimmed = station_poles_sdf.loc(axis=1)[
            'location_number',
            'Latitude',
            'Longitude',
            'station_name',
            'Tech',
            'Comments',
            'Field_Conditions',
            'Light_Count_1',
            'pole_type',
            'height',
            'class',
            'pole_year',
            'District',
            'City',
            'County'
        ]
        field = "location_number"
        query = field + ' != ' + field
        station_added_poles_sdf = station_poles_trimmed.query(query)
        station_added_poles_sdf['Layer'] = item.title
        self.station_added_poles_count = len(station_added_poles_sdf)
        print(f'Found {self.station_added_poles_count} Added Poles in {item_title}')
        self.added_poles_sdf = self.added_poles_sdf.append(station_added_poles_sdf, ignore_index=True)
        return self.added_poles_sdf, self.station_added_poles_count

    def compile_added_poles(self):
        columns = [
            'location_number',
            'Latitude',
            'Longitude',
            'station_name',
            'Tech',
            'Comments',
            'Field_Conditions',
            'Light_Count_1',
            'pole_type',
            'height',
            'class',
            'pole_year',
            'District',
            'City',
            'County'
        ]
        # self.raw_stations = self.raw_stations()
        self.added_poles_sdf = pd.DataFrame(columns=columns)
        for number, station in enumerate(self.searches, start=1):
            print('==================================================================================================')
            print(f"{number} of {len(self.searches)}: {station}")
            station_poles = self.items_search_sleep(station, self.search_type)
            for i, item in enumerate(station_poles, start=1):
                print(f"{i} of {len(station_poles)}: {item.title}")
                try:
                    self.locate_added_poles(item)
                except KeyError as ke:
                    print("Item does not contain expected fields. Moving to next item.")
                except Exception as e:
                    print(e)
                    print("Moving to next item.")
        folder = f'results/{self.date}'
        name = f'{self.district.replace(" ", "_")}_Added_Poles_{self.date}.csv'
        os.makedirs(folder, exist_ok=True)
        self.added_poles_sdf.to_csv(f'{folder}/{name}', index=False)
        print(f'Exported {folder}')
        pass

    def update_definition(self):
        for item in self.search_items:
            station_flc = FeatureLayerCollection.fromitem(item)
            update = {'maxRecordCount': 3000}
            station_flc.manager.update_definition(update)
            print(f"Updated {item.title}'s definition with {update}")
            # time.sleep(random.uniform(1, 10))

    def create_map(self):
        for map in self.search_items:
            map_station_name = self.get_station_name(map)
            wm = WebMap()
            wm_properties = {'title': f"{map_station_name}_New", 'folder': self.ago_folder}
            wm.save(wm_properties)
            return wm

    def get_queried_sql(self):
        queried_sql = pd.read_sql(
            f"SELECT {self.sql_select} FROM {self.sql_from} WHERE {self.sql_where}", con=self.sql_engine)
        return queried_sql

    def stations_to_sql(self):
        engine_str = 'mssql+pyodbc://TS-TYLER/AEP2021StreetlightAudit?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0'
        d = BasicDatabaseManager(connection_str=engine_str)
        engine = d.engine
        columns = d.query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'test'",
            return_rows=True)
        with engine.begin() as connection:
            self.station_poles_trimmed.to_sql('test', con=connection, if_exists='append', index=False)
        # d.insert('dbo.tblQAQCData', qaqc_data)
        # d.insert('dbo.tblRawData', raw_data)
        print()

    def get_sql_data(self):
        pass

    def create_deliverable(self):
        sql_table_df = self.get_queried_sql()
        header = [
            'SL_Template.csv',
            '#Rows',
            'Contractor Code',
            'CO',
            'ST',
            'Parent Code',
            'Y'
        ]

        columns = [
            'PDS_LOC_NB',
            'CITY',
            'COUNTY',
            'LATITUDE',
            'LONGITUDE',
            'OWNER',
            'QUANTITY',
            'POLE TYPE',
            'FIXTURE_TYPE',
            'LIGHT TYPE',
            'LAMP TYPE',
            'LAMP SIZE',
            'ARM',
            'TIMESTAMP'
        ]

        mapping = {
            'Location Number': columns[0],
            'City': columns[1],
            'County': columns[2],
            'Latitude': columns[3],
            'Longitude': columns[4],
            'ownership': columns[5],
            'Light Count_1': columns[6],
            'pole_type': columns[7],
            'Fixture Type_1': columns[8],
            'Light Type_1': columns[9],
            'Power Source_1': columns[10],
            'Watts_1': columns[11],
            'Arm Length_1': columns[12],
            'EditDate': columns[13]
        }
        renamed_df = sql_table_df.rename(columns=mapping)
        renamed_df['LAMP TYPE'] = 'WATTS'
        lights_df = renamed_df.loc[renamed_df['QUANTITY'] != '']
        lights_df = lights_df.loc[lights_df['QUANTITY'] != 'Other (Input Comment)']
        folder = f"results/{self.now}"
        os.makedirs(folder, exist_ok=True)
        csv_name = f'{folder}/AEP_{self.district}_Deliverable_{self.now}.csv'
        with open(csv_name, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            for i, light in lights_df.iterrows():
                writer.writerow(light)
        print(f"Generated {csv_name}")
        return lights_df

    def set_feature_manager(self):
        search = 'Laredo_Poles'
        feature_search = self.items_search(search, 'Feature Layer')
        for item in feature_search:
            if item.title == search:
                feature = item
        feature_manager = ArcGISFeatureManager(feature, self.token)
        return feature_manager

    def create_replica(self, query: str, name: str, layers: str = '0,1,2,3,4', out_path: str = '/downloads'):
        feature_manager = self.set_feature_manager()
        name = name
        layers = layers
        out_path = out_path
        query = query
        feature_manager.create_replica(name, layers, out_path, query)
        quit()

    def get_station_table(self):
        mapping = {
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
            'CreationDate': 'CreationDate',
            'Creator': 'Creator',
            'EditDate': 'EditDate',
            'Editor': 'Editor',
            'Foreign_Pole_Number': 'Foreign Pole Number',
            'Pwr_Source_Loc_Num': 'Power Source Location Number',
            'ownership': 'ownership',
            'record_timestamp': 'Timestamp',
            'SHAPE': 'x',
            # 'SHAPE': 'y',
            'Device': 'InvoiceNum',
            'blank': 'InvoiceDate',
            'creation_date': 'CreationDate_Data',
            'edit_date': 'EditDate_Data',

        }
        abilene_poles_search = self.manager.content_search(title_search='Abilene', item_type='Feature Layer')
        abilene_poles_fl = None
        for item in abilene_poles_search['items']:
            if item.title == 'AEP Abilene Poles v2':
                abilene_poles_fl = item.layers[0]
        for i, station in enumerate(self.searches):
            df_for_sql = pd.DataFrame(
                columns=['OBJECTID', 'Location Number', 'Latitude', 'Longitude', 'Tech', 'District', 'City', 'County',
                         'Height', 'Class', 'Pole Year', 'Field Conditions', 'Light Count', 'Fixture Type_1',
                         'Light Type_1', 'Watts_1', 'Light Target_1', 'Power Source_1', 'Arm Length_1', 'Comments',
                         'Circuit Name', 'Station Name', 'Pole Number Missing', 'Fixture Type_2', 'Light Type_2',
                         'Watts_2', 'Light Target_2', 'Power Source_2', 'Arm Length_2', 'Fixture Type_3',
                         'Light Type_3', 'Watts_3', 'Light Target_3', 'Power Source_3', 'Arm Length_3',
                         'Fixture Type_4', 'Light Type_4', 'Watts_4', 'Light Target_4', 'Power Source_4',
                         'Arm Length_4', 'Pole Type', 'Pole_Dir_1', 'Mount_Dir_1', 'Bottom_Dir_1', 'Pole_Dir_2',
                         'Mount_Dir_2', 'Bottom_Dir_2', 'Pole_Dir_3', 'Mount_Dir_3', 'Bottom_Dir_3', 'Pole_Dir_4',
                         'Mount_Dir_4', 'Bottom_Dir_4', 'GlobalID', 'CreationDate', 'Creator', 'EditDate', 'Editor',
                         'Power Source Location Number', 'ownership', 'Foreign Pole Number', 'Timestamp', 'x', 'y'])
            print(f'{i + 1} of {len(self.searches)}: Creating dataframe for {station}')
            abilene_station_df = abilene_poles_fl.query(where=f"station_name='{station}'").sdf
            abilene_station_df = abilene_station_df[[
                'OBJECTID',
                'location_number',
                'Latitude',
                'Longitude',
                'Tech',
                'District',
                'City',
                'County',
                'height',
                'class',
                'pole_year',
                'Field_Conditions',
                'Light_Count_1',
                'Fixture_Type_1',
                'Light_Type_1',
                'Watts_1',
                'Light_Target_1',
                'Power_Source_1',
                'Arm_Length_1',
                'Comments',
                'circuit_name',
                'station_name',
                'Pole_Num_Missing',
                'Fixture_Type_2',
                'Light_Type_2',
                'Watts_2',
                'Light_Target_2',
                'Power_Source_2',
                'Arm_Length_2',
                'Fixture_Type_3',
                'Light_Type_3',
                'Watts_3',
                'Light_Target_3',
                'Power_Source_3',
                'Arm_Length_3',
                'Fixture_Type_4',
                'Light_Type_4',
                'Watts_4',
                'Light_Target_4',
                'Power_Source_4',
                'Arm_Length_4',
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
                'Foreign_Pole_Number',
                'Pwr_Source_Loc_Num',
                'ownership',
                'record_timestamp',
                'SHAPE',
                # 'SHAPE',
                'Device',
                'blank',
                'creation_date',
                'edit_date'
            ]]
            abilene_station_df_rename = abilene_station_df.rename(columns=mapping)
            abilene_station_df_rename.insert(64, 'y', 0)
            abilene_station_df_rename['x'] = abilene_station_df_rename['Longitude']
            abilene_station_df_rename['y'] = abilene_station_df_rename['Latitude']
            abilene_station_df_rename['CreationDate'] = abilene_station_df_rename['CreationDate'].apply(
                lambda x: datetime.strptime(str(x).split(".")[0], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y %H:%M"))
            abilene_station_df_rename['EditDate'] = abilene_station_df_rename['EditDate'].apply(
                lambda x: datetime.strptime(str(x).split(".")[0], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y %H:%M"))
            abilene_station_df_rename = abilene_station_df_rename.drop(
                columns=['InvoiceNum', 'InvoiceDate', 'CreationDate_Data', 'EditDate_Data'])
            df_for_sql = df_for_sql.append(abilene_station_df_rename, ignore_index=True)
            folder = f"Z:/Audits and Inventories/AEP/AEP STREETLIGHT AUDIT 2021/{self.district}/Raw_QAQC_Data"
            os.makedirs(folder, exist_ok=True)
            df_for_sql.to_csv(folder, index=False)
            print(f'    Created sql csv file for {station}')
        pass

    def ago_poles_to_sql_poles(self):
        mapping = {
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
            'CreationDate': 'CreationDate',
            'Creator': 'Creator',
            'EditDate': 'EditDate',
            'Editor': 'Editor',
            'Foreign_Pole_Number': 'Foreign Pole Number',
            'Pwr_Source_Loc_Num': 'Power Source Location Number',
            'ownership': 'ownership',
            'record_timestamp': 'Timestamp',
            'SHAPE': 'x',
            # 'SHAPE': 'y',
            'Device': 'InvoiceNum',
            'blank': 'InvoiceDate',
            'creation_date': 'CreationDate_Data',
            'edit_date': 'EditDate_Data',

        }
        ago_poles_df = pd.read_csv(self.csv_file_path)
        df_for_sql = pd.DataFrame(
            columns=['OBJECTID', 'Location Number', 'Latitude', 'Longitude', 'Tech', 'District', 'City', 'County',
                     'Height', 'Class', 'Pole Year', 'Field Conditions', 'Light Count', 'Fixture Type_1',
                     'Light Type_1', 'Watts_1', 'Light Target_1', 'Power Source_1', 'Arm Length_1', 'Comments',
                     'Circuit Name', 'Station Name', 'Pole Number Missing', 'Fixture Type_2', 'Light Type_2',
                     'Watts_2', 'Light Target_2', 'Power Source_2', 'Arm Length_2', 'Fixture Type_3',
                     'Light Type_3', 'Watts_3', 'Light Target_3', 'Power Source_3', 'Arm Length_3',
                     'Fixture Type_4', 'Light Type_4', 'Watts_4', 'Light Target_4', 'Power Source_4',
                     'Arm Length_4', 'Pole Type', 'Pole_Dir_1', 'Mount_Dir_1', 'Bottom_Dir_1', 'Pole_Dir_2',
                     'Mount_Dir_2', 'Bottom_Dir_2', 'Pole_Dir_3', 'Mount_Dir_3', 'Bottom_Dir_3', 'Pole_Dir_4',
                     'Mount_Dir_4', 'Bottom_Dir_4', 'GlobalID', 'CreationDate', 'Creator', 'EditDate', 'Editor',
                     'Power Source Location Number', 'ownership', 'Foreign Pole Number', 'Timestamp', 'x', 'y'])
        ago_poles_df_rename = ago_poles_df.rename(columns=mapping)
        ago_poles_df_rename.insert(64, 'y', 0)
        ago_poles_df_rename['x'] = ago_poles_df_rename['Longitude']
        ago_poles_df_rename['y'] = ago_poles_df_rename['Latitude']
        # ago_poles_df_rename['CreationDate'] = ago_poles_df_rename['CreationDate'].apply(
        #     lambda x: datetime.strptime(str(x).split(".")[0], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y %H:%M"))
        # ago_poles_df_rename['EditDate'] = ago_poles_df_rename['EditDate'].apply(
        #     lambda x: datetime.strptime(str(x).split(".")[0], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y %H:%M"))
        # ago_poles_df_rename = ago_poles_df_rename.drop(
        #     columns=['InvoiceNum', 'InvoiceDate', 'CreationDate_Data', 'EditDate_Data'])
        df_for_sql = pd.concat([df_for_sql, ago_poles_df_rename], join='inner')
        # folder = f"Z:/Audits and Inventories/AEP/AEP STREETLIGHT AUDIT 2021/{self.district}/Raw_QAQC_Data"
        folder = f'{self.folder}'
        os.makedirs(folder, exist_ok=True)
        df_for_sql.to_csv(f"{folder}/0{self.district}_{self.status}_sql_{self.now}.csv", index=False)

    pass

    def add_qaqc_notes(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Adding QAQC Notes to maps')
        title_search = 'QAQC Notes'
        qaqc_notes_search = self.manager.content_search(title_search=title_search, item_type='Feature Layer')
        for item in qaqc_notes_search['items']:
            if item.title == title_search:
                qaqc_notes_fl = item.layers[0]
                break
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm.add_layer(qaqc_notes_fl)
            wm.update()
            print(f'Added QAQC Notes to {wm.item.title}')

    def add_comments(self):
        print('-------------------------------------------------------------------------------------------------------')
        print(f'Adding Comment poles to maps')
        # title_search = 'QAQC Notes'
        # comment_poles_search = self.manager.content_search(title_search=title_search, item_type='Feature Layer')
        # for item in comment_poles_search['items']:
        #     if item.title == title_search:
        #         comment_poles_fl = item.layers[0]
        #         break
        # for i, map in enumerate(self.search_items, start=1):
        #     wm = WebMap(map)
        #     wm.add_layer(comment_poles_fl)
        #     wm.update()
        #     print(f'Added Comment poles to {wm.item.title}')
        # print(f'Added Comment poles to all maps in list')
        for i, map in enumerate(self.search_items, start=1):
            wm = WebMap(map)
            wm_layers = []
            wm_name = wm.item['title']
            map_station_name = self.get_station_name(map)
            station_name = map_station_name.replace('_', ' ').replace('AEP ', '').replace(f'{self.district.upper()} ',
                                                                                          '')
            poles_search = self.items_search_sleep(station_name, 'Feature Layer')
            search_result_num = []
            search_result_created = []
            search_result_match = []
            for i, item in enumerate(poles_search):
                search_result_num.append(i)
                search_result_created.append(item.created)
                if len(station_name.split()) == 1:
                    if station_name == item.title.replace(f'{self.district.upper()} ', '').replace('AEP ', '').replace(
                            ' Poles', '').replace('_', ' '):
                        search_result_match.append('yes')
                if len(station_name.split()) > 1:
                    if (' '.join(station_name.split()[:-1]) in item.title.replace('_', ' ')) and (station_name.split()[
                                                                                                      -1] ==
                                                                                                  item.title.split('_')[
                                                                                                      -1]):
                        search_result_match.append('yes')
                if len(station_name.split()) < 1:
                    search_result_match.append('missing station_name')
                else:
                    search_result_match.append('no')

            results = list(zip(search_result_num, search_result_created, search_result_match))
            results_sorted = sorted(results, key=operator.itemgetter(1), reverse=True)
            # num_match = dict(zip(search_result_num, search_result_match))

            matches = []
            for result in results_sorted:
                if result[2] == 'yes':
                    matches.append(result)
            try:
                match = matches[0][0]
                poles_layer = poles_search[match].layers[0]
            except:
                try:
                    poles_layer = poles_search[0].layers[0]
                except:
                    pass
            # station_poles = poles_layer.query(where=f"station_name='{station_name}'")
            station_poles = poles_layer.query()
            if len(station_poles.features) == 0:
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{station_name}%' AND station_name NOT LIKE '%_B' AND station_name NOT LIKE '%_C'AND station_name NOT LIKE '%_D' AND station_name NOT LIKE '%_E' AND station_name NOT LIKE '%_F' AND station_name NOT LIKE '%_G'")
            if len(station_poles.features) == 0:
                station_name = station_name.split()
                l1 = station_name[:-2]
                l1.append('_'.join(station_name[-2:]))
                station_name = ' '.join(l1)
                station_poles = poles_layer.query(
                    where=f"station_name LIKE '%{''.join(station_name.split('_')[:-1])}%' AND station_name LIKE '%{''.join(map_station_name.split('_')[-1])}%'")
            poles_station_name = station_poles.sdf.station_name[0]
            wm.add_layer(poles_layer, {'title': f"{station_name} Poles w Comments", 'visibility': True,
                                       'renderer': Renderer.aep_tx_pole_renderer,
                                       'layerDefinition': Renderer.comment_poles})
            wm.update()

    def split_csv(self):
        print(f"Splitting {self.csv_file_path} by {self.column_name}")
        df = pd.read_csv(self.csv_file_path)
        folder = f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021/{self.district}/Raw_QAQC_Data'
        os.makedirs(folder, exist_ok=True)
        for (x), group in df.groupby([self.column_name]):
            group.to_csv(f'{folder}/{self.district}_{x}_{self.status}.csv', index=False)
            print(f"Created csv for {x} {self.status}")

    def convert_csv_to_sdf(self):
        gpd.read_file(self.csv_file_path, driver="CSV", X_POSSIBLE_NAMES="longitude", Y_POSSIBLE_NAMES="latitude")

    def read_spatial_data(self):
        print("Reading spatial data")
        self.counties_sdf = gpd.read_file('data/TX_Counties.shp')
        self.cities_sdf = gpd.read_file('data/TX_Counties.shp')
        self.poles_sdf = gpd.read_file('data/SA_Poles.shp')

    def get_counties(self):
        print("Getting list of counties")
        self.counties_list = []
        for pole in self.poles_sdf.itertuples():
            if (pole.County not in self.counties_list) & (pole.County != None):
                self.counties_list.append(pole.County)
        return self.counties_list

    def relate_counties_cities(self):
        print("Relating stations to counties and cities")
        self.poles_sdf = gpd.read_file('data/SA_Poles.shp')
        self.counties_cities_dict = dict.fromkeys(self.get_counties(), [])
        for pole in self.poles_sdf.itertuples():
            if (pole.City != None) & (self.counties_cities_dict[pole.County] != []):
                if (pole.City not in self.counties_cities_dict[pole.County]):
                    self.counties_cities_dict[pole.County].append(pole.City)
        print(self.counties_cities_dict)
        return self.counties_cities_dict

    def export_counties_cities_relate(self):
        self.relate_counties_cities()
        print("Exporting stations relate")
        df = pd.DataFrame(self.counties_cities_dict)
        csv = f'results/{self.district}_counties_cities.csv'
        df.to_csv(csv)
        print(f"Exported as {csv}")

    def relate_stations_to_cities(self):
        stations_cities = []
        for pole in self.poles_sdf.itertuples():
            if pole['station_na'] == self.search:
                stations_cities.append(pole['City'])

    def relate_counties_to_cities(self):
        pass

    def list_files(self, folder):
        # create a list of file and subdirectories
        # names in the given directory
        files_list = os.listdir(folder)
        self.files = []
        # Iterate over all the entries
        for entry in files_list:
            # Create full path
            path = os.path.join(folder, entry)
            # If entry is a directory then get the list of files in this directory
            if os.path.isdir(path):
                self.files = self.files + self.list_files(path)
            else:
                self.files.append(path)
        # folder_name = folder.split("\\")[:-1]
        # files.to_csv(f'{folder}/0file_listing_{self.now}.csv', index=False)
        return self.files

    def get_attachments(self):
        print('.......................................................................................................')
        print(f'Downloading attachments for items in search')
        root_folder = f'Y:\\AEP Streetlight Audit\\{self.district}'
        os.makedirs(root_folder, exist_ok=True)

        for i, item in enumerate(self.search_items, start=1):
            item_name = item.title
            print('===================================================================================================')
            item_folder = f'{root_folder}/{item_name}'
            os.makedirs(item_folder, exist_ok=True)
            # print(f'Downloading layer csv for {item_name}')
            # item_sdf = item.layers[0].query().sdf
            # item_sdf.to_csv(f'{item_folder}/{item_name}_{self.now}.csv', index=False)
            print(f'Listing files in {item_folder} and its subdirectories')
            file_paths = self.list_files(item_folder)
            item_files = [os.path.basename(x) for x in file_paths]
            print(f'Found {len(self.files)} in {item_folder}')
            print('---------------------------------------------------------------------------------------------------')
            tables = item.tables
            success_count = 0
            size_count = 0
            failed_count = 0
            exists_count = 0
            attachment_list = []
            success_list = []
            failure_list = []
            exists_list = []
            for table in tables:
                table_name = table.properties.name
                print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
                print(f'Downloading table csv for {item_name}/{table_name}')
                table_folder = f'{item_folder}/{table_name}'
                os.makedirs(table_folder, exist_ok=True)
                table_sdf = table.query().sdf
                table_sdf.to_csv(f'{table_folder}/{table_name}_{self.now}.csv', index=False)
                objectids = table.query(where='1=1', return_ids_only=True)
                objectid_list = objectids['objectIds']
                print(f'Downloading attachments for {item_name}/{table_name}')
                for objectid in range(len(objectid_list)):
                    try:
                        row_attachments = table.attachments.get_list(oid=objectid)
                        print(
                            '-----------------------------------------------------------------------------------------')
                        attachment_parent_global_id = row_attachments[0]['parentGlobalId']
                        print(f'{objectid + 1} of {len(objectid_list)}:  {attachment_parent_global_id}')
                        if len(row_attachments) > 0:
                            for attachment_num in range(len(row_attachments)):
                                attachment_id = row_attachments[attachment_num]['id']
                                attachment_global_id = row_attachments[attachment_num]['globalId']
                                attachment_name = row_attachments[attachment_num]['name']
                                attachment_size = row_attachments[attachment_num]['size']
                                attachment_file_name = f'{attachment_parent_global_id}_{attachment_name}'
                                light_folder = f'{table_folder}/{attachment_parent_global_id}'
                                os.makedirs(light_folder, exist_ok=True)
                                full_path = f'{light_folder}/{attachment_file_name}'
                                if attachment_name not in item_files:
                                    print(
                                        f'Downloading {attachment_name} which is {round(attachment_size / 1048576, 2)}mb')
                                    attachment_path = table.attachments.download(oid=objectid,
                                                                                 attachment_id=attachment_id,
                                                                                 save_path=light_folder)
                                    attachment_list.append(attachment_path)
                                else:
                                    print(f'{attachment_name} file already exists')
                                    item_files.remove(attachment_name)
                                    exists_list.append(full_path)
                                    exists_count += 1
                    except Exception as e:
                        print(f'There was an issue found while trying to download attachments for {item_name}')
                        print(e)
                        continue
                for item in attachment_list:
                    if os.path.isfile(item):
                        success_list.append(item)
                        success_count += 1
                    else:
                        failure_list.append(item)
                        failed_count += 1
            success_df = pd.DataSeries(success_list)
            failed_df = pd.DataSeries(failure_list)
            exists_df = pd.DataSeries(exists_list)

            writer = pd.ExcelWriter(f'{item_folder}/{item_name}_Results_{self.now}', engine='xlsxwriter')
            success_df.to_excel(writer, sheet_name='Success')
            failed_df.to_excel(writer, sheet_name='Failed')
            exists_df.to_excel(writer, sheet_name='Exists')
            writer.save()

            print(f'Downloads: {success_count} totaling {round(size_count / 1048576, 2)}mb')
            print(f'Fails: {failed_count}')
            print(f'Already exists: {exists_count}')

    def create_fgdb(self):
        output = f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021/{self.district}\Deliverables\FGDBs'
        name = 'Abilene'
        arcpy.management.CreateFileGDB(output, name)


if __name__ == '__main__':
    ago = AGO(
        searches=['MSS - ',
            # 'AEP Abilene Poles v2'
            # 'Laredo_Poles',
            # 'HARTFORD ST',
            # 'ELM CREEK_B',
            # 'VOGEL ST',
            # 'VOGEL ST_B',
            # 'WALNUT ST',
            # 'ABILENE EAST',
            # 'MAPLE ST',
            # 'MAPLE ST_B',
            # 'ABILENE COUNTRY CLUB',
            # 'OVER STREET 12KV',
            # 'MCMURRY',
            # 'MCMURRY_B',
            # 'MCMURRY_C',
            # 'SHELTON ST',
            # 'SHELTON ST_B',
            # 'SHELTON ST_C',
            # 'ABILENE PLANT',
            # 'ABILENE PLANT_B',
            # 'ABILENE OIL MILL',

            # 'COMSTOCK',
            # 'AMISTAD DAM',
            # 'PICACHO',
            # 'BUENA VISTA_A',
            # 'BUENA VISTA_B',
            # 'BUENA VISTA_C',
            # 'DEL RIO CITY_A',
            # 'DEL RIO CITY_B',
            # 'DEL RIO CITY_C',
            # 'DEL RIO CITY_D',
            # 'DEL RIO CITY_E',
            # 'DEL RIO CITY_F',
            # 'HAMILTON ROAD_A',
            # 'HAMILTON ROAD_B',
            # 'BRACKETTVILLE_A',
            # 'BRACKETTVILLE_B',
            # 'ASPHALT MINES',
            # 'UVALDE_A',
            # 'UVALDE_B',
            # 'UVALDE_C',
            # 'UVALDE_D',
            # 'UVALDE_E',
            # 'UVALDE_F',
            # 'READING',
            # 'KNIPPA',
            # 'SABINAL',
            # 'BANDERA ELECTRIC',
            # 'CAMPWOOD_A',
            # 'CAMPWOOD_B',
            # 'ROCKSPRINGS',
            # 'LYTLE_A',
            # 'LYTLE_B',
            # 'DEVINE_A',
            # 'DEVINE_B',
            # 'DEVINE_C',
            # 'PLEASANTON_A',
            # 'PLEASANTON_B',
            # 'PLEASANTON_C',
            # 'PLEASANTON_D',
            # 'JOURDANTON_A',
            # 'JOURDANTON_B',
            # 'JOURDANTON_C',
            # 'JOURDANTON_D',
            # 'CHARLOTTE_A',
            # 'CHARLOTTE_B',
            # 'CHARLOTTE_C',
            # 'FRIO_A',
            # 'FRIO_B',
            # 'PEARSALL_A',
            # 'PEARSALL_B',
            # 'DILLEY_A',
            # 'DILLEY_B',
            # 'COTULLA_A',
            # 'COTULLA_B',
            # 'COTULLA_C',
            # 'ENCINAL',
            # 'HOLCOMB_A',
            # 'HOLCOMB_B',
            # 'CATARINA',
            # 'ASHERTON',
            # 'CARRIZO SPRINGS_A',
            # 'CARRIZO SPRINGS_B',
            # 'DIMMIT_A',
            # 'DIMMIT_B',
            # 'BIG WELLS_A',
            # 'BIG WELLS_B',
            # 'CRYSTAL CITY_A',
            # 'CRYSTAL CITY_B',
            # 'CRYSTAL CITY_C',
            # 'CRYSTAL CITY_D',
            # 'LA PRYOR_A',
            # 'LA PRYOR_B',
            # 'CONOCO',
            # 'MAVERICK_A',
            # 'MAVERICK_B',
            # 'EAGLE PASS HYDRO',
            # 'ESCONDIDO_A',
            # 'ESCONDIDO_B',
            # 'EAGLE PASS CITY_A',
            # 'EAGLE PASS CITY_B',
            # 'EAGLE PASS CITY_C',
            # 'EAGLE PASS CITY_D',
            # 'EAGLE PASS CITY_E',
            # 'EAGLE PASS CITY_F',
            # 'PUEBLO_A',
            # 'PUEBLO_B',
            # 'PUEBLO_C',
            # 'UNITEC',
            # 'MINES ROAD_A',
            # 'MINES ROAD_B',
            # 'MINES ROAD_C',
            # 'MINES ROAD_D',
            # 'MILO_A',
            # 'MILO_B',
            # 'LAS CRUCES',
            # 'LAREDO PLANT',
            # 'DEL MAR_A',
            # 'DEL MAR_B',
            # 'DEL MAR_C',
            # 'UNIVERSITY',
            # 'ZACATE CREEK_A',
            # 'ZACATE CREEK_B',
            # 'ZACATE CREEK_C',
            # 'ANNA STREET_A',
            # 'ANNA STREET_B',
            # 'ANNA STREET_C',
            # 'WASHINGTON STREET_A',
            # 'WASHINGTON STREET_B',
            # 'WASHINGTON STREET_C',
            # 'WASHINGTON STREET_D',
            # 'WASHINGTON STREET_E',
            # 'WASHINGTON STREET_F',
            # 'LAREDO HEIGHTS_A',
            # 'LAREDO HEIGHTS_B',
            # 'LAREDO HEIGHTS_C',
            # 'GATEWAY_A',
            # 'GATEWAY_B',
            # 'GATEWAY_C',
            # 'SANTO NINO_A',
            # 'SANTO NINO_B',
            # 'SANTO NINO_C',
            # 'SANTO NINO_D',
            # 'SIERRA VISTA_A',
            # 'SIERRA VISTA_B',
            # 'RIO BRAVO_A',
            # 'RIO BRAVO_B',
            # 'RIO BRAVO_C',
            # 'SAN YGNACIO',
            # 'ZAPATA_A',
            # 'ZAPATA_B',
            # 'ZAPATA_C',
            # 'ZAPATA_D',
            # 'ZAPATA_E',
            # 'RANDADO_A',
            # 'RANDADO_B',
            # 'BRUNI_A',
            # 'BRUNI_B',
            # 'BRUNI_C',
            # 'BRUNI_D',
            # 'CRESTONIO_A',
            # 'CRESTONIO_B',
            # 'CRESTONIO_C',
            # 'CRESTONIO_D',
            # 'CRESTONIO_E',
            # 'CRESTONIO_F',
            # 'FREER_A',
            # 'FREER_B',
            # 'FREER_C',
            # 'FREER_D',
            # 'FREER_E',
            # 'GOVERNMENT WELLS_A',
            # 'GOVERNMENT WELLS_B',
            # 'GOVERNMENT WELLS_C',
            # 'SAN DIEGO_A',
            # 'SAN DIEGO_B',
            # 'SAN DIEGO_C',
            # 'SAN DIEGO_D',
            # 'SAN DIEGO_E',
            # 'STADIUM_A',
            # 'STADIUM_B',
            # 'STADIUM_C',
            # 'STADIUM_D',
            # 'STADIUM_E',
            # 'ALICE_A',
            # 'ALICE_B',
            # 'ALICE_C',
            # 'ALICE_D',
            # 'NORTH ELLA',
            # 'PREMONT_A',
            # 'PREMONT_B',
            # 'PREMONT_C',
            # 'PREMONT_D',
            # 'PREMONT_E',
            # 'FALFURRIAS_A',
            # 'FALFURRIAS_B',
            # 'FALFURRIAS_C',
            # 'FALFURRIAS_D',
            # 'FALFURRIAS_E',
            # 'FALFURRIAS_F',
            # 'FALFURRIAS_G',
            # 'RACHAL_A',
            # 'RACHAL_B',
            # 'RACHAL_C',

            # # 'Laredo_Poles',
            # # # 'SanAngelo'
            # 'STERLING CITY',
            # 'PERKINS PROTHO',
            # 'SILVER',
            # 'BRONTE AMBASSADOR',
            # 'EDITH HUMBLE',
            # 'ROBERT LEE',
            # 'BRONTE',
            # 'FT CHADBOURNE',
            # 'FT CHADBOURNE_B',
            # 'WINTERS',
            # 'WINTERS_B',
            # 'BALLINGER',
            # 'BALLINGER_B',
            # 'TALPA ATLANTIC',
            # 'VALERA HUMBLE',
            # 'SANTA ANNA',
            # 'BRADY CITY',
            # 'MELVIN',
            # 'EDEN',
            # 'EOLA',
            # 'PAINT ROCK',
            # 'ROWENA',
            # 'MILES',
            # 'SA GRAPE CREEK',
            # 'SA GRAPE CREEK_B',
            # 'SA GRAPE CREEK_C',
            # 'SA GRAPE CREEK_D',
            # 'SA LAKE DR',
            # 'SA LAKE DR_B',
            # 'SA NORTH',
            # 'SA NORTH_B',
            # 'SA NORTH_C',
            # 'PAULANN',
            # 'SA COKE ST',
            # 'SA EMERSON ST',
            # 'SA WALNUT STREET',
            # 'SA WALNUT STREET_B',
            # 'HIGHLAND',
            # 'SA CONCHO',
            # 'SA JACKSON ST',
            # 'SA JACKSON ST_B',
            # 'SA JACKSON ST_C',
            # 'SA AVENUE N',
            # 'COLLEGE HILLS',
            # 'BLUFFS',
            # 'SA SOUTH',
            # 'SA SOUTH_B',
            # 'BEN FICKLIN',
            # 'BEN FICKLIN_B',
            # 'SA SOUTHLAND HILLS',
            # 'SA MATHIS FIELD',
            # 'TANKERSLY (CVEC)',
            # 'TANKERSLY (CVEC)_B',
            # 'MERTZON',
            # 'MERTZON_B',
            # 'CHRISTOVAL',
            # 'CHRISTOVAL_B',
            # 'BARNHART',
            # 'MIDWAY LANE',
            # 'OZONA',
            # 'OZONA_B',
            # 'CROCKETT HEIGHTS',
            # 'SONORA ATLANTIC (SWTEC)',
            # 'SONORA 138 SUB',
            # 'SONORA',
            # 'FRIESS RANCH',
            # 'ELDORADO',
            # 'YELLOWJACKET',
            # 'YELLOWJACKET_B',
            # 'JUNCTION',
            # 'SHEFFIELD',
            # 'IRAAN',
            # 'MESA VIEW',
            # 'RUSSEK STREET',
            # 'RUSSEK STREET_B',
            # 'HUMBLE KEMPER',
            # 'POWELL FIELD',
            # 'SANTA RITA',
            # 'RANKIN',
            # 'RANKIN_B',
            # 'MCCAMEY',
            # 'MCCAMEY_B',
            # 'MCCAMEY_C',
            # 'NORTH MCCAMEY',
            # 'INDIAN MESA',
            # 'BOBCAT HILLS',
            # 'RIO PECOS',
            # 'SUN VALLEY',
            # 'MASTERSON FIELD',
            # 'PECOS VALLEY',
            # 'SPUDDER FLAT',
            # 'MCELROY',
            # 'DUNEFIELD (N CRANE)',
            # 'HOEFFS ROAD',
            # 'VERHALEN',
            # 'SARAGOSA',
            # 'SARAGOSA_B',
            # 'CHERRY CREEK',
            # 'CRYO',
            # 'FT DAVIS',
            # 'FT DAVIS_B',
            # 'VALENTINE',
            # 'VALENTINE_B',
            # 'MARFA',
            # 'MARFA_B',
            # 'PAISANO',
            # 'ALPINE 12KV',
            # 'ALPINE 12KV_B',
            # 'ALPINE 12KV_C',
            # 'ALPINE 12KV_D',
            # 'ALPINE 12KV_E',
            # 'BRYANTS RANCH',
            # 'SHAFTER',
            # 'GONZALES',
            # 'GONZALES_B',
            # 'GONZALES_C',
            # 'JSA',

            # 'TANKERSLY'

            # 'ALICE_A',
            # 'ALICE_B',
            # 'ALICE_C',
            # 'AMISTAD DAM',
            # 'ANNA STREET_A',
            # 'ANNA STREET_B',
            # 'ANNA STREET_C',
            # 'ASHERTON',
            # 'ASPHALT MINES',
            # 'BANDERA ELECTRIC',
            # 'BIG WELLS_A',
            # 'BIG WELLS_B',
            # 'BRACKETTVILLE_A',
            # 'BRACKETTVILLE_B',
            # 'BRUNI_D',
            # 'BUENA VISTA_A',
            # 'BUENA VISTA_B',
            # 'BUENA VISTA_C',
            # 'CAMPWOOD_A',
            # 'CAMPWOOD_B',
            # 'CARRIZO SPRINGS_A',
            # 'CARRIZO SPRINGS_B',
            # 'CATARINA',
            # 'CHARLOTTE_A',
            # 'CHARLOTTE_B',
            # 'CHARLOTTE_C',
            # 'COMSTOCK',
            # 'CONOCO',
            # 'COTULLA_A',
            # 'COTULLA_B',
            # 'COTULLA_C',
            # 'CRESTONIO_B',
            # 'CRESTONIO_E',
            # 'CRESTONIO_F',
            # 'CRYSTAL CITY_A',
            # 'CRYSTAL CITY_B',
            # 'CRYSTAL CITY_C',
            # 'CRYSTAL CITY_D',
            # 'DEL MAR_A',
            # 'DEL MAR_B',
            # 'DEL MAR_C',
            # 'DEL RIO CITY_A',
            # 'DEL RIO CITY_B',
            # 'DEL RIO CITY_C',
            # 'DEL RIO CITY_D',
            # 'DEL RIO CITY_E',
            # 'DEL RIO CITY_F',
            # 'DEVINE_A',
            # 'DEVINE_B',
            # 'DEVINE_C',
            # 'DILLEY_A',
            # 'DILLEY_B',
            # 'DIMMIT_A',
            # 'DIMMIT_B',
            # 'EAGLE PASS CITY_A',
            # 'EAGLE PASS CITY_B',
            # 'EAGLE PASS CITY_C',
            # 'EAGLE PASS CITY_D',
            # 'EAGLE PASS CITY_E',
            # 'EAGLE PASS CITY_F',
            # 'EAGLE PASS HYDRO',
            # 'ENCINAL',
            # 'ESCONDIDO_A',
            # 'ESCONDIDO_B',
            # 'FALFURRIAS_A',
            # 'FALFURRIAS_D',
            # 'FALFURRIAS_E',
            # 'FALFURRIAS_F',
            # 'FALFURRIAS_G',
            # 'FREER_A',
            # 'FREER_C',
            # 'FREER_D',
            # 'FREER_E',
            # 'FRIO_A',
            # 'FRIO_B',
            # 'GOVERNMENT WELLS_B',
            # 'HAMILTON ROAD_A',
            # 'HAMILTON ROAD_B',
            # 'HOLCOMB_A',
            # 'HOLCOMB_B',
            # 'JOURDANTON_A',
            # 'JOURDANTON_B',
            # 'JOURDANTON_C',
            # 'JOURDANTON_D',
            # 'KNIPPA',
            # 'LA PRYOR_A',
            # 'LA PRYOR_B',
            # 'LAREDO HEIGHTS_A',
            # 'LAREDO HEIGHTS_B',
            # 'LAREDO PLANT',
            # 'LAS CRUCES',
            # 'LYTLE_A',
            # 'LYTLE_B',
            # 'MAVERICK_A',
            # 'MAVERICK_B',
            # 'MILO_A',
            # 'MILO_B',
            # 'MINES ROAD_A',
            # 'MINES ROAD_B',
            # 'MINES ROAD_C',
            # 'MINES ROAD_D',
            # 'NORTH ELLA',
            # 'PEARSALL_A',
            # 'PEARSALL_B',
            # 'PICACHO',
            # 'PLEASANTON_A',
            # 'PLEASANTON_B',
            # 'PLEASANTON_C',
            # 'PLEASANTON_D',
            # 'PREMONT_B',
            # 'PREMONT_C',
            # 'PREMONT_D',
            # 'PUEBLO_A',
            # 'PUEBLO_B',
            # 'PUEBLO_C',
            # 'RACHAL_A',
            # 'RACHAL_B',
            # 'RACHAL_C',
            # 'RANDADO_A',
            # 'RANDADO_B',
            # 'READING',
            # 'ROCKSPRINGS',
            # 'SABINAL',
            # 'SAN DIEGO_A',
            # 'SAN DIEGO_B',
            # 'SAN DIEGO_C',
            # 'SAN DIEGO_D',
            # 'SAN DIEGO_E',
            # 'SIERRA VISTA_A',
            # 'STADIUM_A',
            # 'STADIUM_B',
            # 'STADIUM_C',
            # 'STADIUM_D',
            # 'STADIUM_E',
            # 'UNITEC',
            # 'UNIVERSITY',
            # 'UVALDE_A',
            # 'UVALDE_B',
            # 'UVALDE_C',
            # 'UVALDE_D',
            # 'UVALDE_E',
            # 'UVALDE_F',
            # 'WASHINGTON STREET_A',
            # 'WASHINGTON STREET_B',
            # 'WASHINGTON STREET_C',
            # 'WASHINGTON STREET_D',
            # 'WASHINGTON STREET_E',
            # 'WASHINGTON STREET_F',
            # 'ZACATE CREEK_A',
            # 'ZACATE CREEK_B',
            # 'ZACATE CREEK_C',

        ],
        fgdb_folder_path=r'C:\Users\TechServPC\PycharmProjects\AEP-TX\replicas\outputs\zips',
        ago_folder='MSS',
        lights_folder_path=r'C:/Work/Client/AEP/SanAngelo/Data/Lights_JSON',
        folder=r'Y:\AEP Streetlight Audit\Abilene',
        district='Abilene',
        # search_type='Web Map',
        search_type='Feature Layer',
        lights_name='Laredo_Lights',
        sql_select="[Location Number], [City], [County], [Latitude], [Longitude], [ownership], [Light Count_1], [pole_type], [Fixture Type_1], [Light Type_1], [Power Source_1], [Watts_1], [Arm Length_1], [EditDate]",
        sql_from='[AEP2021StreetlightAudit].[dbo].[tblQAQCData]',
        sql_where="District = 'Laredo'",
        output_name="0Laredo_poles.csv",
        csv_file_path=r'downloads/202203181426/0Laredo_QAQC_sql_202203181447.csv',
        status='QAQC',
        column_name='Station Name'
    )
    # ago.admin
    # ago.create_deliverable()
    # ago.upload_station_poles()
    # ago.publish_station_poles()
    # ago.rename_layers()
    # ago.split_lights_by_station()
    # ago.items_search_exact(''.join(ago.searches), ago.search_type)
    # ago.update_symbology()
    # ago.item_summary()
    # ago.compile_added_poles()

    # ago.get_station_table()

    # ago.read_spatial_data()
    # ago.export_counties_cities_relate()

    # ago.get_queried_sql()

    # ago.merge_csvs()
    # ago.ago_poles_to_sql_poles()
    # ago.split_csv()

    # quit()

    # district = 'Laredo'
    # existing = os.listdir(
    #     f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\\{district}\Deliverables\FGDBs')
    # existing_formatted = [x.replace(f'AEP_{district}_','').replace("_Poles", "").replace("_"," ") for x in existing]
    # file_count = []
    # missing = []
    # found = []
    # ago.list_files(r'Y:\AEP Streetlight Audit')
    for i, search in enumerate(ago.searches):
        # print("=======================================================================================================")
        # print(f'Search {i + 1} of {len(ago.searches)}: {search}')
        #     # ago.items_search_exact(search, ago.search_type)
        #     #     # search_word_list = []
        #     #     # for y in search.split(' '):
        #     #     #     cap = y.capitalize()
        #     #     #     search_word_list.append(cap)
        #     #     # search_word_str = ' '.join(['AEP SA', ' '.join(search_word_list)])
        #     # maps = ago.items_search_exact(search_word_str, ago.search_type)
        #     # ago.create_replica(name='Test',
        #     #                    layers='0,1,2,3,4',
        #     #                    out_path='replicas',
        #     #                    query="{\"0\":{\"where\": \"station_name='COMSTOCK'\"}}")
        # search_results = ago.items_search_sleep(search, ago.search_type)

        # maps = ago.items_search_sleep(search)
        # for item in maps:
        #     if item.id == '484c88e6eb7c4bb0bb9c628f49b2a918':
        #         x = item
        # ago.get_attachments()

    #     if search.replace("_"," ") in existing_formatted:
    #         found.append(search)
    #     else:
    #         missing.append(search)
    # for item in found:
    #     dict = {}
    #     dict['name'] = item
    #     count = sum(map(lambda x : x == item, found))
    #     dict['count'] = count
    #     file_count.append(dict)
    # for item in missing:
    #     dict = {}
    #     dict['name'] = item
    #     dict['count'] = 0
    #     file_count.append(dict)
    # df = pd.DataFrame(file_count)
    # f = 'results/rectify_fgdb/'
    # os.makedirs(f, exist_ok=True)
    # df.to_csv(f'{f}/{district}_FGDBrectify.csv')

    # if ago.search_count > 0:
    #     pass
    #         # ago.update_definition()
    #         # ago.update_tags('Counties')
    #         ago.download_items_fgdb()
    #         # ago.download_items_fgdb_nowait()
    #         # ago.job_statuses()

    #         ago.item_summary()

    # ago.create_map()
    # ago.set_basemap()
    # ago.remove_all_layers()
    #     ago.add_layers(ago.lights_name)
    # ago.remove_lights()
    # ago.set_extent()

    # ago.check_station_names(ago.lights_name)
    ago.add_poles()
    # ago.add_lights(ago.lights_name)
    # ago.update_layer_symbology()
    # ago.download_items_csv()
    # ago.add_qaqc_notes()
    # ago.add_comments()
    # ago.qaqc_layer_symbology()
    # ago.qaqc_layer_order()
    # ago.update_maps()
