import re
from typing import Union, Callable
import uuid
import os
import threading
import pickle
import time

import requests

from ArcGIS_wrapper import ArcGISFeature, ArcGISManager
from logger import Logger
from zipper import zipper_f


class ThreadingManager:
    def __init__(self):
        self.threads = dict()

    def create_thread(self, f: Callable, args: Union[None, list] = None, kwargs: Union[None, dict] = None,
                      identifier: str = ''):
        identifier if identifier else uuid.uuid4()
        self.threads[identifier] = {'thread': threading.Thread(target=f, args=args if args else []), 'started': False,
                                    'kwargs': kwargs}

    def start_thread(self, identifier: str):
        self.threads[identifier]['started'] = True
        self.threads[identifier]['thread'].start()

    def start_all_threads(self):
        pass

    def join_threads(self):
        for key in self.threads:
            self.threads[key]['thread'].join()
            self.threads[key]['started'] = 'Finished'

    def clean_threads(self):
        self.threads = dict()

    def run_script(self, script_path: str, thread_id: str):
        '''
        note, only works with terminal interface function and reads kwargs as: --x 10 (--keyword value)
        :param kwargs:
        :return:
        '''

        kwargs = self.threads[thread_id]['kwargs']

        kwargs_str = ' '.join([f'--{k} {v}' for k, v in kwargs.items()])

        os.system(f'start cmd /K python {script_path} {kwargs_str}')


def create_unique_stations_1(unique_stations):
    unique_stations_dict = {}
    while unique_stations:
        for unique_station in unique_stations:
            print(unique_station)
            if str(unique_station) not in unique_stations_dict.keys() and '_' not in str(unique_station):
                unique_stations_dict[unique_station] = {'stations': [unique_station], 'status': 'not_downloaded',
                                                        'path': {'statusUrl': None}, 'token': g.token}
                unique_stations.remove(unique_station)
            else:
                for uni_key in unique_stations_dict:
                    if str(uni_key).strip().lower() == str(unique_station).split('_')[0].strip().lower():
                        unique_stations_dict[uni_key]['stations'].append(unique_station)
                        try:
                            unique_stations.remove(unique_station)
                        except Exception:
                            print(uni_key, unique_station)
                            pass
    return unique_stations_dict


def create_unique_stations_2(unique_stations):
    # unique_stations_dict = {}
    # for item in unique_stations:
    #     unique_stations_dict[unique_station] = {'stations': [unique_station], 'status': 'not_downloaded',
    #                                             'path': {'statusUrl': None}, 'token': g.token}
    return {unique_station: {'stations': [unique_station], 'status': 'not_downloaded', 'path': {'statusUrl': None},
                             'token': g.token} for unique_station in unique_stations}


if __name__ == '__main__':

    # setup; need arc online user and password, field name for unique stations, data output,

    url, user, password = 'https://techserv.maps.arcgis.com', 'jjoiner31', 'Map2021$'
    field = 'station_name'
    district = 'Abilene'
    layer_name = 'AEP Abilene Poles v2'

    data_path = '../replicas'
    output_path = '../replicas/outputs'

    t = ThreadingManager()

    # setup and login to arcgis online

    g = ArcGISManager(url, user, password)

    # search for the AEP feature server

    search_results = g.search_content(f'title: {layer_name}', item_type='Feature Service')
    for item in search_results:
        if layer_name == item.title:
            l = item

    if 'stations_check.pickle' in os.listdir():
        with open('stations_check.pickle', 'rb') as file:
            unique_stations_dict = pickle.load(file)
    #     for item in unique_stations_dict:
    #         unique_stations_dict[item]['token'] = g.token
    #         # unique_stations_dict[item]['path'] = {'statusUrl':unique_stations_dict[item]['path']}
    #         if unique_stations_dict[item]['path']['statusUrl']:
    #             unique_stations_dict[item]['path'][
    #                 'statusUrl'] = unique_stations_dict[item]['path']['statusUrl'].split('?token=')[0]
    #         unique_stations_dict[item].update({'replica_path':unique_stations_dict[item]['path']['statusUrl'].split('?token=')[0]})
    #
    # with open('stations_check.pickle', 'wb') as file:
    #     pickle.dump(unique_stations_dict, file)
    # quit()

    # create ArcGISFeature

    l_flc = ArcGISFeature(l, g.token)

    if 'stations_check.pickle' not in os.listdir():

        stations = l_flc.query_item(0, [field], result_record_count=5000)
        stations_list = list()

        for i, station in enumerate(stations):
            print(i, len(station), stations)
            stations_list.extend([item.attributes['station_name'] for item in station.features])

        unique_stations = [item for item in list(set(stations_list)) if item != None]

        unique_stations_dict = create_unique_stations_2(unique_stations)

        with open('stations_check.pickle', 'wb') as file:
            pickle.dump(unique_stations_dict, file)

    try:
        # build a where clause
        for key, value in unique_stations_dict.items():
            # states: not_downloaded,downloaded, url_created, processed
            # if the item was not downloaded or a url was created go ahead and create the where clause
            if not value['path']['statusUrl'] and not any(
                    map(lambda x: key.replace(' ', '_') in x, os.listdir(output_path))):
                print(f'generating link for {key}')
                where_clause = f"station_name = '{value['stations'][0].strip()}'".replace('\\', '')
                if len(value) > 1:
                    for item in value['stations'][1:]:
                        where_clause += f" or station_name = '{item.strip()}'"
                link = l_flc.create_replica(key, '0,2,3,4,6,7,9', data_path, where_clause, asynchronous=True)
                unique_stations_dict[key]['status'] = 'url_created'
                unique_stations_dict[key]['path'] = link
                t.create_thread(t.run_script, ['./asyn_collector.py', key],
                                kwargs={'url': link['statusUrl'],  # need to use replica_path instead fix later
                                        'data_path': data_path,
                                        'output_path': output_path,
                                        'output_name': f'AEP_{district}',
                                        'layer_name': f'AEP_{district}_Poles',
                                        'st_name': key.replace(' ', '_')}, identifier=key)

            elif not any(map(lambda x: key.replace(' ', '_') in x, os.listdir(output_path))):
                print(f'getting url for {key}')
                unique_stations_dict[key]['status'] = 'url_created'
                t.create_thread(t.run_script, ['./asyn_collector.py', key],
                                kwargs={'url': unique_stations_dict[key]['path']['statusUrl'],
                                        'data_path': data_path,
                                        'output_path': output_path,
                                        'output_name': f'AEP_{district}',
                                        'layer_name': f'AEP_{district}_Poles',
                                        'st_name': key.replace(' ', '_')}, identifier=key)

        count = 0
        for thread in t.threads:
            print(f'starting thread {thread}')
            t.threads[thread]['thread'].start()
            time.sleep(5)
            count += 1
            if count == 5:
                i = ''
                while i.lower() != 'y':
                    i = input('continue? (y/n)')
                count = 0


    except Exception as e:
        print(e)
    finally:
        with open('stations_check.pickle', 'wb') as file:
            pickle.dump(unique_stations_dict, file)

    t.join_threads()

    # grab list of current items in data folder

    # initiate create replica with async on; log into a json which is what; if there is an issue with the program we can check to see if we already have items in the output
    # and if so just skip them

    # iterate over gathered urls and download zips into folder
    # with a zip downloaded lets open it up and get the station name (geopandas or arcpy)
    # rename both gdb and zip with station name (only use STATION not STATION_B, ...

    quit()
