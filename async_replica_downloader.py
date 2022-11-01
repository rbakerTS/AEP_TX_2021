import io
import shutil
import os
import requests
import time
import asyncio
import aiohttp
import aiofiles
import random
import contextvars
import functools
import pickle
import re

from ArcGIS_wrapper import ArcGISFeature, ArcGISManager

async def download_file(token:str, url: str, output_path: str,q:asyncio.Queue) -> None:
    r = requests.get(f'{url}?token={token}')

    zip_link = re.findall(r'>https.*\.zip', str(r.content))
    while True:
        try:
            link = f'{zip_link[0].strip().replace(">", "")}?token={token}'
            print(f'link: {link}')
            break
        except Exception as e:
            print(f'{e}\nretrying in {3} seconds\n{"*" * 200}')
            time.sleep(3)
            continue

    file = zip_link[0].replace('\\', '/').split('/')[-1]

    async with aiohttp.ClientSession() as session:
        async with session.get(link) as resp:
            if resp.status==200:
                f = await resp.read()
        async with aiofiles.open(os.path.join(output_path,file),'wb') as file:
            await file.write(f)


async def main(stations_dict:dict,token,data_path):

    q = asyncio.Queue() # may need this but I don't think so

    downloaders = [asyncio.create_task(
        download_file(token,stations_dict[key]['path']['statusUrl'],data_path,q)) for key in list(stations_dict.keys())[:3]
        if not any(map(lambda x: key.replace(' ', '_') in x, os.listdir('./data')))
    ]
    await asyncio.gather(*downloaders)


if __name__ == '__main__':

    url, user, password = 'https://techserv.maps.arcgis.com', "jjoiner31", "Map2021$"
    field = 'station_name'
    save_file = 'stations_check.pickle'

    data_path = './data'
    output_path = './data/outputs_12_10_2021'
    title = 'AEP Abilene Poles'

    g = ArcGISManager(url, user, password)

    l = g.search_content(f'title: {title}', item_type='Feature Service')[0]

    l_flc = ArcGISFeature(l, g.token)

    if save_file in os.listdir():
        with open(save_file, 'rb') as file:
            unique_stations_dict = pickle.load(file)
    else:

        stations = l_flc.query_item(0, [field], result_record_count=1000)
        stations_list = list()

        for i, station in enumerate(stations):
                print(i, len(station), stations)
                stations_list.extend([item.attributes['station_name'] for item in station.features])


        unique_stations = list(set(stations_list))

        unique_stations_dict = dict()

        for unique_station in unique_stations:
            unique_stations_dict[unique_station] = {'stations': [unique_station], 'status': 'not_downloaded',
                                                    'path': {'statusUrl': None}
                                                    }

            with open(save_file, 'wb') as file:
                pickle.dump(unique_stations_dict, file)


    try:
        # build a where clause
        for key, value in unique_stations_dict.items():
            try:
                if not value['path']['statusUrl'] and not any(
                        map(lambda x: key.replace(' ', '_') in x, os.listdir('./data'))):
                    print(f'getting link for {key}')
                    where_clause = f"station_name = '{value['stations'][0].strip()}'".replace('\\', '')
                    if len(value) > 1:
                        for item in value['stations'][1:]:
                            where_clause += f" or station_name = '{item.strip()}'"

                    while True:
                        try:
                            link = l_flc.create_replica(key, '0,1,2,3,4', data_path, where_clause, asynchronous=True)
                            break
                        except Exception as e:
                            print(e,'sleeping for 5 secs and trying again')
                            time.sleep(5)
                    unique_stations_dict[key]['status'] = 'url_created'
                    unique_stations_dict[key]['path'] = link
                else:
                    print(f'already have a link or file generated for {key}')
            except Exception as e:
                print(e)

        asyncio.run(main(unique_stations_dict,g.token,'./data'))
    finally:
        with open(save_file, 'wb') as file:
            pickle.dump(unique_stations_dict, file)
