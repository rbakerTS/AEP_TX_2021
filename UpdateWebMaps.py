from ArcGIS_python_api.ArcGIS_python_api_wrapper import ArcGISManager, ArcGISFeatureManager, QueryGenerator_LFC, \
    QueryGenerator_Layer
from arcgis.mapping import WebMap
from datetime import datetime, date, time
import arcgis
import arcgis.features
from arcgis.gis import GIS, Item
from dataclasses import dataclass
from typing import List, Any
import Renderer
from AGO_Manager import AGO_manager
import json


@dataclass
class customWebMap:
    raw_wm: WebMap

    @property
    def map_title(self):
        return self.raw_wm.item.title

    @property
    def map_id(self):
        return self.raw_wm.item.id

    @property
    def basemap_title(self):
        return self.raw_wm.basemap.baseMapLayers[0].title

    @property
    def layers(self):
        return self.raw_wm.layers

    @property
    def layer_ids(self):
        layers_ids = []
        for layer in self.layers:
            layers_ids.append(layer.itemId)
        return layers_ids

    @property
    def layer_titles(self):
        layers_titles = []
        for layer in self.layers:
            layers_titles.append(layer.title)
        return layers_titles

    @property
    def layers_renderers(self):
        layers_symbols = []
        for i, layer in enumerate(self.layers):
            layers_symbols.append(layer.layerDefinition.drawingInfo.renderer)
        return layers_symbols

    @property
    def feature_count(self):
        feature_count_list = []
        for i, layer in enumerate(self.layers):
            feature_count_list.append(len(layer.featureCollection.layers[0].featureSet.features))
        return feature_count_list


class Update_WM(customWebMap):
    def get_station_name(self):
        poles_fc = ArcGISFeatureManager(poles_search)
        unique_stations = poles_fc.get_unique_items(0, 'station_name')
        survey_station_name = self.map_title.replace("AEP Abilene", "").replace(' v2', '').strip().lower().replace(' ',
                                                                                                                   '_')
        for name in unique_stations:
            if name.strip().lower().replace(' ', '_') == survey_station_name:
                self.station_name = name
                print(f"Station Name is {name}")
                break

    def remove_all_layers(self):
        x = len(self.raw_wm.layers)
        for i in range(x - 1):
            print(f"Removing {wm_custom.layer_titles[i]} layer")
            layer_to_remove = self.raw_wm.layers[0]
            self.raw_wm.remove_layer(layer_to_remove)
        print(f"Removing {wm_custom.layer_titles[0]} layer")
        layer_to_remove = self.raw_wm.layers[0]
        self.raw_wm.remove_layer(layer_to_remove)

    def add_lights(self):
        print(f"Adding {self.station_name} Lights layer")
        survey_lights = lights_search.layers[0].query(where=f"station_name='{self.station_name}'")
        # self.raw_wm.add_layer(survey_lights, {'title': f"{self.station_name} Lights", 'visibility': True,
        #                                       'renderer': Renderer.aep_tx_light_renderer})
        self.raw_wm.add_layer(survey_lights, {'title': f"{self.station_name} Lights", 'visibility': True})

    def add_poles(self):
        print(f"Adding {self.station_name} Poles layer")
        self.survey_poles = poles_search.layers[0].query(where=f"station_name='{self.station_name}'")
        self.raw_wm.add_layer(self.survey_poles, {'title': f"{self.station_name} Poles", 'visibility': True,
                                                  'renderer': Renderer.aep_tx_pole_renderer})

    def add_added_poles(self):
        pass

    # Need to gather max x, max y, min x and min y from self.survey_poles features and use that to set map extent
    # def set_extent(self):
    #     pass

    def set_basemap(self, basemap: str):
        print(f"Setting basemap to {basemap}")
        self.raw_wm.basemap = basemap

    def update_wm(self):
        # print(f"Committing changes to {map.title}")
        wm.update()


if __name__ == '__main__':
    print('\n')
    with open('secrets.json', 'r') as file:
        username = json.load(file)['username']
        password = json.load(file)['password']
    manager = AGO_manager(username, password)
    created_after = '2021-11-01'
    created_before = '2021-12-08'
    modified_after = ''
    modified_before = ''
    map_title = 'aep abilene map'
    poles_title = 'AEP Abilene Poles v2'
    added_poles_title = 'AEP_Abilene_Poles_v2_Fix_Added_Pole'

    poles_search = manager.content_search(title_search=poles_title, item_type='Feature Layer')[0]
    lights_title = 'AbileneLights'
    lights_search = manager.content_search(title_search=lights_title, item_type='Feature Layer')[0]
    basemap = 'hybrid'
    added_poles_search = manager.content_search(title_search=added_poles_title, item_type='Feature Layer')[0]

    maps = manager.content_search(created_after, created_before, modified_after, modified_before, map_title, 'Web Map')
    map_count = len(maps)
    print('\n--------------------------------------------\n')
    success = []
    fail = []
    for i, map in enumerate(maps):
        try:
            start_time = datetime.now()
            print(f'Start time: {start_time}')
            print(f'Item: {i + 1} of {map_count} - {map.title} Web Map')
            print(f'Gathering Web Map data')
            wm = WebMap(map)
            wm_custom = customWebMap(wm)
            update_wm = Update_WM(wm)
            update_wm.get_station_name()
            # update_wm.remove_all_layers()
            # update_wm.add_poles()
            # update_wm.add_lights()

            # update_wm.set_extent()
            # update_wm.set_basemap(basemap)
            update_wm.update_wm()
            print(
                f"Web Map after updates: https://techserv.maps.arcgis.com/apps/mapviewer/index.html?webmap={wm_custom.map_id}")
            for i, layer in enumerate(wm_custom.layers):
                print(f"Layer: {wm_custom.layer_titles[i]} - {wm_custom.feature_count[i]} features")
            print(f"Basemap: {wm.basemap.title}")
            end_time = datetime.now()
            print(f'End time: {end_time}')
            time_elapsed = end_time - start_time
            print(f'Time elapsed: {time_elapsed}')
            print('--------------------------------------------\n')
            success.append(map)
        except Exception as e:
            print(e)
            fail.append(map)
    print(f'Maps updated successfully: ')
    print(success)
    with open("sucess.json", 'w') as success_json:
        json.dump(success, success_json)
    print(f'Maps not updated: ')
    print(fail)
    with open("fail.json", 'w') as fail_json:
        json.dump(fail, fail_json)

    quit()
