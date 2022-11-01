from typing import Union, Dict, List
import time
import datetime

import arcgis
import arcgis.features
from arcgis.gis import GIS, Item
from arcgis.features import FeatureLayerCollection


class QueryGenerator_LFC():
    def __init__(self, l_flc: arcgis.features.FeatureLayerCollection, layer_pos: int, *args, **kwargs):
        self.offset = 0
        self.l_flc = l_flc

        self.layer_pos = layer_pos

        self.args = args
        self.kwargs = kwargs

        self.len = 0

    def __len__(self):
        return self.len

    def __iter__(self):
        return self

    def __next__(self):
        q: arcgis.features.FeatureSet = self.l_flc.layers[self.layer_pos].query(result_offset=self.offset, *self.args,
                                                                                **self.kwargs)
        if not q.features:
            raise StopIteration
        self.offset += len(q.features)
        self.len = len(q.features)
        return q

    def __repr__(self):
        return f'offset is: {self.offset}'


class QueryGenerator_Layer():
    def __init__(self, layer, *args, **kwargs):
        self.offset = 0
        self.layer = layer

        self.args = args
        self.kwargs = kwargs

        self.len = 0

    def __len__(self):
        return self.len

    def __iter__(self):
        return self

    def __next__(self):
        q: arcgis.features.FeatureSet = self.layer.query(result_offset=self.offset, *self.args,
                                                         **self.kwargs)
        if not q.features:
            raise StopIteration
        self.offset += len(q.features)
        self.len = len(q.features)
        return q

    def __repr__(self):
        return f'offset is: {self.offset}'


# todo may want to create a class ArcGISLayerManger(): since so many methods in the ArcGISFeatureManager expect a layer pos

class ArcGISFeatureManager():

    # todo may want to create an introspection method to get a bunch of info such as field names, layer names, layer pos,
    #  record counts, etc, could also set it up as an object to pick and choose parts; might be better instead of making multiple
    #  methods that each require a given layer position; for the method try to get use getattr and setattr to mass produce useful attributes
    # todo may want to make a method to add fields into the update in case we try to pass in a dataset that has too many fields
    # todo maybe make an easier way to pass the token whenever we just create an instance
    # todo may wan to create a sinlge update/delete/insert/ and a seperate batch process
    # todo may want to add in a private method for the insert that can try to capture geoms (looks for columns like lat,long, geom, geometry, etc...)

    def __init__(self, layer: arcgis.gis.Item, token: Union[None, str] = None, layer_pos: int = 0) -> None:
        self._l = layer
        self._token = token
        self.layer_pos = layer_pos

    @property
    def l_flc(self) -> arcgis.features.FeatureLayerCollection:
        return FeatureLayerCollection.fromitem(self._l)

    @property
    def capabilities(self) -> str:
        # todo consider adding more properties to return other parts of the definition that are important to us (data.g. max_record_count)
        return self.l_flc.properties.capabilities

    @property
    def url(self) -> str:
        return f'{self.l_flc.url}?token={self._token}'

    @property
    def admin_url(self) -> str:
        s = self.url.split('/')
        index = 0
        for i, item in enumerate(s):
            if item == 'rest':
                index = i + 1
                break
        s.insert(index, 'admin')
        return '/'.join(s)

    @property
    def columns_and_types(self) -> list:
        items = dict()
        for item in self._l.layers[self.layer_pos].properties.fields:

            try:
                python_type = [t for t, type_name in
                               zip([int, str, float, int, datetime.date], ['integer', 'string', 'double', 'id', 'date'])
                               if
                               type_name in item.type.lower()][0]
            except IndexError:
                python_type = 'NULL'
            items[item.name] = {'esri_type': item.type, 'python_type': python_type}

        return items

    def query_item(self, layer_pos: int = 0, out_fields: Union[list, None] = None, where: Union[str, None] = None,
                   result_record_count: Union[int, None] = None,
                   *args,
                   **kwargs) -> QueryGenerator_LFC:
        # todo consider how to specifiy the layer, either through the title or position
        # todo may want to write this in a way to overload it with different types of items; layers, tables
        # todo may want to consider if you want explicity keyword args for out_fields, where, etc; or just pass in *args/**kwargs

        kwargs.update({name: para for name, para in
                       zip(['where', 'out_fields', 'result_record_count'], [where, out_fields, result_record_count]) if
                       para})
        kwargs['return_all_records'] = False if result_record_count else True

        return QueryGenerator_LFC(self.l_flc, layer_pos, **kwargs)

    def update_items(self, data: List[Dict], field_to_join: str, field_type: str = 'string',
                     return_left_over: bool = False,
                     result_record_count: int = 1000, layer_pos: int = 0, chunk_size: int = 500):
        # todo may reduce this and have this be an outside function that uses the reduced method
        left_overs = list()
        data_join_field_pos = dict()
        offset = 0
        unique_items = list()
        for index, item in enumerate(data):
            try:
                data_join_field_pos[str(item[field_to_join])] = {'found': False, 'index': index}
                if field_type.lower() == 'string':
                    unique_items.append(f"'{item[field_to_join]}'")
                else:
                    unique_items.append(f"{item[field_to_join]}")

            except Exception as e:
                left_overs.append(item)
                print(e)

        # todo remove this and add this to _utils
        chunks = list()
        nested_chunk = list()
        for index, unique_item in enumerate(unique_items):
            nested_chunk.append(unique_item)
            if index % chunk_size == 0:
                chunks.append(nested_chunk)
                nested_chunk = list()
        chunks.append(nested_chunk)

        for chunk in chunks:
            where = f"{field_to_join} IN ({','.join(chunk)})"
            time.sleep(1)
            q = self.query_item(result_record_count=result_record_count, where=where)

            while True:
                try:
                    f = next(q)
                except StopIteration:
                    break

                updated_features = list()
                for feature in f.features:

                    j = str(feature.attributes[field_to_join])
                    print(f'on feature {offset} looking for field: {field_to_join} ({j})')
                    if any(map(lambda x: x == j, data_join_field_pos.keys())):
                        if data_join_field_pos[j]['found'] != True:
                            new_feature = data[data_join_field_pos[j]["index"]]

                            print(f'{j} was found in data_join_field_pos')
                            print(f'updating feature {offset} with {new_feature}')

                            feature.attributes.update(new_feature)

                            data_join_field_pos[j]['found'] = True
                            updated_features.append(feature)
                        else:
                            print(f'{j} was already found, skpping')
                        offset += 1
                        print('-' * 100)
                print(f'updating features: \n{"*" * 200}')
                if updated_features:
                    self.l_flc.layers[layer_pos].edit_features(updates=updated_features)

        if return_left_over:
            for _, value in data_join_field_pos.items():
                if value['found'] != True:
                    left_overs.append(data[value['index']])
            return left_overs

    def insert_items(self, data=List[Dict], geometry=List[Dict], layer_pos: int = 0) -> None:
        data = [{'attributes': d, 'geometry': g} for d, g in zip(data, geometry)]
        self.l_flc.layers[layer_pos].edit_features(adds=data)

    def insert_items2(self, data: List[Dict], geometry: List[Dict], structure: dict, layer_pos: int = 0) -> None:
        # todo allow for structure to be applied to insert items; that or create sanatization/structure functions
        updated_data = list()
        for item in data:
            s = structure.copy()
            s.update(item)
            updated_data.append(s)

        data_final = [{'attributes': d, 'geometry': g} for d, g in zip(updated_data, geometry)]
        self.l_flc.layers[layer_pos].edit_features(adds=data_final)

    def delete_items(self):
        pass

    #
    # def delete_identical_based_on_attribute(self,query_gen:QueryGenerator,attribute_name:str,layer_pos:int = 0):
    #     unique_items = list()
    #
    #     while True:
    #         try:
    #             f = next(query_gen)
    #         except StopIteration:
    #             break
    #
    #         for feature in f.features:
    #             id = feature.get_value(attribute_name)
    #             if id not in unique_items:
    #                 unique_items.append(id)
    #             else:
    #                 self.l_flc.layers[layer_pos].edit_features(deletes=str(feature.get_value('FID')))
    #
    # def delete_identical_based_on_geom(self, query_gen: QueryGenerator, distance:int = 10, layer_pos: int = 0):
    #     unique_items = list()
    #     points = list()
    #     while True:
    #         try:
    #             f = next(query_gen)
    #         except StopIteration:
    #             break
    #
    #         for feature in f.features:
    #             points.append((feature.get_value('FID'),Point(feature.geometry['x'],feature.geometry['y'])))
    #             # id = feature.get_value(attribute_name)
    #             # if id not in unique_items:
    #             #     unique_items.append(id)
    #             # else:
    #             #     self.l_flc.layers[layer_pos].edit_features(deletes=str(feature.get_value('FID')))
    #     # for point in points:
    #     #     if
    #     # pass

    def create_replica(self, name: str, layers: str, out_path: str, query: str = '', syn_model: str = 'none',
                       return_attachments: bool = True, attachments_sync_direction: str = 'bidirectional',
                       data_format: str = 'filegdb', asynchronous: bool = False, update_definition: bool = True,
                       add_token: bool = False, *args, **kwargs) -> Union[None, dict]:

        # todo raise an error if async is true and there is no token
        layer_query = query if not query else {"0": {"where": query}}

        if update_definition:
            if not self.l_flc.properties.syncEnabled:
                self.update_definition(syncEnabled='true')
            if 'Extract' not in self.capabilities:
                self.update_definition(capabilities=','.join([*self.capabilities.split(','), 'Extract']))

        r = self.l_flc.replicas.create(replica_name=name, layer_queries=layer_query, layers=layers,
                                       sync_model=syn_model, return_attachments=return_attachments,
                                       attachments_sync_direction=attachments_sync_direction, data_format=data_format,
                                       out_path=out_path, asynchronous=asynchronous, *args, **kwargs)
        if asynchronous:
            r['statusUrl'] = f'{r["statusUrl"]}?token={self._token}' if add_token else r['statusUrl']
        return r

    def list_replicas(self):
        return self.l_flc.replicas.get_list()

    def add_field(self, name: str, data_type: str, alias: str = "", length: Union[None, int] = None,
                  nullable: bool = True,
                  editable: bool = True, visible: bool = True, domain=None) -> None:

        alias = name if not alias else alias
        local_vars = locals()
        local_vars['type'] = data_type

        if not length:
            del local_vars['length']

        for item in ['self', 'data_type']:
            del local_vars[item]
        self._add_to_definition(**local_vars)

    def update_definition(self, **kwargs) -> None:
        # todo may want to add some explicit methods like update sync, capabilities, record count
        self.l_flc.manager.update_definition(kwargs)

    def _add_to_definition(self, layer_pos: int = 0, **kwargs):
        update_dict = {'fields': [kwargs]}
        self._l.layers[layer_pos].manager.add_to_definition(update_dict)


class ArcGISManager(GIS):

    # todo add feedback/ __repr__ to see if we are logged in upon creation

    def __init__(self, url: str, user: str, password: str) -> None:
        super().__init__(url, user, password)

    @property
    def token(self):
        return self._con.token

    def search_content(self, query: str, item_type: str = '', max_items: int = 10, return_first: bool = True, *args,
                       **kwargs) -> Union[List[Item], Item]:
        # todo consider a structured_search_content to help facilicate the searching based on owners, titles, etc\
        items = self.content.search(query, item_type, max_items=max_items, *args, **kwargs)
        return items[0] if return_first else items

    # def save_item(self,item:Item,path:str='') -> Any:
    #     return item.get_data()
    #
    #


if __name__ == '__main__':
    # todo move usage examples below into readme
    import json

    url = 'https://techserv.maps.arcgis.com'

    with open('../secrets.json', 'r') as file:
        p = json.load(file)
        user = p['user'].strip()
        password = p['password'].strip()

    g = ArcGISManager(url, user, password)
    l = g.search_content('title: Delta_Poles owner:jjoiner31', item_type='Feature Service')
    lfc = ArcGISFeatureManager(l, token=g.token)
    print(lfc.columns_and_types)

    quit()

    # l = g.search_content('title: test_points_10000_with_id', item_type='Feature Service')
    # l_flc = ArcGISFeatureManager(l, g.token)
    #

    # **** check out survey123 form ************************************************************************************
    # id = '8ea511fcaa5d46debcce9dc95240da62'
    # l = g.search_content(f'id:{id}', item_type='Form', return_first=False)

    # **** add additional fields to layer ******************************************************************************
    # https://gist.github.com/mpayson/471516f8a103eba05287402226473bd1
    # l_flc.add_field('fav_shoe', 'esriFieldTypeString',)

    # **** delete attribute based on same identical geo ******************************************************************************

    # **** delete attribute based on same identical geo ******************************************************************************
    # l = g.search_content('title: test_points_10000_with_id', item_type='Feature Service')[0]
    # l_flc = ArcGISFeatureManager(l, g.token)
    # d = [{'color': 'orange','long':-97.1133966314274,'lat':30.1162104383905,'uuid':'1572f116-d2b4-457a-ba74-dc14ef0aff2a'}]
    # g = [{'x': -97.1133966314274, 'y': 30.1162104383905}]
    # l_flc.insert_items(data=d, geometry=g)
    # q = l_flc.query_item(result_record_count=1000)
    # l_flc.delete_identical_based_on_geom(distance=2,query_gen=q)

    # **** delete attribute based on same identical field ******************************************************************************
    # l = g.search_content('title: test_points_10000_with_id', item_type='Feature Service')[0]
    # l_flc = ArcGISFeatureManager(l, g.token)
    # d = [{'color': 'orange','long':-97.1133966314274,'lat':30.1162104383905,'uuid':'1572f116-d2b4-457a-ba74-dc14ef0aff2a'}]
    # g = [{'x': -97.1133966314274, 'y': 30.1162104383905}]
    # l_flc.insert_items(data=d, geometry=g)
    # # q = l_flc.query_item(result_record_count=1000)
    # # l_flc.delete_identical('uuid',q)

    # quit()

    # # **** get unique values from field ******************************************************************************
    # l = g.search_content('title: test_points_10000', item_type='Feature Service')[0]
    # l_flc = ArcGISFeatureManager(l, g.token)
    # unique = l_flc.get_unique_items(layer_pos=0, field='color')
    # quit()

    # **** inserting attributes **************************************************************************************
    # l = g.search_content('title: test_points_10000', item_type='Feature Service')[0]
    # l_flc = ArcGISFeatureManager(l, g.token)
    #
    # d = [{'color': 'orange'}]
    # g = [{'x': 100, 'y': 100}]
    # l_flc.insert_items(data=d, geometry=g)
    # quit()

    # **** updating attributes **************************************************************************************
    # l = g.search_content('title: test_points_10000', item_type='Feature Service')[0]
    # l_flc = ArcGISFeature(l, g.token)
    # q = l_flc.query_item(result_record_count=1000)
    #
    # f = next(q)
    # d = [{'FID': 2, 'color': 'orange'}, {'FID': 10005, 'color': 'purple'}]
    # r = l_flc.update_items_with_data('FID', data=d, query_gen=q, return_found_items=True)
    # quit()

# **** Listing replicas **************************************************************************************
# l = g.search_content('title: test_points_10000', item_type='Feature Service')[0]
# l_flc = ArcGISFeature(l, g.token)
# r = l_flc.list_replicas()
# quit()
# **** ARCGISWRapper Examples **************************************************************************************
# seach example
# l = g.search_content(query='title: Texas owner: kmcnew_TechServ', item_type='Feature Service')

# feature class example***********************************************************
# item = g.search_content(query='title: test_points owner:kmcnew_TechServ', item_type='Feature Service')[0]
# l = ArcGISFeature(item, g.token)
# f = l.get_fields(0)
# r = l.create_replica('test', '0', './data', asynchronous=True)
# results = l.query_item(0, result_record_count=10)

# create_replica example and downloading the zip**********************************************************
# item = g.search_content(query='title: test_points owner:kmcnew_TechServ', item_type='Feature Service')[0]
# l_flc = ArcGISFeature(item, g.token)
# r = l_flc.create_replica('test','0','./data',asynchronous=True)
# # getting zip link
# r = requests.get(
#     'https://services8.arcgis.com/gChEJJDXE5xyPrQH/arcgis/rest/services/test_points_10000/FeatureServer/jobs/27d2cdbd-a4d7-48b6-b934-4178bbfe0b59?token=NTEbCo1ZYiFZZ2mZ7HDmpzEmXyBKRrFI1TuTHznSi3W7DhmNKb6BtBnHsipkMwmBxy0FdGuBzNEJeR_UnRIBf6ssszAsG-YeJSOpxyWOA2cFbNhd_c_WLzkeSWU732oOmh5y32b2ULavv26lU-NOv-fMjHTr4LO-4nMJOkzLqJI.')
# zip_link = re.findall(r'>https.*\.zip', str(r.content))
# link = f'{zip_link[0].strip().replace(">", "")}?token={g.token}'
# r2 = requests.get(link, stream=True)
#
# # z = zipfile.ZipFile(io.BytesIO(r2.content))
# # z.extractall('./data/test.zip')
# with open('./data/test.zip', 'wb') as file:
#     for chunk in r2.iter_content(chunk_size=128):
#         file.write(chunk)
# query example ***********************************************************
# l = g.search_content('title: test_points_10000 owner:kmcnew_TechServ', item_type='Feature Service')
# l_flc = ArcGISFeature(l[0],g.token)
# r = l_flc.query_item(0,result_record_count=700)
#
# import pandas as pd
# import geopandas as gpd
# from shapely.geometry import Point
#
# for index,item in enumerate(r):
#     s = item
#
#     geos = [Point(item.long,item.lat) for item in s.sdf.itertuples()]
#     gdf = gpd.GeoDataFrame(geometry=geos)
#     gdf.crs = {'init':'epsg:4326'}
#     gdf.to_file(f'./data/test_item_{index}.shp')
#
#     print(index,len(s.features),r)
# quit()

# **** Other Examples **********************************************************************************************
# l = g.search_content('title: test_points_10000 owner:kmcnew_TechServ', item_type='Feature Service')
#
# # example of making a change to the definition
# l_flc = FeatureLayerCollection.fromitem(l[0])
# # l_flc.manager.update_definition({'maxRecordCount':30000})
#
# # example of getting some basic polygons of countrys/counties
# us = Country.get('US')
# hidalgo = us.subgeographies.states['Texas'].counties['Hidalgo_County']
#
# # example of settingu up a spatial reference for querying (when query its like utilizing a spatial join)
# f = intersects(hidalgo.geometry, sr=hidalgo.geometry['spatialReference'])
#
# # with open('./data/last_item','r') as file:
# #     last_item = int(file.read())
#
# # below is an example of getting all the records utilizing a iteration
# # note you will get empty lists when it does not find anything so make a check to see if we are done query or not
# r = l_flc.layers[0].query(result_record_count=3000, return_all_records=False, result_offset=1000, out_sr=4326)
#
# master_q = list()
# for x in range(200):
#     print(x)
#     try:
#         master_q.append(
#             g.query_item(l_flc, result_record_count=3000, result_offset=x * 2000, geometry_filter=f, out_sr=4326))
#     except Exception as data:
#         print(data)
#         with open('./data/last_item', 'w') as file:
#             file.write(x)
#         break
# with open('../../Vexus/data/texas_sample.pickle', 'wb') as file:
#     pickle.dump(master_q, file)
# quit()

# replica example ******************************************************************
# l = g.search_content(query='title: AEP_RGV_Poles', item_type='Feature Service')
# l_flc = FeatureLayerCollection.fromitem(l[0])
# if not l_flc.properties.syncEnabled:
#     l_flc.manager.update_definition(
#         {'syncEnabled': 'true'})  # here we check to see if sync is Enabled and set it to True if it is not
# print(
#     l_flc.properties.capabilities)  # we can see all the capabilities here; I think you can change them here or the admin link in arconline
# # note you will need both sync and extract to create a replica
# # import string
# # for char in string.ascii_uppercase[:7]

# layer_query = {"0": {
#     "where": "station_name = 'ELSA' OR station_name = 'ELSA_B' OR station_name = 'ELSA_C' OR station_name = 'ELSA_D' OR station_name = 'ELSA_E' OR station_name = 'ELSA_F' OR station_name = 'ELSA_G'"}}
# token = g._con.token
# replica = l_flc.replicas.create(replica_name='test_replica5',
#                                 layer_queries=layer_query,
#                                 layers='0,1,2,3,4',
#                                 sync_model='none',
#                                 data_format='filegdb',
#                                 out_path='./data',
#                                 asynchronous=True)  # may need a token added to the url to work :(
#
# quit()

# temp *********************************************************************************
