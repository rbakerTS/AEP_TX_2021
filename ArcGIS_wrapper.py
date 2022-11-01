from typing import Union

import arcgis
import arcgis.features
from arcgis.gis import GIS
from arcgis.geoenrichment import Country
from arcgis.geometry.filters import intersects
from arcgis.features import FeatureLayerCollection


# todo add functionality to add fields and insert data into them

def add_fields_and_data():
    pass


class QueryGenerator():
    def __init__(self, l_flc: arcgis.features.FeatureLayerCollection, layer_pos: int, *args, **kwargs):
        self.offset = 0
        self.l_flc = l_flc

        self.layer_pos = layer_pos

        self.args = args
        self.kwargs = kwargs

    def __iter__(self):
        return self

    def __next__(self):
        q: arcgis.features.FeatureSet = self.l_flc.layers[self.layer_pos].query(result_offset=self.offset, *self.args,
                                                                                **self.kwargs)
        if not q.features:
            raise StopIteration
        self.offset += len(q.features)
        return q

    def __repr__(self):
        return f'offset is: {self.offset}'


class ArcGISFeature():

    # todo may want to create an introspection method to get a bunch of info such as field names, layer names, layer pos,
    #  record counts, etc, could also set it up as an object to pick and choose parts; might be better instead of making multiple
    #  methods that each require a given layer position

    # todo maybe make an easier way to pass the token whenever we just create an instance

    def __init__(self, layer: arcgis.gis.Item, token: Union[None, str] = None) -> None:
        self._l = layer
        self._token = token

    @property
    def l_flc(self) -> arcgis.features.FeatureLayerCollection:
        return FeatureLayerCollection.fromitem(self._l)

    @property
    def capabilities(self) -> str:
        # todo consider adding more properties to return other parts of the definition that are important to us (e.g. max_record_count)
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

    def query_item(self, layer_pos: int = 0, out_fields: Union[list, None] = None, where: Union[str, None] = None,
                   result_record_count: Union[int, None] = None,
                   *args,
                   **kwargs) -> arcgis.features.FeatureSet:
        # todo consider how to specifiy the layer, either through the title or position
        # todo may want to write this in a way to overload it with different types of items; layers, tables
        # todo may want to consider if you want explicity keyword args for out_fields, where, etc; or just pass in *args/**kwargs

        kwargs.update({name: para for name, para in
                       zip(['where', 'out_fields', 'result_record_count'], [where, out_fields, result_record_count]) if
                       para})
        kwargs['return_all_records'] = False if result_record_count else True

        return QueryGenerator(self.l_flc, layer_pos, **kwargs)
        # return self.l_flc.layers[layer_pos].query(*args, **kwargs)

    def update_definition(self, **kwargs) -> None:
        # todo may want to add some explicit methods like update sync, capabilities, record count
        self.l_flc.manager.update_definition(kwargs)

    def create_replica(self,
                       name: str,
                       layers: str,
                       out_path: str,
                       query: str = '',
                       syn_model: str = 'none',
                       return_attachments:bool = True,
                       attachments_sync_direction:str = 'bidirectional',
                       data_format: str = 'filegdb',
                       asynchronous: bool = False,
                       update_definition: bool = True,
                       add_token:bool = False,
                       *args,
                       **kwargs) -> Union[None, dict]:

        # todo raise an error if async is true and there is no token

        layer_query = query if not query else {"0": {"where": query}}

        if update_definition:
            if not self.l_flc.properties.syncEnabled:
                self.update_definition(syncEnabled='true')
            if 'Extract' not in self.capabilities:
                self.update_definition(capabilities=','.join([*self.capabilities.split(','), 'Extract']))

        r = self.l_flc.replicas.create(replica_name=name,
                                       layer_queries=layer_query,
                                       layers=layers,
                                       sync_model=syn_model,
                                       return_attachments=return_attachments,
                                       attachments_sync_direction = attachments_sync_direction,
                                       data_format=data_format,
                                       out_path=out_path,
                                       asynchronous=asynchronous, *args, **kwargs)
        if asynchronous:
            r['statusUrl'] = f'{r["statusUrl"]}?token={self._token}' if add_token else r['statusUrl']
        return r


class ArcGISManager(GIS):

    def __init__(self, url: str, user: str, password: str) -> None:
        super().__init__(url, user, password)

    @property
    def token(self):
        return self._con.token

    def search_content(self, query: str, item_type: str = '', max_items: int = 10, *args, **kwargs):
        # todo consider a structured_search_content to help facilicate the searching based on owners, titles, etc\
        return self.content.search(query, item_type, max_items, *args, **kwargs)
