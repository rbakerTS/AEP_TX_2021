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
from arcpy import da

if __name__ == '__main__':
    print('\n')
    # with open('secrets.json', 'r') as file:
    #     username = json.load(file)['user']
    #     password = json.load(file)['password']
    # gis = AGO_manager('jjoiner31', 'Map2021$')
    #
    # abilene_id = '3f2557fe8a014a2280820a406a9a9704'
    # rgv_id = '827d69bc0f294ca29e3e7003801886d9'
    #
    # item = gis.content.get(abilene_id)
    # item.export(f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Deliverable\Abilene\output/ab',
    #             'File Geodatabase')
    #
    # item = gis.content.get(rgv_id)
    # item.export(f'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Deliverable\RGV\output/rgv',
    #             'File Geodatabase')
    editor = da.Editor(wkspc)
    editor.startEditing(True, True)
    editor.startOperation()
    insert = da.InsertCursor(target_fc, target_list)
    insert.insertRow(row)
    editor.stopOperation()
    editor.stopEditing(True)
