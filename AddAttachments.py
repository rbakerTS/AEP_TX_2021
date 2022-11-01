from __future__ import annotations

import json
import os
from dataclasses import dataclass
from time import sleep

from arcgis.features import Table

from AGO_Manager import AGO_manager


def GIS():
    with open('secrets.json') as file:
        x = json.load(file)
    username = x['user']
    password = x['password']
    return AGO_manager(username, password)


@dataclass
class customTable:
    custom_table: Table

    @property
    def table_name(self):
        return self.custom_table.properties.name

    @property
    def table_url(self):
        return self.custom_table.url

    @property
    def table_attachments(self):
        return self.custom_table.attachments

    @property
    def table_relationships(self):
        return self.custom_table.properties.relationships

    @property
    def rows_set(self, query: str | None = None):
        return self.custom_table.query()

    @property
    def rows_list(self, query: str | None = None):
        return [x['attributes'] for x in self.custom_table.query().value['features']]


class UpdateTable(customTable):
    def list_attachments(self, objectid: int):
        try:
            return self.custom_table.attachments.get_list(objectid)
        except:
            print(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!\nFailed')
    def add_attachments(self, s, objectid: int, attachment_path: str):
        try:
            return self.custom_table.attachments.add(objectid, attachment_path)
        except:
            print(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!\nFailed')

if __name__ == '__main__':
    gis = GIS()

    '''AEP RGV'''
    layer_id = '827d69bc0f294ca29e3e7003801886d9'
    tables = gis.content.get(layer_id).tables
    tables_dict = {}
    for table in tables:
        t = UpdateTable(table)
        tables_dict[t.table_name] = t.rows_list
    # root_path = r'Y:\AEP_RGV_Poles'
    # root_files = os.listdir(root_path)
    # attachments_dict = {}
    # count = 0
    # for root_file in root_files:
    #     if '.' not in root_file:
    #         folder_files = os.listdir(f'{root_path}/{root_file}')
    #         for folder_file in folder_files:
    #             pole_attachments_dict = {}
    #             if '.' not in folder_file:
    #                 pole_attachments_dict['Table_Name'] = root_file
    #                 pole_attachments_dict['GlobalID'] = folder_file
    #                 attachment_files = {
    #                     x: {'Attachment_Name': x, 'Attachment_Path': f'{root_path}/{root_file}/{folder_file}/{x}'} for x
    #                     in os.listdir(f'{root_path}/{root_file}/{folder_file}')}
    #                 pole_attachments_dict['Attachments'] = attachment_files
    #                 attachments_dict[folder_file] = pole_attachments_dict
    #                 count += 1
    #                 print(f'{count}')
    # with open('RGV_Images.json', 'w') as file:
    #     json.dump(attachments_dict, file)

    with open(r'C:\Users\TechServPC\PycharmProjects\AEP-TX\RGV_Images.json', 'r') as file:
        attachments_dict = json.load(file)
    count = 0
    for table in tables:
        t = UpdateTable(table)
        print(f'-------------------------------\nTable: {t.table_name}')
        for row in t.rows_list:
            oid = row['OBJECTID']
            gid = row['GlobalID']
            print(f'OID: {oid}')
            att_list = [x['name'] for x in t.list_attachments(oid)]
            try:
                attachments = attachments_dict[gid]
            except:
                print(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!\nFailed')
                continue
            for k, v in attachments['Attachments'].items():
                name = v['Attachment_Name']
                if name not in att_list:
                    print(f'Attaching {name}')
                    s = 1
                    t.add_attachments(s, oid, v['Attachment_Path'])

                else:
                    print(f'{name} already attached')
                count += 1
                print(count)
