import pickle
import shutil
import arcpy

import csv
from typing import List, Tuple, Dict, Type, Union
import json

import os
import sys

from typing import Callable, Union
from threading import Thread
from dataclasses import dataclass
import time

import uuid


# todo: limit you can add to run  so many threads at once
class ThreadingManager:
    def __init__(self):
        self.threads = dict()

    def create_thread(self, f: Callable, args: Union[None, list] = None, kwargs: Union[None, dict] = None,
                      identifier: str = ''):
        identifier if identifier else uuid.uuid4()
        self.threads[identifier] = {'thread': Thread(target=f, args=args if args else []), 'started': False,
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

    def check_if_alive(self, item):
        return self.threads[item]['thread'].is_alive()

    def __getitem__(self, key):
        return self.threads[key]


@dataclass
class Shape():
    x: float
    y: float
    srid: int


class PoleRow:
    def __init__(self, **kwargs):
        self._create_attributes(**kwargs)

    def _create_attributes(self, **kwargs):
        for key, value in kwargs.items():
            if key.lower() == 'shape':
                shape = json.loads(value.replace("'", '"'))
                setattr(self, key, Shape(shape['x'], shape['y'], shape['spatialReference']['wkid']))
            else:
                setattr(self, key, value)


class Row:
    def __init__(self, **kwargs):
        self._create_attributes(**kwargs)

    def _create_attributes(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class DataContainer:
    def __init__(self, rows):
        self._rows = rows
        self._columns = None
        self._column_pos = 0
        self._data = self._format_data()

    @property
    def columns(self) -> List[str]:
        if not self._columns:
            self._columns = self._rows[self._column_pos]
        return self._columns

    @property
    def data(self):
        return self._data

    def _format_data(self) -> List[Row]:
        temp_rows = self._rows.copy()
        columns = self._rows[self._column_pos]
        temp_rows.pop(self._column_pos)

        self._data = self._parse_rows(columns, temp_rows)

        return self._data

    def _parse_rows(self, columns, rows):
        return [Row(**{k: v for k, v in zip(columns, row)}) for row in rows]

    def __getitem__(self, item: int) -> Row:
        return self._data[item]

    def __len__(self):
        return len(self._data)


class PoleDataContainer(DataContainer):
    def __init__(self, rows, filter: bool = True, filter_column: str = 'station_name', filter_value: str = ''):
        super().__init__(rows)
        self._filter = filter
        self._filter_column = filter_column
        self._filter_value = filter_value
        if self._filter == True:
            self._filter_pos = self._find_pos()
            self._rows = self._filter_rows()
            self._data = self._format_data()
        else:
            self._data = self._format_data()

    def _find_pos(self):
        if not self._columns:
            self._columns = self._rows[self._column_pos]
        for i, column in enumerate(self._columns):
            if self._filter_column == column:
                return i

    def _filter_rows(self):
        f_rows = []
        for row in self._rows:
            if row[self._filter_pos] == self._filter_value:
                    f_rows.append(row)
        return f_rows

    def _parse_rows(self, columns, rows):
        return [PoleRow(**{k: v for k, v in zip(columns, row)}) for row in rows]


def read_csv(csv_name: str) -> List[list]:
    with open(csv_name, 'r') as file:
        reader = list(csv.reader(file))
    return reader


def arcpy_create_insert_cursor(feature_or_table_path: str, fields: List[str]):
    return arcpy.da.InsertCursor(feature_or_table_path, fields)


class Progress:
    def __init__(self, title, total):
        self._title = title
        self._total = total
        self._current = 0

    def update(self, current):
        self._current = current

    def add(self):
        self._current += 1

    def __repr__(self):
        return f'{self._title}: {self._current}/{self._total}'

    def __str__(self):
        return f'{self._title}: {self._current}/{self._total}'


class Printer:
    def __init__(self, progresses):
        self.progresses = progresses

    def __repr__(self):
        return '\t'.join([str(progress) for progress in self.progresses])


def printer_job(printer: Printer):
    flag = True
    while flag:
        progresses = printer.progresses
        flag = False

        for progress in progresses:
            # if progress._current != progress._total:
            if progress._current != 0:
                flag = True
        sys.stdout.write(f'\r{printer}')
        sys.stdout.flush()
        time.sleep(1)


if __name__ == '__main__':

    t = ThreadingManager()

    # Input variables **************************************************************************************************
    poles_name = 'AEP_RGV_Poles'
    sub_name = 'PHARR'
    root = r'Y:\AEP_RGV_Poles'
    attachment_columns = ['GlobalID', 'path']

    # Generated variables **********************************************************************************************
    gdb_name = poles_name
    gdb_file_name = f'{gdb_name}_{sub_name}.gdb'
    gdb_path = os.path.join(root, gdb_file_name)
    arcpy.env.workspace = gdb_path
    file_list = os.listdir(root)

    print(f'Generating FGBDs from {poles_name}')
    # Reading archived CSVs ********************************************************************************************
    print('Reading archived CSVs')
    pole_file_path = f'{root}/{poles_name}.csv'
    image_relationship_file_names = [x for x in os.listdir(root) if "Image_" in x and ".csv" not in x]
    images_relationship_data = [read_csv(os.path.join(root, f'{image_csv}.csv')) for image_csv in
                                image_relationship_file_names]

    if f'{poles_name}.pickle' not in file_list:
        print(f"Reading pole table")
        pole_csv_data = read_csv(pole_file_path)
        with open(f'{poles_name}.pickle', 'wb') as file:
            pickle.dump(pole_csv_data, file)
    else:
        print(f"Pole table already read")
        with open(f'{poles_name}.pickle', 'rb') as file:
            pole_csv_data = pickle.load(file)

    data = PoleDataContainer(pole_csv_data, filter_value=sub_name)
    gid_list = []
    for row in data:
        ...

    for related_table, name in zip(images_relationship_data, image_relationship_file_names):
        if f'{name}_Attachment.csv' not in file_list:
            print(f"Reading {name} table")
            attachment_rows = []

            image_relationship_data = DataContainer(related_table)
            image_name_dir = os.path.join(root, name)

            # threading setup

            workers = 3

            rows = list(image_relationship_data)
            rows_pool = rows

            thread_names = [f'thread{x}' for x in range(workers)]
            progress = Progress('total', len(rows_pool))

            printer = Printer([progress])


            def get_dir2(rows_pool, image_name_dir, attachment_rows, progress):
                while rows_pool:
                    row = rows_pool.pop()
                    progress.update(len(rows_pool))
                    if row.GUID:
                        global_id = row.GlobalID
                        image_specific_dir = os.path.join(image_name_dir, global_id)
                        try:
                            global_id_and_image_paths = [os.path.join(global_id, image) for image in
                                                         os.listdir(image_specific_dir)]
                        except:
                            global_id_and_image_paths = []
                        attachment_rows.append(global_id_and_image_paths)


            for t_name in thread_names:
                t.create_thread(get_dir2, [rows_pool, image_name_dir, attachment_rows, progress], identifier=t_name)
                t.start_thread(t_name)

            t.create_thread(printer_job, args=[printer], identifier='printer_thread')
            t.start_thread('printer_thread')

            t.join_threads()

            print(f'Creating attachment csv for {name}')
            with open(f'{root}/{name}_Attachment.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(attachment_columns)
                for row in attachment_rows:
                    for image in row:
                        global_id, image_path = image.split('\\')
                        temp_row = [global_id, os.path.normpath(image)]
                        writer.writerow(temp_row)
        else:
            print(f"{name} table already read")

    # # gdb stuff ******************************************************************************************************
    # Feature Class ****************************************************************************************************

    layers = []
    if gdb_file_name not in os.listdir(root):
        print(f'Creating {gdb_path}')
        arcpy.management.CreateFileGDB(root, f'{gdb_name}_{sub_name}')
    else:
        print(f'Truncating data in {gdb_path}')
        for dirpath, dirnames, filenames in arcpy.da.Walk(gdb_path):
            for filename in filenames:
                layers.append(os.path.join(dirpath, filename))

        for layer in layers:
            if 'rel' not in layer.lower() and 'Image__A' not in layer:
                arcpy.TruncateTable_management(layer)
        layers = [x.split('\\')[-1] for x in layers]

    point_feature_path = os.path.join(gdb_path, poles_name)
    if poles_name not in layers:
        print(f'\nCreating {poles_name}')
        arcpy.CreateFeatureclass_management(gdb_path, poles_name, 'POINT', )
        arcpy.management.DefineProjection(point_feature_path, arcpy.SpatialReference(4326))

    try:
        poles_fields_list = [x.name.lower() for x in arcpy.ListFields(f'{gdb_path}/{poles_name}')]
    except:
        poles_fields_list = []

    print(f'Building schema for {poles_name}')
    for column in data.columns:
        if column.lower() in poles_fields_list:
            print(f'{column} already in {poles_name}')
        elif 'objectid' in column.lower() or 'shape' in column.lower():
            continue
        else:
            print(f'Adding {column} to {poles_name}')
            arcpy.management.AddField(point_feature_path, column, 'TEXT')

    fields_to_insert_into = ['SHAPE@XY', *[column for column in data.columns if column.lower() != 'shape']]
    point_feature_cursor = arcpy_create_insert_cursor(point_feature_path, fields_to_insert_into)

    print(f'Inserting rows into {poles_name}')
    with point_feature_cursor as cursor:
        for row in data:
            shape = row.SHAPE
            point = arcpy.PointGeometry(arcpy.Point(shape.x, shape.y), arcpy.SpatialReference(shape.srid))
            cursor.insertRow((point, *list({k: v for k, v in row.__dict__.items() if k.lower() != 'shape'}.values())))

    # Image Relate Tables **********************************************************************************************

    for related_table, name in zip(images_relationship_data, image_relationship_file_names):
        # todo add query
        image_relationship_data = DataContainer(related_table)
        table_name = name.split('.')[0]
        table_path = os.path.join(gdb_path, table_name)

        if table_name not in layers:
            print(f'Creating {table_name}')
            arcpy.management.CreateTable(gdb_path, table_name)

        table_cursor = arcpy_create_insert_cursor(table_path, image_relationship_data.columns)

        try:
            table_fields_list = [x.name.lower() for x in arcpy.ListFields(f'{gdb_path}/{table_name}')]
        except:
            table_fields_list = []

        print(f'Building schema for {table_name}')
        for column in image_relationship_data.columns:
            if column.lower() in table_fields_list:
                print(f'{column} already in {table_name}')
            elif column.lower() != 'objectid':
                print(f'Adding {column} to {table_name}')
                arcpy.management.AddField(table_path, column, 'TEXT')

        print(f'Inserting rows into {table_name}')
        editor = arcpy.da.Editor(gdb_path)
        editor.startEditing(False, False)
        with table_cursor as cursor:
            for row in image_relationship_data:
                values = {k: v for k, v in row.__dict__.items()}.values()

                cursor.insertRow([*list(values)])
        editor.stopEditing(True)

        if f'{table_name}_relate' not in layers:
            print(f'Creating relationship class {table_name}_relate')
            arcpy.management.CreateRelationshipClass(poles_name, table_name, f'{table_name}_relate', 'SIMPLE',
                                                     'attributes from test table', 'attributes from point feature',
                                                     'NONE', 'ONE_TO_MANY', 'NONE', 'GlobalID', 'GUID')

        # Attachments ************************************************************************************************
        arcpy.EnableAttachments_management(table_path)
        in_match_join_field = in_join_field = 'GlobalID'
        in_match_table = f'{root}/{table_name}_Attachment.csv'
        in_match_path_field = 'path'
        in_working_folder = os.path.join(root, table_name)

        arcpy.management.AddAttachments(
            table_path,
            in_join_field,
            in_match_table,
            in_match_join_field,
            in_match_path_field,
            in_working_folder)
