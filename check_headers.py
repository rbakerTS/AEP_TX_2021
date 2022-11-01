# from excelutilities import get_excel_headers
import os
import pathlib

import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join, split, splitext
import time
from datetime import date, datetime, timedelta


class TableCompare:
    def __init__(self, expected_headers: list, sql_engine: str = '', sql_db: str = '', sql_table: str = '',
                 expected_table: str = '', data_folder: str = '', export_folder: str = '', days_ago: int = 1,
                 fix: bool = False):
        self.fix = fix

        if expected_headers is None:
            expected_headers = list()
        self.days_ago_dt = datetime.now() - timedelta(days=days_ago)

        print('Locating expected data')
        sql_engine = sql_engine
        sql_db = sql_db
        sql_from = f'[{sql_db}].[dbo].[{sql_table}]'
        expected_table = expected_table
        expected_file_folder = os.path.split(expected_table)[0]
        expected_file_name = os.path.split(expected_table)[1]
        expected_file_type = os.path.splitext(expected_file_name)[1]
        expected_headers = expected_headers
        if sql_engine != '' or sql_db != '' or sql_from != '':
            try:
                self.expected = pd.read_sql(f"SELECT TOP (10000) * FROM {sql_from}", con=sql_engine)
            except Exception as e:
                print('Could not read sql table.')
                print(e)
                quit()
        elif expected_table != '':
            try:
                self.expected = pd.read_csv(expected_table)
            except:
                try:
                    self.expected = pd.read_excel(expected_table)
                except Exception as e:
                    print('Could not locate or read expected table.')
                    print(e)
                    quit()
        elif expected_headers != None:
            self.expected_headers = expected_headers
        else:
            print('Could not load expected table and no expected headers given.')
            quit()
        print('Expected data loaded')

        print('Locating data to be checked')
        self.data_folder = data_folder
        print(f'Checking {self.data_folder} for files')
        data_list = [f for f in listdir(self.data_folder) if isfile(join(self.data_folder, f))]
        if len(data_list) == 0:
            print(f'No files located in {self.data_folder}')
            quit()
        data_dict = {}
        for file in data_list:
            file_date = datetime.fromtimestamp(pathlib.Path('/'.join([self.data_folder, file])).stat().st_mtime)
            data_dict[file] = file_date
        self.data_list_filtered = []
        for k, v in data_dict.items():
            if v > self.days_ago_dt:
                self.data_list_filtered.append(k)
        if len(self.data_list_filtered) > 0:
            print('Files to check:')
            print(self.data_list_filtered)
        else:
            print('No files found matching filter.')

        if export_folder != '':
            print('Setting export folder')
            self.export_folder = export_folder
            print(f'Export folder set to {self.export_folder}')

    def read_file(self, data_file):
        print(f'Reading in {data_file}')
        file_path = "/".join([self.data_folder, data_file])
        file_type = os.path.splitext(file_path)[1]
        data_df = None
        if file_type == '.csv':
            data_df = pd.read_csv(file_path, index=False)
        elif file_type == '.xlsx' or file_type == '.xls':
            data_df = pd.read_excel(file_path, index=False)
        return data_df

    def check_headers(self):
        if self.expected_headers == None:
            self.expected_headers = list(self.expected.columns)

        for i, file in enumerate(self.data_list_filtered):
            print(
                f'{i + 1} of {len(self.data_list_filtered)}: Checking to see if {file} headers match expected headers')
            file_df = self.read_file(file)
            data_headers = list(file_df.columns)
            for i, item in enumerate(data_headers):
                if item not in self.expected_headers:
                    print(f'{file_df.columns[i]} != {self.expected.columns[i]}')
                    if self.fix == True:
                        file_df.rename(columns={file_df.columns[i]: self.expected.columns[i]}, inplace=True)
                        print(f'--> {file_df.columns[i]}')
            if self.fix == True:
                self.export_csv(file_df, file)
            print(f'Finished checking {file} headers')
            print()

    def check_data_types(self):
        for i, file in enumerate(self.data_list_filtered):
            print(
                f'{i + 1} of {len(self.data_list_filtered)}: Checking to see if {file} data types match expected data types')

            # expected_headers_types = {header: set(list(expected[header].apply(lambda x: type(x)))) for header in
            #                           expected_headers}
            # expected_headers_types_list = list(expected_headers_types.items())
            # data_headers_types = {header: set(list(data[header].apply(lambda x: type(x)))) for header in data_headers}
            # data_headers_types_list = list(data_headers_types.items())
        #     print('checking to see if data headers types does not have expected headers types')
        #     for i, item in enumerate(data_headers_types_list):
        #         if item not in expected_headers_types_list:
        #             # print(f'{data.columns[i]}: {data[data.columns[i]].dtype} != {expected[data.columns[i]].dtype}')
        #             data[data.columns[i]] = data[data.columns[i]].astype(expected[data.columns[i]].dtype)
        #             # print(f'--> {data[data.columns[i]].dtype}')
        # #     print(f'Exporting corrected {k} to {export_folder}')
        # #     data = data.rename(columns={'Light Count_1': 'Light Count'})
        # #     data.to_csv('/'.join([export_folder, k]), index=False)
        # # else:
        # #     print(f'No need to correct {k}')
        pass

    def export_csv(self, data_df, data_name):
        print(f"Exporting {data_name}")
        data_df.to_csv("/".join([self.data_folder, data_name]), index=False)
        print(f"Exported {data_name} to {self.data_folder}")

if __name__ == '__main__':
    t = TableCompare(
        expected_headers=['OBJECTID', 'Location Number', 'Latitude', 'Longitude', 'Tech', 'District',
                          'City', 'County', 'Height', 'Class', 'Pole Year', 'Field Conditions',
                          'Light Count', 'Fixture Type_1', 'Light Type_1', 'Watts_1', 'Light Target_1',
                          'Power Source_1', 'Arm Length_1', 'Comments', 'Circuit Name', 'Station Name',
                          'Pole Number Missing', 'Fixture Type_2', 'Light Type_2', 'Watts_2',
                          'Light Target_2', 'Power Source_2', 'Arm Length_2', 'Fixture Type_3',
                          'Light Type_3', 'Watts_3', 'Light Target_3', 'Power Source_3', 'Arm Length_3',
                          'Fixture Type_4', 'Light Type_4', 'Watts_4', 'Light Target_4', 'Power Source_4',
                          'Arm Length_4', 'Pole Type', 'Pole_Dir_1', 'Mount_Dir_1', 'Bottom_Dir_1',
                          'Pole_Dir_2', 'Mount_Dir_2', 'Bottom_Dir_2', 'Pole_Dir_3', 'Mount_Dir_3',
                          'Bottom_Dir_3', 'Pole_Dir_4', 'Mount_Dir_4', 'Bottom_Dir_4', 'GlobalID',
                          'CreationDate', 'Creator', 'EditDate', 'Editor', 'Power Source Location Number',
                          'ownership', 'Foreign Pole Number', 'Timestamp', 'x', 'y'],
        expected_table=r'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Abilene\Raw_QAQC_Data\ABILENE OIL MILL_QAQC.csv',
        sql_engine='mssql+pyodbc://TS-TYLER/AEP2021StreetlightAudit?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0',
        sql_db='AEP2021StreetlightAudit',
        sql_table='dbo.tblQAQCData',
        data_folder=r'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Abilene\Raw_QAQC_Data',
        export_folder='results',
        days_ago=1,
        fix=False
    )
