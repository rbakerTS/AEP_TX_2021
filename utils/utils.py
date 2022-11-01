from typing import List, Tuple, Callable
import os
import json

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyproj import Transformer, CRS

from ArcGIS_wrapper import ArcGISFeature

basic_rules_to_try = [*[f"lambda x: {t}(x)" for t in ['int', 'str', 'float']]]




def standardize_values(data: List[dict], columns_and_types: List[tuple], dataset_name: str) -> List[dict]:
    # to insure we are opening up the rules.json from the settings # todo need a more permanant storage of rules
    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'settings', 'rules.json'), 'r') as file:
        rules = json.load(file)

    if dataset_name not in rules.keys():
        rules[dataset_name] = {}

    new_data_list = list()
    for item in data:
        new_item = dict()
        for key, value in item.items():
            if key not in rules[dataset_name].keys() and key in columns_and_types.keys():
                old_value = value
                t = columns_and_types[key]['python_type']
                if not isinstance(value, t):
                    value_str = ""
                    while not isinstance(value, t):
                        print(
                            f'value of: {value} is type {type(value)} when it needs to be {columns_and_types[key]["python_type"]}')

                        for rule in basic_rules_to_try:
                            print(f'trying rule: {rule}')
                            value_str = rule
                            value_str_eval = eval(value_str)
                            try:
                                if isinstance(value_str_eval, Callable):
                                    value = value_str_eval(value)
                                else:
                                    value = value_str_eval
                            except Exception as e:
                                print(e)
                                continue
                            if isinstance(value, t):
                                break
                        if isinstance(value, t):
                            break

                        value_str = input("What should value should be applied to fix the typing?: ")
                        if value_str == 'skip':
                            break
                        value_str_eval = eval(value_str)
                        if isinstance(value_str_eval, Callable):
                            value = value_str_eval(value)
                        else:
                            value = value_str_eval

                    print(
                        f'creating new rule: when value is {old_value} (or similar) of type {type(old_value)} will be converted to {value} of type {type(value)}')

                    rules[dataset_name][key] = value_str
                new_item[key] = value
            else:
                if key in rules.keys() and rules[dataset_name][key] != 'skip':
                    new_item[key] = eval(rules[dataset_name][key])
                else:
                    new_item[key] = value

        new_data_list.append(new_item)

    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'settings', 'rules.json'), 'w') as file:
        json.dump(rules, file)
    return new_data_list


def standardize_keys(data: List[dict]) -> List[dict]:
    new_data_list = list()
    for item in data:
        new_item = dict()
        for key, value in item.items():
            new_item[key.replace(' ', '_')] = value
        new_data_list.append(new_item)
    return new_data_list


def merge_csv(folder):
    files = os.listdir(folder)
    logger.info(f'Found {len(files)}: {files}')
    master_file = files[0]
    master_filepath = folder + "/" + master_file
    try:
        master_df = pd.read_csv(master_filepath)
        logger.debug(f'read in master df')
        for file in files[1:]:
            if 'csv' not in file:
                continue
            logger.debug(f'readingg in file: {file}')
            filepath = folder + "/" + file
            x = pd.read_csv(filepath)
            master_df = pd.concat([master_df, x], ignore_index=True)
        df = master_df
        return df
    except pd.errors.ParserError as e:
        logger.critical(e)
        raise pd.errors.ParserError()


def reproject(inproj: str, outproj: str, x: float, y: float) -> Tuple:
    from_crs = CRS(inproj)
    out_crs = CRS(outproj)
    t = Transformer.from_crs(from_crs, out_crs, always_xy=True)
    return t.transform(x, y)


def dataframe_to_geodataframe(df: pd.DataFrame, lat_column: str, long_column: str, srid: int = 4326):
    # currently only supports lat and long columns and Point objects
    data_list = df.to_dict(orient='records')
    geoms = [Point(item[long_column], item[lat_column]) for item in data_list]
    gdf = gpd.GeoDataFrame(df, geometry=geoms)
    gdf.crs = {'init': f'epsg:{srid}'}

    # gdf.to_file('./data/test_points_raw.shp')  # may have to change this later
    return gdf


def update_status_based_on_reservation(df: pd.DataFrame, reservation_gdf: gpd.GeoDataFrame, lat_column,
                                       long_column, *args, **kwargs) -> gpd.GeoDataFrame:
    # if it just intersects set status to uncollected

    gdf = dataframe_to_geodataframe(df, lat_column, long_column)
    reservation_gdf_keys = list(reservation_gdf.keys())
    sjoin = gpd.sjoin(gdf, reservation_gdf, how='left')
    sjoin['Status'] = sjoin.apply(lambda x: 'uncollected' if str(x.index_right) != 'nan' else x.Status, axis=1)
    # sjoin['Status'] = 'uncollected'
    sjoin = sjoin.drop(columns=reservation_gdf_keys)
    return sjoin


def preprocessing(tasks: list, **kwargs) -> pd.DataFrame:
    master_df = None
    for task in tasks:
        master_df = TASKS[task](**kwargs)
    return master_df


def add_and_fill_column(df: pd.DataFrame, column_name: str, column_fill, *args, **kwargs):
    df[column_name] = column_fill
    return df


def add_attacher_columns_to_df(df: pd.DataFrame, attachers: List[dict]) -> pd.DataFrame:
    master_dict = {key: list() for key in list(attachers[0].keys())}

    for item in attachers:
        for key in master_dict:
            master_dict[key].append(item[key])

    for key in master_dict:
        df[key] = master_dict[key]

    return df


def add_attacher_columns_to_AGO(l_flc: ArcGISFeature, columns: list) -> None:
    for column in columns:
        l_flc.add_field(column, 'esriFieldTypeDouble')


def get_unique_attachers(df: pd.DataFrame, columns: list) -> dict:
    column_values = df[columns].values.ravel()
    unique_values = [item for item in list(pd.unique(column_values)) if str(item) != 'nan']
    return {key: 0 for key in unique_values}


def aggregate_attachers(df: pd.DataFrame, AGO_columns: list) -> pd.DataFrame:
    keys = [key for key in df.keys() if 'attacher' in str(key).lower()]
    unique_values_dict = get_unique_attachers(df, keys)

    data = {'columns': ['Attachers'], 'data': list(), 'temp_data': list()}
    for item in df.iterrows():

        temp_unique_value_dict = unique_values_dict.copy()

        for nested_item in item[1]:
            if nested_item in list(temp_unique_value_dict.keys()):
                temp_unique_value_dict[nested_item] += 1

        # data['data'].append(['\n'.join([f'{key}: {value}' for key, value in temp_unique_value_dict.items()])])
        data['temp_data'].append(temp_unique_value_dict)

    # df_attachments = pd.DataFrame(columns=data['columns'], data=data['data'])

    df = add_attacher_columns_to_df(df, data['temp_data'])

    # return df.merge(df_attachments, left_index=True, right_index=True)
    return df


def fill_null_mr_type_columns(df: pd.DataFrame, value='To Be Completed', *args, **kwargs) -> pd.DataFrame:
    df['MR_RecType'].fillna(value=value, inplace=True)
    return df


# todo remove this and just import them as needed whenever you need to apply a preprocessing task
TASKS = {
    'update_status_based_on_reservation': update_status_based_on_reservation,
    'add_and_fill_column': add_and_fill_column,
    'aggregate_attachers': aggregate_attachers,
    'fill_null_mr_type_columns': fill_null_mr_type_columns,
}
