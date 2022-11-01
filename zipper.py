import os
import zipfile
import shutil
import time
import datetime

from tqdm import tqdm

import fiona
import geopandas as gpd


# todo: sort function to only grab name (not xxxxx_B but xxxxx)

def make_archive(source, destination):
    base = os.path.basename(destination)
    name = base.split('.')[0]
    format = base.split('.')[1]
    archive_from = os.path.dirname(source)
    archive_to = os.path.basename(source.strip(os.sep))
    print(source, destination, archive_from, archive_to)
    shutil.make_archive(name, format, archive_from, archive_to)
    shutil.move('%s.%s' % (name, format), destination)


def get_station_name(gdb_path: str, layer_name: str) -> str:
    layer = [layer for layer in fiona.listlayers(gdb_path) if layer.lower() == layer_name.lower()][
        0]
    gdf = gpd.read_file(gdb_path, layer=layer)
    station_name = gdf['station_name'][0]
    return station_name.split('_')[0]


def zipper_f(data_path:str, file: str, output_path: str,output_name:str,layer_name:str,st_name:str = ''):
    try:
        l = list()
        src = os.path.join(data_path, file).replace('\\','/')
        tmp_src = os.path.join(data_path,'temp')
        try:
            os.mkdir(tmp_src)
        except Exception as e:
            pass
        l.append(f'file: {file}\nsize: {round(os.path.getsize(src) / (pow(1024, 3)), 6)} Gigabytes')
        print(l[-1])

        # copy
        l.append(f'copying started:{datetime.datetime.now()}')
        print(l[-1])
        s1 = time.time()
        shutil.copy(src, tmp_src)
        l.append(f'copying ended: {datetime.datetime.now()}\ntotal time copying: {time.time() - s1}')
        print(l[-1])

        # unzip file
        l.append(f'unzipping started: {datetime.datetime.now()}')
        print(l[-1])
        s2 = time.time()
        with zipfile.ZipFile(os.path.join(tmp_src, file)) as zf:
            file_gdb = zf.namelist()[0].split('/')[0]
            print(file_gdb)
            for member in tqdm(zf.infolist(), desc='Extracting '):
                try:
                    zf.extract(member, tmp_src)
                except zipfile.error as e:
                    l.append(f'error!: {e}')
                    print(l[-1])
        l.append(f'unzipping ended: {datetime.datetime.now()}\ntotal time unzipping: {time.time() - s2}')
        print(l[-1])

        # open gdb and get station name
        l.append(f'opening up gdb')
        print(l[-1])

        # station_name = get_station_name(os.path.join(tmp_src, file_gdb),layer_name) if not st_name else st_name
        station_name = st_name
        l.append(f'station name for {file_gdb} is {station_name}')
        print(l[-1])
        time.sleep(1)

        # rename file
        l.append(f'unzipping2 started: {datetime.datetime.now()}')
        print(l[-1])
        s2 = time.time()
        with zipfile.ZipFile(os.path.join(tmp_src, file)) as zf:
            for member in tqdm(zf.infolist(), desc='Extracting '):
                try:
                    zf.extract(member, os.path.join(output_path, 'temp'))
                except zipfile.error as e:
                    l.append(f'error!: {e}')
                    print(l[-1])

        l.append(f'unzipping2 ended: {datetime.datetime.now()}\ntotal time unzipping: {time.time() - s2}')
        print(l[-1])

        l.append(f'renaming file')
        print(l[-1])
        s = [f for f in os.listdir(os.path.join(output_path, 'temp'))][0]
        os.rename(os.path.join(output_path, 'temp', s),
                  os.path.join(output_path, f'{output_name}_{station_name}.gdb'))

        # rezip file
        l.append(f'making new archive')
        print(l[-1])
        make_archive(os.path.join(output_path, f'{output_name}_{station_name}.gdb'),
                     os.path.join(output_path, f'{output_name}_{station_name}.zip'))
        # os.remove(os.path.join(output_path, f'AEP_CC_DELIVERY_{station_name}.gdb'))
        #
        # # upload into outputs 2
        print('*' * 200)
        time.sleep(1)
    except Exception as e:
        print(e)
        print('*' * 200)
        return False
    return True


if __name__ == '__main__':

    output_path = './data/outputs'  # for testing or if local output is desired
    data_path = 'C:/Users/kmcnew/PycharmProjects/AEPReplica/data'

    zipper_f(data_path,'-543837347977200015.zip',output_path,'test','AEP_RGV_Poles')

    # # # download each file into temp data directory

    # # data_path = '//ts-tyler/Joint Use/Audits and Inventories/AEP/AEP STREETLIGHT AUDIT 2021/Corpus Christi/Deliverables/20210706/20210716/202107161325/20210719'
    # temp_path = '../data/temp/super_temp'
    # # output_path = '//ts-tyler/Joint Use/Audits and Inventories/AEP/AEP STREETLIGHT AUDIT 2021/Corpus Christi/Deliverables/20210706/outputs_2'
    # output_path = '../data/outputs'  # for testing or if local output is desired

    # for file in os.listdir(data_path):
    #     try:
    #
    #         if 'zip' not in file:
    #             continue
    #
    #         l = list()
    #         src = os.path.join(data_path, file)
    #         tmp_src = os.path.join(temp_path, file)
    #         l.append(f'file: {file}\nsize: {round(os.path.getsize(src) / (pow(1024, 3)), 3)} Gigabytes')
    #         print(l[-1])
    #
    #         # copy
    #         l.append(f'copying started:{datetime.datetime.now()}')
    #         print(l[-1])
    #         s1 = time.time()
    #         shutil.copy(src, temp_path)
    #         l.append(f'copying ended: {datetime.datetime.now()}\ntotal time copying: {time.time() - s1}')
    #         print(l[-1])
    #
    #         # unzip file
    #         l.append(f'unzipping started: {datetime.datetime.now()}')
    #         print(l[-1])
    #         s2 = time.time()
    #         file_gdb = None
    #         with zipfile.ZipFile(os.path.join(temp_path, file)) as zf:
    #             file_gdb = zf.namelist()[0].split('/')[0]
    #             print(file_gdb)
    #             for member in tqdm(zf.infolist(), desc='Extracting '):
    #                 try:
    #                     zf.extract(member, temp_path)
    #                 except zipfile.error as e:
    #                     l.append(f'error!: {e}')
    #                     print(l[-1])
    #         l.append(f'unzipping ended: {datetime.datetime.now()}\ntotal time unzipping: {time.time() - s2}')
    #         print(l[-1])
    #
    #         # open gdb and get station name
    #         l.append(f'opening up gdb')
    #         print(l[-1])
    #
    #
    #         def get_station_name(gdb_path: str) -> str:
    #             import fiona
    #             import geopandas as gpd
    #
    #             layer = [layer for layer in fiona.listlayers(gdb_path) if layer.lower() == 'corpuschristi_poles_live'][
    #                 0]
    #             gdf = gpd.read_file(gdb_path, layer=layer)
    #             station_name = gdf['station_name'][0]
    #             return station_name
    #
    #
    #         station_name = get_station_name(os.path.join(temp_path, file_gdb))
    #         l.append(f'station name for {file_gdb} is {station_name}')
    #         print(l[-1])
    #         time.sleep(1)
    #
    #         # rename file
    #         l.append(f'unzipping2 started: {datetime.datetime.now()}')
    #         print(l[-1])
    #         s2 = time.time()
    #         with zipfile.ZipFile(os.path.join(temp_path, file)) as zf:
    #             for member in tqdm(zf.infolist(), desc='Extracting '):
    #                 try:
    #                     zf.extract(member, os.path.join(output_path, 'temp'))
    #                 except zipfile.error as e:
    #                     l.append(f'error!: {e}')
    #                     print(l[-1])
    #
    #         l.append(f'unzipping2 ended: {datetime.datetime.now()}\ntotal time unzipping: {time.time() - s2}')
    #         print(l[-1])
    #
    #         l.append(f'renaming file')
    #         print(l[-1])
    #         s = [f for f in os.listdir(os.path.join(output_path, 'temp'))][0]
    #         os.rename(os.path.join(output_path, 'temp', s),
    #                   os.path.join(output_path, f'AEP_CC_DELIVERY_{station_name}.gdb'))
    #
    #         # rezip file
    #         l.append(f'making new archive')
    #         print(l[-1])
    #         make_archive(os.path.join(output_path, f'AEP_CC_DELIVERY_{station_name}.gdb'),
    #                      os.path.join(output_path, f'AEP_CC_DELIVERY_{station_name}.zip'))
    #         # os.remove(os.path.join(output_path, f'AEP_CC_DELIVERY_{station_name}.gdb'))
    #         #
    #         # # upload into outputs 2
    #         print('*' * 200)
    #         time.sleep(1)
    #     except Exception as e:
    #         print(e)
    #         print('*' * 200)

    quit()
