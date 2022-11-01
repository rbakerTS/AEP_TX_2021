import re
import shutil
from typing import Union, Callable
import time
import requests
import os

from zipper import zipper_f
from ArcGIS_wrapper import ArcGISManager

import time


def _parse_args(*args, _arg_delimenter: str = '--', ) -> dict:
    return {args[index].replace('-', ''): item for index, item in enumerate(args[1:]) if '--' not in item}


# todo may consider a change in this since functions could be evalulated; may require more sanitization
def _check_and_convert_args(**kwargs):
    for key, value in kwargs.items():
        new_value = value
        try:
            new_value = eval(value)
            # quit()
        except (NameError, SyntaxError):
            pass
        kwargs[key] = new_value
    return kwargs


# can allow for either a function to be directly inserted or a function called from a commands file in the settings with the function name as the input (str)
def terminal_wrapper(function: Union[str, Callable], *args, commands: Union[dict, None] = None,
                     convert_args: bool = True):
    parsed_kwargs = _parse_args(*args)
    converted_kwargs = _check_and_convert_args(**parsed_kwargs) if convert_args else parsed_kwargs
    print(converted_kwargs)
    return commands[function](**converted_kwargs) if commands else function(**converted_kwargs)


def download_file(url: str, output_path: str, chunk_size: int = 3000) -> None:
    r = requests.get(url, stream=True)
    with open(output_path, 'wb') as file:
        for chunk in r.iter_content(chunk_size):
            file.write(chunk)

def download_file2(url: str, output_path: str) -> None:
    r = requests.get(url, stream=True)
    with open(output_path, 'wb') as file:
        shutil.copyfileobj(r.raw,file)


def download_and_alter_zip(url, data_path, output_path, output_name, layer_name,st_name):
    t = 30
    user_url, user, password = 'https://techserv.maps.arcgis.com', 'jjoiner31', 'Map2021$'

    while True:
        g = ArcGISManager(user_url,user,password)
        r = requests.get(f'{url}?token={g.token}')
        print(f'getting url: {url}?token={g.token}')
        zip_link = re.findall(r'>https.*\.zip', str(r.content))
        print('got url')
        print('getting link')

        try:
            link = f'{zip_link[0].strip().replace(">", "")}?token={g.token}'
            print(f'link: {link}')
        except Exception as e:
            print(f'{e}\nretrying in {t} seconds\n{"*"*200}')
            time.sleep(t)
            continue

        print('trying to get zip')
        file = zip_link[0].replace('\\', '/').split('/')[-1]
        print(f'got zip : {link}')
        download_file2(link, os.path.join(data_path, file))
        zipper_f(data_path, file, output_path, output_name, layer_name,st_name)
        return None

if __name__ == '__main__':
    # token = 'Q9Mcol322D06gnBGtI386gbIuz27eE95mdedggTFjapUkacHeu3Pwk4pCMZnmtFfM8B5GJT1ldBbKdn-0Xe8yqVNgKTcCZnPXjrwbgbYJvCEL_IO0ZJ6rYhHpMywFdCLon3Lt_awpr7faZPjCDv4IObesoJnAwLmibkhJ8WE5aQ.'
    # download_and_alter_zip(f'https://services8.arcgis.com/gChEJJDXE5xyPrQH/arcgis/rest/services/test_points_10000/FeatureServer/jobs/784f2d15-6636-4aa5-8cd5-1aa533e34953?token={token}',token,'./data','data/outputs','test','test_points_10000')

    import sys

    args = sys.argv[1:]
    print(args)
    r = terminal_wrapper(download_and_alter_zip, *args)

    quit()
