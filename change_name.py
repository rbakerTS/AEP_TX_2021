import os
import requests
import time
import asyncio
import aiohttp
import aiofiles

import pickle
import re

from ArcGIS_wrapper import ArcGISFeature, ArcGISManager
from zipper import zipper_f

if __name__ == "__main__":
    root = r'Z:\Audits and Inventories\AEP\AEP STREETLIGHT AUDIT 2021\Deliverable\RGV\output'
    for item in os.listdir(root):
        name = item.replace('.zip', '')

        zipper_f(root, item, root, 'RioGrandeValley_Poles', name, name)
