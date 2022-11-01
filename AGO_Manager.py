from ArcGIS_python_api.ArcGIS_python_api_wrapper import ArcGISManager
from datetime import datetime
from arcgis.gis import GroupManager, ContentManager
import os
import json


class AGO_manager():
    def __init__(self, user: str, password: str, url: str = 'https://techserv.maps.arcgis.com'):
        self.url = url
        self.manager = ArcGISManager(url, user, password)
        now_str = datetime.strftime(datetime.now(), '%Y-%m-%d')
        self.now = self.convert_to_ago_date(now_str)
        self.content = ContentManager(self.manager)
        pass

    def content(self):
        return self.manager.content

    def convert_to_ago_date(self, date: str) -> int:
        epoch = datetime.utcfromtimestamp(1970)
        date_dt = datetime.strptime(date, '%Y-%m-%d')
        # self.ago_date = int((date_dt - epoch).total_seconds() * 1000)
        return int((date_dt - epoch).total_seconds() * 1000)

    def content_search(self, created_after: str = '', created_before: str = '', modified_after: str = '',
                       modified_before: str = '', title_search: str = '', category: str = '', item_type: str = '', max_items: int = 3000,
                       return_first: bool = False):
        content_query = ''
        if created_after and created_before:
            after_ago = self.convert_to_ago_date(created_after)
            before_ago = self.convert_to_ago_date(created_before)
            created_range = f'created:[{after_ago} TO {before_ago}]'
            content_query += created_range
        elif created_after:
            after_ago = self.convert_to_ago_date(created_after)

            before_ago = self.convert_to_ago_date(created_before)
            created_range = f'created:[{after_ago} TO {before_ago}]'
            content_query += created_range
        if modified_after and modified_before:
            after_ago = self.convert_to_ago_date(modified_after)
            before_ago = self.convert_to_ago_date(modified_before)
            modified_range = f' edited:[{after_ago} TO {before_ago}]'
            content_query += modified_range
        if title_search:
            content_query += f' title:{title_search}'
        item_type = item_type
        max_items = max_items
        return_first = return_first
        print(
            f"Searching by '{content_query}'.")
        items = self.manager.search_content(query=content_query, categories=category, item_type=item_type, max_items=max_items,
                                            return_first=False)
        if isinstance(items, list):
            pass
        else:
            items = list(items)
        print(f"{len(items)} items found.")
        results = {}
        results['items'] = items
        results['count'] = len(items)
        return results

    def group_search(self, query: str = '1=1'):
        all_groups = GroupManager.search(query)
        groups = []
        for group in all_groups:
            title = group.title()
            contents = str(group.contents())
            members = str(group.get_members())
            groups.append({'title': title, 'members': members, 'contents': contents})
        return groups

    def create_folder(self, folder_name: str):
        self.manager.content.create_folder(folder=folder_name)

    def upload_file(self, data, title, tags, folder, overwrite: bool = True,
                    type: str = 'Feature Service'):
        return self.manager.content.add(data=data,
                                        item_properties={'type': type, 'title': title, 'tags': tags,
                                                         'overwrite': overwrite},
                                        folder=folder)

    def publish_item(self, name: str, item, max_count: int = 3000):
        parameters = {'name': name, 'maxRecordCount': max_count}
        layer_item = item.publish(publish_parameters=parameters)
        layer_item.update(item_properties={'title': name})
        pass

    def share_items(self, items: list, groups: list, everyone: bool = False, org: bool = False):
        self.manager.content.share_items(items=items, everyone=everyone, org=org, groups=groups)


if __name__ == '__main__':
    with open('../Oncor/secrets.json', 'r') as file:
        username = json.load(file)['username']
        password = json.load(file)['password']

    a = AGO_manager(username, password)
    # a.group_search()
    x = a.content_search(
        title_search='SanAngelo_Poles',
        item_type='Feature Layer',
        created_after='2021-11-25',
        created_before='2021-12-19'
    )[0]

print()
