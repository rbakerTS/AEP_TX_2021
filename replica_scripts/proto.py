import pickle

from ArcGIS_wrapper import ArcGISManager

url, user, password = 'https://techserv.maps.arcgis.com', 'kmcnew_TechServ', 'j@KK737493962022'
g = ArcGISManager(url, user, password)

with open('stations_check.pickle','rb') as file:
    t = pickle.load(file)


quit()