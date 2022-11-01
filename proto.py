import os
import pandas as pd

replicas_list_w_ext = os.listdir(r'C:\Users\TechServPC\Downloads\outputs_12_10_2021')
replicas_list = []
for replica in replicas_list_w_ext:
    replica_no_ext = os.path.splitext(replica)[0]
    replica_split = replica_no_ext.split('_')[1:]
    replica_trimmed = '_'.join(replica_split)
    replica_trimmed = replica_trimmed.replace('_', ' ')
    replicas_list.append(replica_trimmed)
station_df = pd.read_excel(
    r'C:\Users\TechServPC\OneDrive - TechServ\AEP StreetLight Project Management\San Angelo\AEP_Tracking_Field.xlsx')
station_list = list(station_df.iloc[:, 2])
station_list_formatted = []
for station in station_list:
    station_formatted = station.replace('_', ' ')
    station_list_formatted.append(station_formatted)

in_list = []
out_list = []
error_list = []
for x in station_list_formatted:
    if x in replicas_list:
        in_list.append(x)
    elif x not in replicas_list:
        out_list.append(x)
    else:
        error_list.append(x)
out_df = pd.DataFrame(out_list)
out_df.to_csv(r'C:\Users\TechServPC\Downloads\outputs_12_10_2021\missing_replicas.csv')
pass