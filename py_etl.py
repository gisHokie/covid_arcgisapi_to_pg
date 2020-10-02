'''
author: Scott D. McDermott
date: 10/01/2020
Summary: get API Restful data and push it to Postgres/Postgis database

COVID URL
https://opendata.arcgis.com/datasets/bbb2e4f589ba40d692fab712ae37b9ac_4.geojson
'''

# https://gis.stackexchange.com/questions/258874/convert-simple-postgis-polygon-layer-to-polygon-topogeometry
# using miniconda3 "py_geo" environment (this is a custom environment)
import shutil, os
import json
import datetime
from osgeo import ogr, osr, gdal
import yaml
import sys
import requests

#Custom modules
import modules.shapefile_to_postgres as stp

#Paths
yaml_file = 'py_etl_config.yaml'
yaml_system_path_file = os.path.join(sys.path[0], yaml_file)   # will need to have full path to file

yaml_list = ''


# Get the configs from yaml file
with open(yaml_system_path_file, 'r') as yfile:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    yaml_list = yaml.load(yfile, Loader=yaml.FullLoader)

# Get YAML Values
version = yaml_list['version']
main_title = yaml_list['main_title']
# Set the Postgres, there is only one so no need to loop
pg_host = yaml_list['pg']['pg_host']
pg_port = yaml_list['pg']['pg_port']
pg_database = yaml_list['pg']['pg_database']
pg_user = yaml_list['pg']['pg_user']
pg_pwd =yaml_list['pg']['pg_pwd']
pg_schema = yaml_list['pg']['pg_schema']
# Read through the apis (for now one api per yaml)
api_source = yaml_list['apis']['api_source']
api_description = yaml_list['apis']['api_description']
api_url = yaml_list['apis']['api_url']
api_data_type = yaml_list['apis']['api_data_type']
feat_class_name = yaml_list['apis']['feat_class_name']
effective_date = yaml_list['apis']['effective_date']   #Effective date for HZ Map
expires_date = yaml_list['apis']['expires_date']     # set this to be alwasys NULL in Postgres                

# Call the API and get data
r = requests.get(api_url)
data = r.json()

# postgres connection object
dbase_conn = {
    'host': pg_host,
    'dbname': pg_database,
    'user': pg_user,
    'password': pg_pwd,
    'port': pg_port
    }
print(dbase_conn)
conn = stp.p_conn(dbase_conn)
cur = conn.cursor()

# Truncate the table
sql_select = 'TRUNCATE TABLE {} '.format( feat_class_name )
cur.execute(sql_select)

# Read each feature
# Structure feature class to add expires date field
for ft in data['features']:
    #print(ft['properties']['OBJECTID'])
    # add effective date
    lng = ft['properties']['Long_']
    lat = ft['properties']['Lat']
    ft['properties']['effective'] = effective_date
   # ft['properties']['expires'] = expires_date
    ft['properties']['geom'] = 'POINT(' + str(lng) + ' ' + str(lat) + ')'
    #print(ft['properties'])

    # change None to 0 for counts
    if ft['properties']['People_Tested']  == None:  ft['properties']['People_Tested'] = 0
    if ft['properties']['People_Hospitalized'] == None: ft['properties']['People_Hospitalized'] = 0
    if ft['properties']['FIPS'] == None: ft['properties']['FIPS'] = 0
    # some names have single quote that needs to be escaped
    new_admin2 = ft['properties']['Admin2']
    if "'" in new_admin2: 
        new_admin2 = new_admin2.replace("'", "''") 
    if "'" in ft['properties']['Combined_Key']: ft['properties']['Combined_Key'] = ft['properties']['Combined_Key'].replace("'", "''")
    #print (ft['properties']['Admin2'])

    if ft['properties']['Lat'] is not None and ft['properties']['Lat'] is not None:
        # SQL Statement to INSERT DATA (MOVE THIS TO POSTGRESQL IN FUTURE)
        columns = list(ft['properties'].keys())
        sql_string = 'INSERT INTO {} '.format( feat_class_name )
        sql_string += '(' + ', '.join(columns) + ')\nVALUES '
        sql_string += '('
        sql_string +=  str(ft['properties']['OBJECTID']) + ',\'' +  str(ft['properties']['Province_State']) + '\',\'' +  ft['properties']['Country_Region'] 
        sql_string += '\',\'' +  str(datetime.date.today()) + '\',\'' +  str(ft['properties']['Lat']) + '\',\'' +  str(ft['properties']['Long_'])
        sql_string += '\',\'' +  str(ft['properties']['Confirmed']) + '\',\'' +  str(ft['properties']['Recovered']) + '\',\'' +  str(ft['properties']['Deaths']) 
        sql_string += '\',\'' +  str(ft['properties']['Active']) + "\',\'" +  new_admin2 + "\',\'" +  str(ft['properties']['FIPS'] )
        sql_string += '\',\'' +  ft['properties']['Combined_Key'] + '\',\'' +  str(ft['properties']['Incident_Rate']) + '\',\'' +  str(ft['properties']['People_Tested']) 
        sql_string += '\',\'' +  str(ft['properties']['People_Hospitalized']) + '\',\'' +  str(ft['properties']['UID']) + '\',\'' +  ft['properties']['ISO3']
        sql_string += '\',\'' +  ft['properties']['effective'] +  '\',\'' + str(ft['properties']['geom']) + '\''
        sql_string += ');'
        print(sql_string)
        cur.execute(sql_string)
        conn.commit()
        print(sql_string)
#INSERT INTO covid_cases_esri (OBJECTID, Province_State, Country_Region, Last_Update, Lat, Long_, Confirmed, 
#Recovered, Deaths, Active, Admin2, FIPS, Combined_Key, Incident_Rate, People_Tested, People_Hospitalized, UID, ISO3, effective, expires, geom)

# Run a select statement to verify data is added
sql_select = 'SELECT OBJECTID, Province_State, Confirmed FROM {} '.format( feat_class_name )
cur.execute(sql_select)
for  OBJECTID, Province_State, Confirmed in cur:
    print('{0}: {1}'.format(OBJECTID, Province_State))
#closing database connection.  Need to close it to allow the next INSERT
if(conn):
	conn.close()
