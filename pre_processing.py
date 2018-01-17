#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 24 10:12:34 2017

@author: jose-luis
"""


import os
import shutil

import subprocess
#from  pyproj import Proj,transform

import psycopg2 as db
import psycopg2.extras
from psycopg2 import sql
from encrypt import decryptCredentials
from procedures import refreshProcedures
import yaml


#%%
#Setting up credentials for database access. These should have been previously encrypted
token = b'gAAAAABaVgNb96o6n1Kixc3fHKQWyEPN7jnJvXv_NJs65yjvJDqZZOH4w9aTyYJD28kx3iJr4EG0nsqTgxv_PRCOPKjkGPlQHycz8BuRTr25vETKiPAbLT28CJWLYLnWMllF_M1sGj_GErPOciHOQiraNUuo6IJMlVnUVMR5FvhP7YtqCKwtLSk0yefn4HU2fc6I5x1NNd94'
credentials = decryptCredentials(token,'martita')

#%%  Refreshing stored procedures if necessary.

refresh = True
if refresh :
    refreshProcedures(credentials['database'],credentials['username'],credentials['host'],credentials['password'])
    
#%% Adding shapefile containing all of Norway's rivers    
   
# Connecting to database

try : 
    conn = db.connect("dbname={} user={} host={} password={}".format(credentials['database'],credentials['username'],credentials['host'],credentials['password']))
except :
    print("Unable to connect")

cursor = conn.cursor()   
    
#Shapefile info and schema.table where it will be stored
addRivers = False
riversShp = '/home/jose-luis/Documents/GeoData/RiversNVE/Elv_Elvenett.shp'
epsg_num = 3006 #epsg number for the shapefile, failed to obtain it programatically
schema = 'norway'
table = 'rivers'

rivers_cmd = "shp2pgsql -I -d -s {} {} {}.{}"
psql_cmd = "PGPASSWORD={} psql -U {} -d {} -h {} -p 5432 -q"


#The loadRivers() procedureas add a shapefile to the table using shp2pgsql
if addRivers :
        print("Loading all norwegian rivers...")
#        cursor.execute("SELECT procedures.loadRivers(%s,%s,%s,%s);", (riversShp, epsg_number, schema, table ) )
#        conn.commit()
        subprocess.check_call(rivers_cmd.format(epsg_num,riversShp, schema, table ) + ' | ' + \
                      psql_cmd.format(credentials['password'],credentials['username'],credentials['database'],credentials['host']), \
                      shell=True, stdout=open(os.devnull, 'wb') ) 
        print("Done!") 

#%% Adding all DEM to the database 
#This assumes that the dem filename contains information about its projection   
#This is bound to take a long time    
#It will also create two tables with the raster extent
        
addDEM = False
folderDEM = '/home/jose-luis/Documents/GeoData/DEM'
schema = 'norway'     
load_cmd = "raster2pgsql -I -C -M -b 1 -r -s {} -d -t 10x10 {}/*z{}.tif {}.{}"
psql_cmd = "PGPASSWORD={} psql -U {} -d {} -h {} -p 5432 -q"

if addDEM :
    print("Loading all elevation rasters in folder {}".format(folderDEM))
    table = 'demutm32'
    epsg_num = 32632
    subprocess.check_call(load_cmd.format(epsg_num, folderDEM, str(epsg_num)[-2:], schema, table ) + ' | ' + \
                          psql_cmd.format(credentials['password'],credentials['username'],credentials['database'],credentials['host']), \
                          shell=True, stdout=open(os.devnull, 'wb') ) 
#    cursor.execute("SELECT procedures.loadDem(%s,%s,%s,%s);",(folderDEM,epsg_num,schema,table))
#    conn.commit()
    cursor.execute("SELECT procedures.setExtentTable(%s,%s);",(schema,epsg_num))
    conn.commit()
    table = 'demutm33'
    epsg_num = 32633
    subprocess.check_call(load_cmd.format(epsg_num,folderDEM,str(epsg_num)[-2:], schema, table ) + ' | ' + \
                      psql_cmd.format(credentials['password'],credentials['username'],credentials['database'],credentials['host']), \
                      shell=True, stdout=open(os.devnull, 'wb') ) 
#    cursor.execute("SELECT procedures.loadDem(%s,%s,%s,%s);",(folderDEM,epsg_num,schema,table))
#    conn.commit()
    cursor.execute("SELECT procedures.setExtentTable(%s,%s);",(schema,epsg_num))
    conn.commit()
    print('Done!')

#%% Adding stations (coordinates) where the basins will be delineated
#Reading station data from a yaml file
stationsFile = 'stations.yaml'    
stations = yaml.load(open(stationsFile))  
db.extras.register_composite('station_info',cursor)

#Re-arranging data as a list of tuples and passing it to pg with the help
#of pyscopg2 extras
allStations = list()
for i in stations:
    i=i['station']
    data = ( i['station_name'],
             i['station_id'],
             i['longitude'],
             i['latitude'],
             i['buffer'],
             i['epsg']
           )
    allStations.append(data)

cursor.execute("SELECT procedures.initializeStations();")
conn.commit()

cursor.execute("SELECT procedures.addStations( %s::station_info[] );",(allStations,))
conn.commit()

#%% Initializing schema to store results

resultsSchema = 'basins'
cursor.execute("SELECT procedures.initializeResultsSchema( %s );",(resultsSchema,))
conn.commit()


#%% Getting raster around station

#Creating table to store results
cursor.execute(" SELECT procedures.createDataTable(%s,%s,%s);", (resultsSchema,'dem',32632))
conn.commit()
cursor.execute(" SELECT procedures.createDataTable(%s,%s,%s);", (resultsSchema,'dem',32633))
conn.commit()

#Getting buffer raster around station
#Getting clipped rivers
#Burning-in rivers
print("Loading base data...")
cursor.execute("SELECT procedures.generateBaseData(%s,%s);",(resultsSchema,32632));
cursor.execute("SELECT procedures.generateBaseData(%s,%s);",(resultsSchema,32633));
conn.commit()
print("Done!")

#%% Basin delineation

#Creating table to store results. One table per dem projection
cursor.execute("SELECT procedures.createResultsTable(%s,%s,%s);",(resultsSchema,'results',32632));
cursor.execute("SELECT procedures.createResultsTable(%s,%s,%s);",(resultsSchema,'results',32633));
conn.commit()

#Creating folder to store intermediary results
tempDir = './Trash/'
if os.path.exists(tempDir) : 
    shutil.rmtree(tempDir)
os.mkdir(tempDir)

#Creating strings for the taudem, raster2pgsql and shp2pgsql commands
#Getting dem with burned-in rivers from database
get_dem_cmd =           """gdal_translate -of GTiff PG:"host={} port='5432' dbname={} user={} password={} schema='{}' table='{}'  column='{}' where='{}' " {} """
#Filling dem
fill_cmd =              """mpiexec -n 8 pitremove -z {} -fel {}"""
#Computation of flow direction
flow_dir_cmd=           """mpiexec -n 8 d8flowdir -fel {} -p {}"""
#Computation of flow accumulation
flow_acc_cmd=           """mpiexec -n 8 aread8 -p {}  -nc -ad8 {}"""
#Getting outlet from database as a shapefile
station_as_shp_cmd =    """pgsql2shp -g outlet -f {} -h localhost -u {} -P {} {} "SELECT a.station_name, a.station_id, a.outlet FROM {} AS a WHERE a.station_id={}" """
#Delineating watershed
watershed_cmd=          """mpiexec -n 8 gagewatershed -p {} -o {} -gw {}"""
#Uploading watershed dem to database
rpg_cmd =               """raster2pgsql -b 1 -s {} -d {} {} | PGPASSWORD={} psql -U {} -d {} -h {} -p 5432"""

#Fetching all stations for a given projection
cursor.execute(""" SELECT station_id,station_name FROM basins.dem32; """);
conn.commit()
rows=cursor.fetchall()
epsg_num = 32632
suffix = str(epsg_num)[-2:]
tableName = 'results' + suffix
print("Processing stations for epsg {} ...".format(epsg_num))
for row in rows:
    sid = row[0]
    station_name = row[1]
    print(station_name)
    #Getting dem with burned rivers (falling within buffersize)
    subprocess.check_call(get_dem_cmd.format(credentials['host'],credentials['database'],credentials['username'],credentials['password'],
                                             'basins','dem' + suffix,'river_rast','station_id='+str(sid), tempDir + 'el.tif'), shell=True, stdout=open(os.devnull, 'wb'))
    #Filling dem
    subprocess.check_call(fill_cmd.format(tempDir + 'el.tif', tempDir + 'fel.tif'),                                     shell=True, stdout=open(os.devnull, 'wb'))
    #Get flow direction
    subprocess.check_call(flow_dir_cmd.format(tempDir + 'fel.tif', tempDir + 'd8.tif'),                                 shell=True, stdout=open(os.devnull, 'wb'))
    #Get flow accumulation
    subprocess.check_call(flow_acc_cmd.format(tempDir + 'd8.tif', tempDir + 'flow_acc.tif'),                            shell=True, stdout=open(os.devnull, 'wb'))
    #Getting station shapefile from postgis (necessary to specify the outlet in taudem)
    subprocess.check_call(station_as_shp_cmd.format(tempDir + 'station', credentials['username'], credentials['password'], credentials['database'],
                                                    'basins.dem' + suffix, sid),                                        shell=True, stdout=open(os.devnull, 'wb'))
    #Computing watershed for outlet
    subprocess.check_call(watershed_cmd.format(tempDir + 'd8.tif',tempDir + 'station.shp', tempDir + 'watershed.tif'),  shell=True, stdout=open(os.devnull, 'wb')) 
    #Uploading watershed raster to postgis
    tempTable = 'dummy'
    subprocess.check_call(rpg_cmd.format(str(epsg_num), tempDir + 'watershed.tif',resultsSchema + '.' + tempTable,
                                         credentials['password'],credentials['username'],
                                         credentials['database'],credentials['host']),                                  shell=True, stdout=open(os.devnull, 'wb')) 
    cursor.execute(sql.SQL(''' INSERT INTO {}.{}(station_id, station_name, rast)
                                         SELECT b.station_id, b.station_name, ST_MapAlgebra(a.rast, '1BB', '[rast]') 
                                         FROM (SELECT station_id,station_name FROM basins.stations) as b, (SELECT rast FROM {}.{}) AS a
                                         WHERE b.station_id=%s;
                               DROP TABLE {}.{};         
                              
                           '''
                           ).format(sql.Identifier(resultsSchema), sql.Identifier(tableName),
                                    sql.Identifier(resultsSchema), sql.Identifier(tempTable), 
                                    sql.Identifier(resultsSchema), sql.Identifier(tempTable) ),                                          
                   (sid,)
                  )
    conn.commit()
    

#Removing junk
[os.remove(os.path.join('.',f)) for f in os.listdir('.') if f.endswith(".tif")]    
    
#Changing data type to boolean for watershed raster
cursor.execute(sql.SQL('''  UPDATE {}.{}
                             SET basin = ST_Polygon(rast);
                            CREATE INDEX {} ON {}.{} USING GIST(basin);                         
                       ''').format(sql.Identifier(resultsSchema), sql.Identifier(tableName),
                                   sql.Identifier(tableName + '_idx'),
                                   sql.Identifier(resultsSchema),sql.Identifier(tableName)
                                  )           
               )
conn.commit();


cursor.execute(sql.SQL('''  UPDATE {}.{} as b
                             SET rast = ST_Clip(a.rast,basin)
                                        FROM {}.{} AS a 
                                        WHERE b.station_id = a.station_id;
                                                   
                       '''
                       ).format(sql.Identifier(resultsSchema), sql.Identifier(tableName),
                                   sql.Identifier(resultsSchema) ,sql.Identifier('dem' + suffix)
                               )           
               )
conn.commit();

print("Done!")



              
#cursor.execute(")
#It should be an array of tuples with: station_name, station_id, longitude, latitude, EPSG number, bufferSize => (str,int,double,double,int)
#stations=(('test_station',1,8,67,4326))    
    
#
##%% Creating schema to store results
#    
#cursor.execute(''' DROP SCHEMA IF EXISTS geoData CASCADE;
#               ''')    
#    
#
#
#
#
##%% Creating postgresql database with postgis extension to store data and results
#
##Creating connection to database
#
#    
#
#cursor = conn.cursor()
##Wiping out database (this is idiotic but done for educational purposes)
##cursor.execute("DROP OWNED BY lecheps CASCADE;")
#cursor.execute("DROP SCHEMA IF EXISTS geoData CASCADE;")
#cursor.execute("DROP SCHEMA IF EXISTS dem CASCADE;")
#cursor.execute("DROP SCHEMA IF EXISTS dummy CASCADE;")
#cursor.execute("CREATE SCHEMA geoData;")
#cursor.execute("CREATE SCHEMA dem;")
#cursor.execute("CREATE SCHEMA dummy;")
##cursor.execute("CREATE SCHEMA global;")
#
#
##Making postgis the schema for the database
##cursor.execute('ALTER DATABASE \"Basins_HWI\" SET search_path=public, postgis, contrib,topology;')
##cursor.execute('ALTER DATABASE \"Basins_HWI\" SET postgis.gdal_enabled_drivers = \'ENABLE_ALL\';')
##cursor.execute('CREATE EXTENSION postgis;')
##cursor.execute('CREATE EXTENSION postgis_topology;')
##cursor.execute('ALTER DATABASE \"Basins_HWI\" SET search_path=public, postgis, contrib,topology;')
##cursor.execute('ALTER DATABA \"Basins_HWI\" SET postgis.gdal_enabled_drivers = \'ENABLE_ALL\';')
##cursor.execute('SELECT pg_reload_conf();')
##cursor.execute('SET postgis.enable_outdb_rasters TO True;')
#
#
##Creating tables to store data
##cursor.execute("CREATE TABLE geoData.dem(rid SERIAL PRIMARY KEY,rasterDEM raster,name varchar(256),projection varchar(256), x_min double precision, y_max double precision, x_max double precision, y_min double precision);")
##cursor.execute("CREATE TABLE geoData.basins(sid SERIAL PRIMARY KEY, dem raster, rivers geometry, basins geometry, name varchar(256), projection varchar(256));")
##cursor.execute("CREATE TABLE geoData.coverage(sid SERIAL PRIMARY KEY, name varchar(256), shape geometry);")
##cursor.execute("CREATE TABLE geoData.demutm32(sid SERIAL PRIMARY KEY, stationID integer, EPSG integer, dem raster);")
##cursor.execute("CREATE TABLE geoData.demutm33(sid SERIAL PRIMARY KEY, stationID integer, EPSG integer, dem raster);")
##cursor.execute("CREATE TABLE geoData.rivers(sid SERIAL PRIMARY KEY, stationID integer, EPSG integer, geom33 geometry(MULTILINESTRINGZ,32633),geom32 geometry(MULTILINESTRINGZ,32632));")
#
#conn.commit()
#     
#
#
##%%
#
##Defining path to dem, should have been previously downloaded
#dtm_folder = "/home/jose-luis/Dropbox/NIVA/Catchment_delineation/dtm10"
#path = Path(dtm_folder).glob('**/*.dem')
##Defining path to stations, should have been previously created
##Adding stations shapefile. This shapefile is an input that was created beforehand
#addToTable('/home/jose-luis/Dropbox/NIVA/Catchment_delineation/shapefiles/stations_wgs84.shp','stations','POINT') #obs:EPSG:4326
#
##Adding streams shape file, should have been previously downloaded from NVE
#stream_shapefile = '/home/jose-luis/Dropbox/NIVA/Catchment_delineation/shapefiles/NVE_41551B14_1511853202126_10664/NVEData/Elv/Elv_Elvenett.shp'
#river_proj = getProjInfo(stream_shapefile)
##Adding streams to postgis
##Commenting it out because this is slow and we want to do it only one time
##cmd = 'shp2pgsql -d -s 3006 ' + stream_shapefile + 'norway.rivers | PGPASSWORD=\'kakaroto\' psql -U lecheps -d \'Basins_HWI\' -h localhost -p 5432' 
##subprocess.check_call(cmd, shell=True, stdout=open(os.devnull, 'wb') ) 
#
##Getting information from all downloaded raster files
#rasterInfo={}
#print("Getting raster information...")
#for file in path:
##    print(file)
#    cmd = 'gdalinfo -json ' + str(file)
#    dummy = subprocess.check_output(cmd, shell=True)
#    rasterInfo[str(file)] = json.loads(dummy.decode('utf-8'))
#    rasterInfo[str(file)]['projection'] = getProjInfo((str(file)))     
#print("Done!")    
#
##Adding rasters to database 
##This is time-consuming so better to do it just onceprint('Loading rasters...')   
##Please note that in order for this to work, the raster loaded first in each command should be of the full extent (5401*5401 for dtm10)
##There should be no underscores in the raster filenames to be loaded into the database
##Otherwise, strange things happen and the raster metadata are not loaded correctly into the database.
##This was done by creatively renaming rasters fullfilling such conditionas as '1z32.tif' and '1z33.tif'.
##This could probably be done programatically.
#
#print("Loading rasters into database...") 
##cmd = 'raster2pgsql -I -C -F -b 1 -x -r -s 32632 -c -t 100x100 ' + dtm_folder + '/tiff/*z32.tif  norway.demutm32 | PGPASSWORD=\'kakaroto\' psql -U lecheps -d \'Basins_HWI\' -h localhost -p 5432'    
##subprocess.check_call(cmd, shell=True, stdout=open(os.devnull, 'wb') ) 
##cmd = 'raster2pgsql -I -C -F -b 1 -x -r -s 32633 -c -t 100x100 ' + dtm_folder + '/tiff/*z33.tif  norway.demutm33 | PGPASSWORD=\'kakaroto\' psql -U lecheps -d \'Basins_HWI\' -h localhost -p 5432'  
##subprocess.check_call(cmd, shell=True, stdout=open(os.devnull, 'wb') ) 
#print("Done!")
#
##Storing extent in norway schema
#print("Setting extent shape for available rasters...")
#cursor.execute("""DROP TABLE IF EXISTS norway.coverage32;
#                DROP TABLE IF EXISTS norway.coverage33;
#                CREATE TABLE norway.coverage32(sid SERIAL PRIMARY KEY, geom geometry(POLYGON,3006), filename TEXT);
#                CREATE TABLE norway.coverage33(sid SERIAL PRIMARY KEY, geom geometry(POLYGON,3006), filename TEXT);
#                
#                CREATE INDEX norway_coverage32_idx ON norway.coverage32 USING GIST(geom);
#                CREATE INDEX norway_coverage33_idx ON norway.coverage33 USING GIST(geom);
#                
#                INSERT INTO norway.coverage32 (sid,geom,filename) 
#                SELECT rid,ST_Transform(ST_Envelope(a.rast),3006),
#                filename
#                FROM (SELECT rid,rast,filename from norway.demutm32) AS a;
#                	     
#                INSERT INTO norway.coverage33(sid,geom,filename) 
#                SELECT rid,ST_Transform(ST_Envelope(a.rast),3006),
#                filename
#                FROM (SELECT rid,rast,filename from norway.demutm33) AS a;""")
#               
#conn.commit()   
#print('Done!')
#
#
##%%
#bufferSize=1000 #Distance in meter around the station for the buffer
#
#
##Creating table of rasters defined by a buffer around the station. This is done for both utm32 and utm33
#
##First ensuring the the station name and identifiers are unique
#cursor.execute(''' ALTER TABLE geodata.stations ADD UNIQUE ("station na"); 
#                   ALTER TABLE geodata.stations ADD UNIQUE (ogc_fid);
#               ''')
#
#cursor.execute(''' DROP TABLE IF EXISTS geodata.dem33;
#                   DROP TABLE IF EXISTS geodata.dem32;
#                   
#                   CREATE TABLE geodata.dem33(sid SERIAL PRIMARY KEY, station_id INTEGER REFERENCES geodata.stations(ogc_fid), 
#                   station_name varchar(80) REFERENCES geodata.stations("station na"),
#                   rast raster, rivers geometry(MULTILINESTRINGZ, 32633), limits geometry(POLYGON,3006), river_rast raster, outlet geometry(POINT,32633));
#                   
#                   CREATE TABLE geodata.dem32(sid SERIAL PRIMARY KEY, station_id INTEGER REFERENCES geodata.stations(ogc_fid),
#                   station_name varchar(80) REFERENCES geodata.stations("station na"),
#                   rast raster, rivers geometry(MULTILINESTRINGZ, 32632), limits geometry(POLYGON,3006), river_rast raster, outlet geometry(POINT,32632));
#                   
#                   COMMENT ON COLUMN geodata.dem32.rast IS 'dem raster clipped at a certain distance from the station';
#                   COMMENT ON COLUMN geodata.dem32.limits IS 'extent of the dem';
#                   COMMENT ON COLUMN geodata.dem32.rivers IS 'rivers falling within the clipped raster. The river shapefile was obtained from NVE';
#                   COMMENT ON COLUMN geodata.dem32.river_rast IS 'clipped dem with rivers burned in';
#                   
#                   COMMENT ON COLUMN geodata.dem33.rast IS 'dem raster clipped at a certain distance from the station';
#                   COMMENT ON COLUMN geodata.dem33.limits IS 'extent of the dem';
#                   COMMENT ON COLUMN geodata.dem33.rivers IS 'rivers falling within the clipped raster. The river shapefile was obtained from NVE';
#                   COMMENT ON COLUMN geodata.dem33.river_rast IS 'clipped dem with rivers burned in';                  
#               ''')
#conn.commit()
#                   
##Mergin dem into a single dem covered by the bufffer                   
#cursor.execute(''' INSERT INTO geodata.dem32(station_id,station_name,rast) 
#                   SELECT ogc_fid, "station na", ST_Union(d.rast,1) 
#                       FROM ( SELECT ogc_fid,"station na", sid 
#                              FROM geodata.stations AS a, norway.coverage32 AS b
#                              WHERE ST_Intersects(b.geom, ST_Buffer(ST_Transform(a.wkb_geometry,3006),%s))
#                            ) AS c
#                       INNER JOIN norway.demutm32 AS d
#                           ON d.rid=c.sid
#                       GROUP BY ogc_fid,"station na";
#                   
#                    UPDATE geodata.dem32
#                    SET limits=ST_Transform(ST_Envelope(rast),3006);
#                    
#                    UPDATE geodata.dem32
#                    SET rivers=(SELECT ST_Transform(ST_Union(ST_Intersection(limits,b.geom)),32632) FROM norway.rivers as b WHERE ST_Intersects(limits,b.geom));
#                    
#                    UPDATE geodata.dem32
#                    SET river_rast=ST_MapAlgebra(rast,
#                                                 ST_AsRaster(rivers,rast,'1BB'),
#                                                 '[rast1] - 10 * [rast2]',
#                                                 '32BF',
#                                                 'INTERSECTION',
#                                                 NULL,
#                                                 '[rast1]');
#                    
#                    UPDATE geodata.dem32
#                    SET    outlet = st_closestpoint(rivers,st_transform(a.wkb_geometry,32632))
#                    FROM   geodata.stations as a 
#                    WHERE  station_id = a.ogc_fid;
#                    
#                    CREATE INDEX geodata_dem_32_outlet_idx ON geodata.dem32 USING GIST(outlet);
#                        
#                       ''',(bufferSize,));
#                       
#                       
#conn.commit()  
#
#cursor.execute(''' INSERT INTO geodata.dem33(station_id,station_name,rast) 
#                   SELECT ogc_fid, "station na", ST_Union(d.rast,1) 
#                       FROM ( SELECT ogc_fid,"station na", sid 
#                              FROM geodata.stations AS a, norway.coverage33 AS b
#                              WHERE ST_Intersects(b.geom, ST_Buffer(ST_Transform(a.wkb_geometry,3006),%s))
#                            ) AS c
#                       INNER JOIN norway.demutm33 AS d
#                           ON d.rid=c.sid
#                       GROUP BY ogc_fid,"station na";
#                   
#                    UPDATE geodata.dem33
#                    SET limits=ST_Transform(ST_Envelope(rast),3006);;
#                       
#                    UPDATE geodata.dem33
#                    SET rivers= (SELECT ST_Transform(ST_Union(ST_Intersection(limits,b.geom)),32633) FROM norway.rivers as b WHERE ST_Intersects(limits,b.geom));
#   
#                    UPDATE geodata.dem33
#                    SET river_rast=ST_MapAlgebra(rast,
#                                                 ST_AsRaster(rivers,rast,'1BB'),
#                                                 '[rast1] - 10 * [rast2]',
#                                                 '32BF',
#                                                 'INTERSECTION',
#                                                 NULL,
#                                                 '[rast1]');                 
#                    
#                    UPDATE geodata.dem33
#                    SET    outlet = st_closestpoint(rivers,st_transform(a.wkb_geometry,32633))
#                    FROM   geodata.stations as a 
#                    WHERE  station_id = a.ogc_fid;
#                
#                    CREATE INDEX geodata_dem_33_outlet_idx ON geodata.dem33 USING GIST(outlet);
#                    
#                     ''',(bufferSize,));
#conn.commit()   
#
##Creating table to store catchment delineation results
#cursor.execute(''' DROP TABLE IF EXISTS geodata.basins33;
#                   DROP TABLE IF EXISTS geodata.basins32;
#                   CREATE TABLE geodata.basins33(sid SERIAL PRIMARY KEY, station_id INTEGER,station_name varchar(80),rast raster,geom geometry(MULTIPOLYGON,32633));
#                   CREATE TABLE geodata.basins32(sid SERIAL PRIMARY KEY, station_id INTEGER,station_name varchar(80),rast raster,geom geometry(MULTIPOLYGON,32632));
#               ''');
#conn.commit()   
#
#
##%%
#
##Hydrological processing using the taudem_package
#
##Filling in depressions in the dem with burned-in rivers
#cursor.execute(""" SELECT station_id,station_name FROM geodata.dem32; """);
#conn.commit()
#rows=cursor.fetchall()
#
##Defining commands
#fill_cmd =              """mpiexec -n 8 pitremove -z {} -fel {}"""
#rpg_cmd =               """raster2pgsql -b 1 -s {} -a {} {} | PGPASSWORD='kakaroto' psql -U lecheps -d 'Basins_HWI' -h localhost -p 5432"""
#flow_dir_cmd=           """mpiexec -n 8 d8flowdir -fel {} -p {}"""
#flow_acc_cmd=           """mpiexec -n 8 aread8 -p {}  -nc -ad8 {}"""
#station_as_shp_cmd =    """ pgsql2shp -g outlet -f {} -h localhost -u lecheps -P kakaroto 'Basins_HWI' "SELECT a.station_name, a.outlet FROM {} AS a WHERE a.station_id={}" """
#get_dem_cmd =           """ gdal_translate -of GTiff PG:"host='localhost' port='5432' dbname='Basins_HWI' user='lecheps' password='kakaroto' schema='{}' table='{}'  column='{}' where='{}' " {} """
#watershed_cmd=          """mpiexec -n 8 gagewatershed -p {} -o {} -gw {}"""
#
#for row in rows:
#    sid = row[0]
#    station_name = row[1]
#    print(station_name)
#    #watershed delineation
#    #getting dem with burned rivers (falling within buffersize)
#    subprocess.check_call(get_dem_cmd.format('geodata','dem32','river_rast','station_id='+str(sid),'el.tif'),   shell=True, stdout=open(os.devnull, 'wb'))
#    #Filling dem
#    subprocess.check_call(fill_cmd.format('el.tif','fel.tif'),                                                  shell=True, stdout=open(os.devnull, 'wb'))
#    #Get flow direction
#    subprocess.check_call(flow_dir_cmd.format('fel.tif','d8.tif'),                                              shell=True, stdout=open(os.devnull, 'wb'))
#    #Get flow accumulation
#    subprocess.check_call(flow_acc_cmd.format('d8.tif','flow_acc.tif'),                                         shell=True, stdout=open(os.devnull, 'wb'))
#    #Getting station shapefile from postgis (necessary to specify the outlet in taudem)
#    subprocess.check_call(station_as_shp_cmd.format('station','geodata.dem32',sid),                             shell=True, stdout=open(os.devnull, 'wb'))
#    #Computing watershed for outlet
#    subprocess.check_call(watershed_cmd.format('d8.tif','station.shp','watershed.tif'),                         shell=True, stdout=open(os.devnull, 'wb')) 
#    #Uploading watershed raster to postgis
#    subprocess.check_call(rpg_cmd.format('32632','watershed.tif','geodata.basins32'),                           shell=True, stdout=open(os.devnull, 'wb')) 
#    cursor.execute(''' UPDATE geodata.basins32
#                       SET station_id=%s,
#                           station_name=%s
#                       WHERE sid IN (
#                                     SELECT MAX(sid) FROM geodata.basins32
#                                     );''' ,
#                  (sid,station_name))
#    conn.commit()
#    
#    
##Changing data type to boolean for watershed raster
#cursor.execute('''UPDATE geodata.basins32
#                  SET rast=ST_MapAlgebra(rast,'1BB','[rast]');
#                  SELECT AddRasterConstraints('geodata'::name,'basins32'::name,'rast'::name);
#               ''')
#conn.commit;
#
##Getting multipolygon from all watershed rasters
#cursor.execute('''UPDATE geodata.basins32
#                  SET geom=ST_Polygon(rast);
#                  CREATE INDEX geodata_basins_shape_idx ON geodata.basins32 USING GIST(geom);
#               ''')
#conn.commit;
#               
#
#
#
##gdal_translate -of GTiff PG:"host='localhost' port='5432' dbname='Basins_HWI' user='lecheps' password='kakaroto' schema='geodata' table='demutm33' where=rid=1" dummy.tiff      
#    
##    link = raster_dir + '/' + name # str(row[0]) + '_' + str(row[1])
##    cmd = 'ln -s ./' + row[2] + ' ' + link 
##    subprocess.check_call(cmd,shell=True,stdout=open(os.devnull, 'wb'))
#    #Clipping the rasters according to the buffer
#    
#    
#    
##cmd = 'raster2pgsql -I -C -F ' + raster_dir + '/*.dem geodata.dem | PGPASSWORD=\'kakaroto\' psql -U lecheps -d \'Basins_HWI\' -h localhost -p 5432' 
##print(cmd)
##subprocess.check_call(cmd, shell=True, stdout=open(os.devnull, 'wb') )
#    
#    
#
##for record in DBF(results, encoding='utf-8'):
##    #Getting raster info
##    raster_name = './' + rasterInfo[record['NAME']]['description']
##    coordinate_system = rasterInfo[record['NAME']]['coordinateSystem']
##    coordinate_system_string = coordinate_system['wkt']
##    #Getting station coordinats
##    latitude = record['Latitude']
##    longitude = record['Longitude']
##    distance_to_clip = 2500  #in m (utm)
##    
##    if coordinate_system['wkt'].find('UTM Zone 32') != -1 :
##        coordinates = utm32(longitude,latitude)
##        UTM = '32'
##    elif coordinate_system['wkt'].find('UTM Zone 33'):
##        coordinates = utm33(longitude,latitude)
##        UTM = '33'
##    #Defining square around station coordinates
##    x = coordinates[0]
##    y = coordinates[1]
##    station_ID = record['Station ID']
##    
##    if UTM == '32' :
##        x_min,y_min = transform(utm32,proj_rivers, x - distance_to_clip, y - distance_to_clip)
##        x_max,y_max = transform(utm32,proj_rivers, x + distance_to_clip, y + distance_to_clip)
##    elif UTM == '33' :
##        x_min,y_min = transform(utm33,proj_rivers, x - distance_to_clip, y - distance_to_clip)
##        x_max,y_max = transform(utm33,proj_rivers, x + distance_to_clip, y + distance_to_clip)
#        
#
#    
#    #Clipping DEM according to above buffer
##    output = './Processed/clipped_dem/' + record['Station ID'] + UTM + '_clipped'
##    extent = str(x - distance_to_clip) + ' ' + str(y + distance_to_clip) + ' ' + str(x + distance_to_clip) + ' ' + str(y - distance_to_clip)
##    cmd = 'gdal_translate -of GTiff -ot Float32 -projwin ' + extent + ' -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 ' + raster_name + ' ' + output
##    subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb') )
##    #Clipping and reprojecting river shapefile
##    output = './Processed/clipped_dem/rivers_clipped.shp'
##    extent = str(x_min) + ' ' + str(y_max) + ' ' + str(x_max) + ' ' + str(y_min) 
##    cmd = 'ogr2ogr -t_srs ' + coordinate_system_string +' -clipsrc ' + extent + ' ' + output + ' ' + stream_shapefile + ' -overwrite'
##    print(cmd)
##    subprocess.check_call(cmd, shell=True )
##    #Burning in rivers to raster
##    #First creating a nodata raster with the same extent and projection as the buffer
##    output = './Processed/clipped_dem/dummy.tif'
##    cmd = 'gdal_calc.py -A ' + raster_name + ' -B ' + raster_name + ' --outfile=' + output + ' --calc=\"A-B\" --NoDataValue=0 --type=Byte --overwrite'
##    subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb') )  
##    print(cmd)
##    #Actually burning in the streams
##    output_stream = './Processed/clipped_dem/' + record['Station ID'] + UTM + '_stream.tif'
##    cmd = 'gdal_rasterize -burn 1 -n 2 pitremove -z ' + output + ' -fel ' + output_filled
##    #Filling pits
##    output_filled = './Processed/clipped_dem/' + record['Station ID'] + UTM + '_clipped_filled.tif'
##    cmd = 'mpiexec -n 2 pitremove -z ' + output + ' -fel ' + output_filled
##    subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb') )      
##    #Getting D8 flow direction
##    output_d8 = './Processed/clipped_dem/' + record['Station ID'] + UTM + '_clipped_D8.tif'
##    cmd = 'mpiexec -n 2 d8flowdir -fel ' + output_filled + ' -p ' + output_d8 
##    subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb') )
##    
##           
#
#
##    #Getting cumulative area (flow accumulation)
##    output_area =   './Processed/clipped_dem/' + record['Station ID'] + UTM + '_clipped_cum_area.tif'  
##    cmd = 'mpiexec -n 2 aread8 -p ' + output_d8 + ' -nc -ad8 ' + output_area 
##    subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb') )
##    #Getting stream network (raster)
##    output_stream =   './Processed/clipped_dem/' + record['Station ID'] + UTM + '_clipped_stream.tif'  
##    cmd = 'mpiexec -n 2 -ssa ' + output_area + ' -thresh 100 -src ' + output_stream 
###    cmd = 'gdaltransform -s_srs EPSG:4326' str(coordinate_system)
##    #Some offline magic to split the stations shapefiles
##    if coordinate_system['wkt'].find('UTM Zone 32') != -1 :
##        path = Path('./Processed/utm_32_stations').glob('**/*' + station_ID + '.shp')
##    elif coordinate_system['wkt'].find('UTM Zone 33') != -1:
##        path = Path('./Processed/utm_33_stations').glob('**/*' + station_ID + '.shp')
##    filename = str(list(path)[0].absolute())
##    output_watershed =   './Processed/clipped_dem/' + station_ID + UTM + '_watershed.tif'  
##    cmd = 'mpiexec -n 2 gagewatershed -p ' + output_d8 + ' -o \"' + filename + '\" -gw ' + output_watershed
##    print(cmd)
##    subprocess.check_call(cmd, shell=True,stdout=open(os.devnull, 'wb')  )
##%%    
#    
#
##for record in DBF(results, encoding='utf-8'):
##    station_ID = record['Station ID']
##    coordinate_system = rasterInfo[record['NAME']]['coordinateSystem']
##    if coordinate_system['wkt'].find('UTM Zone 32') != -1 :
##        path = Path('./Processed/utm_32_stations').glob('**/*' + station_ID + '.shp')
##    elif coordinate_system['wkt'].find('UTM Zone 33'):
##        path = Path('./Processed/utm_33_stations').glob('**/*' + station_ID + '.shp')
##   filename = list(path)[0].absolute()
##       
#conn.commit()
#conn.close()