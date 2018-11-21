# -*- coding: utf-8 -*-

from fabric.api import *
import os

#env.hosts = ['catchment.niva.no']
env.hosts = ['104.196.99.99']
env.user = 'jose-luis'
env.key_filename = '/home/jose-luis/.ssh/jose-luisKey'
env.roledefs = { 'stage': ['catchment.niva.no'],
                'production': [''],  #                               
               }

global path, file

#------------------------------------------------------------------------------------------------------------
#Download DEM data from Kartverket
    
def makeDir():
    run('rm -rf {}'.format(path))
    run('mkdir -p {}'.format(path))
    run('mkdir -p {}/data'.format(path))
    put(file, path)
    
def getUrls():
    run('cd {} && python3 {}'.format(path,file))
    
def downloadZip():
    run('cd {}/data && parallel -j 100 wget < ../urls.csv &> /dev/null'.format(path))
    
def unZip():
    run('cd {}/data && parallel -j 4 unzip ::: *.zip'.format(path))
    
def clean():
    run('cd {}/data && rm *.zip'.format(path))
    
#------------------------------------------------------------------------------------------------------------
#Tile, transform and burn rivers from downloaded DEM    
    
def buildVrt():
    run('cd {}/data && rm -f norway* && gdalbuildvrt norway.vrt *.tif'.format(path))
    
def transformVrt():
    run('cd {}/data && gdal_translate norway.vrt norway.tif'.format(path))

def resampleRast(x_size,y_size):
    run('cd {}/data && gdalwarp -overwrite -s_srs EPSG:3045 -t_srs EPSG:3035 -tr {} {} -r cubicspline norway.tif norway_resampled.tif'.format(path,x_size, y_size))

def burnRivers(river_layer, shapefile):
    run('cd {}/data && cp norway_resampled.tif norway_burned_rivers.tif && gdal_rasterize -b 1 -burn -20 -add -l {} {} norway_burned_rivers.tif'.format(path,river_layer,shapefile))

#------------------------------------------------------------------------------------------------------------
#Copy data to and from "expensive" instance

def makeTar(fileList):
    run('cd data && tar -cf hydroData.tar {}'.format(fileList))

def putInBucket():
    run('rm -rf data/tmp')
    run('mkdir data/tmp')
    run('gcsfuse jlg-bucket data/tmp')
    run('cd data && cp hydroData.tar tmp/ && sudo umount tmp')
    

def getFromBucket():
    run('cd dem/data && rm -rf tmp && mkdir tmp && gcsfuse jlg-bucket tmp')
    run('cd dem/data && tar -xf tmp/hydroData.tar')
    run ('cd dem/data && sudo umount tmp')    

def get_file(filename): 
    run('rm -rf dem/data/tmp')
    run('cd dem/data && mkdir tmp && gcsfuse jlg-bucket tmp')
    run('cd dem/data && tar-cf norway.tar {}'.format(filename))
    run('cd dem/data && cp norway.tar tmp/ && sudo umount tmp' )
    #get('/home/jose-luis/dem/data/' + filename, '/tmp/backpublish-{}'.format(filename))
    

def put_file(filename):
    run('rm -rf data/tmp')
    run('cd data && mkdir tmp && gcsfuse jlg-bucket tmp')
    run('cd data && tar -xf tmp/norway.tar ./'.format(filename))
    run('sudo umount data/tmp')
    
#------------------------------------------------------------------------------------------------------------
#Hydrological processing using TauDEM
def pitremove(num_processors,filename):
    run('cd data && mpiexec -n {} pitremove -z {} -fel fel.tif'.format(num_processors,filename))

def flowdir(num_processors):
    run('cd data && mpiexec -n {} d8flowdir -fel fel.tif -p flow_dir.tif'.format(num_processors))

def area(num_processors):
    run('cd data && mpiexec -n {} aread8 -p flow_dir.tif -nc -ad8 flow_acc.tif'.format(num_processors))
        
    
#------------------------------------------------------------------------------------------------------------      
#Loading DEMs to geodatabase

def loadDEMs(name,db,U,h,p):
    epsg_num = 3035
    schema = 'norway'
    load_cmd = "raster2pgsql -I -C -M -b 1 -r -s {} -d -t auto -l 2,4,8,16,32,64 dem/data/{} {}.{}"
    psql_cmd = "PGPASSWORD={} psql -U {} -d {} -h {} -p 5432 -q"
    cmd = load_cmd.format(epsg_num, name, schema, name[:-4] ) + ' | ' + psql_cmd.format(p, U, db, 'localhost')
    run(cmd)
        

#------------------------------------------------------------------------------------------------------------
def createTmpDir(path):
    run('rm -rf {0} && mkdir {0} && cd {0} && chmod a+w {0}'.format(path))

def getStationIdList(schema,table,path):
    run('''echo "\copy (SELECT station_id FROM {}.{}Shp) TO '{}stations.csv' (format csv);" | psql -d geonorway'''.format(schema,table,path))

def generateShp( user, password, db, schema, table, columnOutlet, path ):
    put('getShp.sh', path  + 'getShp.sh') 
    run('chmod +x {}getShp.sh'.format(path))
    put('mpirun.sh', path  + 'mpirun.sh') 
    run('chmod +x {}mpirun.sh'.format(path))
    run('cd {0} && ./getShp.sh {0} {1} {2} {3} {4} {5} {6}'.format(path, ' stations.csv ', "'" + user + "'", schema, table, columnOutlet, password)) 

def generateSingleShp( user, password, db, schema, table, columnOutlet, path, fid ):
    put('getShp.sh', path  + 'getShp.sh') 
    run('chmod +x {}getShp.sh'.format(path))
    put('mpirun.sh', path  + 'mpirun.sh') 
    run('chmod +x {}mpirun.sh'.format(path))
    run('cd {0} && ./getShp.sh {0} {1} {2} {3} {4} {5} {6} {7}'.format(path, ' stations.csv ', "'" + user + "'", schema, table, columnOutlet, password,fid)) 
  
        
#This will only work for a superuser 
def generateRast(password, db, schema, table, columnRast, path ):
    run(''' echo "SELECT procedures.dumpAsTif('{}','{}','{}','{}');" | PGPASSWORD={} psql -d {} -h localhost -U postgres         '''.format(schema,table,columnRast,path,password,db)    
       );  

#------------------------------------------------------------------------------------------------------------
@task                                                                         
def backpublish(source_role, target_role, filename, target_ip):                                    
    get_file.roles = (source_role,)                                       
    execute(get_file,filename)
    
    execute(put_file,filename,hosts=target_ip) 
    
@task    
def loadDEM(path_,file_):
    global path, file
    path = path_
    file = file_
    execute(makeDir)
    execute(getUrls)
    execute(downloadZip)
    execute(unZip)
    execute(clean)
    
@task
def preprocessDEM(path_, river_layer, shapefile, x_size, y_size):
    print("I'm alive")
    global path
    path = path_
    burnRivers.roles = ('stage',)
    resampleRast.roles = ('stage',) 
    buildVrt.roles = ('stage',)
    transformVrt.roles = ('stage',)
    execute(buildVrt)
    execute(transformVrt)
    execute(resampleRast, x_size, y_size)
    execute(burnRivers, river_layer, shapefile)    
 

@task
def hydrology(filename,num_processors,target_ip):
    execute(pitremove,num_processors,filename,hosts=target_ip)
    execute(flowdir,num_processors,hosts=target_ip)
    execute(area,num_processors,hosts=target_ip)
        
        
@task
def getHydroData(fileList, target_ip):
    print(fileList, target_ip)
    execute(makeTar,fileList, hosts = target_ip)
    execute(putInBucket, hosts = target_ip)
    getFromBucket.roles = ('stage', )
    execute(getFromBucket)
            
@task 
def loadDEMToDB(nameList,db,U,h,p):
    print(nameList)
    loadDEMs.roles = ('stage', )
    nameList = nameList.split(";")
    for name in nameList:
        print(name)
        execute(loadDEMs,name,db,U,h,p);
        
@task 
def processBasin(user, password, db, schema,table,columnRast,columnOutlet,path):
    createTmpDir.roles=('stage',)
    getStationIdList.roles=('stage',)
    generateShp.roles=('stage',)
    generateRast.roles=('stage',)
        
    execute(createTmpDir,path)
    execute(getStationIdList,schema,table,path)
    #execute(generateRast, password, db, schema, table, columnRast, path)
    execute(generateShp, user, password, db, schema, table, columnOutlet, path)
    
@task 
def processSingleBasin(user, password, db, schema,table,columnRast,columnOutlet,path,fid):
    createTmpDir.roles=('stage',)
    generateSingleShp.roles=('stage',)
    execute(createTmpDir,path)
    execute(generateSingleShp, user, password, db, schema, table, columnOutlet, path,fid)
    