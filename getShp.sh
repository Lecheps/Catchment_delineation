#!/bin/bash

tempPath=\'$1\'
tempFile=$2
user=$3
schema=$4
table=$5
column=$6
password=$7
id=$8

echo 'Arguments:' $1 $2 $3 $4 $5 $6 $7 $8

cd $1

sqlCmd='SELECT a.station_name, a.station_id, a.'$column' FROM '$schema.$table'Shp AS a  WHERE a.station_id='$id

echo $sqlCmd

pgsql2shp -g "$column" -f stations$id -h localhost -u "$user" -P "$password" geonorway "$sqlCmd"

read -r -d '' PGRaster <<-EOM        
PG:dbname=geonorway host=localhost port=5432 user=$user password=$password schema=$schema table=flow column=rast where='station_id=$id' mode=2
EOM
#echo $PGRaster

#gagewatershed -p flow_dir"$p".tif -o stations"$p".shp -gw watershed"$p".tif
#mpiexec -stdin 2 -q -n 8 gagewatershed -p "$PGRaster" -o stations"$p".shp -gw watershed"$p".tif 
echo "Starting run!"
./mpirun.sh "$PGRaster" stations$id  watershed$id basin$id 
echo "Finished run!"
#(mpiexec -n 8 gagewatershed -p "$PGRaster" -o stations"$p".shp -gw watershed"$p".tif &> /dev/null &)
#while [ ! -f watershed$p.tif ]
#do
#      sleep 1
#done


#gdal_polygonize.py -f "ESRI Shapefile" watershed"$p".tif basin"$p".shp 

shp2pgsql -s 3035 basin$id.shp $schema.dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432 -q 


#        raster2pgsql -b 1 -s 3035 -t auto -d watershed"$p".tif basins.dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432 -q
read -r -d '' SQL <<- EOM
    INSERT INTO $schema.resultsShp(station_id,station_name,basin)
    SELECT b.station_id, b.station_name, ST_Union(a.geom)
    FROM $schema.stations AS b, $schema.dummy AS a
    WHERE b.station_id=$id
    GROUP BY station_id, station_name;
    DROP TABLE $schema.dummy;             
EOM
 echo "Why are you looking at me funny?"
#            INSERT INTO basins.resultsRast(station_id, station_name, rast)
#            SELECT b.station_id, b.station_name, ST_MapAlgebra(a.rast, '1BB', '[rast]')
#            FROM basins.stations as b, basins.dummy AS a
#            WHERE b.station_id=$p;

#GROUP BY b.station_id,b.station_name;
echo $SQL | psql -d geonorway 


# read -r -d '' SQL <<- EOM
#     DROP INDEX IF EXISTS basin_idx_$schema CASCADE;
#     CREATE INDEX basin_idx_$schema ON $schema.resultsShp USING GIST(basin);
# EOM
#echo $SQL | psql -d geonorway


# while read p ;do
#         sqlCmd='SELECT a.station_name, a.station_id, a.'$column' FROM '$schema.$table'Shp AS a  WHERE a.station_id='$p
#         echo $sqlCmd
#         pgsql2shp -g "$column" -f stations"$p" -h localhost -u "$user" -P "$password" geonorway "$sqlCmd"

#         read -r -d '' PGRaster <<-EOM        
#         PG:dbname=geonorway host=localhost port=5432 user=$user password=$password schema=$schema table=flow column=rast where='station_id=$p' mode=2
# EOM
#         #echo $PGRaster
        
#         #gagewatershed -p flow_dir"$p".tif -o stations"$p".shp -gw watershed"$p".tif
#         #mpiexec -stdin 2 -q -n 8 gagewatershed -p "$PGRaster" -o stations"$p".shp -gw watershed"$p".tif 
#         echo "Starting run!"
#         ./mpirun.sh "$PGRaster" stations$p  watershed$p basin$p 
#         echo "Finished run!"
#         #(mpiexec -n 8 gagewatershed -p "$PGRaster" -o stations"$p".shp -gw watershed"$p".tif &> /dev/null &)
#         #while [ ! -f watershed$p.tif ]
#         #do
#         #      sleep 1
#         #done
        
        
#         #gdal_polygonize.py -f "ESRI Shapefile" watershed"$p".tif basin"$p".shp 
        
#         shp2pgsql -s 3035 basin"$p".shp $schema.dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432 -q 
        
        
# #        raster2pgsql -b 1 -s 3035 -t auto -d watershed"$p".tif basins.dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432 -q
#         read -r -d '' SQL <<- EOM
#             INSERT INTO $schema.resultsShp(station_id,station_name,basin)
#             SELECT b.station_id, b.station_name, ST_Union(a.geom)
#             FROM $schema.stations AS b, $schema.dummy AS a
#             WHERE b.station_id=$p
#             GROUP BY station_id, station_name;
#             DROP TABLE $schema.dummy;             
# EOM
#          echo "Why are you looking at me funny?"
# #            INSERT INTO basins.resultsRast(station_id, station_name, rast)
# #            SELECT b.station_id, b.station_name, ST_MapAlgebra(a.rast, '1BB', '[rast]')
# #            FROM basins.stations as b, basins.dummy AS a
# #            WHERE b.station_id=$p;

# #GROUP BY b.station_id,b.station_name;
#         echo $SQL | psql -d geonorway 
# done < $tempFile

# read -r -d '' SQL <<- EOM
#     DROP INDEX IF EXISTS basin_idx_$schema CASCADE;
#     CREATE INDEX basin_idx_$schema ON $schema.resultsShp USING GIST(basin);
# EOM
# #echo $SQL | psql -d geonorway