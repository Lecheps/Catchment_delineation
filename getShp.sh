#!/bin/bash

tempPath=\'$1\'
tempFile=$2
user=$3
schema=$4
table=$5
column=$6
password=$7

cd $1
while read p ;do
        sqlCmd='SELECT a.station_name, a.station_id, a.'$column' FROM '$schema.$table'Shp AS a  WHERE a.station_id='$p
        echo $sqlCmd
        pgsql2shp -g "$column" -f stations"$p" -h localhost -u "$user" -P "$password" geonorway "$sqlCmd"

        read -r -d '' PGRaster <<-EOM        
        PG:dbname=geonorway host=localhost port=5432 user=$user password=$password schema=basins table=flow column=rast where='station_id=$p' mode=2
EOM
        echo $PGRaster
        
        #gagewatershed -p flow_dir"$p".tif -o stations"$p".shp -gw watershed"$p".tif
        gagewatershed -p "$PGRaster" -o stations"$p".shp -gw watershed"$p".tif 
        
        gdal_polygonize.py -f "ESRI Shapefile" watershed"$p".tif basin"$p".shp 
        
        shp2pgsql -s 3035 basin"$p".shp basins.dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432 -q
        
#        raster2pgsql -b 1 -s 3035 -t auto -d watershed"$p".tif basins.dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432 -q
        read -r -d '' SQL <<- EOM
            INSERT INTO basins.resultsShp(station_id,station_name,basin)
            SELECT b.station_id, b.station_name, ST_Union(a.geom)
            FROM basins.stations AS b, basins.dummy AS a
            WHERE b.station_id=$p
            GROUP BY station_id, station_name;
            DROP TABLE basins.dummy;             
EOM

#            INSERT INTO basins.resultsRast(station_id, station_name, rast)
#            SELECT b.station_id, b.station_name, ST_MapAlgebra(a.rast, '1BB', '[rast]')
#            FROM basins.stations as b, basins.dummy AS a
#            WHERE b.station_id=$p;

#GROUP BY b.station_id,b.station_name;
        echo $SQL | psql -d geonorway
done < $2

read -r -d '' SQL <<- EOM
    INSERT INTO basins.resultsShp(station_id,station_name)
    SELECT station_id, station_name FROM basins.stations;
    WITH polygons AS
    (SELECT station_id, ST_Union(rast) as r FROM basins.resultsRast GROUP BY station_id)
    UPDATE basins.resultsShp as b SET basin = ST_Polygon(r) FROM polygons WHERE polygons.station_id=b.station_id;
    CREATE INDEX basin_idx ON basins.resultsShp USING GIST(basin);
EOM
#echo $SQL | psql -d geonorway

