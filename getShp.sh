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
        sqlCmd='SELECT a.station_name, a.station_id, a.'$column' FROM '$schema.$table' AS a  WHERE a.station_id='$p
        echo $sqlCmd
        pgsql2shp -g "$column" -f stations"$p" -h localhost -u "$user" -P "$password" geonorway "$sqlCmd"
        gagewatershed -p flow_dir"$p".tif -o stations"$p".shp -gw watershed"$p".tif 
        raster2pgsql -b 1 -s 3035 -d watershed"$p".tif "$schema".dummy | PGPASSWORD="$password" psql -U "$user" -d geonorway -h localhost -p 5432
        read -r -d '' SQL <<- EOM
            INSERT INTO basins.results(station_id, station_name, rast)
            SELECT b.station_id, b.station_name, ST_MapAlgebra(a.rast, '1BB', '[rast]')
            FROM (SELECT station_id,station_name FROM basins.stations) as b, (SELECT rast FROM basins.dummy) AS a
            WHERE b.station_id=$p;
            DROP TABLE basins.dummy;
             
EOM
        echo $SQL | psql -d geonorway
done < $2

read -r -d '' SQL <<- EOM
    UPDATE basins.results
    SET basin = ST_Polygon(rast);
    CREATE INDEX basin_idx ON basins.results USING GIST(basin);
EOM
echo $SQL | psql -d geonorway

