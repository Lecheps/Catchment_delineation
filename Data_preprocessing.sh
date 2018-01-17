#! /bin/bash

FILENAME=Norwegian_small_catchments.csv
DESTINATION="./shapefiles" 

mkdir -p $DESTINATION


#Create dataset in default coordinate system
ogr2ogr "$DESTINATION/stations_wgs84.shp" $FILENAME -oo X_POSSIBLE_NAMES=Lon* -oo Y_POSSIBLE_NAMES=Lat* -lco ENCODING=UTF-8 -overwrite

#Duplicate dataset as UTM32
ogr2ogr "$DESTINATION/stations_utm32.shp"  "$DESTINATION/stations_wgs84.shp" -t_srs EPSG:32632 -s_srs EPSG:4326 -a_srs EPSG:32632 -overwrite

#Duplicate dataset as UTM33
ogr2ogr "$DESTINATION/stations_utm33.shp"  "$DESTINATION/stations_wgs84.shp" -t_srs EPSG:32633 -s_srs EPSG:4326 -a_srs EPSG:32633 -overwrite


DTMFOLDER="./dtm10"

for f in "$DTMFOLDER/*.dem"
do
	echo gdalinfo $f \n
done
