#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  9 10:14:00 2018

@author: jose-luis

This is a script that will create and upload stored procedures to the database, so they are also documented in this python script


"""
import psycopg2 as db

def refreshProcedures(dbname,user,host,password) :


    connection_cmd=''' dbname={} user={} host={} password={}''' 
    
    #Connecting to database
    try : 
        conn = db.connect(connection_cmd.format(dbname,user,host,password))
    except :
        print("Unable to connect")
        
    cursor = conn.cursor()
    
    #Creating schema to store procedures
    cursor.execute(''' DROP SCHEMA IF EXISTS procedures CASCADE;
                       DROP SCHEMA IF EXISTS norwayShp CASCADE;
                       CREATE SCHEMA procedures;
                       CREATE EXTENSION IF NOT EXISTS plsh;
                       
                       DROP TYPE IF EXISTS station_info CASCADE;  
                    	CREATE TYPE station_info AS (	
                                                    station_name varchar(80),
                						               station_id INTEGER,
                						               longitude DOUBLE PRECISION,
                						               latitude DOUBLE PRECISION,
                                                    buffer DOUBLE PRECISION,    
                                                    epsg INTEGER
                					                 );

                  ''')
    conn.commit
    
 
    
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.initializeStations() RETURNS void AS $$
                        BEGIN
                        	DROP TABLE IF EXISTS procedures.stations;                       
                        	CREATE TABLE procedures.stations( 
                                                             sid SERIAL PRIMARY KEY,
                                                             station_name varchar(80) UNIQUE,
                        				                        station_id INTEGER UNIQUE,
                        				                        longitude DOUBLE PRECISION,
                        				                        latitude DOUBLE PRECISION,
                        				                        geom geometry(POINT,4326),
                                                             buffer DOUBLE PRECISION ,
                                                             epsg INTEGER
                        				                     );                       
                          RETURN;
                        END; 
                        $$ LANGUAGE PLPGSQL;   
                  ''' )
    
    conn.commit()
    
    
    #Creating schema to store data
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.initializeSchema( _schema text  ) RETURNS void AS $$
                    BEGIN
                        EXECUTE 'DROP SCHEMA IF EXISTS ' || _schema || ' CASCADE';
                        EXECUTE 'CREATE SCHEMA ' || _schema;
                    RETURN;
                    END; 
                    $$ LANGUAGE PLPGSQL;   
              ''')
    
    conn.commit()
    
    #Creating schema to store data
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.initializeResultsSchema( _schema text  ) RETURNS void AS $$
                    BEGIN
                        EXECUTE 'DROP SCHEMA IF EXISTS ' || _schema || ' CASCADE';
                        EXECUTE 'CREATE SCHEMA ' || _schema;
                        EXECUTE 'ALTER TABLE procedures.stations
                        SET SCHEMA ' || _schema;
                        
                  
                    RETURN;
                    END; 
                    $$ LANGUAGE PLPGSQL;   
              ''')
    
    conn.commit()
    
    
    
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.addStations( _station_array station_info[] ) RETURNS void AS $$
                        DECLARE 
                        	element station_info;
                        BEGIN
                        	FOREACH element IN ARRAY _station_array
                        	LOOP
                        		INSERT INTO procedures.stations (station_name,station_id,longitude,latitude,buffer,epsg) 
                        			VALUES( element.station_name,
                        				    element.station_id,
                        				    element.longitude,
                        				    element.latitude,
                                          element.buffer,
                                          element.epsg
                                        );
                        	END LOOP;
                        	
                        	UPDATE procedures.stations
                        	SET geom=ST_Transform(ST_SetSRID(ST_MakePoint(longitude,latitude),epsg),4326);
                        	CREATE INDEX stations_idx ON procedures.stations USING GIST(geom);
                          ALTER TABLE procedures.stations
                          DROP COLUMN epsg;
                        
                        RETURN;
                        END; 
                        $$ LANGUAGE PLPGSQL;
                  ''')
                                    
    conn.commit()          
    
    
    
    #Argumemts path/to/rivers.shp , epsg number, schema , table, password , database , username
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.loadRivers(text,integer,text,text,text,text,text) RETURNS void AS 
                       '#!/bin/sh
                        shp2pgsql -I -d -s $2 $1 $3.$4 | PGPASSWORD=$5 psql -p 5432 -d $6 -h localhost -U $7
                       ' LANGUAGE plsh;                       
                   ''')
    conn.commit()
    
    
    cursor.execute(''' CREATE OR REPLACE FUNCTION procedures.loadDem(text,integer,text,text) RETURNS text AS
                   '#!/bin/bash
                   
                    raster2pgsql -I -M -F -b 1 -r -s $2 -d -t 100x100 $1/*z${2:end-2}.tif  $3.$4 | psql -p5432 
                    
                   ' LANGUAGE plsh
                   ''')
    conn.commit()
    
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.setExtentTable( _schema text, _table text ) RETURNS void AS $$
                    BEGIN
                        EXECUTE 'ALTER TABLE ' || _schema || '.' || _table ||
                                 ' ADD COLUMN extent geometry(POLYGON, 3035);'; 

                        EXECUTE 'UPDATE ' ||_schema || '.' || _table ||
                                ' SET extent = St_Envelope(rast);';
                                
                        EXECUTE 'CREATE INDEX extent'|| _table ||'_idx ON ' ||_schema || '.' || _table ||' USING GIST(extent);';        
                  
                    RETURN;
                    END; 
                    $$ LANGUAGE PLPGSQL;   
              ''')
    
    conn.commit()    
    
    
    
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.createDataTable( _schema text, _table text ) RETURNS void AS $$
                DECLARE 
                    st text = _schema || '.' || _table ;
                    stations text = _schema || '.stations';
                BEGIN
                    EXECUTE 'CREATE TABLE ' || st || '(sid SERIAL PRIMARY KEY, 
                                                       station_id INTEGER REFERENCES ' || stations || '(station_id), 
                                                       station_name varchar(80) REFERENCES ' || stations || '(station_name),
                                                       rast raster, 
                                                       rivers geometry(MULTILINESTRINGZ, 3035 ), 
                                                       limits geometry(POLYGON, 3035), 
                                                       river_rast raster, 
                                                       outlet geometry(POINT, 3035));';    
                    
                    
              
                RETURN;
                END; 
                $$ LANGUAGE PLPGSQL;    
          ''')
    
    conn.commit() 
    
    
    cursor.execute(''' CREATE OR REPLACE FUNCTION procedures.generateBaseData(_schema text) RETURNS void AS $$
                   DECLARE
                       
                       resultsTable text = _schema || '.dem'; 
                       demTable     text = 'norway.el';
                   BEGIN
                       EXECUTE 'INSERT INTO ' || resultsTable || '(station_id,station_name,outlet,rast) 
                                   SELECT station_id, station_name, ST_Transform(geom,3035), ST_Union(c.rast,1) FROM
                                        (
                                        SELECT 
                                        	a.station_id, a.station_name, a.geom, b.rast 
                                        		FROM basins.stations as a, ' || demTable || ' as b
                                        		WHERE ST_Intersects(b.extent,
                                                                    St_Buffer(St_Transform(a.geom,3035),
                                                                              a.buffer
                                                                              ) 
                                                                    )
                                        ) as c
                                        GROUP BY station_id, station_name, geom;
                               ';
                               
                   EXECUTE 'UPDATE ' || resultsTable || 
                           ' SET limits = ST_Transform(ST_Envelope(rast),3035); ';
                      
                    EXECUTE 'UPDATE ' || resultsTable ||
                            ' SET rivers = (SELECT ST_Transform(ST_Union(ST_Intersection(limits,a.geom)),3035 ) FROM norway.rivers as a WHERE ST_Intersects(limits,a.geom));';
                    
                    EXECUTE 'UPDATE ' || resultsTable || 
                            '  SET river_rast = ST_MapAlgebra( rast,
                                                               ST_AsRaster(rivers,rast,''1BB''),
                                                               ''[rast1] - 10 * [rast2]'',
                                                               ''32BF'',
                                                               ''NTERSECTION'',
                                                               NULL,
                                                               ''[rast1]''
                                                                );';
                    EXECUTE 'UPDATE ' || resultsTable ||
                            ' SET outlet = ST_closestPoint(rivers,outlet);';
                            
                    EXECUTE 'CREATE INDEX outlet_idx ON ' || resultsTable || ' USING GIST(outlet);';
                               
                       
                               
                               
                   RETURN;
                   END;
                       $$ LANGUAGE PLPGSQL;                   
                   ''')
    conn.commit()
    
    
    cursor.execute('''  CREATE OR REPLACE FUNCTION procedures.createResultsTable( _schema text, _table text, _epsg int ) RETURNS void AS $$
             DECLARE 
                 st text = _schema || '.' || _table || SUBSTRING(_epsg::text FROM '..$');
                 stations text = _schema || '.stations';
             BEGIN
                 EXECUTE 'CREATE TABLE ' || st || '(sid SERIAL PRIMARY KEY, station_id INTEGER UNIQUE REFERENCES ' || stations || '(station_id), 
                 station_name varchar(80) UNIQUE REFERENCES  ' || stations || '(station_name),
                 rast raster, basin geometry(MULTIPOLYGON, ' || _epsg || '));';            
             RETURN;
             END; 
             $$ LANGUAGE PLPGSQL;   
             ''')
    
    conn.commit() 
    


    
    
    
    conn.close()
