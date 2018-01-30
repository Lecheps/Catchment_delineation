import json,requests
import hashlib
import numpy as np
import pandas as pd
import re
import pprint


metnoId = ''
metnoUrl = ''

#%%-----------------------------------------------------------------------------

def init(_metnoId,_metnoUrl) :
    global metnoId, metnoUrl
    metnoId = _metnoId
    metnoUrl = _metnoUrl
    
#%%----------------------------------------------------------------------------
def getStationsInPolygon(polygon,dateInterval) :
    stationsDict = {}
    insideStations = {}
    queryParameters = {}
    queryParameters['geometry'] = polygon
    queryParameters['validtime'] = dateInterval
    query = requests.get(metnoUrl + '/sources/v0.jsonld',
                         queryParameters,
                         auth=(metnoId, ''))
    try :
        insideStations = json.loads(query.text)
    except :
        print(query)
    #pprint.pprint(insideStations)
    if insideStations['data']:
        for myDict in insideStations['data']:
            if myDict['@type']=='SensorSystem':
                stationsDict[myDict['name']] = {'id' : myDict['id'] , 
                                            'coordinates' : myDict['geometry']['coordinates'] }
    else :
        insideStations = {}
            
    return stationsDict,insideStations

#%%----------------------------------------------------------------------------
def getAvailableDatasets(stationIds) :
    queryParameters = {}
    queryDict = {}
    queryParameters['ids'] = stationIds
    query = requests.get(metnoUrl + '/elements/v0.jsonld?lang=en-US',
                         queryParameters,
                         auth=(metnoId, ''))
    try :
        queryDict = json.loads(query.text)
    except :
        print(query)
               
    return queryDict


#%% ---------------------------------------------------------------------------

def listAllTimeseries(stationsID,dateInterval) :
    queryParameters = {}
    queryParameters['sources'] = stationsID
    queryParameters['referencetime'] = dateInterval
    
    query = requests.get(metnoUrl + '/observations/availableTimeSeries/v0.jsonld',
                         queryParameters,
                         auth=(metnoId, '')
                        )
    stations = {}
    try :
        stations = json.loads(query.text)
    except :
        print(query)
    
    return stations['data']

#%% ---------------------------------------------------------------------------

def getStationsWithTimeSeries(stationsID,dateInterval,parameterList) :
    queryParameters = {}
    queryParameters['sources'] = stationsID
    queryParameters['elements'] = ','.join(parameterList)
    queryParameters['referencetime'] = dateInterval
    
    query = requests.get(metnoUrl + '/observations/availableTimeSeries/v0.jsonld',
                         queryParameters,
                         auth=(metnoId, '')
                        )
    stations = {}
    try :
        stations = json.loads(query.text)
    except :
        print(query)
    
    #Finding unique identifiers in order to accumulate data within a day
    if 'data' in stations :
        
        for i in stations['data'] :
            myHash = hashlib.md5( str.encode( i['elementId'] + i['sourceId'] ) )
            i['hash'] = myHash.hexdigest()
    #        print(i['hash'], i['elementId'] + i['sourceId'])
        
        #Going through unique hashes, keeping only values measured at 0600
        for i in set([d['hash'] for d in stations['data']]) :
            stations['data'] = [x for x in stations['data'] if x['hash']!=i or (x['hash']==i and x['timeOffset']=='PT06H')]
        for i in stations['data'] :
            i['sourceId'] = i['sourceId'].replace(':0','')
    return stations

#%%

def downloadStations(stations,stationsDict,dateInterval) :
    dataParameters = {}
    dataParameters['referencetime'] =  dateInterval
    cnt = 0
    allData = pd.DataFrame()
    print('Downloading data: ') 
    if not stations['data'] :
        for i in stations['data'] :
            dataParameters['elements'] = i['elementId']
            dataParameters['sources'] =  i['sourceId']
            dataQuery = requests.get(metnoUrl + '/observations/v0.jsonld',
                                     dataParameters,
                                     auth=(metnoId, '')
                                    )
            try :
                data = json.loads(dataQuery.text)
            except :
                print(dataQuery)
                    
            values = np.array([i['observations'][0]['value'] for i in data['data']])
            timestamp = np.array([i['referenceTime'] for i in data['data']],dtype='datetime64[D]')
            columnName = i['elementId'] + "\n" + stationsDict[i['sourceId']]
            displayStr = i['elementId'] + ' ' + stationsDict[i['sourceId']]
    #        print("Progress: {}".format(displayStr), end="\b" * len(displayStr))
            print(displayStr)
            if cnt == 0 :
                allData = pd.DataFrame(data=values,index=timestamp,columns=[columnName])
            else :
                allData = pd.merge(allData,pd.DataFrame(data=values,index=timestamp,columns=[columnName]),left_index = True, right_index = True, how = 'outer') 
            cnt = cnt + 1
#        backspace(len(displayStr))
        precipitation = allData.filter(regex="precipitation")
        precipitation[precipitation==-1] = 0 #Need to check if -1 means no precipitation or missing measurement
        temperature = allData.filter(regex="temperature")   
        precipitation.rename(columns=lambda x : re.sub('^.*\n','',x),inplace=True)
        temperature.rename(columns=lambda x : re.sub('^.*\n','',x),inplace=True) 
        precipitation.to_pickle('precipitation')
        temperature.to_pickle('temperature')
    return allData


def downloadData(station,elements,timeOffset,dateInterval) :
    print('Started download of {} in {}...'.format(elements,station))
    dataParameters = {}
    dataParameters['referencetime'] =  dateInterval
    dataParameters['elements'] = elements
    dataParameters['sources'] = station
    dataParameters['timeoffsets'] = timeOffset
    dataQuery = requests.get(metnoUrl + '/observations/v0.jsonld',
                             dataParameters,
                             auth=(metnoId, '')
                             )
    try :
        data = json.loads(dataQuery.text)
    except :
        print(dataQuery)
    #pprint.pprint(data)    
    values = np.array([i['observations'][0]['value'] for i in data['data']])
    timestamp = np.array([i['referenceTime'] for i in data['data']],dtype='datetime64')
    result = pd.DataFrame(data=values,index=timestamp,columns=[elements])
    print('Done!')
    return(result,data)