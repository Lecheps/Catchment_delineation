# -*- coding: utf-8 -*-
"""
DEM data download from kartverket
"""
import feedparser
import asyncio
import concurrent.futures
import requests
import zipfile 
from io import BytesIO

from os import mkdir,system
import shutil

#Main atom feed for kartverket, listing all data
mainFeedKartverket = 'http://nedlasting.geonorge.no/geonorge/Tjenestefeed.xml'
allFeeds = feedparser.parse(mainFeedKartverket)

#Finding feed with utm33 data for dtm 10
#Finding entry containing DTM, 10, UTM32, and TIFF
titles = [i.title for i in allFeeds.entries]
utmLinks = [s.link for s in allFeeds.entries if 'DTM50' in s.title and 'TIFF' in s.title]
utmFeed=feedparser.parse(utmLinks[0])

#Downloading all the files 
allLinks = [i.links[0]['href'] for i in utmFeed.entries]

#Creating a list of urls to be download with parallel and wget
out = open('urls.csv','w')
for i in allLinks:
    out.write("%s\n" % i)
out.close()



#for link in allLinks :
#    print('Downloading {}'.format(link))
#    r = requests.get(link)
#    z = zipfile.ZipFile(BytesIO(r.content))
#    z.extractall(demFolder)
    
