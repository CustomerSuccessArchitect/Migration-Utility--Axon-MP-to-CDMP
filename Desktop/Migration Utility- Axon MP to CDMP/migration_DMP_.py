# -*- coding: utf-8 -*-
"""""""""""""""
Created on Tue Jan 17 17:00:22 2023

@author: sjain

This is a Migration Utility which will be fetch Categories, DataSet, DataElements from Axon Marketplace
and Migrate it to Cloud Data Marketplace.

"""""""""""""""""

import pandas as pd
import requests
import json
import logging
import os
from pandas import *

configFile = open("Configuration/Configuration.txt", "r")

#intialiaze Variables
axon_login_url=""
axon_username=""
axon_password=""
cloud_Login_URL=""
cloud_Username=""
cloud_Password=""
CDMP_URL=""
count=0

#read the config file
while True:
    count += 1
  
    # Get next line from file
    line = configFile.readline()
  
    if not line:
        break
        
        
    if 'axon_login_url ' in line:
        str=line.split('=')
        axon_login_url=str[1].strip()
        print ("Axon URL : "+ axon_login_url)
        
    
    if 'axon_username' in line:
        str=line.split('=')
        axon_username =str[1].strip()
        print ("Axon Username : "+ axon_username )
        
    if 'axon_password ' in line:
        str=line.split('=')
        axon_password  =str[1].strip()
        print ("Axon Password : "+ axon_password)
        
    if 'Cloud_Login_URL' in line:
         str=line.split('=')
         cloud_Login_URL =str[1].strip()
         print ("Cloud Login URL : "+ cloud_Login_URL )
         
    if 'Cloud_Username ' in line:
         str=line.split('=')
         cloud_Username =str[1].strip()
         print ("Cloud Username: "+ cloud_Username )
         
    if 'Cloud_Password ' in line:
         str=line.split('=')
         cloud_Password =str[1].strip()
         print ("Cloud Password: "+ cloud_Password  )
         
    if 'CDMP_URL ' in line:
         str=line.split('=')
         CDMP_URL =str[1].strip()
         print ("Cloud Marketplace URL: "+ CDMP_URL  )
         
         

login_check=axon_login_url+'api/login_check'
print (login_check)


payload= {
        'username':axon_username,
            'password':axon_password
        }
noAuth="No Auth"
headers  = {"Content-Type": "application/json"}
#from requests.auth import HTTPBasicAuth
get_colAsset= requests.post(login_check,headers=headers,data=json.dumps(payload))
#print(get_colAsset.text)
resp=get_colAsset.json()
#print (resp['token'])
df_Final = pd.DataFrame()
auth_token=resp['token']
hed = {'Authorization': 'Bearer ' + auth_token,'Content-Type' : 'application/json'}
role_search_URL=axon_login_url+'/unison/v1/unison/_search'
payload_input = {
   "searchScopes":[
      {
         "facetId":"DATASET",
         "fields":[
            "name",
			"refNumber",
			"dataSetId",
            "collectionCategoryNames",
            "collectionNames"

         ],
         "orderList":[
            {
               "field":"id",
               "type":"ASC"
            }
         ],
         "properties":{
            "offset":"0",
            "limit":"100",
            "childrenLevel":"0"
         }
      }
   ],
   "searchGroups":[
      {
         "operator":"START",
         "active":'true',
         "searches":[
            {
               "operator":"START",
               "active":'true',
               "facetId":"DATASET",
               "filterGroups":[
                  {
                     "operator":"START",
                     "filters":[
                        {
                           "operator":"START",
                           "type":"OPTIONS",
                           "properties":{
                              "field":"datasetPublishStatus",
                              "value":"Published"
                           }
                        }
                     ]
                  }
               ]
            }
         ]
      }
   ]
}

get_dmp_datasets=requests.post(role_search_URL,headers=hed,data=json.dumps(payload_input))
dmp_dsets=get_dmp_datasets.json()
str_items=dmp_dsets[0]['items']
my_dict = dict()
my_dataset = dict()
my_attributes = dict()
refid =""
#print(str_items)
for i in str_items:
    ref=i['ref']
    #refid=ref.split(':')[1]
    
        
    TbName = i['values'][0]
    refid = i['values'][1]
    try:
        refid=refid.split('-')[1]
    except:
        refid=i['values'][1]
    #print(refid)
    #print("referenceid " + refid)
    CategoryName= i['values'][3]
    items = []
    items.append(refid)
    items.append(TbName)
    items.append(CategoryName)
    my_dataset.setdefault(i['ref'],items)
    payload_dataelement = {
	"searchGroups": [
		{
			"operator": "START",
			"searches": [
				{
					"operator": "START",
					"facetId": "ATTRIBUTE",
					"filterGroups": []
				},
				{
					"operator": "AND",
					"facetId": "DATASET",
					"filterGroups": [
						{
							"operator": "START",
							"filters": [
								{
									"operator": "START",
									"type": "ID",
									"properties": {
										"field": "id",
										"value": refid
									}
								}
							],
							"filterGroups": []
						}
					]
				}
			]
		}
	],
	"searchScopes": [
		{
			"facetId": "ATTRIBUTE",
			"fields": [
		
				"name",
				"definition"
			],
			"orderList": [],
			"properties": {
				"offset": 0,
				"limit": -1
			}
		}
	]
    }
    get_dmp_datasets=requests.post(role_search_URL,headers=hed,data=json.dumps(payload_dataelement))
    dmp_dsets2=get_dmp_datasets.json()
    try:
        dataelement_items=dmp_dsets2[0]['items']
        atrr = []
        for i in dataelement_items:
            lst_DMP={'reference ID' : refid, 'Category Name' : CategoryName.split(',')[0], 'Table Name' : TbName,'Column Name' : i['values'][0],'Column Description' : i['values'][1]}   
            #Data Frame to add the List_DMP and DataElement
            df_Final=df_Final.append(lst_DMP, ignore_index = True)
        items.append(atrr)
        my_dict.setdefault(i['ref'],items)
    except:
        print('Skipping this reference ID as their are no columns' + ref)
    refid=" "
  

df = pd.DataFrame(data=df_Final)
df.to_excel("Output/CDMP_DS_DE.xlsx", index=False)

print("Dictionary converted into excel...")

# Login to CDMP started

