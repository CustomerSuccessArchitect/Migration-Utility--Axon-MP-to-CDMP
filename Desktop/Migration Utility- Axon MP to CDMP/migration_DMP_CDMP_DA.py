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
            "collectionNames",

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

login_check=cloud_Login_URL+'/identity-service/api/v1/Login'
 
payload= {
        'username':cloud_Username,
            'password':cloud_Password
        }
noAuth="No Auth"
headers  = {"Content-Type": "application/json"}
#from requests.auth import HTTPBasicAuth
get_colAsset= requests.post(login_check,headers=headers,data=json.dumps(payload))
#print(get_colAsset.text)
resp=get_colAsset.json()


#print (resp['sessionId'])
sessionId=resp['sessionId']
clientId=resp['currentOrgName']


gen_jwt_token=cloud_Login_URL+'/identity-service/api/v1/jwt/Token'
headers1 = {
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'IDS-SESSION-ID': sessionId
 #'Authorization': 'Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y'
}

params = {"client_id":'cdmp_app', 'nonce':'123456' }         


get_jwt_token= requests.post(gen_jwt_token,headers=headers1,params=params)
#print(get_jwt_token.text)
jwt_token_resp=get_jwt_token.json()
jwt_token=jwt_token_resp['jwt_token']

df_link= df.copy()
df_link=df_link.drop(columns=['Column Description','Column Name','reference ID'], axis=1)
df_link["Collection ID"] = None
df_link["Dasset ID"] = None


#Create Category
list_Category= df['Category Name']
df_category = pd.DataFrame(list_Category)
#drop duplicates of category
df_category.drop_duplicates(inplace=True)
df_category.reset_index(drop=True, inplace=True)

df35=pd.DataFrame(columns = ['Category Name', 'Cate_ID','DC_Name'])
#cre_category=createCategory(row, file_name)



#Get usgae type ID of  Access Request    
usageType=CDMP_URL+'/cdmp-marketplace/api/v1/usageContext'
headers_us  = {'Authorization': 'Bearer ' + jwt_token,'IDS-SESSION-ID': sessionId,"Content-Type": "application/json"}

get_usageTypeId= requests.get(usageType,headers=headers_us)
#print(get_usageTypeId.text)
usageTypeId=get_usageTypeId.json()
#Access Request ID - used in the data collection creation
ac=usageTypeId['objects'][0]['id']


for index, y in df_category.iterrows():
    #print (df_category[row].to_string(index=False))
    c_name=y['Category Name'] 
    #createCategory(df_category['Category Name'])
    crte_category=CDMP_URL+'/cdmp-marketplace/api/v1/categories'
   # print("Category :"+ crte_category)

    payload2 ={
      "name": c_name,
      "description": "Created from Script",
      "status": "ACTIVE"
    }
    #print(df_category['Category Name'].to_string(index=False))
    #print(payload2)
    headers12  = {'Authorization': 'Bearer ' + jwt_token,'IDS-SESSION-ID': sessionId,"Content-Type": "application/json"}
    create_cat= requests.post(crte_category,headers=headers12,data=json.dumps(payload2))
    #print(create_cat.text)
    create_category=create_cat.json()
    #print(create_category['id'])
    get_cate_id = create_category['id']
    
    
#create Data Collections
    crt_dc=CDMP_URL+'/cdmp-marketplace/api/v1/dataCollections'
   # print("Data Collection :" +crt_dc)
    payload_dc = {
      "name": c_name,
      "description": "Created from Script",
      "categoryId": get_cate_id,
      "usageContextIds": [ac],
      "dataCollectionStatus": "PUBLISHED"
    }
    #print(payload_dc)
    headers_dc  = {'Authorization': 'Bearer ' + jwt_token,'IDS-SESSION-ID': sessionId,"Content-Type": "application/json"}
    create_dc= requests.post(crt_dc,headers=headers_dc,data=json.dumps(payload_dc))
    #print (create_dc.text)
    create_dc_id=create_dc.json()
    #print(create_category['id'])
    get_dc_id = create_dc_id['id']
    
#get data collection id to dataframe
    
    xyz=get_dc_id
    #print(c_name,xyz)
    #str3 =[]
        #elements = elements.astype('string')
    for index, j in df_link.iterrows():
        if c_name==j['Category Name']:
            #print(j['Category Name'])
            j['Collection ID']=xyz
            #print(j['Collection ID'])
    


tablename= df['Table Name']
df_tn = pd.DataFrame(tablename)
#df_tn['Column Name']=df['Column Name']
#drop duplicates of category
df_tn.drop_duplicates(inplace=True)
df_tn.reset_index(drop=True, inplace=True)
elements=pd.DataFrame(columns = ['Column Name'])

for index, k in df_tn.iterrows():
    #print(df['Column Name'])
     if df_tn['Table Name'][index]== df['Table Name'][index]:
         #print(df['Column Name'])
         pass
            
     else:
        #print(k['Table Name'])
        data_Asset =CDMP_URL+'/cdmp-marketplace/api/v1/dataAssets'
        print('Data Asset'+ data_Asset)
        headers_da  = {'Authorization': 'Bearer ' + jwt_token,'IDS-SESSION-ID': sessionId,"Content-Type": "application/json"}
        payload_da = {
          "name": k['Table Name'],
          "description": "string",
          "source": "Axon Data MarketPlace",
          "type": "Table",   
          "status": "ENABLED"  
        }
        #print(payload_da)
        create_da= requests.post(data_Asset,headers=headers_da,data=json.dumps(payload_da))
        #print (create_da.text)
        da_create=create_da.json()
        get_da_id = da_create['id']
        
#addind da id to dataframe
        t_name=k['Table Name']
        abc=get_da_id
        #print(t_name,abc)
        #elements = elements.astype('string')
        for index, n in df_link.iterrows():
            if t_name==n['Table Name']:
                #print(n['Table Name'])
                #print(t_name)
                n['Dasset ID']=abc  
                #print(n['Dasset ID'])

        
#create data elements for data asset
        da_elements=CDMP_URL+'/cdmp-marketplace/api/v1/dataAssets/'
        url_de= da_elements + get_da_id + "/dataElements"
        print('Data Elements :'+ url_de)
        #print(url_de)
         
        str1=k['Table Name']
        str3 =[]
        for index, t in df.iterrows():
            if  str1==t['Table Name']:
                #print(t['Column Name'])
                str2=t['Column Name']
                str3.append(str2)
            else:
                pass
         
        df_str3=pd.DataFrame(str3)
        #print(df_str3[0])
        headers_de  = {'Authorization': 'Bearer ' + jwt_token,'IDS-SESSION-ID': sessionId,"Content-Type": "application/json"}
        #print(get_da_id)
        #params={'id': get_da_id}      
        
        
        for index, m in df_str3.iterrows():
            #print(m[0])
            payload_e = {
                  "name": m[0],  
                  "type": "Column",
                  "status": "ENABLED"
                }
            #print(payload_e)
            create_el= requests.post(url_de,headers=headers_de,data=json.dumps(payload_e))
            #print(create_el.text)



#Bulk link of dataassets to data collections
            
bulk_da_dc=CDMP_URL+'/cdmp-marketplace/api/v1/dataAssets/linkedDatacollections/bulk'
head_da_dc  = {'Authorization': 'Bearer ' + jwt_token,'IDS-SESSION-ID': sessionId,"Content-Type": "application/json"}

for index, d in df_link.iterrows(): 
    if (d['Dasset ID']!= None and d['Collection ID']!=None):
        payload_bulk = {
          "items": [
        {
           "dataCollectionId": d['Collection ID'],
           "dataAssetId": d['Dasset ID']
         }
        ]
        } 
        bulk_link= requests.post(bulk_da_dc,headers=head_da_dc,data=json.dumps(payload_bulk)) 
        #print(bulk_link.text)
    else:
        pass
    
print ("Migration Complete from Axon to CDMP")
    
 
 

 
 

    
 



