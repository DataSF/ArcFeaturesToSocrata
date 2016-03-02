
# coding: utf-8

# In[625]:

import re
import csv
import numpy as np
import inflection
import re
import datetime
import os
from __future__ import division
import requests
from sodapy import Socrata
import yaml
import base64
import arcgis #https://github.com/Schwanksta/python-arcgis-rest-query
import itertools
import datetime
import bson
import json
import pyproj
import time 


# In[675]:

class ArcFeatureToSocrata:
    
    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile
        #self.clientConfigFile = clientConfigFile
        self.configItems = getConfigs( self.inputdir, self.fieldConfigFile)
        self.client = connectToSocrata(inputdir + self.configItems['socrata_client_config_fname'])
        self.field_documentation_field = self.configItems['field_documentation_field']
        self.feature_service_endpoint_field = self.configItems['feature_service_endpoint_field']
        self.layer_field = self.configItems['layer_field']
        self.category_field = self.configItems['category_field']
        self.tags_field = self.configItems['tags_field']
        self.type_field = self.configItems['type_field']
        self.socrata_name_field = self.configItems['socrata_dataset_name_field']
        self.layer_name_field = self.configItems['layer_name_field']
        self.agency_prefix = self.configItems['agency_prefix']
        self.source_attribute_field = self.configItems['source_attribute_field']
        self.destination_attribute_field = self.configItems['destination_attribute_field']
        self.srid_projection = str(self.configItems['srid_projection'])
        self.dataset_base_url = self.configItems['dataset_base_url']
        
    def makeDataSets(self):
        #load the publishing tracker
        schemaLayout, datasets = make_headers_and_rowobj(self.configItems['inputDataDir'] + self.configItems['pubtracker'])
        return datasets
    
    def getAttributesForDataSets(self):
        #load the attribute list
        datasetSchema, allAttributes =  make_headers_and_rowobj(self.configItems['inputDataDir'] + self.configItems['attribute_list'])
        return allAttributes

    def setupDataSetSchema(self, dataset, allAttributes):
        #skip over datasets that are complete or haven't been started\
        if dataset[self.field_documentation_field].lower() == "draft":
            query = 'f=json'
            urlbase = dataset[self.feature_service_endpoint_field]
            layer = dataset[self.layer_field]
            argisFields = getArgisDataType(urlbase, layer, query)
            if argisFields:
                dataset['category'] = dataset[self.category_field]
                dataset['tags'] = dataset[self.tags_field].split(';')
                dataset['geotype'] =  dataset[self.type_field]
                dataset['name'] = dataset[self.socrata_name_field]
                #wkid is the well known identifier for the projection associated with the dataset
                dataset['wkid'] = argisFields['extent']['spatialReference']['latestWkid']
                argisFieldsList, dataset['description'], geometryType = parseDataSetDescription(argisFields, 'fields', 'description', 'geometryType')
                #now get the columns from the attribute definition
                fname_dataset = dataset[self.layer_name_field].lower()
                filterkeys= self.agency_prefix +"."+ fname_dataset
                attributes = filterDictListOnKeyVal(allAttributes, 'Feature Class', [filterkeys])
                #datasetSchema, attributes =  make_headers_and_rowobj(inputdir +fname_dataset)
                dataset['columns'] = self.makeColumns(attributes, argisFieldsList, geometryType)
        return dataset
    
       
    def makeDataSetSchemaForSocrata(self, dataset, allAttributes):
        dataset = self.setupDataSetSchema(dataset, allAttributes)
        if 'columns' in dataset.keys() and 'name' in dataset.keys():
            if len(dataset['columns']) > 1:
                socrataColumnsToKeep = ['name', 'fieldName', 'dataTypeName']
                dataset['socrataColumns'] = filterDictList(dataset['columns'], socrataColumnsToKeep) 
                dataset['socrataColumns'] = [x for x in dataset['socrataColumns'] if len(x.keys()) > 0]
        return dataset

    def makeColumns(self, attributes, argisFieldsList, geometryType):
        regex=re.compile("(.*Do Not Publish.*)|(.*Missing*.)")
        keysToKeep = ['SourceAttribute', 'DestinationAttribute']
        if not( attributes is None):
            columns = []
            for k in range(len(attributes)):
                #filter the do not publish
                kvals = attributes[k].values()
                if len([m.group(0) for l in kvals for m in [regex.search(l)] if m]) == 0:
                    item =  attributes[k][self.source_attribute_field].lower()
                    for itemdict in argisFieldsList:
                        if itemdict['name'].lower() == item:
                            attributes[k]['dataTypeName'] = itemdict['type']
                            attributes[k]['name'] =  attributes[k][self.destination_attribute_field]
                            attributes[k]['fieldName'] = attributes[k][self.destination_attribute_field].lower().replace(" ", '_')
                    columns.append(attributes[k])
                #now add the Geom Column
            geomDict = {'name': 'Geom', 'SourceAttribute': 'geometry', 'fieldName': 'geom','dataTypeName': geometryType }
            columns.append( geomDict)
            columns = mapEsriDataTypes(columns)
            return columns
        else:
            return False
        
    def createGeodataSet(self, dataset):
        if dataset['geotype'] == 'Point':
            new_backend = False
        else:
            new_backend = True
       
        print dataset['socrataColumns']
        print 
        print dataset['description']
        print
        print dataset['tags']
        print
        socrata_dataset = self.client.create(dataset['name'], description=dataset['description'], columns=dataset['socrataColumns'], tags=dataset['tags'], category=dataset['category'], new_backend=new_backend)
        fourXFour = str(socrata_dataset['id'])
        dataset['Dataset URL'] = self.dataset_base_url + fourXFour
        dataset['fourXFour'] = fourXFour
        print "4x4 "+ dataset['fourXFour']
        #try:
            #socrata_dataset = self.client.create(dataset['name'], description=dataset['description'], columns=dataset['socrataColumns'], tags=dataset['tags'], category=dataset['category'], new_backend=new_backend)
            #fourXFour = str(socrata_dataset['id'])
            #dataset['Dataset URL'] = self.dataset_base_url + fourXFour
            #dataset['fourXFour'] = fourXFour
            #print "4x4 "+ dataset['fourXFour']
        #except:
            #$dataset['Dataset URL'] = ''
            #dataset['fourXFour'] = 'Error: did not create dataset'
            #print "4x4 "+ dataset['fourXFour']
        return dataset
    
    def insertGeodataSet(self, dataset):
        insertDataSet = []
        try:
            insertDataSet = self.getDataForSocrataInsert(dataset, dataset['columns'])
        except:
            result = 'Error: could not get data'
            dataset['result'] =  result
            return dataset
        #need to chunk up dataset so we dont get Read timed out errors
        if len(insertDataSet) > 1000 and (not(insertDataSet is None)):
            results = []
            #chunk it
            insertChunks=[insertDataSet[x:x+1000] for x in xrange(0, len(insertDataSet), 1000)]
            #overwrite the dataset on the first insert
            try:
                result = self.client.replace(dataset['fourXFour'], insertChunks[0])
                print result 
                results.append(result)
            except:
                result = 'Error: did not insert dataset chunk'
            for chunk in insertChunks[1:]:
                try:
                    result = self.client.upsert(dataset['fourXFour'], chunk)
                    print result
                    results.append(result)
                except:
                    result = 'Error: did not insert dataset chunk'
            dataset['result'] =  results
            print results
        elif len(insertDataSet) > 0 and len(insertDataSet) < 1000:
            print insertDataSet[0]
            result = self.client.replace(dataset['fourXFour'], insertDataSet) 
            try:
                result = self.client.replace(dataset['fourXFour'], insertDataSet) 
                dataset['result'] =  result
                print dataset['result']
            except:
                dataset['result'] = 'Error: did not insert dataset'
                print "result: "+ dataset['result']
        return dataset
    
    def getDataForSocrataInsert(self, dataset, columns):
        #get a list of all the argis source columns
        argisSourceColumns =  list(itertools.chain(*[column.values() for column in filterDictList(columns, [ self.source_attribute_field]) ]))
        argisSourceColumnDataTypes =  filterDictList(columns, [ self.source_attribute_field, 'fieldName', 'dataTypeName'])
        datasetOut = []
        layer = dataset[self.layer_field]
        try: 
            featureService = arcgis.ArcGIS( dataset[ self.feature_service_endpoint_field] )
            print dataset[self.feature_service_endpoint_field]
        except:
            print "Error: Something went wrong: Couldnt connect to feature service"
    
            #We can specify what projection we want when we query the ARGIS service by using the srid param
            #https://github.com/Schwanksta/python-arcgis-rest-query
            #find out the number of items on the service
   
        try:
            shapeCnt = featureService.get(layer, count_only=True)
            #print featureService.get_descriptor_for_layer(layer)
            print 
            print "Number of records:" + str(shapeCnt)
        except:
            print "Error: Something went wrong: Couldnt get layer count from Feature Service"
        try:
            shapes = featureService.get(layer, fields=argisSourceColumns, srid=self.srid_projection)
            for shape in shapes['features']:
                try:
                    row = filterDict(shape['properties'] , argisSourceColumns)
                except:
                    print "could not filter row"
                dataRow = {}
                try:
                    dataRow = columnLookup( row, dataRow, argisSourceColumnDataTypes )
                except:
                    print "could not find column in lookup"
                try:
                    dataRow['geom'] = formatGeodata(shape['geometry'])
                except:
                    print
                    print "no geom"
                datasetOut.append(dataRow)
        except:
            print "Error: Something went wrong: Couldnt get attributes from feature service"
            try:
                print featureService.get_json(layer, count_only=True)
            except: 
                print "couldn't get attributes as json count"
        return datasetOut



# In[676]:

def columnLookup( row, dataRow, argisSourceColumnDataTypes ):
    for key,val in row.iteritems():
        columnLookup =  (item for item in argisSourceColumnDataTypes if item["SourceAttribute"] == key).next()
        if columnLookup['dataTypeName'] == 'date':
            if not( row[key] is None):
                row[key] = datetime.datetime.fromtimestamp(row[key]/1000.0)
                try:
                    row[key] =   str(row[key].strftime("%m/%d/%Y"))
                except ValueError:
                    row[key] = None
                    dataRow[columnLookup['fieldName']]  = row[key]
                    #dataRow[columnLookup['fieldName']] = json.dumps( row[key], default=date_handler)
            else:
                dataRow[columnLookup['fieldName']] = None
        elif columnLookup['dataTypeName'] == 'text':
            try:
                dataRow[columnLookup['fieldName']] = inflection.titleize(row[key].lower())
            except:
                dataRow[columnLookup['fieldName']]  = row[key]
        else:
            dataRow[columnLookup['fieldName']] = row[key]
    return dataRow


# In[677]:

def formatGeodata(geom):
    if not(geom is None):
        if geom['type'] == 'Point':
            geom =[str(x) for x in geom['coordinates']]
            geom = "(" + ",".join(geom) + ")"
            return geom
        else:
            geom['type'] = mapEsriToSocrata(geom['type'])
            if geom['type'] == 'Line':
                geom['coordinates'] = geom['coordinates'][0]
                geom['type'] = 'LineString'
            return geom
    return geom


# In[678]:

def getConfigs(inputdir, fieldConfigFile):
    fieldItems = 0
    with open(inputdir + fieldConfigFile ,  'r') as stream:
        try:
            fieldItems = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return fieldItems


# In[679]:

def connectToSocrata(clientConfigFile):
    with open(clientConfigFile,  'r') as stream:
        try:
            client_items = yaml.load(stream)
            client = Socrata(client_items['url'],  client_items['app_token'], username=client_items['username'], password=base64.b64decode(client_items['password']))
            return client
        except yaml.YAMLError as exc:
            print(exc)
    return 0


# In[680]:

def make_headers_and_rowobj(fname, keysToKeep=None):
    with open(fname, 'rb') as inf:
        dictList = []
        reader = csv.DictReader(inf)
        for row in reader:
            dictList.append(row)
        schemaLayout = dictList[0].keys()
        if(keysToKeep == None):
            keysToKeep = schemaLayout
        useful = filterDictList(dictList, keysToKeep) 
        return schemaLayout,useful,


# In[681]:

def filterDictList(dictList, keysToKeep):
    return  [ {key: x[key] for key in keysToKeep if key in x.keys() } for x in dictList]

def filterDict(mydict, keysToKeep):
    mydictKeys = mydict.keys()
    return {key: mydict[key] for key in keysToKeep  if key in mydictKeys }

def filterDictListOnKeyVal(dictlist, key, valuelist):
    #filter list of dictionaries with matching values for a given key
    return [dictio for dictio in dictlist if dictio[key] in valuelist]



# In[682]:

def getArgisDataType(urlbase, layer, query):
    url = urlbase +"/" + layer+ "/?" + query
    response = requests.get(url)
    if response.status_code == 200:
        response_json = response.json()
        if 'error' in response_json.keys():
            return False
        else:
            return response_json
    else:
        return False


# In[683]:

def mapEsriToSocrata(val):
    dataTypesDict = { 'esriFieldTypeDate': 'date', 'esriGeometryPoint':'location', 
                      'esriGeometryPolygon':'polygon', 'esriFieldTypeString':'text', 
                      'esriFieldTypeDouble':'number',  'esriGeometryPointLine':'Line' ,
                      'esriFieldTypeOID': 'number', 'esriFieldTypeInteger': 'number', 
                      'esriFieldTypeSmallInteger': 'number', 'esriGeometryPolyline': 'Line', 
                      'MultiLineString': 'Line'}
    dataTypesDictKeys = dataTypesDict.keys()
    if val in dataTypesDictKeys:
        return dataTypesDict[val]
    else:
        return val
  
def mapEsriDataTypes(dictList):
    return [{k: mapEsriToSocrata(v) for k, v in mydict.iteritems()} for mydict in dictList]


# In[684]:

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


# In[685]:

def parseDataSetDescription(argisFields, fieldColumn, descriptionColumn, geometryTypeColumn):
    argisFieldsList = argisFields[fieldColumn]
    description = argisFields[ descriptionColumn]
    geometryType =  argisFields[ geometryTypeColumn]
    return argisFieldsList, description, geometryType


# In[701]:

def main():
    inputdir = '/home/ubuntu/workspace/mta_test/'
    fieldConfigFile = 'fieldConfig.yaml'
    aFTS = ArcFeatureToSocrata(inputdir, fieldConfigFile)
    datasets = aFTS.makeDataSets()
    datasetsAttributes = aFTS.getAttributesForDataSets()
    finalOutputStatus = []
    for dataset in datasets:  
        print "*****"
        dataset = aFTS.makeDataSetSchemaForSocrata(dataset, datasetsAttributes)
        if 'socrataColumns' in dataset.keys():
            print "******"
            print dataset['name']
            print
            print dataset['geotype']
            print 
            if (len(dataset['fourXFour']) == 0) and (dataset['Field Documentation'] == 'Draft'):
                #insertDataSet = aFTS.getDataForSocrataInsert(dataset, dataset['columns'])
                #print insertDataSet[0]
                dataset = aFTS.createGeodataSet(dataset)
                if len(dataset['fourXFour']) >2 :
                    #need to sleep so we can give socrata a chance to update itself
                    time.sleep(5)
                    aFTS.dataset = aFTS.insertGeodataSet(dataset) 
            elif (dataset['Field Documentation'] == 'Draft') and  (len(dataset['fourXFour']) > 0):
                dataset = aFTS.insertGeodataSet(client, dataset)
                print dataset
        else:
            dataset['fourXFour'] = 'Error: did not create dataset'
        finalOutputStatus.append(dataset)


# In[702]:

if __name__ == '__main__' and '__file__' in globals():
    main()


# In[703]:

#main()


# In[ ]:



