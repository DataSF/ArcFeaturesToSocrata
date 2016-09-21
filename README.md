# ArcFeaturesToSocrata

### Extracts geodata from an ArcGis Feature Service and dumps the geodata onto Socrata

### This repo does the following:
* grabs geodata (points, lines, shapes, etc plus attributes) from an ARGIS Online rest api
* grabs the data in web mercator and converts it to to geojoson
* Creates the initial dataset schema in Socrata from the schema found in the geojson
* Inserts/Uploads data from the argis rest api to Socrata using the socrata API; sends the data in chunks as json post requests as opposed to trying to upload an entire file object.
* Supports multiple retries; checks to make sure all the data in the shape file made it up to the socrata API.
* Sends email notifications to receipts to let users know if the job was successful or notifications
* Also includes scripts for updating/upserting the geodatasets

### Other features of this library include:
* config files to set up socrata client and emailer
* config file to set up schema of initial dataset load list and various directories used in the scripts
* Important to note, you will need to provide a csv that outlines the geodatasets that your are trying to migrate/update. For an example: see the file configs/weekly_MTA_Publishing_Tracker.csv
* You will need to define which fields from the rest api you want to keep and how you will rename them. Sees the the file, configs/MTA_FieldDocumentation_AttributeCompleteList.csv for an example
* this library can be called through command line arguements. Args are -u -> update schedule for dataset, -c config file name, -d directory of where to to find the config files. 
* `Usage example: python  /home/ubuntu/mtaGeo/pydev/ArcPyToSocrataUpdateLoad.py -u weekly -c fieldConfig_MTA.yaml -d /home/ubuntu/mtaGeo/configs/ > /home/ubuntu/mtaGeo/logs/weeklyGeo_log.txt`
* test change
