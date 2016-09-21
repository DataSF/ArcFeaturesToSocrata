# coding: utf-8

# ## Update Script

#!/usr/bin/env python
from ConfigUtils import *
from SocrataStuff import *
from EmailerLogger import *
from ArcPyToSocrata import *
from optparse import OptionParser
#handle update schedule--> we have the following update schedules
#daily, weekly, monthly, quartly, annually, as needed
# will make an option for each. 
helpmsgUpdateSchedule = 'Use the -u option plus update schedule. Update Schedule choices are daily, weekly, monthly, quarterly, annual, asNeeded'
parser = OptionParser(usage='usage: %prog [options] ')
parser.add_option('-u', '--updateSchedule',
                      type='choice',
                      action='store',
                      dest='updateSchedule',
                      choices=['daily', 'weekly', 'monthly', 'quarterly', 'annual', 'asNeeded'],
                      default=None,
                      help=helpmsgUpdateSchedule ,)
                      
helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig_MTA.yaml'
parser.add_option('-c', '--configfile',
                      action='store',
                      dest='configFn',
                      default=None,
                      help=helpmsgConfigFile ,)

helpmsgConfigDir = 'Use the -d to add directory path for the config files. EX: /home/ubuntu/configs'
parser.add_option('-d', '--configdir',
                      action='store',
                      dest='configDir',
                      default=None,
                      help=helpmsgConfigDir ,)
                      
(options, args) = parser.parse_args()
if  options.updateSchedule is None:
    print "ERROR: You must indicate an update schedule for the datasets!"
    print helpmsgUpdateSchedule
    exit(1)
elif  options.configFn is None:
    print "ERROR: You must specify a config yaml file!"
    print helpmsgConfigFile
    exit(1)
elif options.configDir is None:
    print "ERROR: You must specify a directory path for the config files!"
    print helpmsgConfigDir
    exit(1)
    
updateSchedule = options.updateSchedule
fieldConfigFile = options.configFn
config_inputdir = options.configDir

cI =  ConfigItems(config_inputdir ,fieldConfigFile  )
configItems = cI.getConfigs()
sc = SocrataClient(config_inputdir, configItems)
client = sc.connectToSocrata()
clientItems = sc.connectToSocrataConfigItems()
lte = logETLLoad(config_inputdir, configItems)
scrud = SocrataCRUD(client, clientItems, configItems)
aFTS = ArcFeatureToSocrata(config_inputdir, configItems, client)
fourXFour = configItems['fourXFour']
columnField = configItems['column_field']


datasets = aFTS.makeDataSets(updateSchedule)
datasetsAttributes = aFTS.getAttributesForDataSets()
finshedDatasets = []

print "****************UPDATING MTA DATASETS******************"

for dataset in datasets:
    dataset = aFTS.makeDataSetSchemaForSocrata(dataset, datasetsAttributes)
    if columnField in dataset.keys():
        print "****updating dataset:**************"
        print dataset['name']
        print
        print dataset['geotype']
        print 
        if "row_id" not in dataset.keys():
               dataset['row_id'] = ''
        if len(dataset[configItems['fourXFour']]) == 9:
           dataset = aFTS.postGeoData( dataset, scrud)
           finshedDatasets.append(dataset)


print "****************FINAL RESULTS************************************"
msg = lte.sendJobStatusEmail(finshedDatasets, updateSchedule)
client.close()
