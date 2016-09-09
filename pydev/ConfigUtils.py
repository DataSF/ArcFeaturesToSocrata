# coding: utf-8

# ## Update Script

#!/usr/bin/env python


from __future__ import division
import geopandas as gpd
import shutil
import fiona
import inflection
import pandas as pd
import re
import csv
import inflection
import re
import datetime
import os
import requests
from sodapy import Socrata
import yaml
import base64
import itertools
import datetime
import bson
import json
import time 
import logging
from retry import retry
from shapely.geometry import mapping, shape
import urllib2
import zipfile
from fiona.crs import from_epsg
from retry import retry
from shapely.geometry import Polygon
from socket import error as SocketError
import errno


class ConfigItems:
    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile
        
    def getConfigs(self):
        configItems = 0
        with open(self.inputdir + self.fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems

if __name__ == "__main__":
    main()