#!/bin/bash
echo "Starting the weekly updata of MTA datasets: $(date)"
/home/ubuntu/miniconda2/bin/python /home/ubuntu/mtaGeo/pydev/ArcPyToSocrataUpdateLoad.py -u weekly -c fieldConfig_MTA.yaml -d /home/ubuntu/mtaGeo/configs/ > /home/ubuntu/mtaGeo/logs/weeklyGeo_log.txt


