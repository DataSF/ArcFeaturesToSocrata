
# coding: utf-8

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
import csv
import time
import datetime
import logging
from retry import retry
import yaml
import os
import itertools
import base64
import inflection
import csv, codecs, cStringIO
from ConfigUtils import *

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class pyLogger:
    def __init__(self, configItems):
        self.logfn = configItems['exception_logfile']
        self.log_dir = configItems['log_dir']
        self.logfile_fullpath = self.log_dir+self.logfn

    def setConfig(self):
        #open a file to clear log
        fo = open(self.logfile_fullpath, "w")
        fo.close
        logging.basicConfig(level=logging.WARN, filename=self.logfile_fullpath, filemode='w')
        logger=logging.getLogger(__name__)
	#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	#logger = logger.setFormatter(formatter)
	return logger

# In[ ]:

class emailer():
    '''
    util class to email stuff to people.
    '''
    def __init__(self, inputdir, configItems):
        self.inputdir = inputdir
        self.configItems = configItems
        self.emailConfigs = self.getEmailerConfigs()
        
        
    def getEmailerConfigs(self):
        emailConfigFile = self.inputdir + self.configItems['email_config_fname']
        with open(emailConfigFile,  'r') as stream:
            try:
                email_items = yaml.load(stream)
                return email_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0
    
    def setConfigs(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        self.server = self.emailConfigs['server_addr']
        self.server_port = self.emailConfigs['server_port']
        self.address =  self.emailConfigs['email_addr']
        self.password = base64.b64decode(self.emailConfigs['email_pass'])
        self.msgBody = msgBody
        self.subjectLine = subject_line
        self.fname_attachment = fname_attachment
        self.fname_attachment_fullpath = fname_attachment_fullpath
        self.recipients = self.emailConfigs['receipients']
        self.recipients =  self.recipients.split(",")
    
    def getEmailConfigs(self):
        return self.emailConfigs
    
    def sendEmails(self, subject_line, msgBody, fname_attachment=None, fname_attachment_fullpath=None):
        self.setConfigs(subject_line, msgBody, fname_attachment, fname_attachment_fullpath)
        fromaddr = self.address
        toaddr = self.recipients
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = ", ".join(toaddr)
        msg['Subject'] = self.subjectLine
        body = self.msgBody 
        msg.attach(MIMEText(body, 'plain'))
          
        #Optional Email Attachment:
        if(not(self.fname_attachment is None and self.fname_attachment_fullpath is None)):
            filename = self.fname_attachment
            attachment = open(self.fname_attachment_fullpath, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
        
        #normal emails, no attachment
        server = smtplib.SMTP(self.server, self.server_port)
        server.starttls()
        server.login(fromaddr, self.password)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()


# In[156]:

class logETLLoad:
    '''
    util class to get job status- aka check to make sure that records were inserted; also emails results to receipients
    '''
    def __init__(self, inputdir, configItems):
        self.keysToRemove = ['columns', 'tags']
        self.log_dir = configItems['log_dir']
        self.dataset_base_url = configItems['dataset_base_url']
        self.failure =  False
        self.job_name = configItems['job_name']
        self.logfile_fname = self.job_name + ".csv"
        self.logfile_fullpath = self.log_dir + self.job_name + ".csv"
        self.configItems =  configItems
        self.inputdir = inputdir
        self.rowsInserted = configItems['dataset_records_cnt_field'] 
        self.shp_records = configItems['src_records_cnt_field']
        self.fourXFour = configItems['fourXFour']
        self.datasetNameField = configItems['dataset_name_field']
        
    def removeKeys(self, dataset):
        for key in self.keysToRemove:
            try:
                remove_columns = dataset.pop(key, None)
            except:
                noKey = True
        return dataset
    
    def sucessStatus(self, dataset):
        dataset = self.removeKeys(dataset)
        if dataset[self.rowsInserted ] == dataset[self.shp_records]:
              dataset['jobStatus'] = "SUCCESS"
        else: 
            dataset['jobStatus'] = "FAILURE"
            self.failure =  True
        return dataset
    
    def makeJobStatusAttachment(self,  finishedDataSets ):
        with open(self.logfile_fullpath, 'w') as csvfile:
            fieldnames = finishedDataSets[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dataset in finishedDataSets:
                try:
                    writer.writerow({ s:str(v).encode("ascii",  'ignore') for s, v in dataset.iteritems()  } )
                except: 
                    print "could not write row"

    def getJobStatus(self, updateSchedule):
        if self.failure: 
            return  "FAILED: " + inflection.titleize(updateSchedule) + " " + self.job_name
        else: 
            return  "SUCCESS: " + inflection.titleize(updateSchedule) + " " +  self.job_name 

    def makeJobStatusMsg( self,  dataset  ):
        msg = dataset['jobStatus'] + ": " + dataset[self.datasetNameField] + "-> Total Rows:" + str(dataset[self.shp_records]) + ", Rows Inserted: " + str(dataset[self.rowsInserted])  + ", Link: "  + self.dataset_base_url + dataset[self.fourXFour] + " \n\n " 
        return msg
    
    def sendJobStatusEmail(self, finishedDataSets, updateSchedule):
        msgBody  = "" 
        for i in range(len(finishedDataSets)):
            #remove the column definitions, check if records where inserted
            dataset = self.sucessStatus( self.removeKeys(finishedDataSets[i]))
            msg = self.makeJobStatusMsg( dataset  )
            msgBody  = msgBody  + msg
        subject_line = self.getJobStatus( updateSchedule )
        email_attachment = self.makeJobStatusAttachment(finishedDataSets)
        e = emailer(self.inputdir, self.configItems)
        emailconfigs = e.getEmailConfigs()
        if os.path.isfile(self.logfile_fullpath):
            e.sendEmails( subject_line, msgBody, self.logfile_fname, self.logfile_fullpath)
        else:
            e.sendEmails( subject_line, msgBody)
        print "****************JOB STATUS******************"
        print subject_line
        print "Email Sent!"
    
    
    
if __name__ == "__main__":
    main()





