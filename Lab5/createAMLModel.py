# Creating aws machine learning model
# This program uploads the finalData.csv file to S3, and used it as a data source to train a binary 
# classification model
import time,sys,random

import boto3

from S3 import S3

sys.path.append('../utils')
import aws

TIMESTAMP  =  time.strftime('%Y-%m-%d-%H-%M-%S')
S3_BUCKET_NAME = "mtadatamon8"
S3_FILE_NAME = 'banking.csv'
S3_URI = "s3://{0}/{1}".format(S3_BUCKET_NAME, S3_FILE_NAME)
DATA_SCHEMA = "aml.csv.schema"

s3 = S3(S3_FILE_NAME)
s3.uploadData()

