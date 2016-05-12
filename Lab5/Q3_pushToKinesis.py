# This program sends the data to kinesis. You do not need to modify this code except the Kinesis stream name.
# Usage python pushToKinesis.py <file name>
# a lambda function will be triggered as a result, that will send it to AWS ML for classification
# Usage python pushToKinesis.py <csv file name with extension>

import sys,csv,json, time

import boto3

sys.path.append('../utils')
import aws


KINESIS_STREAM_NAME = "mta_Kinesis"


def main(fileName):
    
    # connect to kinesis
    kinesis = aws.getClient('kinesis','us-east-1')
    data = [] # list of dictionaries will be sent to kinesis
    
    with open(fileName,'rb') as f:
    	dataReader = csv.DictReader(f)
        for row in dataReader:
            #time.sleep(1)
            kinesis.put_record(StreamName=KINESIS_STREAM_NAME, Data=json.dumps(row), PartitionKey='0')
            #print json.dumps(row)
            #break
        f.close() 
    
    '''
    to test if Kinesis works properly
    while(1):
        kinesis.put_record(StreamName=KINESIS_STREAM_NAME, Data="json.dumps(row)", PartitionKey='0')
        time.sleep(2)
    '''    



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Missing arguments"
        sys.exit(-1)
    if len(sys.argv) > 2:
        print "Extra arguments"
        sys.exit(-1)
    try:
        fileName = sys.argv[1]
        main(fileName)
    except Exception as e:
        pass
