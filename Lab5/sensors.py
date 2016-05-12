import mraa
import math
import time
import boto
import boto.dynamodb2
import datetime

from boto import kinesis
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item

from decimal import Decimal

switch_pin_number=8



ACCOUNT_ID = ''
IDENTITY_POOL_ID = ''
ROLE_ARN = ''

# Use cognito to get an identity.
cognito = boto.connect_cognito_identity()
cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
oidc = cognito.get_open_id_token(cognito_id['IdentityId'])

# Further setup your STS using the code below
sts = boto.connect_sts()
assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])
DYNAMODB_TABLE_NAME = 'rawData'

client_dynamo = boto.dynamodb2.connect_to_region(
    'us-east-1',
    aws_access_key_id=assumedRoleObject.credentials.access_key,
    aws_secret_access_key=assumedRoleObject.credentials.secret_key,
    security_token=assumedRoleObject.credentials.session_token)

from boto.dynamodb2.table import Table
table_dynamo = Table(DYNAMODB_TABLE_NAME, connection=client_dynamo)

tables = client_dynamo.list_tables() 
if DYNAMODB_TABLE_NAME not in tables['TableNames']:
    print DYNAMODB_TABLE_NAME+' not found, creating table, wait 15s'
    table = Table.create(DYNAMODB_TABLE_NAME, schema=[
       HashKey('timestamp'), # defaults to STRING data_type
       RangeKey('timestamp'),],connection=client_dynamo)
    time.sleep(15)
else:

    table = Table(DYNAMODB_TABLE_NAME, schema=[
       HashKey('timestamp'),
       RangeKey('timestamp'),
       ],connection=client_dynamo)  
    print DYNAMODB_TABLE_NAME+' selected' 

def put_in_raw_table(timestamp,temp,light,sound):
    table.put_item(
                    Item(table, data={ 'timestamp': str(timestamp),
                           'temperature': str(temperature),
                           'light': str(light),
                           'sound': str(sound)),
                         })
                      )



#users = Table('temperature2',schema=[
#     HashKey('ID'),
#     RangeKey('temperature'),
#],connection=client_dynamo)



lightSensor = mraa.Aio(0)
tempSensor = mraa.Aio(1)
soundSensor = mraa.Aio(2)


try:
  while(1):
    #collect the temperature data and make temperature readable
    a=tempSensor.read()
    R=1023.0/a-1.0
    R=100000.0*R
    temperature=Decimal(1.0/(math.log(R/100000.0)/4275+1/298.15)-273.15)
    temperature2=str(round(temperature,2))

    #collect the light data
    light = lightSensor.read()

    #collect the sound data
    sound =  soundSensor.read()

    timestamp = time.ctime()
    #add(ID,temperature2)
    print "temperature: %s " %(temperature2)
    print "light: %s " %(str(light))
    print "sound: %s " %(str(sound))

    put_in_raw_table(timestamp,temp,light,sound)
    time.sleep(1)

except KeyboardInterrupt:
        exit(0)