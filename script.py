import pandas as pd
import boto3
import io
import os
import datetime
from datetime import timezone

# Get current date and time
dt = datetime.datetime.now(timezone.utc)

# Environment Variables
DAY = '14'
MONTH = '12'
PREFIX = f"2022/{MONTH}/{DAY}/{dt.hour}/"
BUCKET_NAME = "test-simulator-data"
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')

# Temporary dictionary for associating MAC addresses (beacons) with zones
zone_database = {
    '06DD1C' : 'zone_1',
    '06DFD7' : 'zone_2',
    '06E005' : 'zone_3',
    '06DFCC' : 'zone_4',
    '06DD3B' : 'zone_5',
    '06E213' : 'zone_6',
    '06715A' : 'zone_7',
    '067414' : 'zone_8',
}

# Initialize client and resource
client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key
 
)
buffer = io.BytesIO()
s3 = boto3.resource(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key

)

# Functions
def access_latest_S3_object():
    """
    Accesses the latest S3 object and puts the data into a global data frame
    """
    try:
        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%Y%m%d%H%M%S'))

        objs = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)['Contents']

        for obj in sorted(objs, key=get_last_modified, reverse=True):
            p = obj['Key']
            break

        object = s3.Object(BUCKET_NAME, p)
        object.download_fileobj(buffer)
        buffer.seek(0)
        df = pd.read_csv(buffer, header=None)
        
        return df
    except:
        'There was an issue accessing the latest S3 object'

def get_device_data_frame(data_frame, device_name):
    """
    Isolates specific device and its readings from global data frame
    """
    df = data_frame[data_frame[1] == device_name]
    individual_df = df.reset_index(drop=True)

    return individual_df

def determine_mode_and_zone(df, device_name):
    """
    This will take a the strongest RSSI value column, determine the mode, 
    and use it to place the device in a zone based off of its associated MAC address
    """
    # Get the mode of the strongest RSSI list (sorted into column 18 from the payload)
    df2 = df[df.columns[18]]
    mode_string = df2.mode()[0]

    # Match that string to its associated zone
    for (key,value) in zone_database.items():
        if key == mode_string:
            print(f'Device {device_name} is in {value}')

global_dataframe = access_latest_S3_object()

# Create a list of the devices that are in the object
list_of_device_names = list(dict(global_dataframe[1].value_counts()).keys())

for device_name in list_of_device_names:
    # This loops through the devices in the main dataframe, creates an individual dataframe for a device,
    # and then passes it into the function that determines the mode and the location of this device
    specific_device_df = get_device_data_frame(global_dataframe, device_name)

    determine_mode_and_zone(specific_device_df, device_name)











