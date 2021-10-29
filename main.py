"""
The purpose of this ETL job is to get the
data from the s3://deutsche-boerse-xetra-pds

What is  s3://deutsche-boerse-xetra-pds
- This data set is the the data set from the
deutsche and german trading market which gets
updated every minute and save in a S3 bucket.

- The purpose of playing with this data is being
able to structure it correctly, and respond questions
such as (1) Opening Price (2) Closing Price (3)Daily Traded Volume


"""



#Note: This file will have several steps on how to read some data from some publiv
#CSV in AWS.
#
import boto3
import pandas as pd
from io import StringIO #Create a memory buffer to read the CSV

s3 = boto3.resource('s3')
bucket = s3.Bucket('deutsche-boerse-xetra-pds')

bucket_obj1 = bucket.objects.filter(Prefix='2021-03-15')
bucket_obj2 = bucket.objects.filter(Prefix='2021-03-16')
objects = [obj for obj in bucket_obj1] + [obj for obj in bucket_obj2]

csv_obj_cols = bucket.Object(key=objects[0].key).get().get('Body').read().decode('utf-8')
data = StringIO(csv_obj_cols)
df_cols=pd.read_csv(data, delimiter=",")

df_all = pd.DataFrame(columns = df_cols.columns)
print('We are here')
for obj in objects:
    csv_obj = bucket.Object(key=obj.key).get().get('Body').read().decode('utf-8')
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=',')
    df_all = df_all.append(df, ignore_index = True)

columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice',
       'EndPrice', 'TradedVolume']
df_all = df_all.loc[:, columns]

df_all.dropna(inplace=True)

print(df_all)
