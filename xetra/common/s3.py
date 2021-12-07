'''
Connector and methods accessing S#
Class for interacting with S3 Bucket
'''
import os
import boto3
import pandas as pd
from io import StringIO, BytesIO
import logging

class S3BucketConnector():
    '''
    -------------------------------------------------------------------------
    Function: Constructor for S3BuckeyConnector()
    Purpose: Create an instance of the boto class
    Parameters:
        :param access_key: Access key for accessing S3
        :param secret_key: secret ket for accessing S3
        :param endpoint_url: endpoint for accessing S3
        :param bucket: S3 bucket name

    Note:
        For convention we are putting a dash '_' before
        the variable S3. Python does not have public
        or private variables, but we can put as a convetion
        that this variable should not be touched by putting
        this before the actual variable
    -------------------------------------------------------------------------
    '''
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id = os.environ[access_key],
                                     aws_secret_access_key= os.environ[secret_key])
        self._s3 = self.session.resource(service_name = 's3', endpoint_url = endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
    '''
    -------------------------------------------------------------------------
    Function: connect_boto(src_bucket, trg_bucket)
    
    Purpose: Connect to the boto
    
    -------------------------------------------------------------------------
    
    '''

    def connect_boto(self, src_bucket, trg_bucket):
        s3 = boto3.resource('s3')
        bucket_src = s3.Bucket(src_bucket)
        bucket_trg = s3.Bucket(trg_bucket)
        return bucket_src, bucket_trg

    """
    -------------------------------------------------------------------------
    Function: read_csv_to_df(bucket, key, decoding = 'utf-8', separator = ',')
    
    Purpose: Transform a csv file to a dataframe 
    
    Explanation:
            - Decode the csv file of the bucket
            - Use StringIO to make it into 
                The StringIO module is an in-memory file-like object. 
                This object can be used as input or output to the most 
                function that would expect a standard file object.
            - Create the dataframe using a pandas data frame
                Read a comma-separated values (csv) file into DataFrame.
    Parameters: 
                Bucket, Object, Source bucket or destination bucket
                key, String, Specific csv file that will be converted to csv
    
    Returns:
                df, dataframe, dataframe of the csv
    
    -------------------------------------------------------------------------
    """
    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', separator: str = ','):
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=separator)
        return df

    """
    -------------------------------------------------------------------------
    Function: write_df_to_s3(bucket, df, key)
    
    Purpose: Write the df into an S3 bucket
    
    Explanation:
            - Create an object of BytesIO.
                This class is like StringIO for bytes objects.
            - Trasnform the dataframe into a parquet file.
            - Put parquet file in the S3 bucket.
                
    What is a parquet file format?
            Parquet is an open source file format available to any project 
            in the Hadoop ecosystem. Apache Parquet is designed for efficient 
            as well as performant flat columnar storage format of data 
            compared to row based files like CSV or TSV files. ... 
            Parquet can only read the needed columns therefore greatly 
            minimizing the IO.
            
    Parameters: 
                Bucket, Object, Source bucket or destination bucket.
                df, dataframe, dataframe to be changed to parquet object.
                key, String, Specific csv file that will be converted to csv.
    
    Returns:
                df, dataframe, dataframe of the csv
    
    -------------------------------------------------------------------------
    """
    def write_df_to_s3(self, bucket, df, key):
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

    """
    -------------------------------------------------------------------------
    Function: write_df_to_s3_csv(bucket, df, key)
    
    Purpose: Method to update the metafile.
    
    Explanation:
            - Create an object of StringIO.
            - Transform the dataframe into a csv file.
            - Put csv file in the S3 bucket.
            
    Parameters: 
                Bucket, Object, Source bucket or destination bucket.
                df, dataframe, dataframe to be changed to parquet object.
                key, String, Specific csv file that will be converted to csv.
    
    Returns:
                df, dataframe, dataframe of the csv
    -------------------------------------------------------------------------
    """
    def write_df_to_s3_csv(self, bucket, df, key):
        out_buffer = StringIO()
        df.to_csv(out_buffer, index=False)
        bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True


    """
    -------------------------------------------------------------------------
    Function: list_files_in_prefix(bucket, prefix)
    
    Purpose: Get the key of every object in the file.
             Listing all files with a prefix on the S3 bucket
    
    Explanation:
            - Create a list of the key of every object
            
    Parameters: 
                param: prefix, string, date of the file
    Returns:
                files, list, list of all the files names containing 
                the prefix names
    NOTES:
        
    -------------------------------------------------------------------------
    """
    def list_files_in_prefix(self, prefix: str):
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files



'''
NOTES on Learning:
- Linters in python
    Purpose: increase quality code
    Pylint will be use 

- Unit testing in python
    Unit testing frameworks unittest, pytest
    Testing each callable method
    Goal: high (100%) test coverage -> package coverage
    Using mock, patch for other (imported) objects
    Package moto for mocking S3
    Using stubs (fake databases) or in-memory databases for 
        databases for database connections
        
        - 

'''
