'''
File to store constants

Notes:
    Recall enum is a class in python to create
    enumerations
'''

from enum import Enum

'''
Class: class S3FileTypes(Enum)

Explanation: 
        The purpose of this class is to support the
        file types for S3BucketConnector

'''
class S3FileTypes(Enum):
    CSV = 'csv'
    PARQUET = 'parquet'


'''
Class: class MetaProcessFormat(Enum)

Explanation: 
        The purpose of this class is to support the
        creation of the MetaProcess class

'''
class MetaProcessFormat(Enum):
    META_DATE_FORMAT = "%Y-%m-%d"
    META_PROCESS_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    META_SOURCE_DATE_COL = "source_date"
    META_PROCESS_COL = "datetime_of_processing"
    META_FILE_FORMAT = "csv"


