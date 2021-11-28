'''
Xetra ETl Component
'''

from typing import NamedTuple
from datetime import datetime, timedelta
from xetra_123.xetra.common.s3 import S3BucketConnector
import pandas as pd
import logging
'''
Purpose: 
    Class for source configuration data

Parameters:
    src_first_extract_date: Determines the date for extracting
                            the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col)traded_col: column name for traded volume in source
'''
class XetraSourceConfig(NamedTuple):
    src_columns: str
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_col: str
'''
Purpose: 
    Class for target configuration data

Parameters:
    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: column name for change in previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of teh target file 
'''
class XetraTargetConfig(NamedTuple):
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

'''
Purpose: 
    Reads the Xetra data, transforms and writes
    the transformed to target
    
'''
class XetraETL():

    '''
    Purpose:
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: namedTouple class with source configuration data
        :param trg_args: namedTouple class with targte configuraiton data
    '''
    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.extract_date = ''
        self.extract_date_list = ''
        self.meta_update_list = ''



    """
    -------------------------------------------------------------------------
    Function: extract(bucket, date_list)
    
    Purpose: Extract dataframes from files
    
    Explanation: 
            - Files: Get the key (As a string) of each file in a list
            - df: Create a dataframe that holds all the values of the days
                  of the tables we want to extract
            
            
    Parameters: bucket, obj, S3 source bucket where we are extracting data
                date_list, list, dates of the dataframes we are trying to put together 
    
    Returns: df, dataframe, combined dataframe with the data of all the dates
                
    -------------------------------------------------------------------------
    """
    def extract(bucket, date_list):
        files = [key for date in date_list for key in S3BucketConnector.list_files_in_prefix(bucket, date)]
        df = pd.concat([S3BucketConnector.read_csv_to_df(bucket, obj) for obj in files], ignore_index=True)
        return df

    """
    -------------------------------------------------------------------------
    Function: transform_report1(df, columns, arg_date)
    
    Purpose: Transform the dataframe to create the new cols we want for the
             report
    
    Explanation: 
            - Drop the columns that we do not want from the dataframe
            - Drop all the values that have empty spaces
            - Create a colum called opening prices
            - Create a colum called closing prices
            - Create a colum called previous closing price
            - Create a colum called change previous closing percentage
            
            
    Parameters: df, dateframe, datraframe that must eb transformed
                columns, list of strings, desired columns we want
                arg_date, date, date that we are inquiring for report
    
    Returns: df, dataframe, combined dataframe with the data of all the dates
                
    -------------------------------------------------------------------------
    """
    def transform_report1(df, columns, arg_date):
        df = df.loc[:, columns]
        df.dropna(inplace=True)
        df['opening_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
        df['closing_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')
        df = df.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
        df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
        df['change_prev_closing_%'] = (df['closing_price_eur'] - df['prev_closing_price']) / df['prev_closing_price'] * 100
        df.drop(columns=['prev_closing_price'], inplace=True)
        df = df.round(decimals=2)
        df = df[df.Date >= arg_date]
        return df
    """
    -------------------------------------------------------------------------
    Function: load(bucket, df, trg_key, trg_format, meta_key, extract_date_list)
    
    Purpose: Load transformed dataframe into target bucket
    
    Explanation: 
            - Create string for the key in the proper format
            - Call function to upload file to upload dataframe into bucket
            - Update the metafile
            
    Parameters: bucket, obj, target bucket where we want to load the dataframe
                df, dataframe, transformed dataframe
                trg_key, string, name of the report we are currenlty targeting
                trg_format, string, string of .parquet (Type of file that receives Amazon)
                meta_key, string, name of the metafile
                extract_date_list, list, list of dates
    
    Returns: True       
    -------------------------------------------------------------------------
    """
    def load(bucket, df, trg_key, trg_format, meta_key, extract_date_list):
        key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
        S3BucketConnector.write_df_to_s3(bucket, df, key)
        S3BucketConnector.update_meta_file(bucket, meta_key, extract_date_list)
        return True

    """
    -------------------------------------------------------------------------
    Function: etl_report(src_bucket, trg_bucket, date_list, columns, arg_date, trg_key, trg_format, meta_key)
    
    Purpose: Create the table of the extract values we are looking into.
    
    Explanation:
            - df: call extract(src_bucket, date_list) method to create 
                  Dateframe of the data of the dates choosen.
            - df: call transform_report1(df, columns, arg_date) method
                  to update the dateframe to the reports we want to see
            - extract_date_list: list of the dates missing to update in 
                  the target bucket.
            - call load(trg_bucket, df, trg_key, trg_format, meta_key, extract_date_list)
                to update the method     
    Parameters: 
                src_bucket, Object, Source bucket.
                trg_bucket, Object, destination bucket.
                date_list, list, list of the dates we are updating.
                columns, list, columns of our report
                arg_date, date, date we are looking to have the reports
                trg_key, string, string of the name of the database we will
                                 be exploring
                trg_format, string, the target format we want
                meta_key, string, meta key file in the S3 
    Returns:
                True
    -------------------------------------------------------------------------
    """
    def etl_report(self, src_bucket, trg_bucket, date_list, columns, arg_date, trg_key, trg_format, meta_key):
        df = S3BucketConnector.extract(src_bucket, date_list)
        df = S3BucketConnector.transform_report1(df, columns, arg_date)
        extract_date_list = [date for date in date_list if date >= arg_date]
        self.load(trg_bucket, df, trg_key, trg_format, meta_key, extract_date_list)
        return True

