import pandas as pd
from datetime import datetime, timedelta
import s3 as conn
from xetra_123.xetra.common.s3 import S3BucketConnector
'''
Class for methods for processing the meta file
Note: The reason why we would be using a parenthesis
after the class name is because we are going to be
using subclasses
'''
class MetaProcess():

    def __init__(self):
        print (self)

    """
    -------------------------------------------------------------------------
    Function: update_meta_file(bucket, meta_key, extract_date_list)
    
    Purpose: Update our current meta_file
    
    Explanation:
            - Create new data frame with columns: source_date, date
            - Update the values of 'source_date' with new values
            - Update the values of 'datetime_of_processing' with new values
            - Create in memory df_old to have the current df
            - Put both the df_old and df_new together
            - Update the meta file in the bucket   
    Parameters: 
                Bucket, Object, Source bucket or destination bucket.
                meta_key, string, meta key we are reading
                extract_date_list, list, list that would update the metafile
    Returns:
                None
                      
    NOTE: The purpose of using static method is to use them
          as user functions.
          Static methods are methods that are bound to a class
          rather than its object. Then they do not need the 
          creation of a instance of a class.
          - Static methods do not know nothing about the class
            and just deals with the parameters given
    -------------------------------------------------------------------------
    """
    @staticmethod
    def update_meta_file(bucket, meta_key, extract_date_list):
        df_new = pd.Dateframe(columns=['source_date', 'date'])
        df_new['source_date'] = extract_date_list
        df_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%d')
        df_old = conn.read_csv_to_df(bucket, meta_key)
        df_all = pd.concat(df_old, df_new)
        conn.write_df_to_s3_csv(bucket, df_all, meta_key)

    @staticmethod

    def return_date_list(bucket, arg_date, src_format, meta_key):
        min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            df_meta = S3BucketConnector.read_csv_to_df(bucket, meta_key)
            dates = [(min_date + timedelta(days=x)) for x in range(0, (today - min_date).days + 1)]
            print(dates)
            src_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
            print(src_dates)
            dates_missing = set(dates[1:]) - src_dates
            print(dates_missing)
            if dates_missing:
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                return_dates = [date.strftime(src_format) for date in dates if date >= min_date]
                return_min_date = (min_date + timedelta(days=1)).strftime(src_format)
            else:
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()
        except bucket.session.client('s3').exceptions.NoSuchKey:
            return_dates = [(min_date + timedelta(days=x)).strftime(src_format) for x in
                            range(0, (today - min_date).days + 1)]
            return_min_date = arg_date
        return return_min_date, return_dates
