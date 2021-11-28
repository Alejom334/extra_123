""" TestBucketConnectorMethods"""
import os
import unittest
import boto3
from moto import mock_s3

from xetra_123.xetra.common.s3 import S3BucketConnector

"""
Purpose: Testing the S3BucketConnector class
"""
class TestBucketConnectorMethods(unittest.TestCase):
    """
    Purpose: Setting up the environment
    """
    def setUp(self):
        #mocking S3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        #Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        #Creating S3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        #Creating a bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        #Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_access_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
    """
    Purpose: Executing after unittests
    """
    def tearDown(self):
        # mocking S3 connection stop
        self.mock_s3.stop()

    """
    Purpose Test the list_files_in_prefix method
    for getting 2 file keys as list on the mocked s3 bucket
    """
    def test_list_files_in_prefix_ok(self):

        #Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        #Test init
        csv_content = """ col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        #Method Execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        #Test after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        #CleanUp after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects':[
                    {
                        'Key':key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    """
    Purpose Test the list_files_in_prefix method
    in case of a wring or not existing prefix
    """
    def test_list_files_in_prefix_wrong_prefix(self):
        # Expected results
        prefix_exp = 'no-prefix/'
        # Method Execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Test after method execution
        self.assertTrue(not list_result)


if __name__ == '__main__':
    unittest.main()


