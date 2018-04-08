import boto3
import urllib.request
import shutil
import os

def uploadToS3(url, bucket, S3Resource, fileCache, s3File, fileName):
	if not os.path.exists(fileCache):
		os.makedirs(fileCache)
	try:
		urllib.request.urlretrieve(url, fileCache+fileName)
		s3 = boto3.resource(S3Resource)
		data = open(fileCache+fileName, 'rb')
		s3.Bucket(bucket).put_object(Key=s3File+fileName, Body=data)
		shutil.rmtree(fileCache)
		return fileName +' Successfully Uploaded'
	except:
		return fileName +' Not Uploaded'
