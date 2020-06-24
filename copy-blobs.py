#!/usr/bin/env python3

import boto3
import onevizion
import json
import os
import onevizion

# Read settings
with open('settings','r') as p:
	params = json.loads(p.read())

try:
	OvUserName = params['OV']['UserName']
	OvPassword = params['OV']['Password']
	OvUrl      = params['OV']['Url']
	awsAccessKeyId = params['AWS']['awsAccessKeyId']
	awsSecretAccessKey = params['AWS']['awsSecretAccessKey']
	awsRegion = params['AWS']['awsRegion']
	SourceBucket = params['SourceBucket']
	DestBucket = params['DestBucket']
	ReadyCheckbox = params['ReadyCheckbox']
	BlobIdList    = params['BlobIdList']
except Exception as e:
	raise "Please check settings.json"

client = boto3.client(
				's3',
				region_name=awsRegion,
				aws_access_key_id= awsAccessKeyId,
				aws_secret_access_key = awsSecretAccessKey
				)
s3 = boto3.resource(
				's3',
				region_name=awsRegion,
				aws_access_key_id= awsAccessKeyId,
				aws_secret_access_key = awsSecretAccessKey
				)

def copy_from_source_to_dest_bucket(blobDataId):
	fileName = 'blob_data/{id}'.format(id=blobDataId)
	# get metadata to object
	head = client.head_object(
				Bucket=SourceBucket, 
				Key=fileName)

	obj = s3.Object(DestBucket,fileName)

	# copy_from will pitch a 403 error if file is not in s3 yet
	obj.copy_from(
		CopySource = {
			'Bucket': SourceBucket,
			'Key': fileName
		},
		MetadataDirective='REPLACE',
		ContentType=head['ResponseMetadata']['HTTPHeaders']['content-type'],
		ContentDisposition=head['ResponseMetadata']['HTTPHeaders']['content-disposition']
		)

# make sure api user has RE on the tab with checkbox and list of blobs(ADMIN_CHECKLIST) and RE for the trackor type(Checklist) and R for WEB_SERVICES 
Req = onevizion.Trackor(trackorType = 'Checklist', URL = OvUrl, userName=OvUserName, password=OvPassword)
Req.read(filters = {ReadyCheckbox:'1'}, 
		fields = [ReadyCheckbox,BlobIdList,'TRACKOR_KEY'], 
		sort = {'TRACKOR_KEY':'ASC'}, page = 1, perPage = 1000)

if len(Req.errors)>0:
	# TODO implement better error handling
	quit(1)

for cl in Req.jsonData:
	#print(cl['TRACKOR_KEY'])
	hasErrors = False
	if cl[BlobIdList] is None:
		#print('None')
		Req.update(filters = {'TRACKOR_KEY':cl['TRACKOR_KEY']},fields={ReadyCheckbox:0})
		if len(Req.errors)>0:
			print('could not clear checkbox for {t}'.format(t=cl['TRACKOR_KEY']))
		else:
			print(Req.jsonData)
	else:
		print(cl[BlobIdList].split())
		for blob_data_id in cl[BlobIdList].split():
			try:
				copy_from_source_to_dest_bucket(blob_data_id)
			except Exception as e:
				print('could not process {f} {e}'.format(f=blob_data_id), e=str(e))
				hasErrors = True
				continue
		print (hasErrors)
		if not hasErrors:
			Req.update(filters = {'TRACKOR_KEY':cl['TRACKOR_KEY']},fields={ReadyCheckbox:0})
			if len(Req.errors)>0:
				print('could not clear checkbox for {t}'.format(t=cl['TRACKOR_KEY']))
			else:
				print(Req.jsonData)

'''
head = client.head_object(Bucket='dev-mobilitie.onevizion.com', Key='blob_data/1001102281')
print(head['ResponseMetadata']['HTTPHeaders']['content-type'])

s3 = boto3.resource('s3')
obj = s3.Object('public-dev-mtrac.mobilitie.com','blob_data/1001102281')
obj.copy_from(
	CopySource = {
		'Bucket': 'dev-mobilitie.onevizion.com',
		'Key': 'blob_data/1001102281'
		},
	MetadataDirective='REPLACE',
	ContentType=head['ResponseMetadata']['HTTPHeaders']['content-type'],
	ContentDisposition=head['ResponseMetadata']['HTTPHeaders']['content-disposition']
	)
'''
#obj.set_acl('public-read')
'''
'CH_SEND_ITEMS_TO_PUBLIC_S3'
'CH_CHECKLIST_ITEMS_IDS'
'''
