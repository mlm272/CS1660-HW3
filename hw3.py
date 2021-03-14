import boto3

s3 = boto3.resource('s3',
 	aws_access_key_id='SECRET',
 	aws_secret_access_key='SECRET')

try:
	s3.create_bucket(Bucket='datacont-maggie', CreateBucketConfiguration={'LocationConstraint':'us-east-1'})
except:
	print("this may already exist")
	
bucket = s3.Bucket("datacont-maggie");
bucket.Acl().put(ACL='public-read')

body = open('test.txt', 'rb')

o = s3.Object('datacont-maggie','test').put(Body=body)
s3.Object('datacont-maggie','test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
 	region_name='us-east-1',
 	aws_access_key_id='SECRET',
 	aws_secret_access_key='SECRET'
)

try:
 	table = dyndb.create_table(
 		TableName='DataTable1',
 		KeySchema=[
 			{
 			'AttributeName': 'PartitionKey',
 				'KeyType': 'HASH'
		 	},
 			{
			 'AttributeName': 'RowKey',
			 'KeyType': 'RANGE'
			}
 		],
 		AttributeDefinitions=[
 			{
 				'AttributeName': 'PartitionKey',
 				'AttributeType': 'S'
 			},
 			{
 				'AttributeName': 'RowKey',
 				'AttributeType': 'S'
 			},
 		],
		 ProvisionedThroughput={
 				'ReadCapacityUnits': 5,
 				'WriteCapacityUnits': 5
 		}
 	)
except:
 		#if there is an exception, the table may already exist. if so...
 		table = dyndb.Table("DataTable1")
 		
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable1')
#print(table.item_count)


import csv

with open('experiments.csv','r') as csvfile:
		csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
		for item in csvf:
 			print(item)
 			body = open(item[4], 'rb')
 			s3.Object('datacont-maggie', item[4]).put(Body=body)
 			md = s3.Object('datacont-maggie', item[4]).Acl().put(ACL='public-read')

 			url = "https://s3-us-west-2.amazonaws.com/datacont-maggie/"+item[4]
 			metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
 					'description' : item[3], 'date' : item[2], 'url':url}
 			try:
 					table.put_item(Item=metadata_item)
 			except:
 					print("item may already be there or another failure")

response = table.get_item(
 	Key={
 			'PartitionKey': 'experiment1',
 			'RowKey': 'data1'
 		}
)
item = response['Item']
print(item)

response
