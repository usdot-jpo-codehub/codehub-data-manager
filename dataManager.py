import requests
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import os

ELASTICSEARCH_API_BASE_URL = os.environ.get('ELASTICSEARCH_API_BASE_URL')
ENVIRONMENT = os.environ.get('ENVIRONMENT_NAME')

JSONHeader={'content-type':'application/json'}

def getIndices():
	result = []
	response = requests.get(ELASTICSEARCH_API_BASE_URL + '/_cat/indices?format=JSON')
	
	if (response.status_code == 200):
		responseJSON = json.loads(response.text)
		for index in responseJSON:
			result.append({'name': index['index'], 'docCount': index['docs.count']})

	return result

def getMapping(indexName):
	response = requests.get(ELASTICSEARCH_API_BASE_URL + '/' + indexName + '/_mapping')

	if (response.status_code == 200):
		responseJSON = json.loads(response.text)
	
	result = responseJSON[indexName]

	return result

def loadMapping(targetIndex, mappingJSON):
	response = requests.put(ELASTICSEARCH_API_BASE_URL + '/' + targetIndex, data=json.dumps(mappingJSON), headers=JSONHeader)
	print(response.text)

def getData(indexName):
	result = []

	response = requests.get(ELASTICSEARCH_API_BASE_URL + '/' + indexName + '/_search')

	if (response.status_code == 200):
		responseJSON = json.loads(response.text)
		hits = responseJSON['hits']['hits']

		for hit in hits:
			result.append({'id': hit['_id'], 'source': hit['_source']})

	return result

def loadData(targetIndex, dataJSON):
	result = ''

	for entry in dataJSON:
		indexStr = json.dumps({'create': {'_index': targetIndex, '_id': entry['id']}}) + '\r\n'
		result += indexStr
		dataStr = json.dumps(entry['source']) + '\r\n'
		result += dataStr

	response = requests.post(ELASTICSEARCH_API_BASE_URL + '/_bulk', data=result, headers=JSONHeader)
	print(response.text)

def importIndex(targetIndex, s3FilePath):
	existingIndices = getIndices()

	for index in existingIndices:
		if index['name'] == targetIndex:
			print('THIS INDEX ALREADY EXISTS')
			return

	s3 = boto3.resource('s3')
	payload = s3.Object('codehub-data-manager', s3FilePath).get()['Body'].read()

	sourceData = json.loads(payload)
	loadMapping(targetIndex, sourceData['mapping'])
	loadData(targetIndex, sourceData['data'])

def exportIndex(sourceIndex):
	existingIndices = getIndices()
	result = {}

	for index in existingIndices:
		if index['name'] == sourceIndex:
			print ('Saving index: ' + sourceIndex)
			result['data'] = getData(sourceIndex)
			result['mapping'] = getMapping(sourceIndex)

			s3 = boto3.resource('s3')
			s3.Object('codehub-data-manager', ENVIRONMENT + '/' + sourceIndex + '_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.json').put(Body=json.dumps(result).encode())

			break

	if not result:
		print ("Index " + sourceIndex + " doesn't exist. Exiting.")
		return

	else:
		return result

def listBackups():
	session = boto3.session.Session()
	s3 = session.resource('s3')
	mybucket = s3.Bucket('codehub-data-manager')

	for record in mybucket.objects.all():
		print(record)


def lambda_handler(event, context):
	function = event['function']
	if function == 'export':
		# do the export
		srcIndex = event['srcIndex']
		exportIndex(srcIndex)
	elif function == 'import':
		# do the import
		targetIndex = event['targetIndex']
		sourcePath = event['srcPath']
		importIndex(targetIndex, sourcePath)
	else:
		# throw a fit
		print("Error - ")