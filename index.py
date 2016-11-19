import requests
import uuid
import json
from requests_aws4auth import AWS4Auth
from elasticsearch import (
	Elasticsearch, RequestsHttpConnection, serializer, compat, exceptions)

with open('./credentials.txt') as f:
	credentials = json.loads(f.readlines()[0])
	global host, access, secret, test, doc_type
	host = credentials['host']
	access = credentials['access']
	secret = credentials['secret']
	index = credentials.get('index', 'test')
	doc_type = credentials.get('doc_type', 'doc')

class UnicodeSerializer(serializer.JSONSerializer):
    def dumps(self, data):
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

class ElasticIndex():
	def __init__(self):
		self.es = Elasticsearch(
		    hosts=[
		    	{'host': host, 
		    	'port': 443}],
		    http_auth=AWS4Auth(
				access, 
				secret, 
				"us-east-1", 
				"es"),
		    use_ssl=True,
		    verify_certs=True,
		    connection_class=RequestsHttpConnection,
		    serializer=UnicodeSerializer())
		self.formatting = {
			'index': index,
			'doc_type': doc_type}

	def index(self, data):
		return self.es.index(
			body=data, 
			id=uuid.uuid4(),
			**self.formatting)

if __name__ == "__main__":
	index = 'test'
	search = ElasticIndex()
	print search.index({'hello':'world'})
