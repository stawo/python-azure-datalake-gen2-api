from pyadlgen2.helpers.adlgen2restapiwrapper import ADLGen2RestApiWrapper

class AzureDataLakeGen2():

	def __init__(self, storage_account_name, storage_account_key):

		# Create the blob client, for use in obtaining references to
		# blob storage containers and uploading files to containers.
		self.__storage_account_name = storage_account_name
		self.__storage_account_key = storage_account_key
		
		self.__azure_datalake_rest_api_wrapper = ADLGen2RestApiWrapper(storage_account_name, storage_account_key)
	
	def create_file(self):

		# Create a file
		response = self.__azure_datalake_rest_api_wrapper.path_create(
			filesystem = 'raw-temporary'
			, path = 'test.csv'
			, resource = 'file'
			, request_headers={
				'Content-Encoding' : 'utf-8'
				, 'x-ms-content-type' : 'text/plain'
			}
			)

		response = self.__azure_datalake_rest_api_wrapper.path_update(
			filesystem = 'raw-temporary'
			, path = 'test.csv'
			, action = 'append'
			, position = str(0)
			, request_headers = {
				'Content-Type' : 'text/plain'
				, 'Content-Length' : str(len('{s:1}'.encode('utf-8')))
			}
			, data_to_append = '{s:1}'
		)

		print(response)

		response = self.__azure_datalake_rest_api_wrapper.path_update(
			filesystem = 'raw-temporary'
			, path = 'test.csv'
			, action = 'flush'
			, close = 'true'
			, position = str(len('{s:1}'.encode('utf-8')))
			, request_headers = {
				'Content-Length' : str(0)
				, 'x-ms-content-type' : 'text/plain'
			}
		)

		return response

	def read_file(self, filepath):

		response = self.__azure_datalake_rest_api_wrapper.path_read(
			filesystem = 'raw-temporary'
			, path = 'test.csv'
		)

		return response
