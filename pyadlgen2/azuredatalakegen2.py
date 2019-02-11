"""TODO Fill in the module description

"""

# ---------------------------------------------------------------------

# LIBRARIES

# External Libraries
import os

# Internal Libraries
from pyadlgen2.helpers.adlgen2restapiwrapper import ADLGen2RestApiWrapper

# ---------------------------------------------------------------------

class AzureDataLakeGen2():

	"""TODO Fill in the class description."""
	
	def __init__(self, storage_account_name, storage_account_key):

		self.__storage_account_name = storage_account_name
		self.__storage_account_key = storage_account_key
		
		self.__azure_datalake_rest_api_wrapper = ADLGen2RestApiWrapper(storage_account_name, storage_account_key)
	
	def file_exists(self, file_path):
		"""TODO Fill in the method description"""

		raise NotImplementedError()
		
	def file_create(self, file_path, file_data, file_properties, overwrite_if_exists = False):
		r"""Create a file at the specified path with the specified data.

		Parameters
		----------
		file_path : str
			Absolute path where the file has to be created.
			The first element represents the filesystem (a.k.a container)
			where the file is stored.
			The last part of the path represents the name of the file.
			We can thus see the path as:
			/{filesystem}/{folder1}/.../{folderN}/{filename}
		file_data : 
			The data that will be written inside the file.
		file_properties : dict, OrderedDict
			The properties that will be set for the new file.
		overwrite_if_exists: bool, optional
			If True and the specified `file_path` already exists,
			will overwrite the existing data and properties.

		Returns
		-------
		bool
			Returns True if execution succeed, raises an exception
			otherwise.

		Raises
		------
		FileExistsError
			If the specified `file_path` already exists and
			`overwrite_if_exists` is False.

		"""

		if os.path.isabs(path)

		if self.file_exists(file_path) and not overwrite_if_exists:
			raise FileExistsError('The specified file_path already exists.\n{}'.format(file_path))

		# The creation of a file with the specified properties requires different
		# calls of the API. The steps are:
		# * create an empty file
		# * update the created with the actual data
		# * set the properties

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

		
	def file_delete(self):
		"""TODO Fill in the method description"""
		
		raise NotImplementedError()
		
	def file_read(self):
		"""TODO Fill in the method description"""
		
		raise NotImplementedError()
		
	def file_update(self):
		"""TODO Fill in the method description"""
		
		raise NotImplementedError()
		
	def file_get_properties(self):
		"""TODO Fill in the method description"""
		
		raise NotImplementedError()
		
	def file_set_properties(self):
		"""TODO Fill in the method description"""
		
		raise NotImplementedError()
		
	def read_file(self, filepath):

		response = self.__azure_datalake_rest_api_wrapper.path_read(
			filesystem = 'raw-temporary'
			, path = 'test.csv'
		)

		return response
