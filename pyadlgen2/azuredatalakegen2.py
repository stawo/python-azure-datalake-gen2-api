"""TODO Fill in the module description

"""

# ---------------------------------------------------------------------

# LIBRARIES

# External Libraries
import pathlib
from requests.exceptions import HTTPError

# Internal Libraries
from pyadlgen2.helpers.adlgen2restapiwrapper import ADLGen2RestApiWrapper

# ---------------------------------------------------------------------

class AzureDataLakeGen2():

	"""TODO Fill in the class description."""
	
	def __init__(self, storage_account_name, storage_account_key):

		self.__storage_account_name = storage_account_name
		self.__storage_account_key = storage_account_key
		
		self.__azure_datalake_rest_api_wrapper = ADLGen2RestApiWrapper(storage_account_name, storage_account_key)
	
	def path_exists(self, path):
		"""Checks if a given path exists in the datalake.
		
		Parameters
		----------
		path : str
			Absolute path that has to be checked.
			The first element represents the filesystem (a.k.a container)
			where the file is stored.
			We can thus see the path as:
			/{filesystem}/{path}

			*{path}* is optional.
			`path` can point to either a folder or a file, it
			doesn't matter.

		Returns
		-------
			bool
				True if `path` exists, False otherwise.

		Raises
		------
		HTTPError
			If the call to the REST API fails for some reasons.

		"""

		path = pathlib.PurePosixPath(path)

		if not path.is_absolute():
			raise ValueError('The param [path] must be an absolute path. Value passed:\n{}'.format(path))

		datalake_filesystem = path.parts[1]
		datalake_path = path.relative_to(path.parts[0]+path.parts[1]) \
			if path.relative_to(path.parts[0]+path.parts[1]) != pathlib.PurePosixPath('.') \
			else None

		try:
			self.__azure_datalake_rest_api_wrapper.path_list(
				filesystem = datalake_filesystem
				, recursive = False
				, directory = datalake_path
				)
		
			return True

		except HTTPError as e:
			if str(e).startswith('404 Client Error: The specified path does not exist.'):
				return False
			else:
				raise e

	def path_get_properties(self, path):
		r"""Get the properties of the specified path.

		Parameters
		----------
		path : str
			Absolute path of which we want the properties.
			The first element represents the filesystem (a.k.a container)
			where files are stored.
			We can thus see the path as:
			/{filesystem}/{folder1}/.../{folderN}[/{filename}]

		Returns
		-------
		dict
			Returns a dict containing the properties of the
			specified path.

		Raises
		------
		ValueError
			If the specified `path` is not an absolute path.

		FileNotFoundError
			If the specified `path` does not exist.

		"""
		
		path = pathlib.PurePosixPath(path)

		if not path.is_absolute():
			raise ValueError('The param [path] must be an absolute path. Value passed:\n{}'.format(path))

		datalake_filesystem = path.parts[1]
		datalake_path = path.relative_to(path.parts[0]+path.parts[1]) \
			if path.relative_to(path.parts[0]+path.parts[1]) != pathlib.PurePosixPath('.') \
			else None

		if not self.path_exists(path):
			raise FileNotFoundError('The specified path does not exist.\n{}'.format(path))
		
		response = self.__azure_datalake_rest_api_wrapper.path_get_properties(
			filesystem = datalake_filesystem
			, path = datalake_path
			, upn = True
			, action = 'getStatus'
			)
		
		return response

	def path_is_directory(self, path):
		r"""Checks if `path` exists and is a directory.

		Parameters
		----------
		path : str
			Absolute path which we want to check.
			The first element represents the filesystem (a.k.a container)
			where files are stored.
			We can thus see the path as:
			/{filesystem}/{folder1}/.../{folderN}[/{filename}]

		Returns
		-------
		bool
			Returns True if `path` exists and is a directory, False otherwise.

		Raises
		------
		ValueError
			If the specified `path` is not an absolute path.

		
		"""
		
		path = pathlib.PurePosixPath(path)

		if not path.is_absolute():
			raise ValueError('The param [path] must be an absolute path. Value passed:\n{}'.format(path))

		datalake_filesystem = path.parts[1]
		datalake_path = path.relative_to(path.parts[0]+path.parts[1]) \
			if path.relative_to(path.parts[0]+path.parts[1]) != pathlib.PurePosixPath('.') \
			else None

		if not self.path_exists(path):
			return False
		
		if self.path_get_properties(path)['x-ms-resource-type'] == 'directory':
			return True
		else:
			return False

	def path_is_file(self, path):
		r"""Checks if `path` exists and is a file.

		Parameters
		----------
		path : str
			Absolute path which we want to check.
			The first element represents the filesystem (a.k.a container)
			where files are stored.
			We can thus see the path as:
			/{filesystem}/{folder1}/.../{folderN}[/{filename}]

		Returns
		-------
		bool
			Returns True if `path` exists and is a file, False otherwise.

		Raises
		------
		ValueError
			If the specified `path` is not an absolute path.
		
		"""
		
		path = pathlib.PurePosixPath(path)

		if not path.is_absolute():
			raise ValueError('The param [path] must be an absolute path. Value passed:\n{}'.format(path))

		datalake_filesystem = path.parts[1]
		datalake_path = path.relative_to(path.parts[0]+path.parts[1]) \
			if path.relative_to(path.parts[0]+path.parts[1]) != pathlib.PurePosixPath('.') \
			else None

		if not self.path_exists(path):
			return False
		
		if self.path_get_properties(path)['x-ms-resource-type'] == 'file':
			return True
		else:
			return False

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
		
		file_path = pathlib.PurePosixPath(file_path)

		if not file_path.is_absolute():
			raise ValueError('The param [file_path] must be an absolute path. Value passed:\n{}'.format(file_path))
		
		if self.path_exists(file_path):
			if not overwrite_if_exists:
				raise FileExistsError('The specified file_path already exists and the param [overwrite_if_exists] is set to False.\n{}'.format(file_path))
			
			if not self.path_is_file(file_path):
				raise ValueError('The specified file_path already exists and is not a file.\n{}'.format(file_path))
			
		# The creation of a file with the specified properties requires different
		# calls of the API. The steps are:
		# * create an empty file
		# * update the created with the actual data
		# * flush the data
		# * set the properties

		datalake_filesystem = file_path.parts[1]
		datalake_file_path = file_path.relative_to(file_path.parts[0]+file_path.parts[1]) \
			if file_path.relative_to(file_path.parts[0]+file_path.parts[1]) != pathlib.PurePosixPath('.') \
			else None

		response = self.__azure_datalake_rest_api_wrapper.path_create(
			filesystem = datalake_filesystem
			, path = datalake_file_path
			, resource = 'file'
			, request_headers={
				'Content-Encoding' : 'utf-8'
				, 'x-ms-content-type' : 'text/plain'
			}
			)
		
		response = self.__azure_datalake_rest_api_wrapper.path_update(
			filesystem = datalake_filesystem
			, path = datalake_file_path
			, action = 'append'
			, position = str(0)
			, request_headers = {
				'Content-Type' : 'text/plain'
				, 'x-ms-content-type' : 'text/plain'
				, 'Content-Length' : str(len(file_data.encode('utf-8')))
			}
			, data_to_append = file_data
		)

		print(response)

		response = self.__azure_datalake_rest_api_wrapper.path_update(
			filesystem = datalake_filesystem
			, path = datalake_file_path
			, action = 'flush'
			, close = 'true'
			, position = str(len(file_data.encode('utf-8')))
			, request_headers = {
				'Content-Length' : str(0)
				, 'x-ms-content-type' : 'text/plain'
			}
		)

		# TODO Set Properties

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
		
	def file_set_properties(self):
		"""TODO Fill in the method description"""
		
		raise NotImplementedError()
		
	def read_file(self, filepath):

		response = self.__azure_datalake_rest_api_wrapper.path_read(
			filesystem = 'raw-temporary'
			, path = 'test.csv'
		)

		return response
