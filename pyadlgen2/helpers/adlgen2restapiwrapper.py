from azure.storage.common.sharedaccesssignature import SharedAccessSignature as AccountSharedAccessSignature
from azure.storage.blob.sharedaccesssignature import BlobSharedAccessSignature

from azure.storage.blob.models import (
	BlobPermissions
	, ContainerPermissions
)
from azure.storage.common.models import (
	AccountPermissions
	, Protocol
	, Services
	, ResourceTypes
)

import datetime
import requests
from urllib.parse import urlparse, parse_qs
import json

class ADLGen2RestApiWrapper():
	"""
	Wrapper for the Azure Data Lake Storage Gen2 REST API
	API Version: 2018-11-09
	https://docs.microsoft.com/en-gb/rest/api/storageservices/data-lake-storage-gen2

	The available operations are divided in two groups: *Filesystem* and *Path*.

	# Filesystem
	https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem

	## Create	
	Create Filesystem
	Create a filesystem rooted at the specified location. If the filesystem already exists,
	the operation fails. This operation does not support conditional HTTP requests.

	## Delete	
	Delete Filesystem
	Marks the filesystem for deletion. When a filesystem is deleted, a filesystem with the
	same identifier cannot be created for at least 30 seconds. While the filesystem is being
	deleted, attempts to create a filesystem with the same identifier will fail with status
	code 409 (Conflict), with the service returning additional error information indicating
	that the filesystem is being deleted. All other operations, including operations on any
	files or directories within the filesystem, will fail with status code 404 (Not Found)
	while the filesystem is being deleted. This operation supports conditional HTTP requests.
	
	## Get Properties	
	Get Filesystem Properties.
	All system and user-defined filesystem properties are specified in the response headers.

	## List	
	List Filesystems
	List filesystems and their properties in given account.

	## Set Properties	
	Set Filesystem Properties
	Set properties for the filesystem. This operation supports conditional HTTP requests.
	
	# Path
	https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path

	## Create	
	Create File | Create Directory | Rename File | Rename Directory
	Create or rename a file or directory. By default, the destination is overwritten and
	if the destination already exists and has a lease the lease is broken. This operation
	supports conditional HTTP requests. For more information, see Specifying Conditional
	Headers for Blob Service Operations. To fail if the destination already exists, use a
	conditional request with If-None-Match: "*".

	## Delete	
	Delete File | Delete Directory
	Delete the file or directory. This operation supports conditional HTTP requests.
	
	## Get Properties	
	Get Properties | Get Status | Get Access Control List
	Get Properties returns all system and user defined properties for a path. Get Status
	returns all system defined properties for a path. Get Access Control List returns the
	access control list for a path. This operation supports conditional HTTP requests.
	
	## Lease	
	Lease Path
	Create and manage a lease to restrict write and delete access to the path.
	This operation supports conditional HTTP requests.

	## List	
	List Paths
	List filesystem paths and their properties.

	## Read	
	Read File
	Read the contents of a file. For read operations, range requests are supported.
	This operation supports conditional HTTP requests.

	## Update	
	Append Data | Flush Data | Set Properties | Set Access Control
	Uploads data to be appended to a file, flushes (writes) previously uploaded data
	to a file, sets properties for a file or directory, or sets access control for a
	file or directory. Data can only be appended to a file.
	This operation supports conditional HTTP requests.
	"""
	
	def __init__(self, storage_account_name, storage_account_key):

		# Create the blob client, for use in obtaining references to
		# blob storage containers and uploading files to containers.
		self.__storage_account_name = storage_account_name
		self.__storage_account_key = storage_account_key
		self.__azure_datalake_dns_suffix = 'dfs.core.windows.net'

		self.__account_sas_generator = AccountSharedAccessSignature(storage_account_name, storage_account_key)
		self.__blob_sas_generator = BlobSharedAccessSignature(storage_account_name, storage_account_key)
	

	def filesystem_create(self
		, filesystem
		, timeout = None
	):
		'''
		Create a filesystem rooted at the specified location. If the filesystem already exists, the operation fails.
		This operation does not support conditional HTTP requests.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/create

		Basic variant:
		PUT http://{accountName}.{dnsSuffix}/{filesystem}?resource=filesystem

		With optional parameters:
		PUT http://{accountName}.{dnsSuffix}/{filesystem}?resource=filesystem&timeout={timeout}
		'''
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/{filesystem}'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
			, filesystem=filesystem
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.CONTAINER
			, permission=AccountPermissions(create=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		params['resource']='filesystem'
		
		if not timeout is None:
			params['timeout']=timeout

		# Execute the request
		response = requests.put(url, params=params)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		return True
		
	def filesystem_delete(self):
		'''
		*NOTE*
		This method will not be implemented, just to avoid deleting entire filesystems by mistake.
		"From great power comes great responsibility, and frankly I don't trust myself."

		Marks the filesystem for deletion. When a filesystem is deleted, a filesystem with the same identifier
		cannot be created for at least 30 seconds. While the filesystem is being deleted, attempts to create a
		filesystem with the same identifier will fail with status code 409 (Conflict), with the service
		returning additional error information indicating that the filesystem is being deleted.
		All other operations, including operations on any files or directories within the filesystem, will
		fail with status code 404 (Not Found) while the filesystem is being deleted.
		This operation supports conditional HTTP requests.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/delete

		Basic variant:
		DELETE http://{accountName}.{dnsSuffix}/{filesystem}?resource=filesystem

		With optional parameters:
		DELETE http://{accountName}.{dnsSuffix}/{filesystem}?resource=filesystem&timeout={timeout}
		'''
		raise NotImplementedError('This method will not be implemented, just to avoid deleting entire filesystems by mistake.')
		
	def filesystem_get_properties(self
		, filesystem
		, timeout = None
	):
		'''
		All system and user-defined filesystem properties are specified in the response headers.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/getproperties

		Basic variant:
		HEAD https://{accountName}.{dnsSuffix}/{filesystem}?resource=filesystem

		With optional parameters:
		HEAD https://{accountName}.{dnsSuffix}/{filesystem}?resource=filesystem&timeout={timeout}
		'''
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/{filesystem}'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
			, filesystem=filesystem
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.CONTAINER
			, permission=AccountPermissions(read=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		params['resource']='filesystem'
		
		if not timeout is None:
			params['timeout']=timeout

		# Execute the request
		response = requests.head(url, params=params)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		return response.headers
		
	def filesystem_list(self
		, prefix = None
		, continuation = None
		, maxResults = None
		, timeout = None
	):
		'''
		List filesystems and their properties in given account.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/list

		Basic variant:
		GET https://{accountName}.{dnsSuffix}/?resource=account

		With optional parameters:
		GET https://{accountName}.{dnsSuffix}/?resource=account&prefix={prefix}&continuation={continuation}&maxResults={maxResults}&timeout={timeout}
		'''
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.SERVICE
			, permission=AccountPermissions(list=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		params['resource']='account'
		
		if not prefix is None:
			params['prefix']=prefix
		
		if not continuation is None:
			params['continuation']=continuation
			
		if not maxResults is None:
			params['maxResults']=maxResults
			
		if not timeout is None:
			params['timeout']=timeout

		# Execute the request
		response = requests.get(url, params=params)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		return json.loads(response.text)
		
	def filesystem_set_properties(self):
		'''
		To Be Implemented
		'''
		raise NotImplementedError()
		
	def path_create(self
		, filesystem
		, path
		, resource = None
		, continuation = None
		, mode = None
		, timeout = None
		, request_headers = None
		):
		"""
		Use for: Create File | Create Directory | Rename File | Rename Directory
		Create or rename a file or directory. By default, the destination is overwritten
		and if the destination already exists and has a lease the lease is broken.
		This operation supports conditional HTTP requests.
		To fail if the destination already exists, use a conditional request with If-None-Match: "*".
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create

		Basic variant:
		PUT https://{accountName}.{dnsSuffix}/{filesystem}/{path}
		
		With optional parameters:
		PUT https://{accountName}.{dnsSuffix}/{filesystem}/{path}?resource={resource}&continuation={
		"""
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/{filesystem}/{path}'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
			, filesystem = filesystem
			, path = path
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.OBJECT
			, permission=AccountPermissions(write=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		if not resource is None:
			params['resource']=resource
		
		if not continuation is None:
			params['continuation']=continuation
			
		if not mode is None:
			params['mode']=mode
			
		if not timeout is None:
			params['timeout']=timeout

		# Execute the request
		response = requests.put(url, params=params, headers=request_headers)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		return response.headers
	
	def path_delete(self):
		'''
		To Be Implemented
		'''
		raise NotImplementedError()
	
	def path_get_properties(self):
		'''
		To Be Implemented
		'''
		raise NotImplementedError()
	
	def path_lease(self):
		'''
		To Be Implemented
		'''
		raise NotImplementedError()
	
	def path_list(self
		, filesystem
		, recursive
		, directory = None
		, continuation = None
		, maxResults = None
		, upn = None
		, timeout = None
		, request_headers = None
		):
		'''
		List filesystem paths and their properties.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list

		Basic variant:
		GET https://{accountName}.{dnsSuffix}/{filesystem}?recursive={recursive}&resource=filesystem

		With optional parameters:
		GET https://{accountName}.{dnsSuffix}/{filesystem}?directory={directory}&recursive={recursive}&continuation={continuation}&maxResults={maxResults}&upn={upn}&resource=filesystem&timeout={timeout}
		'''
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/{filesystem}'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
			, filesystem = filesystem
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.CONTAINER
			, permission=AccountPermissions(list=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		# We convert `recursive` to str just in case it's boolean,
		# and we lower it in case we pass 'True' or 'FALSE'.
		params['resource']='filesystem'
		params['recursive']=str(recursive).lower()

		if not directory is None:
			params['directory']=directory
		if not continuation is None:
			params['continuation']=continuation
		if not maxResults is None:
			params['maxResults']=maxResults
		if not upn is None:
			params['upn']=upn
		if not timeout is None:
			params['timeout']=timeout
		
		# Execute the request
		response = requests.get(url, params=params, headers=request_headers)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		return response
	
	def path_read(self
		, filesystem
		, path
		, timeout = None
		, request_headers = None
		):
		"""
		Read the contents of a file. For read operations, range requests are supported.
		This operation supports conditional HTTP requests.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/read

		Basic variant:
		GET https://{accountName}.{dnsSuffix}/{filesystem}/{path}

		With optional parameters:
		GET https://{accountName}.{dnsSuffix}/{filesystem}/{path}?timeout={timeout}
		"""
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/{filesystem}/{path}'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
			, filesystem = filesystem
			, path = path
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.OBJECT
			, permission=AccountPermissions(read=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		if not timeout is None:
			params['timeout']=timeout

		# Execute the request
		response = requests.get(url, params=params, headers=request_headers)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		if response.headers['Content-Type'] == 'text/plain':
			return response.text
		elif response.headers['Content-Type'] == 'application/json':
			return json.loads(response.text)
		elif response.headers['Content-Type'] == 'application/octet-stream':
			raise NotImplementedError()
		else:
			raise TypeError('The returned response has an unknown content type: [{}]'.format(response.headers['Content-Type']))

	def path_update(self
		, filesystem
		, path
		, action
		, position = None
		, retainUncommittedData = None
		, close = None
		, timeout = None
		, request_headers = None
		, data_to_append = None
		):
		'''
		Uploads data to be appended to a file, flushes (writes) previously uploaded data to a file,
		sets properties for a file or directory, or sets access control for a file or directory.
		Data can only be appended to a file.
		This operation supports conditional HTTP requests.
		https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update

		Basic variant:
		PATCH http://{accountName}.{dnsSuffix}/{filesystem}/{path}?action={action}

		With optional parameters:
		PATCH http://{accountName}.{dnsSuffix}/{filesystem}/{path}?action={action}&position={position}&retainUncommittedData={retainUncommittedData}&close={close}&timeout={timeout}
		'''
		
		url = 'https://{storage_account_name}.{azure_datalake_dns_suffix}/{filesystem}/{path}'.format(
			storage_account_name = self.__storage_account_name
			, azure_datalake_dns_suffix = self.__azure_datalake_dns_suffix
			, filesystem = filesystem
			, path = path
		)

		sas_token = self.__account_sas_generator.generate_account(
			services = Services.BLOB
			, resource_types = ResourceTypes.OBJECT
			, permission=AccountPermissions(write=True)
			, expiry=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)
			, start=None
			, ip=None
			, protocol=Protocol.HTTPS
		)

		# Create the params of the query from the sas_token
		params = parse_qs(sas_token)
		# Add specific params for this operation
		params['action']=action
		
		if not position is None:
			params['position']=position
		
		if not retainUncommittedData is None:
			params['retainUncommittedData']=retainUncommittedData
			
		if not close is None:
			params['close']=close
			
		if not timeout is None:
			params['timeout']=timeout

		# Execute the request
		response = requests.patch(url, params=params, headers=request_headers, data=data_to_append)

		# Raise an error if the response code is not a positive one
		response.raise_for_status()

		return response.headers
