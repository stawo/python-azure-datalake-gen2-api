# ---------------------------------------------------------------------

# LIBRARIES

# External Libraries
import unittest
import yaml
import sys

# Internal Libraries
from pyadlgen2.azuredatalakegen2 import AzureDataLakeGen2

# ---------------------------------------------------------------------
# PARAMETERS

TEST_CONFIGURATION_FILE = 'test/config.yaml'

# ---------------------------------------------------------------------

class TestFile(unittest.TestCase):
	'''
	This test class checks if the REST calls use HTTPS as method
	and not HTTP.
	'''

	def setUp(self):
		
		with open(TEST_CONFIGURATION_FILE, 'r') as ymlfile:
			configuration = yaml.load(ymlfile)
		
		self.datalake = AzureDataLakeGen2(
			storage_account_name = configuration['storage_account_name']
			, storage_account_key = configuration['storage_account_key']
		)

	def test_file_creation(self):
		"""
		Test that checks if a file can be created
		"""
		file_name = 'test.txt'
		file_data = 'test'
		
		self.datalake.create_file()

		self.assertEqual(self.datalake.read_file(file_name), file_data)

if __name__ == '__main__':
	unittest.main()