from tests.utils.utilities import create_start_patcher, stop_all_patchers
import tests.utils.setup_constants
import os
import pytest
import unittest.mock as mock
from lib.clients.AwsClient import AwsClient
from lib.clients.AzureClient import AzureClient
from lib.clients.GcpClient import GcpClient
from lib.clients.OpenstackClient import OpenstackClient
from lib.clients.index import create_iaas_client

# test data
class DummyIaasInfo:
    def __init__(self, iaas_name):
        self.name = iaas_name

    def title(self):
        return self.name

configuration_aws = { 
    'iaas': DummyIaasInfo('Aws')
}

configuration_azure = { 
    'iaas': DummyIaasInfo('Azure')
}

configuration_gcp = { 
    'iaas': DummyIaasInfo('Gcp')
}

configuration_openstack = { 
    'iaas': DummyIaasInfo('Openstack')
}
directory_persistent = '/var/vcap/store'
directory_work_list = '/tmp'

def dummy_iaas_client_constructor(operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time=None,poll_maximum_time=None):
    if operation_name == 'invalid':
        raise Exception('Induced exception')

    return None

# tests
class TestIndex:
    patchers = []
    @classmethod
    def setup_class(self):
        patches = create_start_patcher(patch_function='__init__', patch_object=AwsClient, side_effect=dummy_iaas_client_constructor)
        self.awsClientPatch = patches['patcher_start']
        self.patchers.append(patches['patcher'])

        patches = create_start_patcher(patch_function='__init__', patch_object=AzureClient, side_effect=dummy_iaas_client_constructor)
        self.azureClientPatch = patches['patcher_start']
        self.patchers.append(patches['patcher'])

        patches = create_start_patcher(patch_function='__init__', patch_object=GcpClient, side_effect=dummy_iaas_client_constructor)
        self.gcpClientPatch = patches['patcher_start']
        self.patchers.append(patches['patcher'])

        patches = create_start_patcher(patch_function='__init__', patch_object=OpenstackClient, side_effect=dummy_iaas_client_constructor)
        self.openstackClientPatch = patches['patcher_start']
        self.patchers.append(patches['patcher'])

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_create_iaas_client(self):
        dummy_iaas_client = create_iaas_client('backup', configuration_aws, directory_persistent, directory_work_list)
        self.awsClientPatch.assert_called_once()

        dummy_iaas_client = create_iaas_client('backup', configuration_azure, directory_persistent, directory_work_list)
        self.azureClientPatch.assert_called_once()

        dummy_iaas_client = create_iaas_client('backup', configuration_gcp, directory_persistent, directory_work_list)
        self.gcpClientPatch.assert_called_once()

        dummy_iaas_client = create_iaas_client('backup', configuration_openstack, directory_persistent, directory_work_list)
        self.openstackClientPatch.assert_called_once()

    def test_import_error(self):
        configuration_aws['iaas'] = DummyIaasInfo('NotImplemented')
        with mock.patch('sys.exit') as mock_sys_exit:
            dummy_iaas_client = create_iaas_client('backup', configuration_aws, directory_persistent, directory_work_list)
            mock_sys_exit.assert_called_once()
        configuration_aws['iaas'] = DummyIaasInfo('Aws')

    def test_exception_in_constructor(self):
        with mock.patch('sys.exit') as mock_sys_exit:
            dummy_iaas_client = create_iaas_client('invalid', configuration_aws, directory_persistent, directory_work_list)
            mock_sys_exit.assert_called_once()