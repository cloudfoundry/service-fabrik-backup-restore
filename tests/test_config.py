from tests.utils.utilities import create_start_patcher, stop_all_patchers
import tests.utils.setup_constants
import os
import pytest
import unittest.mock as mock
import unittest
from lib.config import build_parser, remove_old_logs_state
import sys

# test data

default_parameters = {
    'iaas': 'aws',
    'type': 'online'
}

credentials_parameters = {
    'aws': {
        'access_key_id': 'secret-key',
        'secret_access_key': 'secret-access-key',
        'region_name': 'valid-region-name',
        'max_retries': 'valid-max-retries'
    },
}

ops_parameters = {
    'backup' : {
        'backup_guid': 'valid-backup-guid',
        'instance_id': 'valid-instance-id',
        'secret': 'valid-secret',
        'container': 'valid-container-name',
        'job_name': 'valid-job-name'
    },
    'restore': {
        'backup_guid': 'valid-backup-guid',
        'instance_id': 'valid-instance-id',
        'secret': 'valid-secret',
        'container': 'valid-container-name',
        'job_name': 'valid-job-name',
        'agent_id': 'valid-agent-id',
        'agent_ip': 'valid-agent-ip'
    },

    'blob_operation': {
        'container': 'valid-container-name'
    }
}

restore_optional_parameters = {
    'agent_id': 'valid-agent-id',
    'agent_ip': 'valid-agent-ip'
}

def create_operation_parameters(operation_name):
    operation_parameters = []

    # default parameters
    for key, value in default_parameters.items():
        operation_parameters.append('--{}'.format(key))
        operation_parameters.append(value)

    # operation specific parameters
    op_specific_parameters = ops_parameters[operation_name] #operation_name = 'backup'/'restore'
    for key, value in op_specific_parameters.items():
        operation_parameters.append('--{}'.format(key))
        operation_parameters.append(value)

    # credentials parameters
    for iaas, credentials in credentials_parameters.items():
        for key, value in credentials.items():
            operation_parameters.append('--{}'.format(key))
            operation_parameters.append(value)

    return operation_parameters

# tests
class TestConfig():
    patchers = []
    @classmethod
    def setup_class(self):
        pass

    @classmethod
    def teardown_class(self):
        pass

    def test_build_parser_backup(self):
        #build the parser for backup
        parser = build_parser('backup')

        #build parameters for backup
        params = create_operation_parameters('backup')

        #pass these parameters to parser and test configuration
        configuration = vars(parser.parse_args(params))
        
        assert configuration['iaas'] == default_parameters['iaas']
        assert configuration['type'] == default_parameters['type']
        
        assert configuration['access_key_id'] == credentials_parameters['aws']['access_key_id']
        assert configuration['secret_access_key'] == credentials_parameters['aws']['secret_access_key']
        assert configuration['region_name'] == credentials_parameters['aws']['region_name']
        assert configuration['max_retries'] == credentials_parameters['aws']['max_retries'] 
        assert configuration['backup_guid'] == ops_parameters['backup']['backup_guid']
        assert configuration['instance_id'] == ops_parameters['backup']['instance_id']
        assert configuration['secret'] == ops_parameters['backup']['secret']
        assert configuration['container'] == ops_parameters['backup']['container']
        assert configuration['job_name'] == ops_parameters['backup']['job_name']

    def test_build_parser_restore(self):
        #build the parser for restore
        parser = build_parser('restore')

        #build parameters for restore
        params = create_operation_parameters('restore')

        #pass these parameters to parser and test configuration
        configuration = vars(parser.parse_args(params))
        
        assert configuration['iaas'] == default_parameters['iaas']
        assert configuration['type'] == default_parameters['type']
        
        assert configuration['access_key_id'] == credentials_parameters['aws']['access_key_id']
        assert configuration['secret_access_key'] == credentials_parameters['aws']['secret_access_key']
        assert configuration['region_name'] == credentials_parameters['aws']['region_name']
        assert configuration['max_retries'] == credentials_parameters['aws']['max_retries']
        assert configuration['backup_guid'] == ops_parameters['restore']['backup_guid']
        assert configuration['instance_id'] == ops_parameters['restore']['instance_id']
        assert configuration['secret'] == ops_parameters['restore']['secret']
        assert configuration['container'] == ops_parameters['restore']['container']
        assert configuration['job_name'] == ops_parameters['restore']['job_name']
        assert configuration['agent_id'] == ops_parameters['restore']['agent_id']
        assert configuration['agent_ip'] == ops_parameters['restore']['agent_ip']

    def test_build_parser_blob_operation(self):
        #build the parser for restore
        parser = build_parser('blob_operation')

        #build parameters for restore
        params = create_operation_parameters('blob_operation')

        #pass these parameters to parser and test configuration
        configuration = vars(parser.parse_args(params))
        
        assert configuration['iaas'] == default_parameters['iaas']
        assert configuration['type'] == default_parameters['type']

        assert configuration['container'] == ops_parameters['blob_operation']['container']

    def test_build_parser_exception(self):
        with pytest.raises(Exception) as e:
            build_parser('invalid_type')
            assert e.message == 'Use either \'backup\' or \'restore\' as type.'

    def test_remove_old_logs(self):
        m = mock.mock_open()
        with mock.patch('builtins.open', m):
            remove_old_logs_state()
        
        directory_logfile = os.getenv('SF_BACKUP_RESTORE_LOG_DIRECTORY')
        directory_last_operation = os.getenv('SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY')
        for operation in ['backup', 'restore']:   
            # +-> Define paths for log and last operation file
            path_log = os.path.join(directory_logfile, operation + '.log')
            path_blue = os.path.join(directory_last_operation, operation + '.lastoperation.blue.json')
            path_green = os.path.join(directory_last_operation, operation + '.lastoperation.green.json')
            path_output_json = os.path.join(directory_logfile, operation + '.output.json')

            m.assert_any_call(path_log, 'w+')
            m.assert_any_call(path_blue, 'w+')
            m.assert_any_call(path_green, 'w+')
            m.assert_any_call(path_output_json, 'w+')