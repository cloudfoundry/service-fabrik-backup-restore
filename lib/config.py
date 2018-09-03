import os
import shutil
from argparse import ArgumentParser
from .utils.merge_dict import merge_dict
from .logger import init_logger

parameters = {
    'iaas': 'the underlying IaaS provider [possible values: aws/azure/gcp/ali/openstack/boshlite]',
    'type': 'online or offline backup [possible values: online/offline]'
}

parameters_credentials = {
    'aws': {
        'access_key_id': 'AWS Access Key ID',
        'secret_access_key': 'AWS Secret Access Key',
        'region_name': 'AWS Region Name',
        'max_retries': 'Max number of retries for SDK AWS client'
    },
    'ali': {
        'access_key_id': 'Ali Access Key ID',
        'secret_access_key': 'Ali Secret Access Key',
        'region_name': 'Ali Region Name',
        'endpoint': 'Ali Endpoint name'
    },
    'azure': {
        'subscription_id': 'Azure subscription id',
        'resource_group': 'Azure resource group name in subscription',
        'client_id': 'Azure Active Directory Application client id',
        'client_secret': 'Azure Active Directory Application Secret',
        'tenant_id': 'Azure Active Directory tenant id',
        'storageAccount': 'Azure storage account name',
        'storageAccessKey': 'Azure storage account key'
    },
    'gcp': {
        'projectId': 'GCP Project id',
        'credentials': 'GCP Service Account Details'
    },
    'openstack': {
        'tenant_id': 'OpenStack Tenant-ID the VM is deployed in',
        'tenant_name': 'OpenStack Tenant-Name',
        'auth_url': 'OpenStack Keystone Authentication URL',
        'user_domain_name': 'OpenStack Domain Name',
        'username': 'OpenStack user name with Swift privileges',
        'password': 'OpenStack user password'
    },
    'credhub': {
        'credhub_url' : 'Credhub endpoint',
        'credhub_uaa_url': 'Endpoint of UAA for credhub',
        'credhub_key': 'key to the iaas credentials object in credhub',
        'credhub_client_id': 'OAUTH client id used in access token generation',
        'credhub_client_secret': 'OAUTH client secret used in access token generation',
        'credhub_username': 'OAUTH user id used for authentication',
        'credhub_user_password': 'OAUTH password used for authentication'
    },
    'boshlite': {}
}

parameters_backup = {
    'backup_guid': 'a guid used to identify the backup',
    'instance_id': 'the ID of the AWS/OpenStack instance to be backed up',
    'secret': 'the password used for the encryption of the backup set',
    'container': 'a container in the object storage in which the backup set can be stored',
    'job_name': 'the name of the service job under which it is registered to monit'
}

parameters_restore = {
    'backup_guid': 'the guid of the backup to be restored',
    'instance_id': 'the ID of the AWS/OpenStack instance to be restored',
    'secret': 'the password used for the decryption of the backup set',
    'container': 'a container in the object storage from which to restore data',
    'job_name': 'the name of the service job under which it is registered to monit',
    'agent_id': 'the agent id, provided by spec.id',
    'agent_ip': 'IP of the agent VM'
}

parameters_blob_operation = {
    'container': 'a container in the object storage from which to restore data'
}

parameters_restore_optional = {
    'agent_id': 'the agent id',
    'agent_ip': 'IP of the agent VM'
}


def _get_parameters_credentials():
    return parameters_credentials


def _get_parameters_backup():
    return merge_dict(parameters, parameters_backup)


def _get_parameters_restore():
    return merge_dict(parameters, parameters_restore)

def _get_parameters_restore_optional():
    return parameters_restore_optional

def _get_parameters_blob_operation():
    return merge_dict(parameters, parameters_blob_operation)

def remove_all_files(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        os.makedirs(dir_path)

# this function will remove all the files in the directories pointed by SF_BACKUP_RESTORE_LOG_DIRECTORY
# and SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY
def remove_old_logs_state():
    directory_logfile = os.getenv('SF_BACKUP_RESTORE_LOG_DIRECTORY')
    directory_last_operation = os.getenv('SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY')

    # Verify that the required environment variables are provided
    assert directory_logfile is not None, 'SF_BACKUP_RESTORE_LOG_DIRECTORY environment variable is not set.'
    assert directory_last_operation is not None, 'SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY environment variable is not set.'

    remove_all_files(directory_logfile)
    remove_all_files(directory_last_operation)
    
def parse_options(type):
    """Parse the required command line options for the given operation type.

    :param type: a string containing either `backup` or `restore`
    :returns: a dictionary containing the key-value pairs of the provided parameters

    :Example:
        ::

            configuration = parse_options('backup')
    """

    # first, remove all the old logs and data
    remove_old_logs_state()

    # TODO: conflict_handler='resolve' is really required ??
    parser = ArgumentParser(conflict_handler='resolve')
    if type == 'backup':
        for name, description in _get_parameters_backup().items():
            parser.add_argument('--{}'.format(name),
                                help=description, required=True)
    elif type == 'restore':
        for name, description in _get_parameters_restore().items():
            if name in _get_parameters_restore_optional().keys():
            	parser.add_argument('--{}'.format(name), help=description, required=False)
            else:
                parser.add_argument('--{}'.format(name), help=description, required=True)
    elif type == 'blob_operation':
        for name, description in _get_parameters_blob_operation().items():
            parser.add_argument('--{}'.format(name),
                                help=description, required=True)
    else:
        raise Exception('Use either \'backup\' or \'restore\' as type.')

    for key, credentials in _get_parameters_credentials().items():
        for name, description in credentials.items():
            parser.add_argument('--{}'.format(name), help=description)
    configuration = vars(parser.parse_args())
    assert configuration['type'] == 'online' or configuration['type'] == 'offline', \
        '--type must be \'online\' or \'offline\''
    return configuration


def initialize(operation_name):
    directory_logfile = os.getenv('SF_BACKUP_RESTORE_LOG_DIRECTORY')
    directory_last_operation = os.getenv(
        'SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY')

    # Verify that the required environment variables are provided
    assert directory_logfile is not None, 'SF_BACKUP_RESTORE_LOG_DIRECTORY environment variable is not set.'
    assert directory_last_operation is not None, 'SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY environment variable is ' \
                                                 'not set.'

    for operation in ['backup', 'restore', 'blob_operation']:
        # +-> Define paths for log and last operation file
        path_log = os.path.join(directory_logfile, operation + '.log')
        path_blue = os.path.join(
            directory_last_operation, operation + '.lastoperation.blue.json')
        path_green = os.path.join(
            directory_last_operation, operation + '.lastoperation.green.json')
        path_link = os.path.join(
            directory_last_operation, operation + '.lastoperation.json')
        # +-> Create .log file if it does not exist
        if not os.path.exists(path_log):
            open(path_log, 'w+').close()
        # +-> Create last operation blue/green files if they do not exist
        if not os.path.exists(path_blue):
            open(path_blue, 'w+').close()
        if not os.path.exists(path_green):
            open(path_green, 'w+').close()
        # +-> Create symlink to blue file and clear old log entries
        if operation == operation_name:
            os.system('ln -sf {} {}'.format(path_blue, path_link))
            open(path_log, 'w+').close()

    init_logger(os.path.join(directory_logfile, operation_name + '.log'))
