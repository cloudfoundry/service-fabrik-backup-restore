import sys

def create_iaas_client(operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time=None,
                       poll_maximum_time=None):
    """Creates an IaaS client which abstracts provider specifics.
    For a given iaas_client, the logger updates the last_operation file for each log message X with level INFO
    by 'state=processing' and 'step=X'.

    :param operation_name: the name of the operation (either 'backup' or 'restore'), used for the lastoperation file
    :type operation_name: string
    :param configuration: dictionary containing configuration and credentials required for estabilishing connections (created from parse_options())
    :type configuration: dict
    :param directory_persistent: path of the persistent directory
    :type directory_persistent: string
    :param directory_work_list: list of paths to directories to work with during the backup/restore procedure
    :type directory_work_list: list
    :param poll_delay_time: time in seconds for polling events, e.g. checking if a created volume is ready (default: 5)
    :type poll_delay_time: integer
    :param poll_maximum_time: maximum waiting time in seconds for events to finish before aborting (default: 300)
    :type poll_maximum_time: integer

    :returns: an ``AwsClient`` or ``OpenstackClient`` or ``BoshliteClient`` object

    :Example:
       ::

           configuration = parse_options('backup')
           iaas_client = create_iaas_client('backup', configuration, '/var/vcap/store',
                                            ['/tmp/service-fabrik-backup/snapshot', '/tmp/service-fabrik-backup/uploads'])
    """
    iaas = configuration['iaas'].title() + 'Client'
    try:
        return getattr(__import__(iaas, globals(), locals(), [], 1), iaas)\
            (operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
             poll_maximum_time)
    except ImportError as error:
        print('[CONFIG] ERROR: <{}> is not implemented: {}'.format(iaas, error), file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        print('[CONFIG] ERROR: Could not create {}: {}'.format(iaas, error), file=sys.stderr)
        sys.exit(1)
