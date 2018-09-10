import os

#setting environment varibale before importing module 
os.environ['SF_IAAS_CLIENT_MAX_RETRY'] = '1'
os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = 'tests/logs'
os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = 'tests/logs'