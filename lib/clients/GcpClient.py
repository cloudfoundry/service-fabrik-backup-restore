from .BaseClient import BaseClient
from googleapiclient import discovery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.storage import Blob

class GcpClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(GcpClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)
        
        self.__gcpCredentials = configuration['credentials']
        self.project_id = configuration['projectId']
        self.tags= [{'Key':'instance_id','Value':self.INSTANCE_ID},{'Key':'job_name','Value':self.JOB_NAME}]

        self.compute_api_name = 'compute'
        self.compute_api_version = 'v1'

        self.compute_client = create_compute_client()
        self.storage_client = create_storage_client()
        
        # +-> Check whether the given container exists and is accessible
        if (not self.get_container()) or (not self.access_container()):
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

        # +-> Get the availability zone of the instance
        self.availability_zone = self._get_availability_zone_of_server(configuration['instance_id'])
        if not self.availability_zone:
            msg = 'Could not retrieve the availability zone of the instance.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

    def create_compute_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_info(self.__gcpCredentials)
            compute_client = discovery.build(self.compute_api_name, self.compute_api_version, credentials=credentials)
            return compute_client
        except Exception as error:
            raise Exception('Creation of compute client failed: {}'.format(error))

    def create_storage_client(self):
        try:
            credentials = service_account.Credentials.from_service_account_info(self.__gcpCredentials)
            storage_client = storage.Client(self.project_id, self.__gcpCredentials)
            return storage_client
        except Exception as error:
            raise Exception('Creation of storage client failed: {}'.format(error))

    def _get_availability_zone_of_server(self, instance_id):
        try:
            expression = 'name eq ' + instance_id
            request = compute_client.instances().aggregatedList(project=self.project_id, filter=expression)
            while request is not None:
                response = request.execute()
                for zone, instances_scoped_list in response['items'].items():
                    if 'warning' in instances_scoped_list:
                        pass
                    elif 'instances' in instances_scoped_list:
                        for instance in instances_scoped_list['instances']:
                            if instance['name'] == instance_id:
                                # Assuming the last part of the url is zone-name
                                return instance['zone'].rsplit('/', 1)[1]

                request = compute_client.instances().aggregatedList_next(previous_request=request, previous_response=response)

            print('[GCP] ERROR: Unable to determine the availability zone of instance{}.\n'.format(instance_id))
            return None
        except Exception as error:
            print('[GCP] ERROR: Unable to determine the availability zone of instance {}.\n{}'.format(instance_id, error))
            return None

    def get_container(self):
        try:
            container = storage_client.get_bucket(self.CONTAINER)
            return container
        except Exception as error:
            self.logger.error('[GCP] [STORAGE] ERROR: Unable to find container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def access_container(self):
        # Test if the container is accessible
        try:
            bucket = storage_client.get_bucket(self.CONTAINER)
            blob = Blob('AccessTestByServiceFabrikPythonLibrary', bucket)
            blob.upload_from_string('Sample Message for AccessTestByServiceFabrikPythonLibrary', content_type='text/plain')
            blob.delete()
            return True
        except Exception as error:
            self.logger.error('[Azure] [STORAGE] ERROR: Unable to access/delete container {}.\n{}'.format(
                self.CONTAINER, error))
            return False
