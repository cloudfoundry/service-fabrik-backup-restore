import oss2
from oss2.headers import RequestHeader
from .BaseClient import BaseClient
from aliyunsdkcore.request import CommonRequest
from aliyunsdkcore.client import AcsClient


class AliClient(BaseClient):
    def __init__(self, operation_name, configuration, directory_persistent, directory_work_list, poll_delay_time,
                 poll_maximum_time):
        super(AliClient, self).__init__(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)
        if configuration['credhub_url'] is None:
            self.__aliCredentials = [configuration['access_key_id'], configuration['secret_access_key']]
            auth = oss2.Auth(
                configuration['access_key_id'], configuration['secret_access_key'])
            endpoint = configuration['endpoint']
        else:
            credentials = self._get_credentials_from_credhub(configuration)
            self.__aliCredentials = credentials
            auth = oss2.Auth(
                credentials['access_key_id'], credentials['secret_access_key'])
            endpoint = credentials['endpoint']
        # +-> Create compute and storage clients
        self.compute_client = self.create_compute_client()

        # +-> Check whether the given container exists
        self.container = self.get_container(auth, endpoint)
        if not self.container:
            msg = 'Could not find or access the given container.'
            self.last_operation(msg, 'failed')
            raise Exception(msg)

    def create_compute_client(self):
        try:
            credentials = self.__aliCredentials
            compute_client = AcsClient(credentials["access_key_id"], credentials["secret_access_key"], credentials["region_name"], auto_retry=True,
            max_retry_time=10, timeout=30)
            return compute_client
        except Exception as error:
            raise Exception(
                'Creation of compute client failed: {}'.format(error))

    def get_common_request(action_name, params, tags=None):
        request = CommonRequest()
        request.set_domain('ecs.aliyuncs.com')
        request.set_version('2014-05-26')
        request.set_action_name(action_name)

        i = 1
        if tags != None:
            for key in tags:
                paramKey = 'Tag.' + str(i) + '.Key'
                request.add_query_param(paramKey, key)
                paramVal = 'Tag.' + str(i) + '.Value'
                request.add_query_param(paramVal, tags[key])
                i += 1

        for key in params:
            request.add_query_param(key, params[key])

        return request

    def get_container(self, auth, endpoint):
        try:
            container = oss2.Bucket(auth, endpoint, self.CONTAINER)
            # Test if the container is accessible
            key = '{}/{}'.format(self.BLOB_PREFIX,
                                 'AccessTestByServiceFabrikPythonLibrary')
            container.put_object(key, 'This is a sample text')
            container.delete_object(key)
            return container
        except Exception as error:
            self.logger.error('[OSS] ERROR: Unable to find or access container {}.\n{}'.format(
                self.CONTAINER, error))
            return None

    def _upload_to_blobstore(self, blob_to_upload_path, blob_target_name):
        log_prefix = '[OSS] [UPLOAD]'

        if self.container:
            self.logger.info(
                '{} Started to upload the tarball to the object storage.'.format(log_prefix))
            try:
                requestHeader = RequestHeader()
                requestHeader.set_server_side_encryption("AES256")
                self.container.put_object_from_file(
                    blob_target_name, blob_to_upload_path, headers=requestHeader)
                self.logger.info('{} SUCCESS: blob_to_upload={}, blob_target_name={}, container={}'
                                 .format(log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_upload={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_upload_path, blob_target_name, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)

    def _download_from_blobstore(self, blob_to_download_name, blob_download_target_path):
        log_prefix = '[OSS] [DOWNLOAD]'

        if self.container:
            self.logger.info('{} Started to download the tarball to target{}.'
                             .format(log_prefix, blob_download_target_path))
            try:
                self.container.get_object(
                    blob_to_download_name, blob_download_target_path).read()
                self.logger.info('{} SUCCESS: blob_to_download={}, blob_target_name={}, container={}'.format(
                    log_prefix, blob_to_download_name, self.CONTAINER, blob_download_target_path))
                return True
            except Exception as error:
                message = '{} ERROR: blob_to_download={}, blob_target_name={}, container={}\n{}'.format(
                    log_prefix, blob_to_download_name, blob_download_target_path, self.CONTAINER, error)
                self.logger.error(message)
                raise Exception(message)

    
    def _get_snapshot_state(snapshot_id, region_id){
        """Gets the snapshot state.
        https://www.alibabacloud.com/help/doc-detail/25641.htm?spm=a2c63.p38356.a3.5.c880458dkimfOs#SnapshotType
        Status can be "progressing" "accomplished" or "failed"
        """
        # Try until snapshot state is either "accomplished" or "failed"
        # Retries for failures also
        try:
            get_snapshot_req_params = {
                'PageSize' : 10,
                'RegionId' : region_id,
                'SnapshotIds' : [snapshot_id]
            }
            get_snapshot_request = get_common_request('DescribeSnapshots', get_snapshot_req_params)
            snapshot_details = self.compute_client.do_action_with_exception(get_snapshot_request)
            snapshot_details_json = json.loads(snapshot_details.decode("utf-8"))
            if len(snapshot_details_json['Snapshots']['Snapshot']) > 1:
                self.logger.info('[ALI] ERROR: More than 1 snapshot found for with name {}'.format(
                snapshot_name))
                return True
            snapshot_state = snapshot_details_json['Snapshots']['Snapshot'][0]['Status']
            if snapshot_state == 'accomplished' || snapshot_state == 'failed':
                return True
            return False
        except Exception as error:
            self.logger.error(
                '[ALI] ERROR: Unable to get snapshot details for {}.\n{}'.format(snapshot_id, error))
            return False
    }

    def _create_snapshot(self, volume_id, description='Service-Fabrik: Automated backup'):
        log_prefix = '[SNAPSHOT] [CREATE]'
        snapshot = None
        snapshot_creation_operation = None
        region_id = self.__aliCredentials["region_name"]
        snapshot_name = self.generate_name_by_prefix(self.SNAPSHOT_PREFIX)
        try:
            snapshot_req_params = {
                'DiskId': volume_id,
                'SnapshotName': snapshot_name,
                'Description': description
            }
            self.logger.info('{} START for volume id {} with tags {} and snapshot name {}'.format(
                log_prefix, volume_id, self.tags, snapshot_name))
            snpshot_creation_request = get_common_request('CreateSnapshot', snapshot_req_params, self.tags)
            snapshot_creation_operation = self.compute_client.do_action_with_exception(snpshot_creation_request)

            self._wait('Waiting for snapshot {} to get ready...'.format(snapshot_name),
                       (lambda snapshot_id, region_id: self._get_snapshot_state(snapshot_id, region_id)),
                       None,
                       snapshot_name, region_id)

            snapshot = self.get_snapshot(snapshot_name)
            if snapshot.status == 'accomplished':
                self._add_snapshot(snapshot.id)
                self.output_json['snapshotId'] = snapshot.id
                self.logger.info('{} SUCCESS: snapshot-id={}, volume-id={}, status={} with tags {}'.format(
                    log_prefix, snapshot.id, volume_id, snapshot.status, self.tags))
            else:
                message = '{} ERROR: snapshot-id={} status={}'.format(
                    log_prefix, snapshot_name, snapshot.status)
                raise Exception(message)
        except Exception as error:
            message = '{} ERROR: volume-id={} and tags={}\n{}'.format(
                log_prefix, volume_id, self.tags, error)
            self.logger.error(message)
            if snapshot or snapshot_creation_operation:
                self.delete_snapshot(snapshot_name)
                snapshot = None
            raise Exception(message)

        return snapshot

    def _copy_snapshot(self, snapshot_id):
        return self.get_snapshot(snapshot_id)

    def _delete_snapshot(self, snapshot_id):
        log_prefix = '[SNAPSHOT] [DELETE]'
        try:
            snapshot_deletion_req_params = {
                'SnapshotName': snapshot_id
            }
            snpshot_deletion_request = get_common_request('DeleteSnapshot', snapshot_deletion_req_params)
            snapshot_deletion_operation = self.compute_client.do_action_with_exception(snpshot_deletion_request)

            self._wait('Waiting for snapshot {} to be deleted...'.format(snapshot_id),
                       lambda id: not self.get_snapshot(id),
                       None,
                       snapshot_id)
            self._remove_snapshot(snapshot_id)
            self.logger.info(
                '{} SUCCESS: snapshot-id={}'.format(
                    log_prefix, snapshot_id))
            return True
        except Exception as error:
            message = '{} ERROR: snapshot-id={}\n{}'.format(
                log_prefix, snapshot_id, error)
            self.logger.error(message)
            raise Exception(message)
    
    def get_snapshot(self, snapshot_name):
        try:
            region_id = self.__aliCredentials["region_name"]
            get_snapshot_req_params = {
                'PageSize' : 10,
                'RegionId' : region_id,
                'SnapshotIds' : [snapshot_id]
            }
            get_snapshot_request = get_common_request('DescribeSnapshots', get_snapshot_req_params)
            snapshot_details = self.compute_client.do_action_with_exception(get_snapshot_request)
            snapshot_details_json = json.loads(snapshot_details.decode("utf-8"))
            if len(snapshot_details_json['Snapshots']['Snapshot']) > 1:
                message = 'More than 1 snapshot found for with name {}'.format(
                snapshot_name)
                raise Exception(message)
            if len(snapshot_details_json['Snapshots']['Snapshot']) == 0:
                return None
            snapshot = snapshot_details_json['Snapshots']['Snapshot'][0]
            return Snapshot(snapshot['SnapshotId'], snapshot['SourceDiskSize'], snapshot['CreationTime'], snapshot['Status'])
        except Exception as error:
            message = '[ALI] ERROR: Error in getting snapshot {}.\n{}'.format(
                snapshot_name, error)
            raise Exception(message)

    def generate_name_by_prefix(self, prefix):
        return '{}-{}-{}'.format(prefix,
                                 random.randrange(10000, 99999),
                                 time.strftime("%Y%m%d%H%M%S"))
