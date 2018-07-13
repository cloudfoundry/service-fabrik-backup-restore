import os
import pytest
import ast
from lib.clients.OpenstackClient import OpenstackClient
from lib.clients.BaseClient import BaseClient
from unittest.mock import patch
from lib.models.Snapshot import Snapshot
from lib.models.Volume import Volume
from keystoneauth1.session import Session as KeystoneSession
from novaclient.v2.servers import Server
from cinderclient.v2.volume_snapshots import Snapshot as CinderSnapshot
from cinderclient.v2.volumes import Volume as CinderVolume
from cinderclient.exceptions import NotFound
from swiftclient.exceptions import ClientException

operation_name = 'backup'
project_id = 'openstack-dev'
valid_container = 'backup-container'
invalid_container = 'invalid-container'
configuration = {
    'credhub_url': None,
    'type': 'online',
    'backup_guid': 'backup-guid',
    'instance_id': 'vm-id',
    'secret': 'xyz',
    'job_name': 'service-job-name',
    'container': valid_container,
    'projectId': project_id,
    'username': 'name',
    'password': 'pass',
    'auth_url': 'url',
    'user_domain_name': 'domain',
    'tenant_id': 'id',
    'tenant_name': project_id
}
directory_persistent = '/var/vcap/store'
directory_work_list = '/tmp'
log_dir = 'tests'
poll_delay_time = 10
poll_maximum_time = 60
availability_zone = 'rot_2_1'
valid_snapshot_name = 'snapshot-id'
not_found_snapshot_name = 'notfound-snapshot-id'
valid_disk_name = 'disk-id'
not_found_disk_name = 'notfound-disk-id'


class CinderClient:
    class volumes:
        def get(volume_id):
            if volume_id == valid_disk_name:
                info = file_to_dict(
                    'tests/data/openstack/cinder.volumes.get.txt')
                return CinderVolume(None, info, True, None)
            elif volume_id == not_found_disk_name:
                raise NotFound(
                    **file_to_dict('tests/data/openstack/cinder.volumes.get.notfound.txt'))

    class volume_snapshots:
        def get(snapshot_id):
            if snapshot_id == valid_snapshot_name:
                info = file_to_dict(
                    'tests/data/openstack/cinder.volume_snapshots.get.txt')
                return CinderSnapshot(None, info, True, None)
            elif snapshot_id == not_found_snapshot_name:
                raise NotFound(
                    **file_to_dict('tests/data/openstack/cinder.volume_snapshots.get.notfound.txt'))


class NovaClient:
    class servers:
        def get(instance_id):
            info = file_to_dict(
                'tests/data/openstack/nova.servers.get.instance_id.txt')
            return Server(None, info, True, None)


class SwiftClient:
    def head_container(self, container):
        if container == valid_container:
            return file_to_dict('tests/data/openstack/swift.head_container.txt')
        elif container == invalid_container:
            raise ClientException(
                **file_to_dict('tests/data/openstack/swift.head_container.notfound.txt'))

    def get_auth(self):
        return [{}]


class SwiftService:
    def upload():
        pass


def file_to_dict(file_path):
    text = open(file_path, 'r').read().replace('\n', '')
    return ast.literal_eval(text)


def create_start_patcher(patch_function, patch_object=None, return_value=None, side_effect=None):
    if patch_object != None:
        patcher = patch.object(patch_object, patch_function)
    else:
        patcher = patch(patch_function)

    patcher_start = patcher.start()
    if return_value != None:
        patcher_start.return_value = return_value

    if side_effect != None:
        patcher_start.side_effect = side_effect

    return patcher


def stop_all_patchers(patchers):
    for patcher in patchers:
        patcher.stop()


class TestOpenstackClient:
    # Store all patchers
    patchers = []

    @classmethod
    def setup_class(self):
        self.patchers.append(create_start_patcher(
            patch_function='last_operation', patch_object=BaseClient))
        self.patchers.append(create_start_patcher(
            patch_function='keystoneauth1.identity.v3.Password'))
        self.patchers.append(create_start_patcher(
            patch_function='keystoneauth1.session.Session'))
        self.patchers.append(create_start_patcher(
            patch_function='get_project_id', patch_object=KeystoneSession))
        self.patchers.append(create_start_patcher(
            patch_function='create_cinder_client', patch_object=OpenstackClient, return_value=CinderClient()))
        self.patchers.append(create_start_patcher(
            patch_function='create_nova_client', patch_object=OpenstackClient, return_value=NovaClient()))
        self.patchers.append(create_start_patcher(
            patch_function='create_swift_client', patch_object=OpenstackClient, return_value=SwiftClient()))
        self.patchers.append(create_start_patcher(
            patch_function='create_swift_service', patch_object=OpenstackClient, return_value=SwiftService()))

        os.environ['SF_BACKUP_RESTORE_LOG_DIRECTORY'] = log_dir
        os.environ['SF_BACKUP_RESTORE_LAST_OPERATION_DIRECTORY'] = log_dir

        self.osClient = OpenstackClient(operation_name, configuration, directory_persistent, directory_work_list,
                                        poll_delay_time, poll_maximum_time)

    @classmethod
    def teardown_class(self):
        stop_all_patchers(self.patchers)

    def test_create_os_client(self):
        assert isinstance(self.osClient.nova, NovaClient)
        assert isinstance(self.osClient.cinder, CinderClient)
        assert isinstance(self.osClient.swift, SwiftClient)
        assert isinstance(self.osClient.swift.service, SwiftService)
        assert self.osClient.availability_zone == availability_zone
        assert self.osClient.container == file_to_dict(
            'tests/data/openstack/swift.head_container.txt')

    def test_get_container_exception(self):
        self.osClient.CONTAINER = invalid_container
        assert self.osClient.get_container() is None
        self.osClient.CONTAINER = valid_container

    def test_get_snapshot(self):
        expected_snapshot = Snapshot(valid_snapshot_name, 40, 'available')
        snapshot = self.osClient.get_snapshot(valid_snapshot_name)
        assert expected_snapshot.id == snapshot.id
        assert expected_snapshot.size == snapshot.size
        assert expected_snapshot.status == snapshot.status

    def test_get_snapshot_exception(self):
        assert self.osClient.get_snapshot(not_found_snapshot_name) is None

    def test_get_volume(self):
        expected_volume = Volume(valid_disk_name, 'in-use', 40)
        volume = self.osClient.get_volume(valid_disk_name)
        assert expected_volume.id == volume.id
        assert expected_volume.size == volume.size
        assert expected_volume.status == volume.status

    def test_get_volume_exception(self):
        self.osClient.get_volume(not_found_disk_name) is None
