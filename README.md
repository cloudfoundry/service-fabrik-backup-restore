# Backup & Restore Python Library for CF Services managed by the Service Fabrik 

## Overview
This library arose during the development of Backup & Restore for the Blueprint-Service as part of the Service Fabrik‘s Backup & Restore offering. It can be used for two IaaS providers, Amazon Web Services and OpenStack, as it abstracts some basic functionalities, like volume creation/deletion/attaching/detaching, by implementing provider-specific clients. 

  However, for local development also a BOSH-Lite Client is provided which allows speeding up the development process before actually testing the scripts on real infrastructure providers. Service teams may like to use the library to avoid implementing the same features from scratch during their service-specific Backup & Restore development.

## Features

The following features are exposed:

- Abstraction of the underlying IaaS provider
- Implicit waiting for operations to be finished before further processing
- Implicit retry logic to catch briefly occurring infrastructure problems
- Reliable clean-up of created resources to avoid orphans (in case of errors)
- Proper exception handling in case of errors
- Enabling easy and transparent backup & restore implementations

## Prerequisites

You need to have [Python3](https://www.python.org/downloads/) as well as [pip3](https://pip.pypa.io/en/stable/installing/) installed. pip3 is used for the installation of the library’s dependencies. Please find further details on links given.


## How to use this project

Assuming, your working directory is ~/workspace/my_service:

### CLONE THIS REPOSITORY

Most likely your working directory is already a git repository so that you may want to add this repository as submodule:
```
$ cd ~/workspace/my_service
$ git submodule add https://github.com/SAP/service-fabrik-backup-restore.git service_fabrik_backup_restore
```
Otherwise, just clone this repository (assuming your working directory is ~/workspace/my_service:
```
$ cd ~/workspace/my_service
$ git clone https://github.com/SAP/service-fabrik-backup-restore.git service_fabrik_backup_restore
```

### INSTALL THE DEPENDENCIES

pip3 is the PyPa recommended tool for installing Python packages:
```
$ cd ~/workspace/my_service/service_fabrik_backup_restore
$ pip3 install -r requirements.txt
```

### DEVELOPING WITH THE LIBRARY
We are using [pytest](https://pytest.readthedocs.io/en/3.5.1/index.html) as a unit tests framework. You can use setup tool to run tests with multiple options:
--coverage or -c : Generate coverage report
--capture or -p : To show stdout/stderr on console
```
python3 setup.py test --coverage --capture
# Or
python3 setup.py test -c -p
```
Or you can also use simple pytest command to do so:
```
pip3 install -r requirements-dev.txt
python3 -m pytest --cov=lib --cov-report html -v tests/ --capture=no
```

### USING THE LIBRARY

Basically, you have to create a new file, e.g. backup.py, and import the Backup & Restore library:
```
$ cd ~/workspace/my_service
$ touch backup.py
```
Within the backup.py, put this line at the top of the file:
```
from service_fabrik_backup_restore import create_iaas_client, parse_options
# your code here
```
Now you can start implementing the backup / restore for your service and take advantage of this library’s functionalities.

For Google cloud the script can be invoked as:
```
credential_json=$(cat service-account.json)
python3 test_br.py                     \
  --iaas=gcp                           \
  --type=online                        \
  --backup_guid=<guid>                 \
  --instance_id=<vm-XXXX-XXXX>         \
  --container=<google-cloud-container> \
  --job_name=<name>                    \
  --credentials="$credential_json"     \
  --projectId=<project id>             \
  --secret=<secret>
```

For Openstack the script can be invoked as:
```
python3 backup.py                  \
  --iaas=openstack                 \
  --type=online                    \
  --backup_guid=<guid>             \
  --instance_id=<instance-id>      \
  --secret=<secret>                \
  --container=<container>          \
  --job_name=<name>                \
  --tenant_id=<tenant-id>          \
  --tenant_name=<tenant-name>      \
  --auth_url=<auth-url>            \
  --user_domain_name=<domain-name> \
  --username=<username>            \
  --password=<password>
```

For AWS the script can be invoked as:

```
python3 test_br.py                     \
  --iaas=aws                           \
  --type=online                        \
  --backup_guid=bkp_guid               \
  --instance_id=< i-0dccbdfa125b9781c> \
  --secret=xxxxxxxx                    \
  --container=<container>              \
  --job_name=<name>                    \
  --access_key_id=<key>                \
  --secret_access_key=<accesskey>      \
  --region_name=<region>
```

## How to obtain support
 
If you need any support, have any question or have found a bug, please report it in the [GitHub bug tracking system](https://github.com/sap/service-fabrik-backup-restore/issues). We shall get back to you.

## LICENSE

This project is licensed under the Apache Software License, v. 2 except as noted otherwise in the [LICENSE](LICENSE) file
