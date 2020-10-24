import argparse
import os
import time

import googleapiclient.discovery

IMAGE_FAMILY = 'basefamily'
PROJECT = 'lbyxero-cloud-cloud'
BUCKET = 'cloud-map-reduce'
ZONE = 'us-central1-a'

# [START list_instances]
def list_instances():
    compute = googleapiclient.discovery.build('compute', 'v1')
    result = compute.instances().list(project=PROJECT, zone=ZONE).execute()
    return result['items'] if 'items' in result else None
# [END list_instances]


# [START create_instance]
def create_instance(compute, project, zone, name, bucket, start_script=None):
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        project=project, family=IMAGE_FAMILY).execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    startup_script = open(start_script, 'r').read() if start_script else ''
    image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    image_caption = "Ready for dessert?"

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                "https://www.googleapis.com/auth/cloud-platform",
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            },]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()
# [END create_instance]


# [START delete_instance]
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
# [END delete_instance]


# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)
# [END wait_for_operation]

def start_instances(instance_type, count):

    compute = googleapiclient.discovery.build('compute', 'v1')
    print('Creating {} {} instances'.format(count, instance_type))

    instance_name = None
    start_script = None
    if instance_type == 'mapper':
        instance_name = 'mapper-{}'
        start_script = 'start_mapperrpc.sh'
    elif instance_type == 'reducer':
        instance_name = 'reducer-{}'
        start_script = 'start_reducerpc.sh'
    elif instance_type == 'kvstore':
        instance_name = 'kvstore'
        start_script = 'start_kvstore.sh'
    elif instance_type == 'master':
        instance_name = 'master'
    else:
        print("Unknown instance type")
        return

    if instance_type == 'kvstore' or instance_type == 'master':
        operation = create_instance(
            compute, PROJECT, ZONE, instance_name, BUCKET, start_script
        )
        wait_for_operation(compute, PROJECT, ZONE, operation['name'])
    else:
        for i in range(count):
            operation = create_instance(
                compute, PROJECT, ZONE, 
                instance_name.format(i), BUCKET, start_script
            )
            wait_for_operation(
                compute, PROJECT, ZONE, operation['name']
            )

    return get_ip_of_instances()

def delete_instances(instance_type, count):

    compute = googleapiclient.discovery.build('compute', 'v1')
    print('Deleting {} {} instances'.format(count, instance_type))

    instance_name = None
    if instance_type == 'mapper':
        instance_name = 'mapper-{}'
    elif instance_type == 'reducer':
        instance_name = 'reducer-{}'
    elif instance_type == 'kvstore':
        instance_name = 'kvstore'
    else:
        print("Unknown instance type")
        return

    if instance_type == 'kvstore':
        operation = delete_instance(compute, PROJECT, ZONE, instance_name)
        wait_for_operation(compute, PROJECT, ZONE, operation['name'])
    else:
        for i in range(count):
            operation = delete_instance(
                compute, PROJECT, ZONE, 
                instance_name.format(i)
            )
            wait_for_operation(
                compute, PROJECT, ZONE, operation['name']
            )

    instances = list_instances()
    print(instances)

def get_ip_of_instances():
    lookup = {}

    for instance in list_instances():
        name = instance['name']
        ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        lookup[name] = ip

    return lookup