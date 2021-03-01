#
#  dataprocpy
#  Tart
#
#  Created by Edbert Dudon on 7/8/19.
#  Copyright © 2019 Project Tart. All rights reserved.
#
#   Test:
#   $ pip install functions-framework
#   $ functions-framework --target my_function
#
#   To invoke: $ curl http://localhost:8080
#

# from flask import Flask
# from flask_cors import CORS
import time
import json

from google.cloud import dataproc_v1 as dataproc
from google.cloud.dataproc_v1.gapic.transports import (
    cluster_controller_grpc_transport)
from google.cloud.dataproc_v1.gapic.transports import (
    job_controller_grpc_transport)
from google.oauth2 import service_account
from google.protobuf.duration_pb2 import Duration

# Don't delete waiting_callback
waiting_callback = False


# [START dataproc_create_cluster]
def create_cluster(dataproc, project_id, region, cluster_name):
    print('Creating cluster...')

    duration_message = Duration()
    duration_message.FromSeconds(10800)

    cluster_data = {
        'project_id': project_id,
        'cluster_name': cluster_name,
        'config': {
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-highmem-2',
                'disk_config': {
                    'boot_disk_size_gb': 20
                }
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-highmem-2',
                'disk_config': {
                    'boot_disk_size_gb': 20
                }
            },
            'lifecycle_config': {
                'idle_delete_ttl': duration_message
            },
            'initialization_actions': [{
                'executable_file': 'gs://tart-90ca2.appspot.com/scripts/initializationScript.sh'
            }]
        }
    }

    cluster = dataproc.create_cluster(project_id, region, cluster_data)
    cluster.add_done_callback(callback)
    global waiting_callback
    waiting_callback = True
# [END dataproc_create_cluster]


def callback(operation_future):
    # Reset global when callback returns.
    global waiting_callback
    waiting_callback = False

def wait_for_cluster_creation():
    """Wait for cluster creation."""
    print('Waiting for cluster creation...')

    while True:
        if not waiting_callback:
            print("Cluster created.")
            break


# [START dataproc_submit_sparkr_job]
def submit_sparkr_job(dataproc, project_id, region, cluster_name, job_file_argument,
                    worksheet):

    job_details = {
        'placement': {
            'cluster_name': cluster_name
        },
        'spark_r_job': {
            'main_r_file_uri': 'gs://tart-90ca2.appspot.com/scripts/sparkR.R',
			'args': [
				job_file_argument,
			]
        },
        'labels': {
            'worksheet': worksheet
        }
    }

    result = dataproc.submit_job(
        project_id=project_id, region=region, job=job_details)
    job_id = result.reference.job_id
    print('Submitted job ID {}.'.format(job_id))
    return job_id
# [END dataproc_submit_sparkr_job]


# [START dataproc_delete]
def delete_cluster(dataproc, project_id, region, cluster_name):
    """Delete the cluster."""
    print("Tearing down cluster.")
    result = dataproc.delete_cluster(
        request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
    )
    result.add_done_callback(callback)
    global waiting_callback
    waiting_callback = True
    # return result


# [END dataproc_delete]


def wait_for_cluster_deletion():
    """Wait for cluster deletion."""
    print("Waiting for cluster deletion...")

    while True:
        if not waiting_callback:
            print("Cluster deleted.")
            break




def createandsubmit(request):
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': 'https://www.tartcl.com',
            # 'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600',
            'Access-Control-Allow-Credentials': 'true'
        }

        return ('', 204, headers)

    request_json = request.get_json()
    authuser = request_json['authuser']
    project_id = 'tart-90ca2'
    region = 'us-central1'
    cluster_name = 'cluster-' + authuser
    duration_message = Duration()
    duration_message.FromSeconds(10800)

    # Job arguments
    worksheet = request_json['worksheet']
    job_file_argument = request_json['jobFileArgument']
    # job_file_save = request_json['jobFileSave']
    credentials = service_account.Credentials.from_service_account_file(
        './tart-90ca2-9d37c42ef480.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client_options = {
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region),
    }

    # Use the default gRPC global endpoints.
    dataproc_cluster_client = dataproc.ClusterControllerClient(
        client_options = client_options, credentials=credentials)

    dataproc_job_client = dataproc.JobControllerClient(
        client_options = client_options, credentials=credentials)

    # else:
    #     region = region
    #     # Use a regional gRPC endpoint. See:
    #     # https://cloud.google.com/dataproc/docs/concepts/regional-endpoints
    #     client_transport = (
    #         cluster_controller_grpc_transport.ClusterControllerGrpcTransport(
    #             address='{}-dataproc.googleapis.com:443'.format(region)))
    #     job_transport = (
    #         job_controller_grpc_transport.JobControllerGrpcTransport(
    #             address='{}-dataproc.googleapis.com:443'.format(region)))
    #     dataproc_cluster_client = dataproc.ClusterControllerClient(
    #         client_transport, credentials=credentials)
    #     dataproc_job_client = dataproc.JobControllerClient(
    #         job_transport, credentials=credentials)
    # # [END dataproc_get_client]

    try:
        create_cluster(dataproc_cluster_client, project_id, region,
                           cluster_name)
        wait_for_cluster_creation()
    except:
        print("Cluster already created")

    try:
        job_resp = submit_sparkr_job(dataproc_job_client, project_id, region, cluster_name, job_file_argument,
                        worksheet)
    except:
        print("Failed submit job")
        delete_cluster(dataproc_cluster_client, project_id, region,
                            cluster_name)
        wait_for_cluster_deletion()

        create_cluster(dataproc_cluster_client, project_id, region,
                           cluster_name)
        wait_for_cluster_creation()

        try:
            job_resp = submit_sparkr_job(dataproc_job_client, project_id, region, cluster_name, job_file_argument,
                            worksheet)
        except:
            print("Failed job retry")
            job_resp =  'failed job'


    headers = {
        'Access-Control-Allow-Origin': 'https://www.tartcl.com',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Credentials': 'true'
    }

    return (job_resp, 200, headers)
