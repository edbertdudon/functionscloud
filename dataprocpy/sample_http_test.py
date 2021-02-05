import json

from google.cloud import dataproc_v1 as dataproc
from google.oauth2 import service_account

project_id = 'tart-90ca2'
region = 'us-central1'
cluster_name = 'cluster-' + 'x9j4unfnydtfcpu4filasmzdb1k3'
credentials = service_account.Credentials.from_service_account_file(
    './tart-90ca2-9d37c42ef480.json',
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Create the job client.
job_client = dataproc.JobControllerClient(client_options={
    'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region),
}, credentials=credentials)

job_response = job_client.list_jobs(project_id, region, cluster_name=cluster_name)


print(job_response.status)