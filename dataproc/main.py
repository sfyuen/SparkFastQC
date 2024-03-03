from google.cloud import dataproc_v1
from google.cloud import storage
from google.cloud.storage.fileio import BlobWriter
from Bio.SeqIO.QualityIO import FastqGeneralIterator
import time
import re

#project ID of Google Cloud
project_id = ""

#Cloud Storage bucket name for upload the fastq/spark job file, and save the result of spark job
bucket_name = ''

#region & cluster name of the Dataproc Cluster
region = ''
cluster_name = ''

#Path to Google Cloud service account JSON file
#The service account should have the below permissions:

#dataproc.clusters.use
#dataproc.jobs.create
#dataproc.jobs.get
#storage.objects.create
#storage.objects.delete
#storage.objects.get
#storage.objects.list
service_account_json=r""

#Path to the fastq file
fastq_fn = 'sample.fastq'
#Path to the sparkjob file
sparkjob_fn = 'sparkjob.py'
#Path to the output file
result_fn = 'result.json'

storage_client = storage.Client.from_service_account_json(service_account_json)
bucket = storage_client.bucket(bucket_name)
def uploader():
    def read_fastq(fn):
        with open(fn) as in_handle:
            for name, sequence, quality in FastqGeneralIterator(in_handle):
                yield f"\n{name}|{sequence}|{quality}"

    blob = bucket.blob('fastq.csv')

    writer = BlobWriter(blob)
    writer.write('name|sequence|quality'.encode())
    for record in read_fastq(fastq_fn):
        writer.write(record.encode())
    writer.close()

    python_blob = bucket.blob('sparkjob.py')
    python_blob.upload_from_filename(sparkjob_fn)

def sparkjob():
    job_client = dataproc_v1.JobControllerClient.from_service_account_json(service_account_json,client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'})

    pyspark_job = dataproc_v1.types.PySparkJob(
        main_python_file_uri=f"gs://{bucket_name}/sparkjob.py",
        args=[bucket_name]
    )

    placement = dataproc_v1.types.JobPlacement(cluster_name=cluster_name)
    main_job = dataproc_v1.types.Job(pyspark_job=pyspark_job, placement=placement)
    job = job_client.submit_job(project_id=project_id, region=region, job=main_job)

    job_id = job.reference.job_id
    job_status = False
    while not job_status:
        job_status = job_client.get_job(project_id=project_id, region=region, job_id=job_id).done
        time.sleep(5)

def savejson():
    blobs = bucket.list_blobs()
    for blob in blobs:
        if re.match(r"^output[/].+[.]json",blob.name):
            with open(result_fn, "wb") as file_obj:
                blob.download_to_file(file_obj)

uploader()
print('uploaded files to gcs')
sparkjob()
print('spark job completed')
savejson()
print('json saved')

