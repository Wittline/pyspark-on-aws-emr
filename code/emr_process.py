import argparse
import logging
import time
import boto3
from botocore.exceptions import ClientError
import emr
import iam
import ec2
import s3
import poller
import json
import os.path

logger = logging.getLogger(__name__)


def create_cluster(prefix = 'default'):

    if prefix.find("cluster") >= 0:
        print("The cluster name cannot contain the word 'cluster'")
        return

    folders = ['scripts', 'logs', 'steps', 'cluster-fleet', 'bootstrap-emr', 'output', 'input']

    prefix = f'{prefix}-{time.time_ns()}'
    folder = 'cluster-fleet'
    cfile = 'cluster-ec2-spot-fleet.json'

    bucket = s3.create_bucket(prefix, folders, logger)

    dict_files_config = { 'bootstrap-emr': 'bootstrap-action.sh',
                          'cluster-fleet': 'cluster-ec2-spot-fleet.json'
                        }

    try:
        print("Reading config files..")
        for k, v in dict_files_config.items():
            if os.path.isfile(v):
                name, ext = os.path.splitext(v)              
                f = open(v,)
                print(f"Uploading {v} to folder {k} ...")
                if ext.lower() == '.json':                    
                    data = json.load(f)
                    s3.put_object(prefix, data, k, v, ext, logger)                    
                else:                
                    s3.upload_to_bucket(prefix, v, k, logger)
                print(f"The file '{v}' was Uploaded")
            else:
                print("The file %s cannot be read", v)
    except ClientError as error:
            print("Error reading the files")
            raise

    time.sleep(5)

    job_flow_role, service_role = iam.create_roles(
        f'{prefix}-ec2-role', 
        f'{prefix}-service-role', logger)

    security_groups = ec2.create_security_groups(prefix, logger)

    print("Wait for a couple of seconds, the roles and profiles need time to propagate...")

    time.sleep(10)

    max_tries = 5
    while True:
        try:
            cluster_id = emr.run_job_flow(
                f'cluster-{prefix}',
                f's3://{prefix}/logs', 
                ['Hadoop', 'Spark'], 
                job_flow_role, 
                service_role,
                security_groups, 
                [], 
                prefix, 
                folder, 
                cfile, 
                f's3://{prefix}/bootstrap-emr/bootstrap-action.sh',
                logger)
            print(f"Running job flow for cluster {cluster_id}...")
            break
        except ClientError as error:
            max_tries -= 1
            if max_tries > 0 and \
                    error.response['Error']['Code'] == 'ValidationException':
                print("Instance profile is not ready, let's give it more time...")
                time.sleep(10)
            else:
                raise

def list_clusters():
    emr.list_clusters(logger)

def terminate_cluster(cluster_id, remove_all = False):
    
    cluster_name = emr.describe_cluster(cluster_id, logger)['Name']
    prefix_name = cluster_name.replace("cluster-", '')

    emr.terminate_cluster(cluster_id, logger)

    if remove_all:
        remove_everything = input(
        f"Do you want to delete the security roles, groups, and bucket (y/n)? ")

        if remove_everything.lower() == 'y':
                iam.delete_roles(prefix_name,logger)
                ec2.delete_security_groups(prefix_name, logger)
                s3.delete_bucket(prefix_name, logger)
        else:
            print(f"Remember that objects kept in Amazon can incur charges")
    else:
        remove_sr = input(
        f"Do you want to delete the security roles (y/n)? ")

        if remove_sr.lower() == 'y':
            iam.delete_roles(prefix_name, logger)
        else:
            print(
            f"Remember that objects kept in Amazon can incur charges")

        remove_sg = input(
        f"Do you want to delete the security groups (y/n)? ")

        if remove_sg.lower() == 'y':
            ec2.delete_security_groups(prefix_name, logger)
        else:
            print(
            f"Remember that objects kept in Amazon can incur charges")

        remove_s3 = input(
        f"Do you want to delete the S3 bucket (y/n)? ")

        if remove_s3.lower() == 'y':
            s3.delete_bucket(prefix_name, logger)
        else:
            print(
            f"Remember that objects kept in Amazon S3 bucket can incur charges")

def get_output_step(jsd, step):
    for s in jsd['steps']:
        if s['name'] == step:
            return s['script_args']['output_uri']


def add_steps(sfile, cluster_id):

    cluster_name = emr.describe_cluster(cluster_id, logger)['Name']
    prefix_name = cluster_name.replace("cluster-", '')

    print('-'*88)
    print ("Reading steps...")
    if os.path.isfile(sfile):        
        name, ext = os.path.splitext(sfile)
        if ext.lower() == '.json':
            f = open(sfile,)
            data = json.load(f)
            print ("Preparing files for steps...")

            for s in data['steps']:
                print (f"Processing step with name {s['name']} and guiid {s['guiid']}...")
                filename= s3.upload_to_bucket(prefix_name,s['script_uri'],'scripts',logger)            
                s['script_uri'] = f's3://{prefix_name}/{filename}'

                if s['script_args']['local_input'] != '':
                    filename= s3.upload_to_bucket(prefix_name,s['script_args']['local_input'],'input',logger)   


                if int(s['script_args']['auto_generate_output']) > 0:
                   s['script_args']['output_uri'] = 'output_' + s['name'] + '_' + str(s['guiid']) + s['script_args']['format_output']
                else: #please validate if there is something else in this field 
                   s['script_args']['output_uri'] = s['script_args']['output_uri']
                
                if int(s['script_args']['input_dependency_from_output_step']) > 0:
                   s['script_args']['input_data'] = get_output_step(data, s['script_args']['from_step'])
                else:
                   s['script_args']['input_data'] = s['script_args']['input_data']
            
            print ("The Steps were formated...")            
            s3.put_object(prefix_name, data,'steps', 'steps.json' , ext, logger)
            print ("The Steps were uploaded to S3")
        else:
            print (f"The steps for the cluster {cluster_id} must be in .json format")            
    else:
        print (f"The file {sfile} does not exists")

def execute_steps(cluster_id):
  
    cluster_name = emr.describe_cluster(cluster_id, logger)['Name']
    prefix_name = cluster_name.replace("cluster-", '')

    jsd = s3.get_data(prefix_name, 'steps', 'steps.json', logger)

    for s in jsd['steps']:

        step_id = emr.add_step(
            cluster_id, 
            s['name'],
            s['script_uri'],
            [
            '--auto_generate_output', s['script_args']['auto_generate_output'],
            '--output_uri', s['script_args']['output_uri'],
            '--format_output', s['script_args']['format_output'],
            '--external_input', s['script_args']['external_input'],
            '--input_dependency_from_output_step', s['script_args']['input_dependency_from_output_step'],
            '--from_step', s['script_args']['from_step'],
            '--input_data', s['script_args']['input_data'],
            '--name_step', s['name'],
            '--description', s['description'],
            '--prefix_name', prefix_name
             ],
             s['executor_memory'],
             s['executor_cores'],
             logger)
             
        poller.status_poller(
            "Waiting for step to complete...",
            'COMPLETED',
            lambda:emr.describe_step(cluster_id, step_id, logger)['Status']['State'], logger)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('-a',
                        '--Action', 
                        type=str, 
                        help = "Type of actions", 
                        metavar = '', 
                        choices=['create_cluster',
                                'list_clusters',
                                'terminate_cluster',
                                'add_steps',
                                'delete_steps',
                                'execute_steps'])

    # Create cluster
    parser.add_argument('-c','--cname', type=str, help = "Name Cluster")

    # Terminate cluster
    parser.add_argument('-idc','--cluster_id', type=str, help = "Id of the cluster")

    # add steps to the cluster
    parser.add_argument('-steps','--sfile', type=str, help = "Add steps from json file")

    # execute steps in clusters
    parser.add_argument('-execute_steps','--Execute steps in cluster', type=str, help = "execute steps involved to the clusters")    

    args = parser.parse_args()

    if args.Action == 'create_cluster':
        if args.cname is not None and args.cname!= '':
            create_cluster(args.cname)
        else:
            print("The argument -c is missing")
    elif args.Action == 'list_clusters':
        list_clusters()
    elif args.Action == 'terminate_cluster':
        if args.cluster_id is not None and args.cluster_id!= '':
            terminate_cluster(args.cluster_id)
        else:
            print("The argument -idc is missing")
    elif args.Action == 'add_steps':
        if args.cluster_id is not None and args.sfile is not None\
        and args.cluster_id != '' and args.sfile != '':
            add_steps(args.sfile, args.cluster_id)
        else:
            print("The argument -idc or -steps is missing")
    elif args.Action == 'execute_steps':
        if args.cluster_id is not None and args.cluster_id!= '':
            execute_steps(args.cluster_id)
        else:
            print("The argument -idc is missing")
    else:
        print("Action is invalid")