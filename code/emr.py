import logging
import boto3
import os.path
import json
from botocore.exceptions import ClientError


def run_job_flow(
        name, log_uri, applications, job_flow_role, service_role,
        security_groups, steps, cfile, logger):



    try:

    print('-'*88)
    print ("Reading fleet configuration...")
    if os.path.isfile(sfile):        
        name, ext = os.path.splitext(sfile)
        if ext.lower() == '.json':
            f = open(sfile,)
            data = json.load(f)
            print ("Preparing files for steps...")
            for s in data['steps']:
                print (f"Processing step with name {s['name']} and guiid {s['guiid']}...")
                filename= s3.upload_to_bucket(prefix_name,s['script_uri'],'scripts',logger)



        emr_client = boto3.client('emr')
        response = emr_client.run_job_flow(
            Name=name,
            LogUri=log_uri,
            ReleaseLabel='emr-6.3.0',
            Instances={

                # 'EmrManagedMasterSecurityGroup': security_groups['manager'].id,
                # 'EmrManagedSlaveSecurityGroup': security_groups['worker'].id,
            },
            Steps=[{
                'Name': step['name'],
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--deploy-mode', 'cluster',
                             step['script_uri'], *step['script_args']]
                }
            } for step in steps],
            Applications=[{
                'Name': app
            } for app in applications],
            JobFlowRole=job_flow_role.name,
            ServiceRole=service_role.name,
            EbsRootVolumeSize=10,
            VisibleToAllUsers=True
        )
        cluster_id = response['JobFlowId']
        logger.info("Created cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't create cluster.")
        raise
    else:
        return cluster_id


def describe_cluster(cluster_id, logger):

    try:
        emr_client = boto3.client('emr')
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster = response['Cluster']
        logger.info("Got data for cluster %s.", cluster['Name'])
    except ClientError:
        logger.exception("Couldn't get data for cluster %s.", cluster_id)
        raise
    else:
        return cluster


def terminate_cluster(cluster_id, logger):

    try:
        emr_client = boto3.client('emr')
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise


def add_step(cluster_id, name, script_uri, script_args, logger):

    try:
        emr_client = boto3.client('emr')
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': name,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--deploy-mode', 'cluster',
                             script_uri, *script_args]
                }
            }])
        step_id = response['StepIds'][0]
        logger.info("Started step with ID %s", step_id)
    except ClientError:
        logger.exception("Couldn't start step %s with URI %s.", name, script_uri)
        raise
    else:
        return step_id


def list_steps(cluster_id, logger):

    try:
        emr_client = boto3.client('emr')
        response = emr_client.list_steps(ClusterId=cluster_id)
        steps = response['Steps']
        logger.info("Got %s steps for cluster %s.", len(steps), cluster_id)
    except ClientError:
        logger.exception("Couldn't get steps for cluster %s.", cluster_id)
        raise
    else:
        return steps


def describe_step(cluster_id, step_id, logger):

    try:
        emr_client = boto3.client("emr")
        response = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)
        step = response['Step']
        logger.info("Got data for step %s.", step_id)
    except ClientError:
        logger.exception("Couldn't get data for step %s.", step_id)
        raise
    else:
        return step


def list_clusters(logger):

    try:    
        emr_client = boto3.client("emr")
        response = emr_client.list_clusters(
            ClusterStates=[
                'STARTING','BOOTSTRAPPING','RUNNING','WAITING','TERMINATING','TERMINATED','TERMINATED_WITH_ERRORS',
            ]
        )
        for cluster in response['Clusters']:
            print(f"Cluster: {cluster['Name']}  Id: {cluster['Id']}")
    except ClientError:
        logger.exception("Couldn't get data about clusters")
        raise
