import argparse
import logging
import time
import boto3
from botocore.exceptions import ClientError
import emr
import ec2
import s3
import poller

logger = logging.getLogger(__name__)


def create_cluster(prefix = 'cluster_default'):

    
    prefix = f'{prefix}-{time.time_ns()}'

    bucket = s3.create_bucket(prefix, logger)

    job_flow_role, service_role = iam.create_roles(
        f'{prefix}-ec2-role', 
        f'{prefix}-service-role')

    security_groups = ec2.create_security_groups(prefix)

    print("Wait for 10 seconds to give roles and profiles time to propagate...")

    time.sleep(10)

    max_tries = 5
    while True:
        try:
            cluster_id = emr.run_job_flow(
                f'{prefix}-cluster',
                f's3://{prefix}/logs',
                True, 
                ['Hadoop', 'Hive', 'Spark'], 
                job_flow_role, 
                service_role,
                security_groups)
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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('-a','--Action', type=str, help = "Type of actions", metavar = '', choices=['create-cluster', 
                                                                                      'list-clusters',
                                                                                      'terminate-cluster',
                                                                                      'add_steps',
                                                                                      'delete_steps',
                                                                                      'execute_steps'])


    # Create cluster
    parser.add_argument('-c','--cname', type=str, help = "Name Cluster")
    parser.add_argument('-cfg','--cfile', type=str, help = "File with the fonfiguration of the emr cluster")


    # Terminate cluster
    parser.add_argument('-idc','--cluster_id', type=str, help = "Id of the cluster")

    # add steps to the cluster
    parser.add_argument('-steps','--sfile', type=str, help = "Add steps from json file")

    # execute steps in clusters
    parser.add_argument('-execute_steps','--Execute steps in cluster', type=str, help = "execute steps involved to the clusters")    


    args = parser.parse_args()

    if args.Action == 'create-cluster':
        create_cluster(args.cname, args.cfile)
    elif args.Action == 'list-clusters':
        list_cluster()
    elif args.Action == 'terminate-cluster':
        terminate_cluster(args.cluster_id)
    elif args.Action == 'add_steps':
        add_steps(args.sfile, args.cluster_id)
    elif args.Action == 'delete_steps':
        delete_steps(args.cluster_id)        
    elif args.Action == 'execute_steps':
        execute_steps(args.cluster_id)
    else:
        print("Action is invalid")