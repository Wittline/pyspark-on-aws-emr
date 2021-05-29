import time
import boto3
from botocore.exceptions import ClientError


def create_security_groups(prefix, logger):

    try:
        ec2_resource = boto3.resource('ec2')
        default_vpc = list(ec2_resource.vpcs.filter(
            Filters=[{'Name': 'isDefault', 'Values': ['true']}]))[0]
        logger.info("Got default VPC %s.", default_vpc.id)
    except ClientError:
        logger.exception("Couldn't get VPCs.")
        raise
    except IndexError:
        logger.exception("No default VPC in the list.")
        raise

    groups = {'manager': None, 'worker': None}
    for group in groups.keys():
        try:
            groups[group] = default_vpc.create_security_group(
                GroupName=f'{prefix}-{group}', Description=f"EMR {group} group.")
            logger.info(
                "Created security group %s in VPC %s.",
                groups[group].id, default_vpc.id)
        except ClientError:
            logger.exception("Couldn't create security group.")
            raise

    return groups


def delete_security_groups(prefix_name, logger):
        
    try:

        ec2_resource = boto3.resource('ec2')
        sgs = list(ec2_resource.security_groups.all())

        sgs_to_delete = [sg for sg in sgs if sg.group_name.startswith(prefix_name)]

        for sg in sgs_to_delete:
            print('{} {}'.format(sg.id, sg.group_name))

        for sg in sgs_to_delete:
            logger.info('Revoking {}'.format(sg.group_name))
            try:
                if sg.ip_permissions:
                    sg.revoke_ingress(IpPermissions=sg.ip_permissions)
            except ClientError:
                raise

        for sg in sgs_to_delete:
            logger.info('Deleting {}'.format(sg.group_name))
            try:
                sg.delete()
            except ClientError:
                raise

    except ClientError:
        logger.exception("Couldn't delete security groups %s.", security_groups)
        raise


    try:
        ec2_resource = boto3.resource('ec2')
        for sg in security_groups.values():
            sg.revoke_ingress(IpPermissions=sg.ip_permissions)
        max_tries = 5
        while True:
            try:
                for sg in security_groups.values():
                    sg.delete()
                break
            except ClientError as error:
                max_tries -= 1
                if max_tries > 0 and \
                        error.response['Error']['Code'] == 'DependencyViolation':
                    logger.warning(
                        "Attempt to delete security group got DependencyViolation. "
                        "Waiting for 10 seconds to let things propagate.")
                    time.sleep(10)
                else:
                    raise
        logger.info("Deleted security groups %s.", security_groups)
    except ClientError:
        logger.exception("Couldn't delete security groups %s.", security_groups)
        raise