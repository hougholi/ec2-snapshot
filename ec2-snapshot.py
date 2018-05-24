import boto3
import datetime

ec2_client = boto3.client('ec2')
retention_days = 7
delete_tag = 'DeleteOn'


def lambda_handler(event, context):
    delete_date = datetime.date.today() + datetime.timedelta(days=retention_days)
    delete_fmt = delete_date.strftime('%Y-%m-%d')
    response = ec2_client.describe_instances(
        Filters=[{'Name': 'tag-key', 'Values': ['backup', 'Backup']}]
    )

    for r in response['Reservations']:
        for i in r['Instances']:
            for tag in i['Tags']:
                if tag['Key'] == 'Name' and tag['Value'] is not None:
                    instance_name = tag['Value']
                    break

            for dev in i['BlockDeviceMappings']:
                vol_id = dev['Ebs']['VolumeId']
                dev_attachment = dev['DeviceName']
                snapshot_id = create_snapshot(vol_id, i['InstanceId'])
                create_tags(snapshot_id, instance_name, delete_fmt)

    purge_snapshots()


def create_snapshot(vol_id, instance_id):
    return ec2_client.create_snapshot(
        VolumeId=vol_id,
        Description=instance_id,
    )['SnapshotId']


def create_tags(snapshot_id, instance_name, delete_fmt):
    ec2_client.create_tags(
        Resources=[snapshot_id],
        Tags=[
            {'Key': 'Name', 'Value': instance_name},
            {'Key': delete_tag, 'Value': delete_fmt},
        ]
    )

def purge_snapshots():
    delete_date = datetime.date.today() - datetime.timedelta(days=retention_days)
    delete_on = delete_date.strftime('%Y-%m-%d')
    filters = [
        {'Name': 'tag-key', 'Values': [delete_tag]}
    ]
    snapshot_response = ec2_client.describe_snapshots(Filters=filters)

    for snap in snapshot_response['Snapshots']:
        for tag in snap['Tags']:
            if tag['Key'] == 'DeleteOn' and tag['Value'] < delete_on:
                print "Deleting snapshot %s" % snap['SnapshotId']
                ec2_client.delete_snapshot(SnapshotId=snap['SnapshotId'])
