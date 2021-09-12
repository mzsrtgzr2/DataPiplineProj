import json
import pulumi
import pulumi_aws as aws

# CONFIG
DB_NAME='dbdemo'
DB_USER='user1'
DB_PASSWORD='p2mk5JK!'
DB_PORT=6610
IAM_ROLE_NAME = 'redshiftrole'


redshift_role = aws.iam.Role(IAM_ROLE_NAME,
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Sid": "",
            "Principal": {
                "Service": "redshift.amazonaws.com",
            },
        }],
    }))

# allow s3 read
aws.iam.RolePolicyAttachment(IAM_ROLE_NAME+'attachment',
    role=redshift_role.name,
    policy_arn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

redshift_cluster = aws.redshift.Cluster("default",
    cluster_identifier="moshe-cluster",
    cluster_type="single-node",
    database_name=DB_NAME,
    master_password=DB_PASSWORD,
    master_username=DB_USER,
    node_type="dc1.large",
    iam_roles=[redshift_role.arn],
    port=DB_PORT,
    skip_final_snapshot=True,
)

pulumi.export('arn', redshift_role.arn)
pulumi.export('host', redshift_cluster.dns_name)