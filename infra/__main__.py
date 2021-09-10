import json
import pulumi
import pulumi_aws as aws
import configparser

# CONFIG
CONFIG_FILE='../dwh.cfg'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

DB_NAME=config.get("CLUSTER","DB_NAME")
DB_USER=config.get("CLUSTER","DB_USER")
DB_PASSWORD=config.get("CLUSTER","DB_PASSWORD")
DB_PORT=int(config.get("CLUSTER","DB_PORT"))
IAM_ROLE_NAME = config.get("IAM_ROLE","NAME")


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
    skip_final_snapshot=True)

pulumi.export('arn', redshift_role.arn)
pulumi.export('host', redshift_cluster.dns_name)