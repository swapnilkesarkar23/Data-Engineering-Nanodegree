import pandas as pd
import boto3
import json
import psycopg2
import configparser
from botocore.exceptions import ClientError


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """Creates an IAM Role for Redshift to access S3
    
        Parameters:
            iam: IAM boto3 client
            DWH_IAM_ROLE_NAME: Desired name for IAM role
            
        Returns: IAM Role ARN
            
    """
    try:
        print('Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(
                {"Version": "2012-10-17",
                 "Statement": [{"Effect": "Allow",
                 "Principal": {"Service": "redshift.amazonaws.com"},
                 "Action": "sts:AssumeRole"}]}
                )
        )
    except Exception as e:
        print(e)
    
    #Attach Policy to the Role
    iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    
    #Get the Role ARN
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    
    return(roleArn)


def create_cluster(redshift, roleArn, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB_USER, DWH_DB_PASSWORD):
    """Creates an redshift cluster
    
        Parameters:
            redshift: Redshift boto3 client
            roleArn: IAM Role ARN
            DWH_DB: Database name
            DWH_CLUSTER_IDENTIFIER: Redshift Cluster Identifier
            DWH_CLUSTER_TYPE: Type of Cluster Single/Multinode Cluster
            DWH_NODE_TYPE: Node Type
            DWH_NUM_NODES: Number of nodes
            DWH_DB_USER: DB user name
            DWH_DB_PASSWORD: DB password
            
    """
    print("Creating an Redshift cluster")
    try:
        response = redshift.create_cluster(
                       DBName=DWH_DB,
                       ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                       ClusterType=DWH_CLUSTER_TYPE,
                       NodeType=DWH_NODE_TYPE,
                       NumberOfNodes=int(DWH_NUM_NODES),
                       MasterUsername=DWH_DB_USER,
                       MasterUserPassword=DWH_DB_PASSWORD,
                       IamRoles=[roleArn]         
    )
    except Exception as e:
        print(e)
    
    # Wait till the CLuster is in available state
    waiter = redshift.get_waiter('cluster_available')
    waiter.wait(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)


def get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER):
    """Returns the redshift cluster properties
        
        Parameters:
            redshift: Redshift boto3 client
            DWH_CLUSTER_IDENTIFIER: Redshift cluster identifier
            
        Returns: Redhsift Cluster properties

    """        
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    
    return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN


def open_port(ec2, myClusterProps, DWH_PORT):
    """Opens specified port in the default security group
        
        Parameters:
            ec2: EC2 boto3 resource
            myClusterProps: Cluster properties
            DWH_PORT: Port which needs to opened
            
    """
    try:
        vpc = ec2.Vpc(id=myClusterProps[0]['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
    
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
    )
    except Exception as e:
        print(e)
        print('Error Creating SG')


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    pd.DataFrame({"Param": 
                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT",                                       "DWH_IAM_ROLE_NAME"],
                  "Value":
                      [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                 })
    
    ec2 = boto3.resource('ec2',
                         region_name='us-west-2',
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    
    create_cluster(redshift, roleArn, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB_USER, DWH_DB_PASSWORD)
    
    myClusterProps = get_cluster_props(redshift, DWH_CLUSTER_IDENTIFIER)
    
    open_port(ec2, myClusterProps, DWH_PORT)

    
if __name__ == "__main__":
    main()