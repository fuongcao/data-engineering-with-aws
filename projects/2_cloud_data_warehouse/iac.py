import pandas as pd
import boto3
import json

##### Load DWH Params from a file
import configparser
config = configparser.ConfigParser()
config.optionxform = lambda option: option.upper()
config.read_file(open('iac.cfg'))

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

DWH_IAM_ROLE_NAME      = config.get("DWH","DWH_IAM_ROLE_NAME")
DWH_IAM_ROLE_ARN       = config.get("DWH","DWH_IAM_ROLE_ARN")

def write_conf():
    config.set("DWH","DWH_IAM_ROLE_ARN", "1223")
    # Writing our configuration file to 'example.ini'
    with open('iac.cfg', 'w') as configfile:
        config.write(configfile)


def create_dwh_role(roleName = DWH_IAM_ROLE_NAME):
    try:
        print("create_dwh_role - Create iam client ...")
        iam = boto3.client('iam',aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name='us-west-2'
                )
        
        
        print("create_dwh_role - Creating a new IAM Role: \"{}\"".format(roleName)); 
        iam.create_role(
            Path='/',
            RoleName=roleName,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )   
        
        print("create_dwh_role - Attaching Policy")

        iam.attach_role_policy(RoleName=roleName,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print("create_dwh_role - Get the IAM role ARN")
        roleArn = iam.get_role(RoleName=roleName)['Role']['Arn']

        print("create_dwh_role - Update the IAM role ARN to iac.cfg")
        config.set("DWH","DWH_IAM_ROLE_ARN", roleArn)
        # Writing our configuration file to 'example.ini'
        with open('iac.cfg', 'w') as configfile:
            config.write(configfile)

        print("create_dwh_role DWH_IAM_ROLE_ARN: \"{}\"".format(roleArn))
        return True
        
    except Exception as e:
        print(e)
        return False
    
def create_dwh_redshift_cluster(clusterIdentifier = DWH_CLUSTER_IDENTIFIER, roleArn = DWH_IAM_ROLE_ARN):
    try:
        print("create_dwh_redshift_cluster - Creating Redshift client ...")
        redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        
        
        response = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=clusterIdentifier,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )  
        
        print("create_dwh_redshift_cluster - \"{}\" !".format(response));
        return True
        
    except Exception as e:
        print(e)
        return False
    
def delete_dwh_role(roleName = DWH_IAM_ROLE_NAME):
    try:
        print("delete_dwh_role - Creating iam client ...")
        iam = boto3.client('iam',aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name='us-west-2'
                )
        print("delete_dwh_role - Detaching role policy ...")
        iam.detach_role_policy(
            RoleName=roleName,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
    
        print("delete_dwh_role - Deleting role \"{}\" ...".format(roleName))
        response = iam.delete_role(
            RoleName=roleName
        )

        print("delete_dwh_role - Role \"{}\" have been deleted !".format(roleName))
        return True
    
    except Exception as e:
        print(e)
        return False
    
def delete_dwh_redshift_cluster(clusterIdentifier = DWH_CLUSTER_IDENTIFIER):
    try:
        print("delete_dwh_redshift_cluster - Create Redshift client ...")
        redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        
        print("delete_dwh_redshift_cluster - Deleting Redshift cluster \"{}\" ...".format(clusterIdentifier))
        response = redshift.delete_cluster(
            ClusterIdentifier=clusterIdentifier,
            SkipFinalClusterSnapshot=True,
        )

        print("delete_dwh_redshift_cluster - Redshift cluster {} !".format(response))
        return True
    except Exception as e:
        print(e)
        return False

    