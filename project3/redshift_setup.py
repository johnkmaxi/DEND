import configparser
import json
import time
import boto3

def create_resources(KEY, SECRET):
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

    iam = boto3.client('iam',
                   aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    return ec2, s3, iam, redshift

def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )
    
def attach_iam_role(iam, DWH_IAM_ROLE_NAME):
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
                  PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
                 )
    
def get_iam_role(iam, DWH_IAM_ROLE_NAME):
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return role_arn

def create_redshift_cluster(redshift, role_arn, DWH_NODE_TYPE, DWH_CLUSTER_TYPE,
                           DWH_NUM_NODES, DB_NAME, DWH_CLUSTER_IDENTIFIER,
                           DB_USER, DB_PASSWORD):
    response = redshift.create_cluster(        
        # TODO: add parameters for hardware
        NodeType=DWH_NODE_TYPE,
        ClusterType=DWH_CLUSTER_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        # TODO: add parameters for identifiers & credentials
        DBName=DB_NAME,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        # TODO: add parameter for role (to allow s3 access)
        IamRoles=[role_arn]
    )

def get_properties(redshift, DWH_CLUSTER_IDENTIFIER):
    properties = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    return properties
    
def check_availability(redshift, DWH_CLUSTER_IDENTIFIER):
    status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
    if status == 'available':
        return True
    else:
        return False

def get_endpoint(properties):
    dwh_endpoint = properties['Endpoint']['Address']
    return dwh_endpoint
    
def get_arn(properties):
    dwh_role_arn = properties['IamRoles'][0]['IamRoleArn']
    return dwh_role_arn

def open_port(ec2, properties, DB_PORT):
    vpc = ec2.Vpc(id=properties['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    #print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DB_PORT),
        ToPort=int(DB_PORT)
    )

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_IAM_ROLE_NAME = config.get('CLUSTER','DWH_IAM_ROLE_NAME')
    DWH_NODE_TYPE = config.get('CLUSTER', 'DWH_NODE_TYPE')
    DWH_CLUSTER_TYPE = config.get('CLUSTER', 'DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('CLUSTER', 'DWH_NUM_NODES')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    print('create resources')
    ec2, s3, iam, redshift = create_resources(KEY, SECRET)
    print('create iam role')
    try:
        create_iam_role(iam, DWH_IAM_ROLE_NAME)
    except Exception as e:
        print(e)
        
    print('attach iam role')
    attach_iam_role(iam, DWH_IAM_ROLE_NAME)
    print('get role arn')
    role_arn = get_iam_role(iam, DWH_IAM_ROLE_NAME)
    print(role_arn)
    print('create redshift cluster')
    create_redshift_cluster(redshift, role_arn, DWH_NODE_TYPE, DWH_CLUSTER_TYPE,
                           DWH_NUM_NODES, DB_NAME, DWH_CLUSTER_IDENTIFIER,
                           DB_USER, DB_PASSWORD)
    
    availability = False
    t1 = time.time()
    while availability == False:
        availability = check_availability(redshift, DWH_CLUSTER_IDENTIFIER)
        print('cluster available: ', availability)
        time.sleep(5)
    t2 = time.time()
    print('{} seconds to spin-up cluster'.format(t2-t1))
    print('get cluster properties')
    cluster_properties = get_properties(redshift, DWH_CLUSTER_IDENTIFIER)
    print('get endpoint')
    redshift_endpoint = get_endpoint(cluster_properties)
    print('get arn')
    redshift_arn = get_arn(cluster_properties)
    print('open port')
    try:
        open_port(ec2, cluster_properties, DB_PORT)
    except Exception as e:
        print(e)
    print('write endpoint and arn to config file')
    print(redshift_endpoint, redshift_arn)
    config.set('IAM_ROLE', 'ARN', redshift_arn)
    config.set('CLUSTER', 'HOST', redshift_endpoint)
    with open('dwh.cfg', 'w+') as f:
        config.write(f)
    print('done')
    
if __name__ == "__main__":
    main()