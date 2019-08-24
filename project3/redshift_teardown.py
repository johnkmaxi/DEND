import configparser
import json
import time
import boto3



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

if __name__ == "__main__":
    main()