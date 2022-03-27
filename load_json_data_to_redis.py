import json
import redis
import boto3
import os
import rediscluster


def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager', region_name=region_name)
    secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    return secret_value_response['SecretString']


nodes = get_secret(os.getenv("MEMORYDBSECRET"), os.getenv("REGION"))
node = nodes.split(":")[0]
useraccount = get_secret(os.getenv("MEMORYDBUSERS"), os.getenv("REGION"))
print(useraccount)
print(node)

useraccount = json.loads(useraccount)

redis_client = rediscluster.RedisCluster(username=useraccount["username"], ssl=True, host=node, port=6379,
                              decode_responses=True, skip_full_coverage_check=True, password=useraccount["password"])

# redis_client = redis.Redis(host='localhost', db=0, decode_responses=True)

with open('products_sets.json', 'r') as outfile:  
    data = json.load(outfile)
    for idx in data["all"]:
        redis_client.sadd("products:all", idx)
    for idx in data["skateboarding"]:
        redis_client.sadd("products:skateboarding", idx)
    for idx in data["sports"]:
        redis_client.sadd("products:sports_and_outdoors", idx)

with open('products.json', 'r') as outfile:  
    data = json.load(outfile)
    for key in data:
        redis_client.hmset("productdetails:"+key, data[key])