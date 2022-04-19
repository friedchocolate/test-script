import json
import redis
import boto3
import os
import rediscluster
from dotenv import load_dotenv

load_dotenv()

# APP config
MEMORYDBSECRET = os.getenv("MEMORYDBSECRET")
MEMORYDBUSERS = os.getenv("MEMORYDBUSERS")
REGION = os.getenv("REGION")
IS_CLUSTER = True
BUCKET_OLD_URL = os.getenv(
    "BUCKET_OLD_URL", "https://cf-templates-4cjngrbix175-us-east-1.s3.amazonaws.com")
BUCKET_NEW_URL = os.getenv(
    "BUCKET_NEW_URL", "https://cf-templates-4cjngrbix175-us-east-1.s3.amazonaws.com")


def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager', region_name=region_name)
    secret_value_response = client.get_secret_value(SecretId=secret_name)
    return secret_value_response['SecretString']


def connect_with_cluster():
    nodes = get_secret(MEMORYDBSECRET, REGION)
    node = nodes.split(":")[0]
    useraccount = get_secret(MEMORYDBUSERS, REGION)
    useraccount = json.loads(useraccount)
    return rediscluster.RedisCluster(username=useraccount["username"], ssl=True, host=node, port=6379, decode_responses=True, skip_full_coverage_check=True, password=useraccount["password"])


if IS_CLUSTER:
    redis_client = connect_with_cluster()
else:
    redis_client = redis.Redis(
        host='localhost', port=6379, db=0, decode_responses=True)

with open('products.json', 'r') as outfile:
    data = json.load(outfile)
    for key in data:
        if "title" not in data[key]:
            data[key]["title"] = "unknown"
        data[key]["stock"] = 5000

        price = data[key]["sellPrice"]
        try:
            price = float(price)
        except:
            price = 10.0
        images = json.loads(data[key]["image"])

        finalImages = []
        for img in images:
            finalImages.append(img.replace(BUCKET_OLD_URL, BUCKET_NEW_URL))

        data[key]["image"] = json.dumps(finalImages)
        # data[key]["sellPrice"] = "${}".format(price)
        data[key]["sellPrice"] = price
        redis_client.hmset("productdetails:"+key, data[key])

with open('products_sets.json', 'r') as outfile:
    data = json.load(outfile)
    with open('products.json', 'r') as outfile:
        products = json.load(outfile)
        for idx, key in enumerate(data):
            try:
                redis_client.delete('products:'+key)
            except:
                pass
            for idx in data[key]:
                redis_client.sadd("products:"+key, idx)


with open('users.json', 'r') as outfile:
    data = json.load(outfile)
    for user in data:
        username = user["username"]
        redis_client.hmset("profile:"+username, {"password": "Test@123"})
