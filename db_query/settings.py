from os import environ as env
from dotenv import load_dotenv

load_dotenv()

ATHENA_USERNAME = env.get("ATHENA_USERNAME")
ATHENA_PASSWORD = env.get("ATHENA_PASSWORD")
AWS_REGION = env.get("AWS_REGION")
S3_LOCATION = env.get("S3_LOCATION")
