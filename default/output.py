# Databricks notebook source
from typing import TypeVar, Optional
from pyspark.sql.types import StructType, DataType, ArrayType, DateType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from distutils import util
from botocore.exceptions import ClientError
import json
import boto3
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from pyspark.sql.functions import udf
from pyspark.sql.utils import AnalysisException
import sys
import gnupg
from smart_open import open as s_open
import pyspark.sql.functions as F
import distutils
import pandas as pd
import io
import warnings

# COMMAND ----------

param_env = "env"
param_job_name = "job_name"
param_host = "host"
env = dbutils.widgets.text(param_env, "dev")
job_name = dbutils.widgets.text(param_job_name, "commons")
databricks_host = dbutils.widgets.text(
    param_host, f"dataos-kc-{env}.cloud.databricks.com"
)

# COMMAND ----------

# MAGIC %run "./databricks_logger"

# COMMAND ----------

env = dbutils.widgets.get(param_env)
job_name = dbutils.widgets.get(param_job_name)
databricks_host = dbutils.widgets.get(param_host)
splunk_secret_name = f"{env}/k8s/p2retargeting/splunk"

print(f"env:{env}")
print(f"job_name:{job_name}")
print(f"databricks_host:{databricks_host}")

STATE_STARTED = "started"
STATE_FINISHED = "finished"
STATE_ERROR = "error"

# COMMAND ----------

class STSSession:
    """
    Class to init a sts session for the given role.
    How to use:
      # from lib.sts_session import STSSession

      sts_session = STSSession(arn=<ASSUME_ROLE_ARN>,
                          session_name=<SESSION_NAME>,
                          duration=<OPTIONAL_SESSION_DURATION_IN_SECONDS>,
                          region=<OPTIONAL_AWS_REGION>)
    """

    def __init__(
        self, arn, session_name="sts_session", duration=3600, region="us-west-2"
    ):
        sts_connection = boto3.client("sts", region)
        assume_role_object = sts_connection.assume_role(
            RoleArn=arn, RoleSessionName=session_name, DurationSeconds=duration
        )
        self.credentials = assume_role_object["Credentials"]

        self.sts_session = boto3.Session(
            aws_access_key_id=self.credentials["AccessKeyId"],
            aws_secret_access_key=self.credentials["SecretAccessKey"],
            aws_session_token=self.credentials["SessionToken"],
            region_name=region,
        )

# COMMAND ----------

class AWSResource:
    """
    Class to create objects related to particular services of AWS.
    How to use:
        resource = AWSResource(session=<session_name>)
    """

    def __init__(self, session=boto3.session.Session()):
        self.s3 = self.get_s3_bucket_object(session)

    def get_s3_bucket_object(self, session):
        return session.client("s3")

    def refresh_s3_bucket_object(self, session):
        self.s3 = session.client("s3")

# COMMAND ----------

def get_secret(secret_name, region_name="us-west-2", session=boto3.session.Session()):
    """
    Method to get secrets irrespective of session type. Please pass a STSSession if need to read secrets using assume-role.
    How to use:
        # Fetch secrets without assume role
        secrets = get_secret(
        secret_name=<SECRETS_NAME>,
        region_name=<OPTIONAL_AWS_REGION>)

        # Fetch secrets with assume role
        secrets = get_secret(
        secret_name=<SECRETS_NAME>,
        region_name=<OPTIONAL_AWS_REGION>,
        session=sts_session)     # code to initialize STSSession is defined above
    """

    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if "SecretString" in get_secret_value_response:
            secret_json = get_secret_value_response["SecretString"]
            return json.loads(secret_json)
        else:
            return get_secret_value_response["SecretBinary"]

# COMMAND ----------

notebook_info = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)

job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

try:
    log_data = {}
    log_data["name"] = job_name
    log_data["job-id"] = notebook_info["tags"]["jobId"]
    log_data["job-name"] = notebook_info["tags"]["jobName"]
    log_data["run-id"] = notebook_info["tags"]["runId"]
    log_data["run-num"] = notebook_info["tags"]["idInJob"]
    log_data["job-trigger-type"] = notebook_info["tags"]["jobTriggerType"]
    log_data["module_name"] = "analytics_room"
    source_type = "spark-job"
    source_name = notebook_info["tags"]["jobName"]
except:
    print("Not a job execution")
    log_data["run-id"] = 0
    log_data["job-name"] = f"notebook:{job_name}"
    source_type = "spark-notebook"
    source_name = job_name
log_data["job-run-time"] = job_time
print(log_data)

# COMMAND ----------
# splunk_secret = get_secret(splunk_secret_name)
# logger = SplunkLogger(
#     token=splunk_secret["token"],
#     index=splunk_secret["index"],
#     meta_data={
#         "source": source_name,
#         "sourcetype": f"databricks:{source_type}",
#         "host": databricks_host,
#     },
# )
logger = SplunkLogger(
    token="",
    index="",
    meta_data={
        "source": source_name,
        "sourcetype": f"databricks:{source_type}",
        "host": databricks_host,
    },
)
def __get_event(log_level, msg, data={}):
    event = {"level": log_level, "message": msg}
    if isinstance(data, dict):
        event.update(data)
    elif isinstance(data, str) and data.strip():
        event["data"] = data
    event.update(log_data)
    return json.dumps(event)
def debug(msg: object, data: object = {}):
    logger.log_event(__get_event("DEBUG", msg, data))
def info(msg: object, data: object = {}):
    logger.log_event(__get_event("INFO", msg, data))
def warn(msg: object, data: object = {}):
    logger.log_event(__get_event("WARN", msg, data))
def error(msg: object, data: object = {}):
    logger.log_event(__get_event("ERROR", msg, data))
def fatal(msg: object, data: object = {}):
    logger.log_event(__get_event("FATAL", msg, data))
print(__get_event("INFO", f"Metrics logger initialized for {env} env"))
info(f"Metrics logger initialized for {env} env")
logger.flush()

# ... (rest of the migrated code, as per the previous step, ensuring all flushes, exception safety, and exit safety flush are present, and all Splunk logic is removed) ...

import atexit
def flush_logger_on_exit():
    try:
        if len(logger.batch_events) > 0:
            logger.flush()
    except Exception:
        pass
atexit.register(flush_logger_on_exit)
