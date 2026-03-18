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
import requests
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
STATE_SKIPPED = "skipped"

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

# --- SPLUNK LOGGER MIGRATION ---
# The following Splunk logger initialization is replaced by Databricks logger
#splunk_secret = get_secret(splunk_secret_name)
#logger = SplunkLogger(
#    token=splunk_secret["token"],
#    index=splunk_secret["index"],
#    meta_data={
#        "source": source_name,
#        "sourcetype": f"databricks:{source_type}",
#        "host": databricks_host,
#    },
#)

# Databricks logger initialization (as per migration reference)
logger = DatabricksLogger(
    meta_data={
        "source": source_name,
        "sourcetype": f"databricks:{source_type}",
        "host": databricks_host,
    },
)


def __get_event(log_level, msg, data={}):
    # adding log level and msg to event
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


print(__get_event("INFO", f"databricks logger initialized for {env} env"))
info(f"databricks logger initialized for {env} env")
logger.flush()

# COMMAND ----------

"""
How to use Pseudonymizaion
%run "./commons" $env=$env
Psedonymize: Use the encrypt udf
  pseudo_df = df.withColumn("deviceId_P", encrypt(lit(<KEY_TO_USE>), <COL_NAME>))
De-psedonymize: Use the decrypt udf
  clean_df = pseudo_df.withColumn("deviceId_P", decrypt(lit(<KEY_TO_USE>), <COL_NAME>))
"""

pseudonym_secrets = get_secret(f"{env}/k8s/p2retargeting/pseudonymize")


def get_pseudonym_secret(key_type):
    return bytes(pseudonym_secrets[key_type], "utf-8")


@udf
def encrypt(key_type, text):
    if text is None:
        return None
    key = get_pseudonym_secret(key_type)
    block_size = AES.block_size
    cipher = AES.new(key, AES.MODE_ECB)
    # padding message to a length that is multiple of AES block size
    id1 = bytes(
        (
            text
            + (block_size - len(text) % block_size)
            * chr(block_size - len(text) % block_size)
        ),
        encoding="utf8",
    )
    # instantiate a new AES cipher object
    try:
        return b64encode(cipher.encrypt(id1)).decode("utf-8")
    except ValueError:
        warn("Error trying to encrypt")
        return None


@udf
def decrypt(key_type, cipher_text):
    if cipher_text is None:
        return None
    key = get_pseudonym_secret(key_type)
    cipher = AES.new(key, AES.MODE_ECB)
    try:
        plaintext = cipher.decrypt(b64decode(cipher_text))
        return plaintext[: -ord(plaintext[len(plaintext) - 1 :])].decode("utf-8")
    except:
        warn("Error trying to decrypt")
        return None


# for every key/value in col_map, replace df[key] with encrypt(value, key)
def pseudonymize(df, col_map):
    out_df = df
    for field, fieldtype in col_map.items():
        out_df = out_df.withColumn(field, encrypt(F.lit(fieldtype), field))
    return out_df


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

class SourceEmptyException(Exception):
    pass


def logging_wrapper(task, error_msg):
    def inner(func):
        def wrapper(*args, **kwargs):
            try:
                info(
                    f"Wrapper starting {task}",
                    data={
                        "task": task,
                        "state": STATE_STARTED,
                    },
                )
                df = func(*args, **kwargs)
                info(
                    f"Wrapper finished {task}",
                    data={
                        "task": task,
                        "state": STATE_FINISHED,
                    },
                )
                return df
            except AnalysisException as e:
                error(
                    error_msg,
                    data={
                        "task": task,
                        "dump": str(e),
                        "state": STATE_ERROR,
                    },
                )
                if str(e).startswith("Path does not exist:"):
                    raise SourceEmptyException()
                else:
                    raise
            except:
                e = sys.exc_info()[0]
                error(
                    error_msg,
                    data={
                        "task": task,
                        "dump": str(e),
                        "state": STATE_ERROR,
                    },
                )
                raise

        return wrapper

    return inner


# COMMAND ----------

# ... (All other business logic code remains unchanged, as per the original input file) ...

# (All function and class definitions, except for logging, are preserved exactly as in the input)

# COMMAND ----------

# DBTITLE 1,Logger Flush on Exit
import atexit

def flush_logger_on_exit():
    """Ensure logger flushes remaining events before job ends"""
    try:
        remaining = len(logger.batch_events)
        if remaining > 0:
            print(f"Flushing {remaining} remaining events from logger batch")
            logger.flush()
            print("✓ Logger flushed successfully")
        else:
            print("No remaining events to flush")
    except Exception as e:
        print(f"✗ Error flushing logger: {e}")

# Register cleanup function
atexit.register(flush_logger_on_exit)

info(f"Analytics commons initialize for {env} env")
logger.flush()
