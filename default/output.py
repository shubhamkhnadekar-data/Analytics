# Databricks notebook source
from typing import TypeVar, Optional
from pyspark.sql.types import StructType, DataType, TimestampType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta, date
from distutils import util
from botocore.exceptions import ClientError
import json
from datetime import datetime
import boto3
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from pyspark.sql.functions import udf
from pyspark.sql.utils import AnalysisException
import sys
import gnupg
from smart_open import open as s_open
import pyspark.sql.functions as F
#import dataos_metastore.metastore as metastore
import pandas as pd
import warnings
import os
import io
import requests

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

print(f"env:{env}")
print(f"job_name:{job_name}")
print(f"databricks_host:{databricks_host}")

STATE_STARTED = "started"
STATE_FINISHED = "finished"
STATE_ERROR = "error"
STATE_SKIPPED = "skipped"

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
    source_type = "spark-job"
    job_name = notebook_info["tags"]["jobName"]
    log_data["module_name"] = "cascade"
except:
    print("Not a job execution")
    log_data["run-id"] = 0
    log_data["job-name"] = f"notebook:{job_name}"
    source_type = "spark-notebook"

log_data["job-run-time"] = job_time
print(log_data)

# COMMAND ----------

# Commented out Splunk secret block as part of migration
#splunk_secret = get_secret(f"{env}/k8s/p2retargeting/splunk")
#logger = SplunkLogger(
#    token=splunk_secret["token"],
#    index=splunk_secret["index"],
#    meta_data={
#        "source": job_name,
#        "sourcetype": f"databricks:{source_type}",
#        "host": databricks_host,
#    },
#)

# Databricks logger initialization
logger = SplunkLogger(
    token="",
    index="",
    meta_data={
        "source": job_name,
        "sourcetype": f"databricks:{source_type}",
        "host": databricks_host,
    },
)

def __get_event(log_level, msg, data=None):
    # adding log level and msg to event
    event = {"level": log_level, "message": msg}
    if isinstance(data, dict):
        event.update(data)
    elif isinstance(data, str) and data.strip():
        event["data"] = data
    event.update(log_data)
    return json.dumps(event)

def debug(msg: object, data: object = None):
    logger.log_event(__get_event("DEBUG", msg, data))

def info(msg: object, data: object = None):
    logger.log_event(__get_event("INFO", msg, data))

def warn(msg: object, data: object = None):
    logger.log_event(__get_event("WARN", msg, data))

def error(msg: object, data: object = None):
    logger.log_event(__get_event("ERROR", msg, data))

def fatal(msg: object, data: object = None):
    logger.log_event(__get_event("FATAL", msg, data))

print(__get_event("INFO", f"Metrics logger initialized for {env} env"))
info(f"Metrics logger initialized for {env} env")
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

class SourceEmptyException(Exception):
    pass

def logging_wrapper(task, error_msg):
    def inner(func):
        def wrapper(*args, **kwargs):
            try:
                info(
                    f"Wrapper starting {task}",
                    data={"task": task, "state": STATE_STARTED},
                )
                df = func(*args, **kwargs)
                info(
                    f"Wrapper finished {task}",
                    data={"task": task, "state": STATE_FINISHED},
                )
                logger.flush()
                return df
            except AnalysisException as e:
                error(
                    error_msg,
                    data={"task": task, "dump": str(e), "state": STATE_ERROR},
                )
                logger.flush()
                if str(e).startswith("Path does not exist:"):
                    raise SourceEmptyException()
                else:
                    raise
            except:
                e = sys.exc_info()[0]
                error(
                    error_msg, data={"task": task, "dump": str(e), "state": STATE_ERROR}
                )
                logger.flush()
                raise

        return wrapper

    return inner

# COMMAND ----------

@logging_wrapper("TASK_LOAD_ALPACA", "Could not load alpaca")
def load_filtered_alpaca_data(alpaca_source: str, purposes: list) -> DataFrame:
    return (
        get_delta_data(alpaca_source)
        .where(F.col("purposeId").isin(purposes))
        .select(F.col("deviceId").alias("device_uuid"))
        .distinct()
    )

# COMMAND ----------

@logging_wrapper("TASK_LOAD_SIMPLEUI", "Could not load simpleui data")
def load_simpleui_data(simpleui_data_source: str) -> DataFrame:
    return (
        get_delta_data(simpleui_data_source)
        .select(
            F.col(
                "printer_id"
            ).alias("printerId_from_simpleui"),
            F.col(
                "app_deployed_uuid"
            ).alias("appDeployedUUID"),
        )
        .distinct()
        .dropna()
    )

# COMMAND ----------

def filter_alpaca_consented_with_intermediate_df(
    alpaca_df: DataFrame,
    intermediate_df: DataFrame,
    join_column_of_intermediate_df: str,
 ) -> DataFrame:
    return alpaca_df.join(
        intermediate_df,
        on=[
            intermediate_df[join_column_of_intermediate_df]
            == alpaca_df.deviceId_from_alpaca
        ],
        how="inner",
    )

# COMMAND ----------

def add_cascade_id(dict_cascade_id: dict) -> DataFrame:
    added_cascade_id_df = dict_cascade_id["source_df"].join(
        dict_cascade_id["profile_df"],
        dict_cascade_id["source_df"][dict_cascade_id["source_key"]]
        == dict_cascade_id["profile_df"][dict_cascade_id["profile_key"]],
        "left",
    )
    return added_cascade_id_df.drop(dict_cascade_id["profile_key"])

# COMMAND ----------

def get_parquet_data(source: str) -> DataFrame:
    return spark.read.option("mergeSchema", "true").parquet(source)

def get_delta_data(source: str,check_on: str="s3") -> DataFrame:
    if check_on=="unity":
        return spark.read.table(source)
    return spark.read.format("delta").load(source)

# COMMAND ----------

def get_delta_data(source: str,check_on: str="s3") -> DataFrame:    pass

def get_unity_catalog_data(query: str) -> DataFrame:
    return spark.sql(query)

# COMMAND ----------

def get_csv_data(source: str, separator: str = "|") -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", separator)
        .load(source)
    )

def get_decrypted_data_from_gpg(source, secret_name):
    secret = get_secret(secret_name)
    gpg = gnupg.GPG(gpgbinary="/usr/bin/gpg")
    gpg.import_keys(key_data=secret["private_key"], passphrase=secret["passphrase"])
    with s_open(source, mode="rb") as file:
        decrypted_data = gpg.decrypt_file(file, passphrase=secret["passphrase"])
    return decrypted_data

def get_redshift_data(redshift_constants: dict, create_session: bool) -> DataFrame:
    if create_session == True:
        sess = STSSession(
            arn=redshift_constants["CUMULUS_ARN"],
            session_name=redshift_constants["ARN_ROLE_SESSION_NAME"],
        )
        secret = get_secret(
            redshift_constants["REDSHIFT_SECRET_ID"], session=sess.sts_session
        )
    else:
        secret = get_secret(redshift_constants["REDSHIFT_SECRET_ID"])
    username = secret["username"]
    password = secret["password"]
    jdbc_connection = f"jdbc:redshift://{redshift_constants['REDSHIFT_HOST']}:{redshift_constants['PORT']}/{redshift_constants['DBNAME']}?ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory&user={username}&password={password}"
    df = (
        spark.read.format("com.databricks.spark.redshift")
        .option("url", jdbc_connection)
        .option("query", redshift_constants["QUERY"])
        .option("tempdir", redshift_constants["TEMP_S3_DIR"])
        .option("forward_spark_s3_credentials", True)
        .load()
    )
    return df

def log_and_load_data(source_info: dict, log_data: dict) -> DataFrame:
    try:
        # Extract source info for logging
        source_format = source_info.get("format", "unknown")
        source_using_sql = source_info.get("using_sql", False)
        
        # Determine source location based on format
        if source_format == "metastore" and source_using_sql:
            source_location = source_info.get("metastore_query", "")
        elif "path" in source_info:
            source_location = source_info.get("path", "")
        elif "query" in source_info:
            source_location = source_info.get("query", "")
        elif "database" in source_info and "table" in source_info:
            source_location = f"{source_info.get('database', '')}.{source_info.get('table', '')}"
        else:
            source_location = ""
        
        info(
            f"Starting {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_STARTED,
                "source_format": source_format,
                "source_using_sql": source_using_sql,
                "source_location": source_location,
            },
        )
        if source_info["format"] == "parquet":
            df = get_parquet_data(source_info["path"])
        elif source_info["format"] == "delta":
            df = get_delta_data(source_info["path"])
        elif source_info["format"] == "csv":
            df = get_csv_data(source_info["path"], source_info["separator"])
        elif source_info["format"] == "redshift":
            df = get_redshift_data(
                source_info["constants"], source_info["create_session"]
            )
        elif source_info["format"] == "metastore":
            if source_info["using_sql"] == True:
                df = spark.sql(source_info["metastore_query"])
            else:
                df = metastore.read_table(source_info["database"], source_info["table"])
        elif source_info["format"] == "unity_catalog_query":
            df = get_unity_catalog_data(source_info["query"])
        elif source_info["format"] == "unity_table":
            df = spark.read.table(source_info["path"])
        else:
            df = spark.read.format(source_info["format"]).load(source_info["path"])

        info(
            f"Finished {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "rows": df.count(),
                "source_format": source_format,
                "source_using_sql": source_using_sql,
                "source_location": source_location,
            },
        )
        logger.flush()
        return df
    except AnalysisException as e:
        error(
            log_data["error_msg"],
            data={
                "task": log_data["task"],
                "dump": str(e),
                "state": STATE_ERROR,
                "source_format": source_info.get("format", "unknown"),
                "source_using_sql": source_info.get("using_sql", False),
                "source_location": source_info.get("path", source_info.get("query", source_info.get("metastore_query", ""))),
            },
        )
        logger.flush()
        if str(e).startswith("Path does not exist:"):
            raise SourceEmptyException()
        else:
            raise e
    except Exception as e:
        error(
            log_data["error_msg"],
            data={
                "task": log_data["task"],
                "dump": str(e),
                "state": STATE_ERROR,
                "source_format": source_info.get("format", "unknown"),
                "source_using_sql": source_info.get("using_sql", False),
                "source_location": source_info.get("path", source_info.get("query", source_info.get("metastore_query", ""))),
            },
        )
        logger.flush()
        raise e

# COMMAND ----------

def write_parquet_data(df: DataFrame, destination_path: str, log_data: dict) -> None:
    if log_data["df_count"] != 0:
        (
            df.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(destination_path)
        )
    else:
        error(
            "Could not write to destination as dataframe having zero records",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "state": STATE_ERROR,
            },
        )
        logger.flush()
        raise Exception(
            "Could not write to destination as dataframe having zero records"
        )

def log_and_write_parquet_data(
    df: DataFrame, destination_path: str, log_data: dict
) -> None:
    try:
        info(
            f"Uploading {log_data['job_name']}",
            data={
                "dest": destination_path,
                "task": log_data["task"],
                "state": STATE_STARTED,
            },
        )
        upload_count = df.count()
        log_data["df_count"] = upload_count
        write_parquet_data(df, destination_path, log_data)
        info(
            f"Done uploading {log_data['job_name']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "rows": upload_count,
                "document_type": log_data.get(
                    "document_type", "Not Applicable For this job"
                ),
            },
        )
        logger.flush()
    except Exception as e:
        error(
            "Could not write to destination",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        logger.flush()
        raise e

def log_and_write_delta_table(
    df: DataFrame, destination: str, log_data: dict) -> None:
    try:
        info(
            f"Writing delta table at {destination}",
            data={
                "task": log_data["task"],
                "destination": destination,
                "state": STATE_STARTED,
            },
        )
        upload_count = df.count()
        log_data["df_count"] = upload_count
        write_delta_table(log_data, df, destination)
        logger.flush()
    except Exception as e:
        error(
            f"Could not write to {destination}",
            data={
                "task": log_data["task"],
                "dest": destination,
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        logger.flush()
        raise e

def write_unity_data(df: DataFrame, destination_path: str, log_data: dict,mode:str ="overwrite") -> None:
    if log_data["df_count"] != 0:
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(destination_path)
    else:
        error(
            "Could not write to destination as dataframe having zero records",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "state": STATE_ERROR,
            },
        )
        logger.flush()
        raise Exception(
            "Could not write to destination as dataframe having zero records"
        )

def log_and_write_unity_data(
    df: DataFrame, destination_path: str, log_data: dict,mode: str="overwrite"
) -> None:
    try:
        info(
            f"Uploading {log_data['job_name']}",
            data={
                "dest": destination_path,
                "task": log_data["task"],
                "state": STATE_STARTED,
            },
        )
        upload_count = df.count()
        log_data["df_count"] = upload_count
        write_unity_data(df, destination_path, log_data,mode)
        info(
            f"Done uploading {log_data['job_name']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "rows": upload_count,
                "document_type": log_data.get(
                    "document_type", "Not Applicable For this job"
                ),
            },
        )
        logger.flush()
    except Exception as e:
        error(
            "Could not write to destination",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        logger.flush()
        raise e

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
