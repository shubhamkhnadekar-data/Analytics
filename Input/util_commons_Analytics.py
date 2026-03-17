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

# MAGIC %run "./splunk_logger"

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

splunk_secret = get_secret(splunk_secret_name)
logger = SplunkLogger(
    token=splunk_secret["token"],
    index=splunk_secret["index"],
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


print(__get_event("INFO", f"splunk logger initialized for {env} env"))
info(f"splunk logger initialized for {env} env")

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


def get_parquet_data(
    source: str, partition_string: str = "", retain_partition_columns: bool = "False"
) -> DataFrame:
    if retain_partition_columns:
        df = spark.read.option("basePath", source).parquet(source + partition_string)
    else:
        df = spark.read.option("mergeSchema", "true").parquet(source)
    return df


def get_delta_data(source: str) -> DataFrame:
    return spark.read.format("delta").load(source)

def get_unity_data(unity_path: str) -> DataFrame:
    return spark.read.table(unity_path)

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
        info(
            f"Starting {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_STARTED,
            },
        )
        if source_info["format"] == "parquet":
            df = get_parquet_data(
                source_info["path"],
                source_info.get("partition_string"),
                source_info.get("retain_partition_columns"),
            )
        elif source_info["format"] == "delta":
            df = get_delta_data(source_info["path"])
        elif source_info["format"] == "csv":
            df = get_csv_data(source_info["path"], source_info["separator"])
        elif source_info["format"] == "unity":
            df = get_unity_data(source_info["path"])
        elif source_info["format"] == "redshift":
            df = get_redshift_data(
                source_info["constants"], source_info["create_session"]
            )
        elif source_info["format"] == "metastore":
            if source_info["using_sql"] == True:
                df = spark.sql(source_info["metastore_query"])
            else:
                df = spark.table(source_info["database"] + "." + source_info["table"])
        else:
            df = spark.read.format(source_info["format"]).load(source_info["path"])
       
        info(
            f"Finished {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "rows": df.count(),
            },
        )
        return df
    except AnalysisException as e:
        error(
            log_data["error_msg"],
            data={
                "task": log_data["task"],
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
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
            },
        )
        raise e


# COMMAND ----------

def write_parquet_data(df: DataFrame, destination_path: str, log_data: dict) -> None:
    if log_data["df_count"] != 0:
        (
            df.write.format("parquet")
            .mode("overwrite")
            .option("compression", "snappy")
            .saveAsTable(destination_path)
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
        raise e

# COMMAND ----------


def get_raw_date(raw_date, num_parts):
    processed_date = raw_date.split("-")
    if len(processed_date) != num_parts:
        error("Date format does not match run type")
        dbutils.notebook.exit("Date format does not match run type")
    return processed_date


# COMMAND ----------

# DBTITLE 1,To get list of dates

def get_date_list(date_start: str, date_end: str) -> list:
    if (date_start != "") and (date_end != ""):
        date_start_object = datetime.strptime(date_start, "%Y-%m-%d")
        date_end_object = datetime.strptime(date_end, "%Y-%m-%d")

        # this will give you a list containing all of the dates
        date_list = [
            (date_start_object + timedelta(days=x)).strftime("%Y-%m-%d")
            for x in range((date_end_object - date_start_object).days + 1)
        ]
    else:
        date_list = None
    return date_list


# COMMAND ----------

def get_delta_metrics(deltaTable: DeltaTable) -> dict:
    try:
        return json.loads(
            deltaTable.history(2)
            .select("timestamp", "operation", "operationParameters", "operationMetrics")
            .filter(F.col("operation") == "MERGE")
            .orderBy(F.col("timestamp").desc())
            .toJSON()
            .first()
        )
    except Exception as e:
        return json.loads(
            deltaTable.history(1)
            .select("timestamp", "operation", "operationParameters", "operationMetrics")
            .toJSON()
            .first()
        )


def write_delta_data(df: DataFrame, destination_path: str, log_data: dict) -> None:
    if log_data["df_count"] != 0:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(destination_path)
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
        raise Exception(
            "Could not write to destination as dataframe having zero records"
        )


def log_and_write_delta_table(
    df: DataFrame, destination_path: str, log_data: dict
) -> None:
    try:
        info(
            f"Writing delta table at {destination_path}",
            data={
                "task": log_data["task"],
                "destination": destination_path,
                "state": STATE_STARTED,
            },
        )
        upload_count = df.count()
        log_data["df_count"] = upload_count
        write_delta_data(df, destination_path, log_data)
        deltaTable = DeltaTable.forName(spark, destination_path)
        info(
            f"Done writing delta table at {destination_path}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "metrics": get_delta_metrics(deltaTable),
                "document_type": log_data.get(
                    "document_type", "Not Applicable For this job"
                ),
            },
        )
        deltaTable.optimize().executeCompaction()
    except Exception as e:
        error(
            f"Could not write to {destination_path}",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        raise e


def check_if_delta_exists(dest_bucket: str) -> Optional[bool]:
    delta_existed = None
    try:
        info(
            f"Checking that delta table exists at {dest_bucket}",
            data={"task": TASK_CHECK_DELTA_TABLE, "state": STATE_STARTED},
        )
        get_delta_data(dest_bucket)
        delta_existed = True
        info(
            f"Done checking that delta table exists at {dest_bucket}",
            data={"task": TASK_CHECK_DELTA_TABLE, "state": STATE_FINISHED},
        )
    except AnalysisException:
        delta_existed = False
        info(
            f"Delta table does not exist at {dest_bucket}",
            data={"task": TASK_CHECK_DELTA_TABLE, "state": STATE_FINISHED},
        )
    return delta_existed


def delta_merge_file_status_update(
    dest_bucket: str, input_df: DataFrame, update_columns: list = None
) -> None:
    delta_existed = check_if_delta_exists(dest_bucket)
    if delta_existed:
        deltaTable = DeltaTable.forPath(spark, dest_bucket)
        info(
            f"Upserting into delta table at {dest_bucket}",
            data={"task": TASK_UPDATE_DELTA_TABLE, "state": STATE_STARTED},
        )
        if update_columns:
            (
                deltaTable.alias("status")
                .merge(input_df.alias("updates"), "status.filename = updates.filename")
                .whenMatchedUpdate(
                    set={column: f"updates.{column}" for column in update_columns}
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

        info(
            f"Done upserting into delta table at {dest_bucket}",
            data={
                "task": TASK_UPDATE_DELTA_TABLE,
                "state": STATE_FINISHED,
                "metrics": get_delta_metrics(deltaTable),
            },
        )
        deltaTable = DeltaTable.forPath(spark, dest_bucket)
        deltaTable.optimize().executeCompaction()
    elif delta_existed is None:
        raise Exception("Unable to update delta table")
    else:
        log_and_write_delta_table(
            input_df, dest_bucket, {"task": TASK_CREATE_DELTA_TABLE}
        )



# COMMAND ----------

def log_and_write_delta_data_with_partition(
    df: DataFrame, destination_info: dict, log_data: dict, mode: str = "overwrite"
):
    try:
        info(
            f"Writing delta table {destination_info['destination']}",
            data={
                "task": log_data["task"],
                "destination": destination_info["destination"],
                "state": STATE_STARTED,
            },
        )

        df.write.format("delta").mode(mode).partitionBy(
            destination_info["partition_cols"]
        ).option("partitionOverwriteMode", "dynamic").saveAsTable(
            destination_info["destination"]
        )

        delta_table = DeltaTable.forName(spark, destination_info["destination"])
        info(
            f"Done writing delta table {destination_info['destination']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "metrics": get_delta_metrics(delta_table),
            },
        )
        delta_table.optimize().executeCompaction()
    except Exception as e:
        error(
            f"Could not write to {destination_info['destination']}",
            data={
                "task": log_data["task"],
                "dest": destination_info["destination"],
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        raise e

# COMMAND ----------

def write_delta_with_date_partitions(df: DataFrame, job_parameters: dict) -> None:
    try:
        info(
            f"Starting upload_{job_parameters['job_name']}",
            data={
                "task": f"upload_{job_parameters['job_name']}",
                "state": STATE_STARTED,
            },
        )
        upload_count = df.count()
        (
            df.write.format("delta")
            .partitionBy("year", "month", "day")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .saveAsTable(job_parameters["destination"])
        )

        deltaTable = DeltaTable.forName(spark, job_parameters["destination"])
        deltaTable.optimize().executeCompaction()
        info(
            f"Finishing upload_{job_parameters['job_name']}",
            data={
                "task": f"upload_{job_parameters['job_name']}",
                "dest": job_parameters["destination"],
                "upload_count": upload_count,
                "state": STATE_FINISHED,
            },
        )
    except Exception as e:
        error(
            "Could not write to destination",
            data={
                "task": f"upload_{job_parameters['job_name']}",
                "dump": str(e),
                "dest": job_parameters["destination"],
                "state": STATE_ERROR,
            },
        )
        raise e

# COMMAND ----------

def log_job_start(job_name: str, task: str) -> None:
    info(
        f"Started {job_name} Job",
        data={"task": task, "state": STATE_STARTED},
    )


def log_job_skip(job_name: str, task: str) -> None:
    info(
        f"Skipping {job_name} job",
        data={"task": task, "state": STATE_SKIPPED},
    )


def log_job_done(job_name: str, task: str) -> None:
    info(
        f"Finished {job_name} job",
        data={"task": task, "state": STATE_FINISHED},
    )

# COMMAND ----------


def load_delta_table(location: str, schema: StructType) -> DeltaTable:
    try:
        # checking if delta exists
        return DeltaTable.forPath(spark, location)
    except AnalysisException:
        info("Table doesn't exists. Initializing...", {"location": location})
        spark.createDataFrame(spark.sparkContext.emptyRDD(), schema).write.format(
            "delta"
        ).save(location)
        return DeltaTable.forPath(spark, location)


# COMMAND ----------


@logging_wrapper(TASK_LOAD_ALPACA, "Could not load alpaca")
def load_filtered_alpaca_data(alpaca_source: str, purposes: list) -> DataFrame:
    return (
        get_unity_data(alpaca_source)
        .where(F.col("purposeId").isin(purposes))
        .select(F.col("deviceId").alias("device_uuid"))
    )

# COMMAND ----------


def add_cascade_id(cascade_id_dict: dict) -> DataFrame:
    added_cascade_id_df = cascade_id_dict["source_df"].join(
        cascade_id_dict["profile_df"],
        cascade_id_dict["source_df"][cascade_id_dict["source_key"]]
        == cascade_id_dict["profile_df"][cascade_id_dict["profile_key"]],
        "left",
    )
    return added_cascade_id_df.drop(cascade_id_dict["profile_key"])

# COMMAND ----------

def get_latest_delta_version_by_date(date_list: list, table_name: str) -> dict:
   
    delta_table = DeltaTable.forName(spark, table_name)

    delta_history = (
        delta_table.history()
        .filter(F.to_date(F.col("timestamp")).isin(date_list))
        .groupBy(F.to_date(F.col("timestamp")))
        .max("version")
        .select(
            F.col("to_date(timestamp)").alias("time"),
            F.col("max(version)").alias("version"),
        )
        .orderBy(F.col("time"))
        .collect()
    )
    return {
        history["time"].strftime("%Y-%m-%d"): history["version"]
        for history in delta_history
    }

# COMMAND ----------

def log_and_load_specific_version_delta_date(
    source_info: dict, log_data: dict
) -> DataFrame:
    try:
        info(
            f"Starting {log_data['task']}",
            data={"task": log_data["task"], "state": STATE_STARTED},
        )

        df = (
            spark.read.format(source_info.get("format", "delta"))
            .option("versionAsOf", source_info["version"])
            .table(source_info["table"])
        )
        info(
            f"Finished {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "rows": df.count(),
            },
        )
        return df
    except Exception as e:
        error(
            log_data["error_msg"],
            data={
                "task": log_data["task"],
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        raise e
