# Databricks notebook source
dbutils.widgets.text("batchdt", "","")

# COMMAND ----------

import os
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *

def replace_round(x):
  return when(isnan(x),None).otherwise(round(x,3))

def replace_values(df):
  for col_name, col_type in df.dtypes:
    if col_type == 'double':
      df = df.withColumn(col_name, replace_round(col(col_name)))
  return df

insertdt = datetime.strptime(dbutils.widgets.get("batchdt"), "%Y%m%d%H")
def preprocess_data(df):
  df = replace_values(df)
  df = df.withColumn("Insert_DT", lit(insertdt))
  df = df.withColumnRenamed('prediction', 'Prediction')
  return df.withColumnRenamed('rowId', 'Row_ID')

jdbc_url="%suser=%s;password=%s" % (os.environ["JDBC_URL"], os.environ["JDBC_USER"], os.environ["JDBC_PASS"])
def write_data_to_sql_dwh(df):
  df.write.jdbc(jdbc_url, "ASBChiller.ASBChiller_Datarobot_Hourly", mode='append')

# COMMAND ----------

#datarobot_api_call

import requests
import sys
# import pandas as pd

#ADF input parameter batchdt, eg '2019010313'
startdate = dbutils.widgets.get("batchdt")
obfilename = "/mnt/asbchillerprocessed/output/"+startdate+".parquet"

#read data from blob output folder
df = spark.read.parquet(obfilename)

df = df.select("*").toPandas()

df_onlysetpoint=df[['CHL_COMMON_CW_Delta_T','CHL_COMMON_CHW_Delta_T','CHL_COMMON_CW_CT_Delta_T']]


API_TOKEN = 'sNturxN1YFTBQckU_2msAE3Hh-13ODDL'
USERNAME = 'datarobot@ascendas-singbridge.com'

DEPLOYMENT_ID = '5cf0cffee21c8f01f022c18a'


class DataRobotPredictionError(Exception):
    pass


def make_datarobot_deployment_predictions(data, deployment_id):
    """
    Make predictions on data provided using DataRobot deployment_id provided.
    See docs for details:
         https://app.datarobot.com/docs/users-guide/deploy/api/new-prediction-api.html

    Parameters
    ----------
    data : str
        Feature1,Feature2
        numeric_value,string
    deployment_id : str
        The ID of the deployment to make predictions with.

    Returns
    -------
    Response schema: https://app.datarobot.com/docs/users-guide/deploy/api/new-prediction-api.html#response-schema

    Raises
    ------
    DataRobotPredictionError if there are issues getting predictions from DataRobot
    """
    # Set HTTP headers. The charset should match the contents of the file.
    headers = {'Content-Type': 'text/plain; charset=UTF-8', 'datarobot-key': 'a6eef7ac-f93e-740c-c284-dd150844fbe3'}

    url = 'https://ascendas-singbridge.orm.datarobot.com/predApi/v1.0/deployments/{deployment_id}/predictions'.format(
        deployment_id=deployment_id
    )
    # Make API request for predictions
    predictions_response = requests.post(url, auth=(USERNAME, API_TOKEN), data=data,
                                         headers=headers)
    _raise_dataroboterror_for_status(predictions_response)
    # Return a Python dict following the schema in the documentation
    return predictions_response.json()


def _raise_dataroboterror_for_status(response):
    """Raise DataRobotPredictionError if the request fails along with the response returned"""
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        errMsg = '{code} Error: {msg}'.format(code=response.status_code, msg=response.text)
        raise DataRobotPredictionError(errMsg)


def main(data, deployment_id):
    """
    Return an exit code on script completion or error. Codes > 0 are errors to the shell.
    Also useful as a usage demonstration of `make_datarobot_deployment_predictions(data, deployment_id)`
    """
    Output=[]
    try:
        predictions = make_datarobot_deployment_predictions(data, deployment_id)
    except DataRobotPredictionError as exc:
        print(exc)
        return 1
    Output.append(predictions)
#    print(predictions)
    return Output

mlresult=(main(df_onlysetpoint.to_csv(), DEPLOYMENT_ID)) 
mlresult=pd.DataFrame(mlresult[0]['data'])[['prediction','rowId']]
mlresult=sqlContext.createDataFrame(mlresult)
mlresult=preprocess_data(mlresult)
mlresult.write.option("header","true").mode("overwrite").csv("/mnt/asbchillerprocessed/mloutput/" + startdate + ".csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.{Connection, DriverManager, Timestamp}
# MAGIC import java.time.format.DateTimeFormatter
# MAGIC import java.time.LocalDateTime
# MAGIC 
# MAGIC val batchdt = dbutils.widgets.get("batchdt")
# MAGIC 
# MAGIC val df = spark.read.format("csv").option("header", "true").load(s"/mnt/asbchillerprocessed/mloutput/${batchdt}.csv")
# MAGIC val insertdt = LocalDateTime.parse(dbutils.widgets.get("batchdt"), fmt).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
# MAGIC 
# MAGIC val sql = s"""DELETE FROM ASBChiller.ASBChiller_Datarobot_Hourly
# MAGIC               WHERE Insert_DT = '${insertdt}';"""
# MAGIC 
# MAGIC val DB_JDBC_CONN = sys.env("JDBC_URL")
# MAGIC val DB_JDBC_USER = sys.env("JDBC_USER")
# MAGIC val DB_JDBC_PASS = sys.env("JDBC_PASS")
# MAGIC 
# MAGIC val conn = DriverManager.getConnection(DB_JDBC_CONN,DB_JDBC_USER,DB_JDBC_PASS)
# MAGIC conn.setAutoCommit(true)
# MAGIC val stmt = conn.createStatement()
# MAGIC try {
# MAGIC   val ret = stmt.executeUpdate(sql)
# MAGIC   println("Ret val: " + ret.toString)
# MAGIC   println("Update count: " + stmt.getUpdateCount())
# MAGIC } catch {
# MAGIC   case e: Exception => e.printStackTrace()
# MAGIC } finally {
# MAGIC   conn.commit()
# MAGIC   stmt.close()
# MAGIC   conn.close()
# MAGIC }

# COMMAND ----------

write_data_to_sql_dwh(mlresult)