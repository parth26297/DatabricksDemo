# Databricks notebook source
# MAGIC %python
# MAGIC # Creates a text widget to get value from ADF
# MAGIC 
# MAGIC dbutils.widgets.text("batchdt", "","")
# MAGIC dbutils.widgets.text("enddate", dbutils.widgets.get("batchdt"), "")
# MAGIC dbutils.widgets.text("filename", "", "")

# COMMAND ----------

startdate <- dbutils.widgets.get("batchdt")
enddate <- dbutils.widgets.get("enddate")
historical_data_file <- dbutils.widgets.get("filename")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

# import necessary libraries
library(SparkR)
library(plyr)
library(dplyr)
library(data.table)

# COMMAND ----------

# preparing current hour filename based on whether it is historical data or not
currentdt <- as.POSIXct(startdate,format="%Y%m%d%H")
date<- format(currentdt,"%Y%m%d")
time<- format(currentdt,"%H")
datetime<- paste(date,time,sep="_")
input_file <- if(historical_data_file == "") paste0(datetime, '*') else historical_data_file
cdtfile<- paste0('/mnt/asbchiller/', input_file)

# COMMAND ----------

#Reading and processing input data from blob storage using spark library
Complete_data<-read.df(cdtfile, source = "csv", header="false", inferSchema = "true")
Complete_data<- SparkR::distinct(Complete_data)
colnames(Complete_data) <- c("TagName", "Timestamp", "value", "Quality")
Complete_data$Timestamp<-substr(Complete_data$Timestamp,1,19)
Complete_data$TagName = regexp_replace(Complete_data$TagName, "hh:\\\\Configuration\\\\", "")

#Reading master mapping data from blob storage using R read.csv function
mapping<-read.df("/mnt/asbchillermasterdata/Tag_variable_name.csv", source = "csv", skiprows=1,header="false", inferSchema = "true")
colnames(mapping) <- c("TagName", "Variables", "Equipment", "Building")

# COMMAND ----------

# merging mapping data to input data
Complete_data<-as.data.frame(Complete_data)
mapping<-as.data.frame(mapping)
mapping<-mapping[!(mapping$Building=="Building"),]
Complete_data<-merge(Complete_data,mapping,by="TagName")
not_merged<-anti_join(mapping, Complete_data, by="TagName")
not_merged_tag<-anti_join(Complete_data, mapping, by="TagName")
Complete_data$TagName<-NULL
# Complete_data$Timestamp<-substr(Complete_data$Timestamp,1,19)
Complete_data$Time<-as.POSIXct(Complete_data$Timestamp,format="%Y-%m-%d %H:%M")
Complete_data$Timestamp<-NULL
Complete_data<-Complete_data[Complete_data$Quality == 0,]
Complete_data$Quality<-NULL
not_merged$value<-NA
not_merged$TagName<-NULL
not_merged$Time<-min(Complete_data$Time)
Complete_data<- rbind(Complete_data,not_merged)

# COMMAND ----------

# pivoting table and converting data into minute period
Complete_data$value<-as.numeric(as.character(Complete_data$value))
Complete_data<-dcast(Complete_data,Time+Building ~ Variables,value.var="value",fun.aggregate =mean,na.rm=T)

is.nan.data.frame <- function(x)
  do.call(cbind, lapply(x, is.nan))

Complete_data[is.nan.data.frame(Complete_data)] <- NA
a<-as.POSIXct(min(Complete_data$Time),format="%Y-%m-%d %H:%M")
b<-as.POSIXct(max(Complete_data$Time),format="%Y-%m-%d %H:%M")
tseq <- seq(from = a, to = b, by = "mins")
Time<-data.frame(Time=tseq)
# colnames(Time)[1]<-"Time"
Complete_data<-merge(Time,Complete_data, by="Time",all.x=T)

# COMMAND ----------

# preparing previous hour processed data file name
lttime<- format(currentdt-3600,"%H") # subtracting 3600 seconds to get previous hour.
ltdatetime<- paste0(date,lttime)
ldtfile<- paste0("/mnt/asbchillerprocessed/output/",ltdatetime,".parquet")

#read process and merge last hour processed data if exists to current hour unprocessed data
if (file.exists(paste0("/dbfs",ldtfile)) && historical_data_file == "") {
  fileinput_count<- 2
  lasthour_data<- read.df(ldtfile, source = "parquet")
  lasthour_data<- as.data.frame(lasthour_data)
  lasthour_data$station_id<- NULL
  lasthour_data$Date<- NULL
  lasthour_data$CHL_Total_KW<- NULL
  lasthour_data$CHL_Total_RT<- NULL
  lasthour_data$CHL_COMMON_CHW_Delta_T<- NULL
  lasthour_data$CHL_COMMON_CW_Delta_T<- NULL
  lasthour_data$CHWP_Total_KW<- NULL
  lasthour_data$CT_Total_KW<- NULL
  lasthour_data$CWP_Total_KW<- NULL
  lasthour_data$Total_KW<- NULL
  lasthour_data$Hour<- NULL
  lasthour_data$PH<- NULL
  lasthour_data$Weekend<- NULL
  lasthour_data$Holiday<- NULL
  lasthour_data$Peak_NotPeak<- NULL
  lasthour_data$Type<- NULL
  lasthour_data$Chiller_Efficiency_calc<- NULL
  lasthour_data$System_Efficiency_calc<- NULL
  lasthour_data$CHL_Load<- NULL
  lasthour_data$Bin_load_common<- NULL
  lasthour_data$Bin_load<- NULL
  lasthour_data$Bin_load_Sort<- NULL
  lasthour_data$Bin_load_common_Sort<- NULL
  lasthour_data$Step_Change<- NULL
  lasthour_data$Step_Change_Final<- NULL
  lasthour_data$Wetbulbtemp<- NULL
  lasthour_data$CHL_COMMON_CW_CT_Delta_T<- NULL
  colnames(lasthour_data)[colnames(lasthour_data)=="Variable_Name"] <- "Variable Name"
  
  Complete_data<- rbind(lasthour_data,Complete_data)
  Complete_data<-Complete_data[order(Complete_data$Time),]
  rm(lasthour_data)
} else {fileinput_count<- 1}

# COMMAND ----------

# Grouping by Building and imputing null values 
process_group <- function(check) {
    if(!(sum(is.na(check)==TRUE) == length(check) || sum(is.na(check)==TRUE) == 0)) {
        j <- 1
        value <- NULL
        while(j <= length(check)) {
            start <- j
            while(is.na(check[j]) && j <= length(check)) {
                j <- j + 1
            }
            count <- j-start
            if(count>0 && count <=60 && j<=length(check) && !is.null(value)) {
                check[start:j-1]<-value
            }
            value <- check[j]
            j=j+1
        }
    }
    check
}

Buildings<-unique(Complete_data$Building)
data <- data.frame()
for (z in Buildings) {
    subset <- Complete_data[Complete_data$Building== z,]
    subset[, -2] <- as.data.frame(subset[, -2] %>% lapply(process_group)) # -2 because Building column is not to be imputed
    data <- rbind.fill(data, subset)
}

# COMMAND ----------

data=data[rowSums(is.na(data)) < ncol(data)-1,]

data$CHL_Total_KW <- rowSums(data[,c("CHL_1_KW", "CHL_2_KW",
                                     "CHL_3_KW","CHL_4_KW")], na.rm=TRUE)

data$CHL_Total_RT <- rowSums(data[,c("CHL_1_COOLING_LOAD_RT", "CHL_2_COOLING_LOAD_RT",
                                     "CHL_3_COOLING_LOAD_RT","CHL_4_COOLING_LOAD_RT")], na.rm=TRUE)

data$CHL_COMMON_CHW_Delta_T=data$CHL_COMMON_CHW_COMMON_RWT-data$CHL_COMMON_CHW_COMMON_SWT

data$CHL_COMMON_CW_Delta_T=data$CHL_COMMON_CW_COMMON_RWT-data$CHL_COMMON_CW_COMMON_SWT

data$CHWP_Total_KW= rowSums(data[,c("CHWP_1_COM_KW", "CHWP_2_COM_KW",
                                    "CHWP_3_COM_KW","CHWP_4_COM_KW")], na.rm=TRUE)

data$CT_Total_KW= rowSums(data[,c("CT_1_KW", "CT_2_KW",
                                  "CT_3_KW","CT_4_KW")], na.rm=TRUE)

data$CWP_Total_KW= rowSums(data[,c("CWP_1_COM_KW", "CWP_2_COM_KW",
                                   "CWP_3_COM_KW","CWP_4_COM_KW")], na.rm=TRUE)

data$Total_KW= rowSums(data[,c("CWP_Total_KW", "CT_Total_KW",
                               "CHWP_Total_KW","CHL_Total_KW")], na.rm=TRUE)

# COMMAND ----------

# 
data$Hour<-as.numeric(substr(data$Time,12,13))

holiday<-read.df("/mnt/asbchillermasterdata/Holidays.csv", source="csv",header="true", inferSchema="true")
holiday<-as.data.frame(holiday)
								 
data$Date=as.POSIXct(data$Time,format="%Y-%m-%d")
data<-merge(data,holiday,by.x="Date",by.y="Holiday",all.x=T)
data$PH[is.na(data$PH)]<-0

weekdays<-c("Monday","Tuesday","Wednesday","Thursday","Friday")
data$Weekend<-ifelse(weekdays(data$Date) %in% weekdays ,0,1)
data$Holiday<-ifelse(data$PH| data$Weekend,1,0)
data$Peak_NotPeak<-ifelse(data$Hour >= 7 & data$Hour < 18 , "Peak","Non Peak" )

# COMMAND ----------

#Replacing null with zeros
data$CHL_1_KW[is.na(data$CHL_1_KW)]<-0
data$CHL_2_KW[is.na(data$CHL_2_KW)]<-0
data$CHL_3_KW[is.na(data$CHL_3_KW)]<-0
data$CHL_4_KW[is.na(data$CHL_4_KW)]<-0

# COMMAND ----------

# Inferring Type values
# Type 1  chiller3 or chiller 4
# Type 2  chiller 1 or chiller 2
# Type 3  Ch 1 or 2 and ch 3 or 4
# Type 4  ch 1 and 2
# Type 5   else 
# Parameters
data$Type =ifelse((data$CHL_3_KW>0 & data$CHL_4_KW==0 & data$CHL_1_KW==0 & data$CHL_2_KW==0)|
                    (data$CHL_3_KW==0 & data$CHL_4_KW >0 & data$CHL_1_KW==0 & data$CHL_2_KW==0),
                  "One Small","Rest")

data$Type =ifelse((data$CHL_3_KW== 0 & data$CHL_4_KW==0 & data$CHL_1_KW > 0 & data$CHL_2_KW==0)|
                    (data$CHL_3_KW==0 & data$CHL_4_KW ==0 & data$CHL_1_KW==0 & data$CHL_2_KW >0),
                  "One Big",data$Type)


data$Type =ifelse((  data$CHL_3_KW > 0 & data$CHL_4_KW==0 & data$CHL_1_KW > 0 & data$CHL_2_KW==0)|
                    (data$CHL_3_KW > 0 & data$CHL_4_KW ==0 & data$CHL_1_KW==0 & data$CHL_2_KW >0)|
                    (data$CHL_3_KW== 0 & data$CHL_4_KW >0 & data$CHL_1_KW > 0 & data$CHL_2_KW==0)|
                    (data$CHL_3_KW== 0 & data$CHL_4_KW >0 & data$CHL_1_KW == 0 & data$CHL_2_KW >0),
                  "One Big One Small",data$Type)


data$Type =ifelse((data$CHL_3_KW== 0 & data$CHL_4_KW==0 & data$CHL_1_KW > 0 & data$CHL_2_KW>0),
                  "Two Big",data$Type)

data$Type =ifelse((data$CHL_3_KW > 0 & data$CHL_4_KW==0 & data$CHL_1_KW > 0 & data$CHL_2_KW>0)|
                    (data$CHL_3_KW== 0 & data$CHL_4_KW > 0 & data$CHL_1_KW > 0 & data$CHL_2_KW>0),
                  "Two Big",data$Type)

# COMMAND ----------

data$Chiller_Efficiency_calc<-data$CHL_Total_KW/data$CHL_Total_RT
data$System_Efficiency_calc<-data$Total_KW/data$CHL_Total_RT

data$CHL_Load<-ifelse(data$Type=="One Small",480,(ifelse(data$Type=="One Big",950,
                                                         (ifelse(data$Type=="One Big One Small",1430,
                                                                 (ifelse(data$Type=="Two Big",1900,NA)))))))

# COMMAND ----------

intervals <- data.frame(cbind(seq(0, 2000, length = 41), seq(50, 2050, length = 41)))

colnames(intervals)<-c("min","max")

intervals$Range<-paste(intervals$min,intervals$max,sep="-")

data$Bin_load_common <- cut(data$CHL_COMMON_COOLING_LOAD_RT,breaks=intervals$min, include.lowest = F, labels = intervals$Range[1:40])
data$Bin_load <- cut(data$CHL_Total_RT,breaks=intervals$min, include.lowest = F, labels = intervals$Range[1:40])
data$Date<-substr(data$Time,1,10)
data$Bin_load_Sort<-substring(data$Bin_load,1,regexpr("-", data$Bin_load) -1)
data$Bin_load_common_Sort<-substring(data$Bin_load_common,1,regexpr("-", data$Bin_load_common) -1)

data<-data[order(data$Time),]
data<-data[!(data$Type == "Rest"),]
data$Step_Change<- 
  ifelse(!(data$Type==lead(data$Type)),
         ifelse(((data$Type=="One Small"& (lead(data$Type)=="One Big"|lead(data$Type)=="One Big One Small"|lead(data$Type)=="Two Big"))|
                   (data$Type=="One Big"& (lead(data$Type)=="One Big One Small"|lead(data$Type)=="Two Big"))|
                   (data$Type=="One Big One Small"& lead(data$Type)=="Two Big")),"Step Up",
                (ifelse((data$Type=="One Big"&lead(data$Type)=="One Small")|
                          (data$Type=="One Big One Small"&(lead(data$Type)=="One Big"|lead(data$Type)=="One Small")) |
                          (data$Type=="Two Big"&(lead(data$Type)=="One Big One Small"|lead(data$Type)=="One Small"|lead(data$Type)=="One Big")),"Step down","Unknown"
                       ))),"Normal")

# COMMAND ----------

check_data<-data[, c("Time","Type","Step_Change")]
check_data<-check_data[order(check_data$Time),]
check_data$Step_Change2<-""
i=1
while(i< (nrow(data)-6)) {
  check=0
  flag=0
  if(check_data$Step_Change[i] =="Step Up"|check_data$Step_Change[i] =="Step down") {
    for( j in 1 :6) {
      if(check_data$Step_Change[i+j] =="Step Up"|check_data$Step_Change[i+j] =="Step down") { 
        flag=1
        check=j
        break
      }
    }
    if(flag==1) {
      check_data$Step_Change2[i:(i+j)]<-"Staging Proccess"
      i=i+j+1
    } else {
      check_data$Step_Change2[i:(i+6)]<-check_data$Step_Change[i:(i+6)]
      i=i+6+1
    }
  } else {
    check_data$Step_Change2[i]<-check_data$Step_Change[i]
    i=i+1
  }  
}

# COMMAND ----------

check_data<-check_data[(!check_data$Step_Change2=="Staging Proccess"),]

check_data<-check_data[order(check_data$Time),]
check_data<-check_data[!(check_data$Type == "Rest"),]
check_data$Step_Change3<- 
  ifelse(!(check_data$Type==lead(check_data$Type)),
         ifelse(((check_data$Type=="One Small"& (lead(check_data$Type)=="One Big"|lead(check_data$Type)=="One Big One Small"|lead(check_data$Type)=="Two Big"))|
                   (check_data$Type=="One Big"& (lead(check_data$Type)=="One Big One Small"|lead(check_data$Type)=="Two Big"))|
                   (check_data$Type=="One Big One Small"& lead(check_data$Type)=="Two Big")),"Step Up",
                (ifelse((check_data$Type=="One Big"&lead(check_data$Type)=="One Small")|
                          (check_data$Type=="One Big One Small"&(lead(check_data$Type)=="One Big"|lead(check_data$Type)=="One Small")) |
                          (check_data$Type=="Two Big"&(lead(check_data$Type)=="One Big One Small"|lead(check_data$Type)=="One Small"|lead(check_data$Type)=="One Big")),"Step down","Unknown"  
                        
                ))),"Normal")


merge<-check_data[,c("Time","Step_Change3")]
colnames(merge)<-c("Time","Step_Change_Final")

data<-merge(data,merge,by="Time",all.x=T)
data$Step_Change_Final[is.na(data$Step_Change_Final)]<-"Staging Proccess"

# COMMAND ----------

# MAGIC %md
# MAGIC # Fetching Weather Data

# COMMAND ----------

# MAGIC %python
# MAGIC # Call Weather API
# MAGIC #startdate = '2019-01-01'
# MAGIC #enddate = '2019-01-01'
# MAGIC import os
# MAGIC from datetime import datetime, timedelta
# MAGIC 
# MAGIC def format_date(d):
# MAGIC   return d[0:4]+'-'+d[4:6]+'-'+d[6:8]
# MAGIC 
# MAGIC historical_data_file = dbutils.widgets.get("filename")
# MAGIC startdate = dbutils.widgets.get("batchdt")
# MAGIC if startdate[8:10] == "00" and not historical_data_file:
# MAGIC   # if its 12 AM fetch previous day weather data as well
# MAGIC   fmt = "%Y%m%d%H"
# MAGIC   startdate = (datetime.strptime(startdate, fmt) - timedelta(1)).strftime(fmt)
# MAGIC 
# MAGIC startdate = format_date(startdate)
# MAGIC enddate = format_date(dbutils.widgets.get("enddate"))
# MAGIC dbutils.notebook.run("weather_api_call", 0,{"startdate": startdate, "enddate": enddate})

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Merging Processed data with Weather Data

# COMMAND ----------

read_weather_data <- function(startdate, enddate) {
  fmt <- "%Y-%m-%d"
  if(startdate == enddate) {
    filename <- paste0("/mnt/asbchillerprocessed/tmp/weather_data/", as.POSIXct(startdate, format=fmt))
    read.df(filename, source="parquet")
  } else {
    a <- as.POSIXct(startdate, format=fmt)
    b <- as.POSIXct(enddate, format=fmt)
    tseq <- seq(from = a, to = b, by = "days")
    days <- lapply(tseq, format, fmt)
    filenames <- paste0("/mnt/asbchillerprocessed/tmp/weather_data/", days)
    spark_dfs <- list()
    for(filename in filenames) {
      sdf <- read.df(filename, source="parquet")
      spark_dfs <- append(spark_dfs, sdf)
    }
    do.call(SparkR::rbind, spark_dfs)
  }
}

# COMMAND ----------

# Reading weather mapping data and weather data from blob storage
format_date <- function(d) {
  paste(substr(d,1,4), substr(d,5,6), substr(d,7,8), sep="-")
}

weather_startdate = startdate
if(substr(weather_startdate, 9, 10) == "00" && historical_data_file == "") {
  # if its 12 AM fetch previous day weather data as well
  fmt = "%Y%m%d%H"
  weather_startdate = format(as.POSIXct(weather_startdate, format=fmt)-3600, fmt)
}

# weather_data_filename <- if(historical_data_file == "") format_date(weather_startdate) else paste(format_date(weather_startdate),  format_date(enddate), sep="_")
weather_mapping<- read.df("/mnt/asbchillermasterdata/weather_mapping.csv", source = "csv", header="true", inferSchema = "true")
weather<- read_weather_data(format_date(weather_startdate), format_date(enddate))

# COMMAND ----------

# Merge Chiller processed data with weather data and
#weather_mapping<-fread("weather_mapping.csv")
weather_mapping<-as.data.frame(weather_mapping)
#weather<-fread("weather_data.csv")
weather<-as.data.frame(weather)
data<-merge(data,weather_mapping,by="Building",all.x=T)

weather$Time<-paste0(substr(weather$Time,1,10)," ",substr(weather$Time,12,19))
weather<-weather[,c("Time","Wetbulbtemp","station_id")]
data<-merge(data,weather,by=c("Time","station_id"),all.x=T)
data$CHL_COMMON_CW_CT_Delta_T=data$CHL_COMMON_CW_COMMON_SWT-data$Wetbulbtemp
colnames(data)[colnames(data)=="Variable Name"] <- "Variable_Name"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Writing to Blob Storage and SQL Data Warehouse

# COMMAND ----------

# Functions to preprocess and write to SQL DWH
round_replace <- function(x) {
  x <- regexp_replace(x, "(NaN|Infinity)", "null")
  bround(x, 3)
}

round_values <- function(df) {
  for (value in dtypes(df)) {
    if(value[2] == 'double') {
      df <- withColumn(df, value[1], round_replace(df[[value[1]]]))
    }
  }
  df
}

jdbc_url <- paste0(Sys.getenv("JDBC_URL"), "user=", Sys.getenv("JDBC_USER"), ";password=", Sys.getenv("JDBC_PASS"))
write_to_sql_dwh <- function(df) {
  write.jdbc(df, jdbc_url, "ASBChiller.ASBChiller_Chiller_Hourly", mode="append")
}

# COMMAND ----------

# write processed data to blob storage
lthrdt <- as.POSIXct(startdate,format="%Y%m%d%H")-3600

if (fileinput_count==2) {
  lthrdata<- filter(data, data$Time<lthrdt)
  lthrdata <- as.DataFrame(lthrdata)
  lthrdata <- round_values(lthrdata)
  write.df(lthrdata,paste0("/mnt/asbchillerprocessed/output/",format(lthrdt, "%Y%m%d%H"),".parquet"), "parquet", "overwrite")
}

cthrdata <- as.data.frame(data)

if(historical_data_file == "") {
  cthrdata<- filter(cthrdata, data$Time>=lthrdt)
}

output_file <- if(historical_data_file == "") startdate else historical_data_file
cthrdata <- as.DataFrame(cthrdata, numPartitions=20)
cthrdata <- round_values(cthrdata)
write.df(cthrdata,paste0("/mnt/asbchillerprocessed/output/",output_file,".parquet"),"parquet", "overwrite")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.{Connection, DriverManager, Timestamp}
# MAGIC import java.time.format.DateTimeFormatter
# MAGIC import java.time.LocalDateTime
# MAGIC 
# MAGIC val startdate = dbutils.widgets.get("batchdt")
# MAGIC val enddate = dbutils.widgets.get("enddate")
# MAGIC val historicalDataFile = dbutils.widgets.get("filename")
# MAGIC 
# MAGIC val processedFile = if(historicalDataFile.isEmpty) {
# MAGIC   val fmt = DateTimeFormatter.ofPattern("yyyyMMddHH")
# MAGIC   LocalDateTime.parse(startdate, fmt).minusHours(1).format(fmt)
# MAGIC } else historicalDataFile
# MAGIC 
# MAGIC 
# MAGIC val df = spark.read.parquet(s"/mnt/asbchillerprocessed/output/${processedFile}.parquet")
# MAGIC val buildings = df.select("Building").distinct.rdd.map(_(0)).collect().filterNot(_ == null)
# MAGIC val minTime = df.sort("Time").first().getAs[Timestamp]("Time")
# MAGIC val maxTime = df.sort($"Time".desc).first().getAs[Timestamp]("Time")
# MAGIC 
# MAGIC val sql = s"""DELETE FROM ASBChiller.ASBChiller_Chiller_Hourly
# MAGIC               WHERE BUILDING IN ('${buildings.mkString("','")}')
# MAGIC               AND [Time] >= '${minTime}' AND [Time] <= '${maxTime}';"""
# MAGIC 
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

if(fileinput_count == 2) {
  write_to_sql_dwh(lthrdata)
} else if(historical_data_file != "") {
  write_to_sql_dwh(cthrdata)
}