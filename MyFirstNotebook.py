# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df_json = spark.read.format('json').option('InferSchema',True).option('header',True).option('multiLine',False).load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df = spark.read.format('csv').option('InferSchema', True).option('header', True).load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SchemaChange

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
    Item_Identifier string,
    Item_Weight string,
    Item_Fat_Content string,
    Item_Visibility double,
    Item_Type string,
    Item_MRP double,
    Outlet_Identifier string,
    Outlet_Establishment_Year int,
    Outlet_Size string,
    Outlet_Location_Type string,
    Outlet_Type string,
    Item_Outlet_Sales double
'''


# COMMAND ----------

df = spark.read.format('csv').schema(my_ddl_schema).option('header', True).load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Statement

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT USING COL

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Application

# COMMAND ----------

# MAGIC %md
# MAGIC Scenerio - 1

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenerio-2

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenerio-3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding a Derived Column Using WITHCOLUMN

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 1

# COMMAND ----------

df = df.withColumn('flag',lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('multiply', col('Item_Weight')*col('Item_MRP'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenerio - 3

# COMMAND ----------

df = df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Low Fat','LF'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SOrting

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SOrting Based on Multiple COlumns

# COMMAND ----------

df.sort(['Item_weight','Item_Visibility'], ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP Function 

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

# COMMAND ----------

df.drop(col('Item_Visibility'), col('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset=['Item_Fat_Content']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###String FUnctions

# COMMAND ----------

df.select(initcap(col('Item_Type'))).display()

# COMMAND ----------

df.select(upper(col('Item_Type'))).display()

# COMMAND ----------

df.select(lower(col('Item_Type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DateFunction Operations

# COMMAND ----------

# MAGIC %md
# MAGIC Current Data

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Curr_Date', current_date())

df.display()

# COMMAND ----------

df = df.withColumn('Week_after', date_add('curr_Date', 7))

df.display()

# COMMAND ----------

df = df.withColumn('Week_before', date_sub('curr_Date', 7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DateDiff function

# COMMAND ----------

df= df.withColumn('datediff', datediff('Curr_Date', 'Week_before'))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('datediff',date_format('Week_before', 'dd-MM-yyyy'))

df.display()

# COMMAND ----------

df = df.withColumn('Week_before', date_format('Week_before', 'dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Handling Null Values

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## There are 2 functions to deal with Null values

# COMMAND ----------

df.dropna(subset=['Item_Weight']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.fillna('NotAvailable', subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Split and Index functions

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type', split('Outlet_Type',' '))

# COMMAND ----------

df_exp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAy Contains Function

# COMMAND ----------

df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

df_exp.withColumn('Type1_flag', array_contains('Outlet_Type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### GroupBy Application

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_Price')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### GRoup Concat gunction under Group by clause

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect List

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot Function

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### when-otherwsie function

# COMMAND ----------

df = df.withColumn('veg_flag', when(col('Item_Type')== 'Meat','Non-Veg').otherwise('veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC df.withColumn('veg_exp_flag',when(((col('Veg_Flag') == 'veg') & (col('Item_MRP')<100)),'veg_inexpensive')\
# MAGIC                             .when(((col('Veg_Flag') == 'veg') & (col('Item_MRP') > 100)),'veg_expensive')\
# MAGIC                             .otherwise('Non_veg'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Joins
# MAGIC
# MAGIC df1.join(df2, df1['ID']==df2['ID'], 'inner').display()
# MAGIC
# MAGIC df1.join(df2, df1['ID']==df2['ID'], 'left').display()
# MAGIC
# MAGIC df1.join(df2, df1['ID']==df2['ID'], 'right').display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowno.',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('Rankk',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('DRankk',dense_rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('Total',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Total',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Writing

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Writing Modes
# MAGIC
# MAGIC ## Append
# MAGIC ## Overwrite
# MAGIC ## Error
# MAGIC ## Ignore

# COMMAND ----------

# MAGIC %md .
# MAGIC ### File Format
# MAGIC
# MAGIC ## Parquet = both are columner file formats, metadat stored in the footer of the file. easy to fetch records when using bigdata because the amount of data read is comparetively very less to row-based file formats.
# MAGIC ## Delta file format = only difference is metadata is stored in seperate table we call it as delta log. this is built on top of parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed vs External Tables

# COMMAND ----------


