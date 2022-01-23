import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, date, timedelta
import time

redshift_jdbc = "jdbc:redshift://redshift-cluster-weige.cmnyuhfynqj7.cn-northwest-1.redshift.amazonaws.com.cn:5439/dev"
redshift_user = "admin"
redshift_pass = "Demo1234"

args = getResolvedOptions(sys.argv, ['dbuser', 'dbpassword', 'dburl', 'mysqlJdbcS3path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("glue_mysql8", args)

connection_mysql8_options = {
    "url": args['dburl'],
    "dbtable": "order",
    "user": args['dbuser'],
    "password": args['dbpassword'],
    "customJdbcDriverS3Path": args['mysqlJdbcS3path'],
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"}
# 从MySQL中读取数据 
# 如果表中有自增id,或者唯一值数值类递增字段， 可以使用如下方式进行增量同步

df_catalog = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=connection_mysql8_options,
    additional_options = {"jobBookmarkKeys":["order_id"],"jobBookmarkKeysSortOrder":"asc"}
    )

## 如果表中没有有增id,或者唯一值数值类递增字段， 可以使用如下方式进行增量同步
# today_begin = datetime.now().strftime("%Y-%m-%d 00:00:00")
# add_info= {"hashexpression":"create_time >= '" + today_begin + "' AND create_time","hashpartitions":"10"}
# df_catalog = glueContext.create_dynamic_frame.from_options(
#     connection_type="mysql",
#     connection_options=connection_mysql8_options,
#     additional_options = add_info
#     )

# use glue api
# df_filter = Filter.apply(frame = df_catalog, f = lambda x: x["amount"] >=10)

# use spark api
df = df_catalog.toDF()
df = df.filter(df["amount"] >=10)


dyn_df = DynamicFrame.fromDF(df, glueContext, "nested")

wirete_redshift_options = {
    "url": redshift_jdbc,
    "dbtable": "order_dwd",
    "user": redshift_user,
    "password": redshift_pass,
    "redshiftTmpDir": "s3://example-output/redshift_dwd_read/"
}

glueContext.write_dynamic_frame.from_options(
    frame=dyn_df,
    connection_type="redshift",
    connection_options=wirete_redshift_options
)
job.commit()