from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

def transform_data(spark):
    ## read data
    df = spark.read.csv("gs://raw-youtube-twoset/raw_youtube_csv.csv", header = True)

    ## remove T and Z in publish
    df_new = df.withColumn("publish", F.regexp_replace("publish", 'T', " "))\
                .withColumn("publish", F.regexp_replace("publish", 'Z', ""))

    ## split date and time
    split_column = F.split("publish", ' ')
    df_transform = df_new.withColumn("publish_date", split_column.getItem(0))\
                    .withColumn("publish_time", split_column.getItem(1))\
                    .withColumn("publish", df_new['publish'].cast(TimestampType()))
    df_transformed = df_transform.withColumn("hour", F.hour("publish")).drop("publish")
 
    ## change category
    df_list = spark.read.csv("gs://raw-youtube-twoset/youtube_category_list.csv", header=True)
    df_list = df_list.withColumnRenamed('title', 'yt_category')
    df_full = df_transformed.join(df_list, df_transformed.category == df_list.id, 'left')
            
    df_full_2 = df_full.drop("category", "id")

    ## change en > EN
    df_final = df_full_2.withColumn("lang", \
                                F.expr(""" CASE WHEN language = 'en' """ +
                                        """ THEN 'EN' """ +
                                        """ ELSE 'None' END"""))
    
    df_final_out = df_final.drop("language")

    ## write to gcs
    df_final_out.coalesce(1).write.csv("gs://processed-youtube-twoset/result_youtube_csv.csv", header = True)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DE Twoset").getOrCreate()
    transform_data(spark)

    spark.stop()