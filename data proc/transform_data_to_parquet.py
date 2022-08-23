from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, IntegerType

def transform_data(spark):
    ## read data
    df = spark.read.csv("gs://raw-youtube-twoset/raw_youtube_csv.csv", header = True)

    ## remove T and Z in publish
    df_new = df.withColumn("publish", F.regexp_replace("publish", 'T', " "))\
                .withColumn("publish", F.regexp_replace("publish", 'Z', ""))

    ## split date and time
    df_transform = df_new.withColumn("publish", df_new['publish'].cast(TimestampType()))
    df_transformed = df_transform.withColumn("publish_date", F.to_date(F.col("publish")))\
                        .withColumn("publish_hour", F.hour("publish"))\
                        .drop("publish")

    ## change en > EN
    df_final = df_transformed.withColumn("language", \
                                F.expr(""" CASE WHEN language = 'en' """ +
                                        """ THEN 'EN' """ +
                                        """ ELSE 'None' END"""))

    ## change category
    df_list = spark.read.csv("gs://raw-youtube-twoset/youtube_category_list.csv", header=True)
    df_list = df_list.withColumnRenamed('title', 'yt_category')
    df_full = df_final.join(df_list, df_final.category == df_list.id, 'left')\
            .drop("category", "id")

    ## change dtypes
    df_out = df_full.withColumn("view_count", df_full['view'].cast(IntegerType()))\
                    .withColumn("like_count", df_full['like'].cast(IntegerType()))\
                    .withColumn("comment_count", df_full['comments'].cast(IntegerType()))\
                    .drop("view", "like", "comments")

    ## write to gcs
    df_out.coalesce(1).write.parquet("gs://processed-youtube-twoset/result_youtube_parquet.parquet")

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DE Twoset").getOrCreate()
    transform_data(spark)

    spark.stop()