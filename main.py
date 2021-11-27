from datetime import date

from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, desc, col, current_date, add_months, unix_timestamp

spark = SparkSession \
    .builder \
    .appName("tp_github_history") \
    .master("local[*]") \
    .getOrCreate()

mnm_file = "data/full.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file)

def exo1():
    print("| exo 1 |")
    # tokenizer = Tokenizer(inputCol="message", outputCol="tokens")
    # tokens = tokenizer.transform(mnm_df)
    # tokens.show(truncate=False)

    # mnm_df.select("message").filter("message is not NULL").show(truncate=False)

    # df.groupby("repo").count().filter("repo is not NULL").sort(desc("count")).limit(10).show(truncate=False)

    return input("Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")

def exo2():
    print("| exo 2 |")


    # df.groupby("repo", "author").count().filter("repo == 'apache/spark'").sort(desc("count")).limit(1).show(truncate=False)

    return input("Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")

def exo3():
    # La request :
    print("| exo 3 |")

    today = date.today()
    # print("Today's date:", today)
    # current_timestamp
    # df.show(truncate=False)
    # df.groupby("repo", "author", "date")\
    #     .count()\
    #     .filter("repo == 'apache/spark'")\
    #     .sort(unix_timestamp(col("date"), "MMM d hh:mm:ss yyyy Z").cast("timestamp")).show(truncate=False)

    df.select("repo", "author", "date")\
        .where(col("repo") == "apache/spark")\
        .sort(desc(unix_timestamp(col("date"), "MMM d hh:mm:ss yyyy Z").cast("timestamp"))).show(truncate=False)

    return input("Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")

def exo4():
    print("| exo 4 |")



    return

def exos():
    if exo1() == 0:
        exit()

    if exo2() == 0:
        exit()

    if exo3() == 0:
        exit()

    if exo4() == 0:
        exit()

    input("input de fin")
    return

if __name__ == '__main__':
    exos()

