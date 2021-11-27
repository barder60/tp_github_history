from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, desc, col

spark = SparkSession \
    .builder \
    .appName("tp_github_history") \
    .master("local[*]") \
    .getOrCreate()

mnm_file = "data/full.csv"

mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file)

def exo1():
    print("| exo 1 |")

    mnm_df.groupby("repo").count().filter("repo is not NULL").sort(desc("count")).limit(10).show(truncate=False)

    return input("Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")

def exo2():
    print("| exo 2 |")

    mnm_df.groupby("repo", "author").count().filter("repo == 'apache/spark'").sort(desc("count")).limit(1).show(truncate=False)

    return input("Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")

def exo3():
    # La request :
    print("| exo 3 |")

    # TODO : print|show la request

    return input("Souhaitez-vous avancer à l excerice suivant ? (0 pour exit | n importe quelle touche pour continuer) : ")

def exo4():
    print("| exo 4 |")

    # TODO : print|show la request

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

