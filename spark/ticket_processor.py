from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, count, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Configuration du Cluster Spark avec dépendance Kafka
# On ajuste la mémoire pour la performance (Point de vigilance)
spark = SparkSession.builder \
    .appName("InduTech_Ticket_Analysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Définition du schéma des données entrantes (doit matcher le producteur)
schema = StructType([
    StructField("ticket_id", IntegerType(), True),
    StructField("client_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("demande", StringType(), True),
    StructField("type_demande", StringType(), True),
    StructField("priorite", StringType(), True)
])

# 3. Lecture du flux Redpanda
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Conversion du binaire Kafka en colonnes lisibles
df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. Transformations et Insights
# Transformation : Assignation automatique d'une équipe de support
df_transformed = df_json.withColumn("equipe_assignee", 
    when(col("type_demande") == "Technique", "Equipe_Alpha_IT")
    .when(col("type_demande") == "Facturation", "Equipe_Finance")
    .when(col("type_demande") == "Commercial", "Equipe_Sales")
    .otherwise("Support_General")
)

# Agrégation : Calcul du nombre de tickets par type (Insight)
df_insights = df_transformed.groupBy("type_demande").count()

# 5. Affichage des résultats dans la console (POC)
query_insights = df_insights.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

# Affichage des tickets transformés
query_transformed = df_transformed.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

# 6. Exportation des données transformées vers le format Parquet
query_export = df_transformed.writeStream \
    .format("parquet") \
    .option("path", "/app/export/tickets_processed") \
    .option("checkpointLocation", "/app/checkpoints/tickets") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

# ON ATTEND LA FIN DE TOUTES LES REQUÊTES ICI (À LA FIN)
spark.streams.awaitAnyTermination()