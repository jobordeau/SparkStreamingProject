import streamlit as st
from pyspark.sql import SparkSession
import time

# Créer une session Spark avec les dépendances Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeRealTimeView") \
    .master("local[4]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.2.0,org.apache.hadoop:hadoop-hdfs:3.2.0") \
    .getOrCreate()

delta_table_path = "hdfs://localhost:19000/user/sparkStreaming/output"

# Ajouter un bouton pour arrêter l'actualisation automatique
refresh = st.sidebar.checkbox("Auto-refresh", value=True)

# Ajouter un slider pour définir l'intervalle de rafraîchissement en secondes
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=1, max_value=60, value=5)

# Fonction pour charger et afficher les données
def load_and_display_data():
    # Lire les données de Delta Lake
    df = spark.read.format("delta").load(delta_table_path)

    # Créer une vue temporaire
    df.createOrReplaceTempView("delta_real_time_view")

    # Requête SQL pour obtenir les données
    word_counts = spark.sql("SELECT word, count FROM delta_real_time_view ORDER BY count DESC LIMIT 10")

    # Afficher les données avec Streamlit
    st.title("Word Count Dashboard")
    st.write("Voici le nombre de mots mis à jour en temps réel:")

    # Convertir le DataFrame Spark en Pandas DataFrame
    word_counts_pd = word_counts.toPandas()

    # Afficher le DataFrame sous forme de tableau
    st.dataframe(word_counts_pd)

    # Afficher le DataFrame sous forme de graphique
    st.bar_chart(word_counts_pd.set_index("word"))

# Boucle pour rafraîchir automatiquement les données
while refresh:
    load_and_display_data()
    time.sleep(refresh_interval)
    st.experimental_rerun()

# Charger et afficher les données une fois si le rafraîchissement automatique est désactivé
if not refresh:
    load_and_display_data()
