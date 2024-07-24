import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import time

# Désactiver l'animation de rafraîchissement
st.markdown("""
    <style>
        .st-ck {
            animation: none !important;
        }
    </style>
""", unsafe_allow_html=True)

# Créer une session Spark avec les dépendances Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeRealTimeView") \
    .master("local[4]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.2.0,org.apache.hadoop:hadoop-hdfs:3.2.0") \
    .getOrCreate()

delta_table_path = "hdfs://localhost:19000/user/sparkStreaming/output"
monthly_count_path = "hdfs://localhost:19000/user/sparkStreaming/monthlyCount"
global_stats_path = "hdfs://localhost:19000/user/sparkStreaming/globalStats"
user_stats_path = "hdfs://localhost:19000/user/sparkStreaming/userStats"
member_csv_path = "main/resources/members.csv"

# Charger le fichier member.csv
members_df = spark.read.option("header", "true").option("sep", ";").csv(member_csv_path)
members_df.createOrReplaceTempView("members_view")

# Ajouter un bouton pour arrêter l'actualisation automatique
refresh = st.sidebar.checkbox("Auto-refresh", value=True)

# Ajouter un slider pour définir l'intervalle de rafraîchissement en secondes
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=1, max_value=60, value=5)

# Fonction pour charger et afficher les données
def load_and_display_data(selected_year=None):
    # Lire les données de Delta Lake
    df = spark.read.format("delta").load(delta_table_path)
    monthly_df = spark.read.format("delta").load(monthly_count_path)
    user_stats_df = spark.read.format("delta").load(user_stats_path)

    # Créer des vues temporaires
    df.createOrReplaceTempView("delta_real_time_view")
    monthly_df.createOrReplaceTempView("monthly_count_view")
    user_stats_df.createOrReplaceTempView("user_stats_view")

    # Requête SQL pour obtenir les données
    word_counts = spark.sql("SELECT word, count FROM delta_real_time_view ORDER BY count DESC LIMIT 10")
    monthly_counts = spark.sql("""
        SELECT
            split(Month, '-')[0] as Year,
            split(Month, '-')[1] as Month,
            count
        FROM monthly_count_view
        ORDER BY Year, Month ASC
    """)
    user_stats = spark.sql("""
        SELECT 
            COALESCE(Nickname, `Member Name`, UserID) as User, 
            count
        FROM user_stats_view 
        LEFT JOIN members_view 
        ON user_stats_view.UserID = members_view.`Member ID`
        ORDER BY count DESC LIMIT 3
    """)


    # Convertir les DataFrames Spark en Pandas DataFrames
    word_counts_pd = word_counts.toPandas()
    monthly_counts_pd = monthly_counts.toPandas()
    user_stats_pd = user_stats.toPandas()

    # Générer un DataFrame avec tous les mois de 1 à 12 pour chaque année
    all_months = pd.DataFrame({
        'Month': [str(i).zfill(2) for i in range(1, 13)]
    })

    # Afficher les données avec Streamlit
    st.title("Discord Dashboard")


    # Afficher le DataFrame sous forme de tableau
    st.write("TOP 10 des mots les plus utilisés")
    st.dataframe(word_counts_pd)

    # Afficher le top 3 des utilisateurs
    st.write("TOP 3 des utilisateurs ayant envoyé le plus de messages")
    st.dataframe(user_stats_pd)

    # Sélectionner une année
    years = monthly_counts_pd['Year'].unique()
    selected_year = st.sidebar.selectbox("Sélectionnez l'année", options=years, index=len(years) - 1)

    if selected_year:
        yearly_data = monthly_counts_pd[monthly_counts_pd['Year'] == selected_year]
        yearly_data = yearly_data.merge(all_months, on='Month', how='right').fillna({'count': 0})
        yearly_data = yearly_data.sort_values(by='Month')
        st.write(f"Nombre de messages par mois pour l'année {selected_year}")
        st.line_chart(yearly_data.set_index('Month')['count'])

# Boucle pour rafraîchir automatiquement les données
while refresh:
    load_and_display_data()
    time.sleep(refresh_interval)
    st.experimental_rerun()

# Charger et afficher les données une fois si le rafraîchissement automatique est désactivé
if not refresh:
    load_and_display_data()
