Comment configurer le projet ?

Commencez par télécharger Kafka, Hadoop, Spark, Java.

Voici mes environnements déjà configurés :
https://we.tl/t-cISGIlbgn7
https://we.tl/t-v7LWNJefMf
https://we.tl/t-uZjWmbm1XO
https://we.tl/t-2asG7jgg8J

Décompressez ces dossiers à la racine du disque C.

Configuration de Kafka

Ouvrez une invite de commande en tant qu'administrateur
cd C:\kafka_2.12-3.7.1\
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

Ouvrez une autre invite de commande en tant qu'administrateur
cd C:\kafka_2.12-3.7.1\
bin\windows\kafka-server-start.bat config\server.properties

Créez le topic Kafka :
Ouvrez une autre invite de commande en tant qu'administrateur.
bin\windows\kafka-topics.bat --create --topic sparkStreaming --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

Configuration de l'IDE

Ouvrez le projet avec IntelliJ.
Il va automatiquement télécharger tous les packages nécessaires.

Configurez le path de jdk-11

![image](https://github.com/user-attachments/assets/437b29c4-5f51-4b24-9c67-a004c50efaef)
![image](https://github.com/user-attachments/assets/59117f69-1409-4923-8dd0-fb93f6eb6fdf)

Configurez les consoles d'exécution pour le Producer et le Consumer.
![image](https://github.com/user-attachments/assets/3c1d1f02-0861-4dcc-9ca8-7664a73a2728)
![image](https://github.com/user-attachments/assets/ae983811-1bb3-42f9-9f0d-e308132f9380)

Test du Producer :

bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sparkStreaming --from-beginning
Cette console affichera les messages reçus par Kafka.

Lancez l'exécution du Producer.
Vous devriez voir plein de lignes s'afficher sur la console. Ce sont toutes les lignes envoyées par le Producer chaque seconde.
![image](https://github.com/user-attachments/assets/4de264b3-bcc2-47af-92b2-0e87b4e20945)

Variables d'environnement système (documentation trouvée sur internet) :

Configurez les variables d'environnement.
Maintenant que nous avons téléchargé et décompressé tous les artefacts, nous devons configurer deux variables d'environnement importantes.

Cliquez sur le bouton Windows et tapez "environment".
![image](https://github.com/user-attachments/assets/13320cec-09ae-42eb-bff6-920a8d156163)

Configurez les variables d'environnement.
Nous configurons la variable d'environnement JAVA_HOME en ajoutant une nouvelle variable d'environnement.

Nom de la variable : JAVA_HOME Valeur de la variable : C:\Java\jdk-11
![image](https://github.com/user-attachments/assets/2dced602-b60d-46eb-827c-e0cfd3ad8ebc)

Faites de même pour la variable d'environnement HADOOP_HOME.

Nom de la variable : HADOOP_HOME Valeur de la variable : C:\Hadoop\hadoop-3.3.0
![image](https://github.com/user-attachments/assets/e4264043-77c0-4219-a828-1ddd533da9b2)

Configuration de la variable d'environnement PATH :
Une fois que nous avons terminé de configurer les deux variables d'environnement ci-dessus, nous devons ajouter les dossiers bin à la variable d'environnement PATH. Cliquez sur Modifier.
![image](https://github.com/user-attachments/assets/02e281df-38b9-4873-8c6c-1f3c9347b662)

Si la variable PATH existe déjà sur votre système, vous pouvez également ajouter manuellement les deux chemins suivants :

%JAVA_HOME%/bin
%HADOOP_HOME%/bin
%SPARK_HOME%/bin
![image](https://github.com/user-attachments/assets/787dab95-935b-4d3c-a56a-89d0f43be2ed)

Vérification de l'installation :
Une fois l'installation terminée, fermez votre fenêtre de terminal et ouvrez-en une nouvelle, puis exécutez la commande suivante pour vérifier :

java -version

Configuration de Hadoop

Ouvrez une invite de commande en tant qu'administrateur :

hdfs namenode -format

%HADOOP_HOME%\sbin\start-dfs.cmd

%HADOOP_HOME%\sbin\start-yarn.cmd

Pour vérifier si c'est bien lancé, utilisez la commande : jps
ou
http://localhost:9870/dfshealth.html#tab-overview
![image](https://github.com/user-attachments/assets/851a751b-79ab-4e82-b86d-3d1b577881da)

hadoop fs -mkdir -p hdfs://localhost:19000/user/sparkStreaming/output/
hadoop fs -mkdir -p hdfs://localhost:19000/user/sparkStreaming/checkpoint/

C:\Windows\System32>hadoop fs -ls hdfs://localhost:19000/user/sparkStreaming/
Found 2 items
drwxr-xr-x   - Jojoc supergroup          0 2024-07-21 13:32 hdfs://localhost:19000/user/sparkStreaming/checkpoint
drwxr-xr-x   - Jojoc supergroup          0 2024-07-21 13:31 hdfs://localhost:19000/user/sparkStreaming/output

C:\Windows\System32>


Test du Consumer :

Exécutez le Producer puis le Consumer.

Vous devriez voir plein de fichiers se créer quand vous rechargez cette page :
http://localhost:9870/explorer.html#/user/sparkStreaming/output

Affichage :

Ouvrez une invite de commande en tant qu'administrateur.
pip install pyspark delta-spark pandas streamlit

Dans le dossier src du projet, lancez cette commande :
python -m streamlit run dashboard.py
