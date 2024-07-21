Comment setup le projet 

Commencez par télécharger kafka, hadoop, spark, java

Voici mes environnement déjà setup :
https://we.tl/t-cISGIlbgn7
https://we.tl/t-v7LWNJefMf
https://we.tl/t-uZjWmbm1XO
https://we.tl/t-2asG7jgg8J

Décompressz ces dossiers à la racine du disque C


Setup kafka

Ouvrez une invite de commande en tant qu'administrateur
cd C:\kafka_2.12-3.7.1\
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

Ouvrez une autre invite de commande en tant qu'administrateur
cd C:\kafka_2.12-3.7.1\
bin\windows\kafka-server-start.bat config\server.properties

Setup IDE

Ouvrez le projet avec intellij
Il va automatiquement télécharger tous les packages nécessaires

Configurez le path de jdk-11 

![image](https://github.com/user-attachments/assets/437b29c4-5f51-4b24-9c67-a004c50efaef)
![image](https://github.com/user-attachments/assets/59117f69-1409-4923-8dd0-fb93f6eb6fdf)


Configurez les consoles d'exécution pour le Producer et le Consumer
![image](https://github.com/user-attachments/assets/3c1d1f02-0861-4dcc-9ca8-7664a73a2728)
![image](https://github.com/user-attachments/assets/ae983811-1bb3-42f9-9f0d-e308132f9380)

Test du Producer :

Créez le topic kafka :
Ouvrez une autre invite de commande en tant qu'administrateur
bin\windows\kafka-topics.bat --create --topic sparkStreaming --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
Cette console d'afficher les message reçu par kafka

Lancez l'exécution de Producer
Vous devriez voir plein de ligne s'afficher sur la console. Ce sont toutes les lignes envoyé par le producer chaque seconde
![image](https://github.com/user-attachments/assets/4de264b3-bcc2-47af-92b2-0e87b4e20945)


Variable d'environnement système (documentation que j'ai trouvé sur internet):

Configure environment variables
Now we've downloaded and unpacked all the artefacts we need to configure two important environment variables.

First you click the windows button and type environment

![image](https://github.com/user-attachments/assets/13320cec-09ae-42eb-bff6-920a8d156163)

Configure Environment variables
We configure JAVA_HOME environment variable

by adding new environment variable.

Variable name : JAVA_HOME Variable value: C:\Java\jdk-11

![image](https://github.com/user-attachments/assets/2dced602-b60d-46eb-827c-e0cfd3ad8ebc)

the same with HADOOP_HOME environment variable

Variable name : HADOOP_HOME Variable value: C:\Hadoop\hadoop-3.3.0

![image](https://github.com/user-attachments/assets/e4264043-77c0-4219-a828-1ddd533da9b2)

b) Configure PATH environment variable
Once we finish setting up the above two environment variables, we need to add the bin folders to the PATH environment variable. We click on Edit

![image](https://github.com/user-attachments/assets/02e281df-38b9-4873-8c6c-1f3c9347b662)

If PATH environment exists in your system, you can also manually add the following two paths to it:

%JAVA_HOME%/bin
%HADOOP_HOME%/bin
%SPARK_HOME%/bin
![image](https://github.com/user-attachments/assets/787dab95-935b-4d3c-a56a-89d0f43be2ed)

Verification of Installation
Once you complete the installation, Close your terminal window and open a new one and please run the following command to verify:

java -version


Setup Hadoop

Ouvrez une invite de commande en tant qu'administrateur :

hdfs namenode -format

%HADOOP_HOME%\sbin\start-dfs.cmd

%HADOOP_HOME%\sbin\start-yarn.cmd

Pour vérfier si c'est bien lancez utilsez la commande : jps
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

Setup Cluster 
Ouvrez une invite de commande en tant qu'administrateur
java -cp "C:\spark-3.5.1-bin-hadoop3\jars\*" org.apache.spark.deploy.master.Master
![image](https://github.com/user-attachments/assets/dd933375-ac70-4a8a-9181-80f4bf7acb25)

Pour lancez le worker vous devez utiliser l'adresse de votre master. Ici c'est spark://192.168.1.146:7077 mais pour vous ce sera différent
java -cp "C:\spark-3.5.1-bin-hadoop3\jars\*" org.apache.spark.deploy.worker.Worker spark://192.168.1.146:7077

Vérifiez que le worker est bien lancé
![image](https://github.com/user-attachments/assets/be4cdfdd-e2a7-4bf1-af9c-f165866f5d78)


Test Consumer:

Exécutez le Producer puis le Consumer

Vous devriez voir plein de fichiers se créer quand vous rechargez cette page
http://localhost:9870/explorer.html#/user/sparkStreaming/output


Affichage :

Ouvrez une invite de commande en tant qu'administrateur
pip install pyspark delta-spark pandas streamlit

Dans le dossier src du projet lancez cette commande :
pip install pyspark delta-spark pandas setuptools streamlit










