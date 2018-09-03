# IMDBTopMovies-HADOOP
Implemented MapReduce to derive top 10 movies from IMDB movie data using Hadoop framework.

Please install Apache Hadoop , Refere the below site for more details
https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

Steps to install and run:
1. Download the project using git commands: $ git clone https://github.com/vasanth-mahendran/weather-data-hadoop.git
2. $ cd weather-data-hadoop 
3. $ mvn install
4. Add the ratings file in Dataset folder to hdfs: hdfs dfs -put /mapreduce/imdb/input/ratings
5. Start the MapReduce application
6. hadoop jar ./target/imdb-0.1.jar /mapreduce/imdb/input/ratings /mapreduce/imdb/output1 /mapreduce/imdb/output2
