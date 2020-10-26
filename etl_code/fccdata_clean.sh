hdfs dfs -mkdir broadband2017
hdfs dfs -mkdir broadband2017/code
hdfs dfs -mkdir broadband2017/input
hdfs dfs -put *.py broadband2017/code
hdfs dfs -chmod 777 broadband2017/code/*.py
hdfs dfs -put broadband2017.csv broadband2017/input
hdfs dfs -ls broadband2017/code
hdfs dfs -ls broadband2017/input

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/sp4494/broadband2017/code/Mapper.py,hdfs://dumbo/user/sp4494/broadband2017/code/Reducer.py -mapper "python Mapper.py" -reducer "python Reducer.py" -input /user/sp4494/broadband2017/input/broadband17.csv -output /user/sp4494/broadband2017/output

hdfs dfs -cat broadband2017/output/part-00000 > clean_broadband17.txt
hdfs dfs -put clean_broadband17.txt /user/sp4494/clean_broadband17_data



hdfs dfs -mkdir broadband2016
hdfs dfs -mkdir broadband2016/code
hdfs dfs -mkdir broadband2016/input
hdfs dfs -put *.py broadband2016/code
hdfs dfs -chmod 777 broadband2016/code/*.py
hdfs dfs -put broadband2016.csv broadband2016/input
hdfs dfs -ls broadband2016/code
hdfs dfs -ls broadband2016/input

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/sp4494/broadband2016/code/Mapper.py,hdfs://dumbo/user/sp4494/broadband2016/code/Reducer.py -mapper "python Mapper.py" -reducer "python Reducer.py" -input /user/sp4494/broadband2016/input/broadband16.csv -output /user/sp4494/broadband2016/output

hdfs dfs -cat broadband2016/output/part-00000 > clean_broadband16.txt
hdfs dfs -put clean_broadband16.txt /user/sp4494/clean_broadband16_data




hdfs dfs -mkdir broadband2015
hdfs dfs -mkdir broadband2015/code
hdfs dfs -mkdir broadband2015/input
hdfs dfs -put *.py broadband2015/code
hdfs dfs -chmod 777 broadband2015/code/*.py
hdfs dfs -put broadband2015.csv broadband2015/input
hdfs dfs -ls broadband2015/code
hdfs dfs -ls broadband2015/input

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/sp4494/broadband2015/code/Mapper.py,hdfs://dumbo/user/sp4494/broadband2015/code/Reducer.py -mapper "python Mapper.py" -reducer "python Reducer.py" -input /user/sp4494/broadband2015/input/broadband15.csv -output /user/sp4494/broadband2015/output

hdfs dfs -cat broadband2015/output/part-00000 > clean_broadband15.txt
hdfs dfs -put clean_broadband15.txt /user/sp4494/clean_broadband15_data