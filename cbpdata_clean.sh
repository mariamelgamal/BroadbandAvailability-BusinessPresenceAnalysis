hdfs dfs -rm -r c16 
hdfs dfs -mkdir c16 
hdfs dfs -mkdir c16/code 
hdfs dfs -mkdir c16/input 

hdfs dfs -put CleanReducer.py c16/code
hdfs dfs -put CBPmodMapper.py c16/code 
hdfs dfs -chmod 777 c16/code/*.py
hdfs dfs -put combine16.txt c16/input 
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/me1553/c16/code/CBPmodMapper.py,hdfs://dumbo/user/me1553/c16/code/CleanReducer.py -mapper "python CBPmodMapper.py" -reducer "python CleanReducer.py" -input /user/me1553/c16/input/combine16.txt -output /user/me1553/c16/output

hdfs dfs -cat c16/output/part-00000> CBPData16.txt
hdfs dfs -put CBPData16.txt /user/me1553/business2016_data





hdfs dfs -rm -r c15 
hdfs dfs -mkdir c15 
hdfs dfs -mkdir c15/code 
hdfs dfs -mkdir c15/input 

hdfs dfs -put CleanReducer.py c15/code
hdfs dfs -put CBPmodMapper.py c15/code 
hdfs dfs -chmod 777 c15/code/*.py
hdfs dfs -put combine15.txt c15/input 
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/me1553/c15/code/CBPmodMapper.py,hdfs://dumbo/user/me1553/c15/code/CleanReducer.py -mapper "python CBPmodMapper.py" -reducer "python CleanReducer.py" -input /user/me1553/c15/input/combine15.txt -output /user/me1553/c15/output

hdfs dfs -cat c16/output/part-00000> CBPData15.txt
hdfs dfs -put CBPData15.txt /user/me1553/business2015_data




hdfs dfs -rm -r c17 
hdfs dfs -mkdir c17 
hdfs dfs -mkdir c17/code 
hdfs dfs -mkdir c17/input 

hdfs dfs -put CleanReducer.py c17/code
hdfs dfs -put CBPmodMapper.py c17/code 
hdfs dfs -chmod 777 c17/code/*.py
hdfs dfs -put combine17.txt c17/input 
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=1 -files hdfs://dumbo/user/me1553/c17/code/CBPmodMapper.py,hdfs://dumbo/user/me1553/c17/code/CleanReducer.py -mapper "python CBPmodMapper.py" -reducer "python CleanReducer.py" -input /user/me1553/c17/input/combine17.txt -output /user/me1553/c17/output

hdfs dfs -cat c17/output/part-00000> CBPData17.txt
hdfs dfs -put CBPData17.txt /user/me1553/business2017_data
