# Hadoop-TFIDF-MapReduce

Program gets Input txts from Hadoop FS. I put these files into /input/localFS folder. Program only gets the tweet text coming after tweet date and account. For every task, there should be an output folder in Hadoop FS. So to start the program, we need to enter input files' location and also 4 output locations. Output txts added to repository.

Initilization command like this:

hadoop jar ./myJars/tfidf.jar /input/localFS /tfidf/task1 /tfidf/task2 /tfidf/task3 /tfidf/task4
