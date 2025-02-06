# mapreduce
Cách chạy chương trình mapreduce:

cd mapreduce/RatingItem

hadoop fs -put contest.data /RatingItem/input

javac -classpath $(hadoop classpath) -d out RatingJob.java RatingMapper.java RatingReducer.java

jar -cvf RatingJob.jar -C out/ .

hadoop jar RatingJob.jar RatingJob /RatingItem/input /RatingItem/output

hadoop fs -cat /RatingItem/output/*
