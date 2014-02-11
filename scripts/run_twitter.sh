SCRIPT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_HOME=$SCRIPT_PATH/../

cd $PROJECT_HOME

export SPARK_HOME=../../incubator-spark

export CP=$SPARK_HOME/examples/target/scala-2.10/spark-examples-assembly-0.9.0-incubating-SNAPSHOT.jar
export CP=$CP:target/scala-2.10/matchmaker-spark-process_2.10-1.0.0.jar:target/scala-2.10/matchmaker-spark-process-assembly-1.0.0.jar
export CP=$CP:.

echo 'Config file: ' $1

java -cp "$CP" com.ripjar.spark.job.App -c $1 --start
