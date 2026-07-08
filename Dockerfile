FROM flink:1.19.1-scala_2.12-java17

ARG JAR_FILE=target/tenant-b-flink-job-*.jar

COPY ${JAR_FILE} /opt/flink/usrlib/tenant-b-flink-job.jar
