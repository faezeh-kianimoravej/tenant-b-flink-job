FROM flink:2.2.1-scala_2.12-java21

ARG JAR_PATH=target/tenant-b-flink-job-0.1.0-SNAPSHOT.jar

COPY ${JAR_PATH} /opt/flink/usrlib/tenant-b-flink-job.jar
