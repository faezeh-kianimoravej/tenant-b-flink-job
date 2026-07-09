FROM flink:2.2.1-java21

ARG JOB_JAR=target/tenant-b-flink-job-0.1.0-SNAPSHOT.jar

COPY ${JOB_JAR} /opt/flink/usrlib/tenant-b-flink-job.jar
