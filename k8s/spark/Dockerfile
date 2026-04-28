FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir pyspark==3.5.0

ADD https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2.jre11/mssql-jdbc-12.4.2.jre11.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.14/gcs-connector-hadoop3-2.2.14-shaded.jar /opt/spark/jars/

RUN chmod 644 /opt/spark/jars/*.jar

RUN mkdir -p /opt/spark/jobs && chmod 755 /opt/spark/jobs

RUN mkdir -p /tmp/spark-logs && chmod -R 777 /tmp/spark-logs

COPY jobs/*.py /opt/spark/jobs

RUN chmod -R 755 /opt/spark/jobs

USER spark

WORKDIR /opt/spark/jobs
