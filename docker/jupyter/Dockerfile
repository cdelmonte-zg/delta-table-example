FROM jupyter/pyspark-notebook:hadoop-3

USER root
COPY requirements.txt .

RUN apt-get update && apt-get install -y curl openjdk-17-jdk  \
    && pip install --no-cache-dir -r requirements.txt && rm requirements.txt \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.5/hadoop-common-3.3.5.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/3.0.0/delta-contribs_2.12-3.0.0.jar \
    && mv hadoop-common-3.3.5.jar /usr/local/spark/jars/ \
    && mv delta-spark_2.12-3.0.0.jar /usr/local/spark/jars/ \
    && mv delta-contribs_2.12-3.0.0.jar /usr/local/spark/jars/ \
    && mv delta-storage-3.0.0.jar /usr/local/spark/jars/
    
ARG NB_USER=jovyan
ARG NB_UID=1000
ARG NB_GID=100

ENV USER ${NB_USER}
ENV HOME /home/${NB_USER}

RUN groupadd -f ${USER} && \
    chown -R ${USER}:${USER} ${HOME}

#TODO: root shouldn't be used, but the certs dir cannot be read (!!)
# USER ${NB_USER}
USER root

RUN export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-contribs_2.12:3.0.0,io.delta:delta-spark_2.12:3.0.0,io.delta:delta-storage:3.0.0,org.apache.hadoop:hadoop-common:3.3.5 pyspark-shell pyspark-shell"
