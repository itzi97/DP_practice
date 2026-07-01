FROM apache/hadoop:3.3.6

USER root

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH

COPY start-hdfs.sh /usr/local/bin/start-hdfs.sh
RUN chmod 0755 /usr/local/bin/start-hdfs.sh

EXPOSE 9870 9000 9864 9868
CMD ["/usr/local/bin/start-hdfs.sh"]
