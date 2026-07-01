#!/usr/bin/env bash
set -e

# directorios de datos dentro del contenedor
mkdir -p /data/dfs/name /data/dfs/data

# Config mínima (pseudo)
cat > "$HADOOP_CONF_DIR/core-site.xml" <<'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://hdfs:9000</value>
  </property>
</configuration>
EOF

cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<'EOF'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:/data/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:/data/dfs/data</value>
  </property>
</configuration>
EOF

# Formatear solo si no existe
if [ ! -f /data/dfs/name/current/VERSION ]; then
  hdfs namenode -format -force
fi

# Arranque de demonios en el mismo contenedor
hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs --daemon start secondarynamenode

# Mantener contenedor vivo
mkdir -p /var/log/hadoop
touch /var/log/hadoop/hadoop-root-namenode-hdfs.log \
      /var/log/hadoop/hadoop-root-datanode-hdfs.log \
      /var/log/hadoop/hadoop-root-secondarynamenode-hdfs.log
tail -F /var/log/hadoop/hadoop-root-*-hdfs.log
