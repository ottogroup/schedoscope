#!/bin/bash

service ntpd start
service mysqld start
service cloudera-quickstart-init start
/home/cloudera/cloudera-manager --express

python /root/parcel-installer.py

cd /opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hive/scripts/metastore/upgrade/mysql
mysql -u root -pcloudera -e 'DROP DATABASE metastore; CREATE DATABASE metastore'
mysql -u root -pcloudera metastore < hive-schema-1.1.0.mysql.sql

su hdfs -c "hdfs dfs -mkdir /hdp"
su hdfs -c "hdfs dfs -chown -R cloudera:cloudera /hdp"

sudo -u cloudera mkdir -p /home/cloudera/.cache
sudo -u cloudera touch /home/cloudera/.cache/schedoscope_history

sudo -u cloudera git clone https://github.com/ottogroup/schedoscope.git /home/cloudera/schedoscope
cd /home/cloudera/schedoscope/

sudo -u cloudera bash