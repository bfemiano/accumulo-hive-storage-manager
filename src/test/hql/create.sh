hive -hiveconf accumulo.instance.id=test \
-hiveconf accumulo.user.name=root \
-hiveconf accumulo.user.pass=password \
-hiveconf accumulo.zookeepers=localhost:2181 \
-f accumulo_create_table.sql
