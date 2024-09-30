from apache_ranger.model.ranger_service import RangerService
from apache_ranger.client.ranger_client import RangerClient
from json import JSONDecodeError

print(f"create-ranger-services: connecting to ranger admin...")
ranger_client = RangerClient('http://localhost:6080', ('admin', 'Rangeradmin1'))

def service_not_exists(service):
    try:
        svc = ranger_client.get_service(service.name)
    except JSONDecodeError:
        return 1
    return 0 if svc is not None else 1


hdfs = RangerService({'name': 'ut_hdfs', 'type': 'hdfs',
                      'configs': {'username': 'hdfs', 'password': 'hdfs',
                                  'fs.default.name': 'hdfs://ranger-hadoop:9000',
                                  'hadoop.security.authentication': 'simple',
                                  'hadoop.security.authorization': 'true'}})

hive = RangerService({'name': 'ut_hive', 'type': 'hive',
                      'configs': {'username': 'hive', 'password': 'hive',
                                  'jdbc.driverClassName': 'org.apache.hive.jdbc.HiveDriver',
                                  'jdbc.url': 'jdbc:hive2://ranger-hive:10000',
                                  'hadoop.security.authorization': 'true'}})

kafka = RangerService({'name': 'ut_kafka', 'type': 'kafka',
                       'configs': {'username': 'kafka', 'password': 'kafka',
                                   'zookeeper.connect': 'ranger-zk.example.com:2181'}})

yarn = RangerService({'name': 'ut_yarn', 'type': 'yarn',
                      'configs': {'username': 'yarn', 'password': 'yarn',
                                  'yarn.url': 'http://ranger-hadoop:8088'}})

hbase = RangerService({'name': 'ut_hbase', 'type': 'hbase',
                       'configs': {'username': 'hbase', 'password': 'hbase',
                                   'hadoop.security.authentication': 'simple',
                                   'hbase.security.authentication': 'simple',
                                   'hadoop.security.authorization': 'true',
                                   'hbase.zookeeper.property.clientPort': '2181',
                                   'hbase.zookeeper.quorum': 'ranger-zk',
                                   'zookeeper.znode.parent': '/hbase'}})

kms = RangerService({'name': 'ut_kms', 'type': 'kms',
                      'configs': {'username': 'keyadmin', 'password': 'rangerR0cks!',
                                  'provider': 'http://ranger-kms:9292'}})

trino = RangerService({'name': 'ut_trino', 'type': 'trino',
                       'configs': {
                           'username': 'trino',
                           'password': 'trino',
                           'jdbc.driverClassName': 'io.trino.jdbc.TrinoDriver',
                           'jdbc.url': 'jdbc:trino://ranger-trino:8080',
                       }})

services = [hdfs, yarn, hive, hbase, kafka, kms, trino]
for service in services:
    try:
        if service_not_exists(service):
            ranger_client.create_service(service)
            print(f"create-ranger-services: {service.name} service created!")
    except Exception as e:
        print(f"create-ranger-services: An exception occured: {e}")
