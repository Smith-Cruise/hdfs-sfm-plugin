# HDFS Small File Merger-SFM
SFM is designed to replace Har, provide editalbe archive file. Based on adaptive readahead strategy, SFM provide high thourghput than Har or origin HDFS when facing massive small files.

## Env

* Project beased on Hadoop 3.3.0, JDK 1.8

## Compile

SFM did not provide `hadoop-hdfs-client.jar` now, you should compile it by yourself.

1. Clone project
2. Run `mvn clean package`
3. Replace HDFS origin client jar directly
4. Just use it, it is compatible with HDFS API.

## Custom version

If you want to compile SFM for specific Hadoop version, maybe you should change `pom.xml`.

## TODO

The document is preparing....
