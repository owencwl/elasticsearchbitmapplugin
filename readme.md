es_bitmap插件开发

maven依赖进行构建打包：

```shell
mvn package
```



插件安装：需要在es集群每台服务器中 进行安装

```shell
#查看plugin
/usr/share/elasticsearch/bin/elasticsearch-plugin list

#删除插件
/usr/share/elasticsearch/bin/elasticsearch-plugin remove bitmap-aggregation-plugin
#安装插件
/usr/share/elasticsearch/bin/elasticsearch-plugin  install "file:///mnt/data1/elasticsearchplugin-1.0-SNAPSHOT.zip"

#安装完插件需要重启es
```















