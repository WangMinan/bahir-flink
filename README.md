<!--
{% comment %}
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Apache Bahir (Flink)

Apache Bahir provides extensions to distributed analytics platforms such as Apache Spark™ and Apache Flink®.

<http://bahir.apache.org/>


This repository is for Apache Flink extensions.

## Contributing a Flink Connector

The Bahir community is very open to new connector contributions for Apache Flink.

We ask contributors to first open a [JIRA issue](http://issues.apache.org/jira/browse/BAHIR) describing the planned changes. Please make sure to put "Flink Streaming Connector" in the "Component/s" field.

Once the community has agreed that the planned changes are suitable, you can open a pull request at the "bahir-flink" repository.
Please follow the same directory structure as the existing code.

The community will review your changes, giving suggestions how to improve the code until we can merge it to the main repository.



## Building Bahir

Bahir is built using [Apache Maven](http://maven.apache.org/)™.
To build Bahir and its example programs, run:

    mvn -DskipTests clean install

## Running tests

Testing first requires [building Bahir](#building-bahir). Once Bahir is built, tests
can be run using:

    mvn test

## 对于本fork的说明

王旻安正在维护这个fork以使flink-connector-influxdb2的连接器继续可用，目前正在适配的版本为 flink 1.20.0 LTS 与 influxdb 2.7 OSS

王旻安模仿 flink-connector-jdbc 重写了整个InfluxDBSource的逻辑以剥离Telegraf，现在InfluxDBSource会根据时间进行分片从指定的bucket-measurement中拉取数据

王旻安模仿 flink-connector-jdbc 修正了InfluxDBSink的部分逻辑，添加了定时flush数据的功能
