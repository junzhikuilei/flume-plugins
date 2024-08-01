# RollingFileManager

Stores events on the local filesystem. Required properties are in **bold**.

| Property Name                               | Default | Description                                                                                                                                                                                                                 |
|---------------------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **channel**                                 | -       |                                                                                                                                                                                                                             |
| **type**                                    | -       | The component type name, needs to be *xyz.kuilei.flume.sink.RollingFileSink*.                                                                                                                                               |
| **sink.directory**                          | -       | The directory where files will be stored                                                                                                                                                                                    |
| **sink.pathManager**                        | DEFAULT | The PathManager implementation to use, needs to be *xyz.kuilei.flume.formatter.output.RollingPathManager$Builder*.                                                                                                          |
| sink.pathManager.extension                  | -       | The file extension if the default PathManager is used.                                                                                                                                                                      |
| sink.pathManager.prefix                     | -       | A character string to add to the beginning of the file name if the default PathManager is used                                                                                                                              |
| sink.rollInterval                           | 30      | Roll the file every 30 seconds. Specifying 0 will disable rolling and cause all events to be written to a single file.                                                                                                      |
| sink.serializer                             | TEXT    | Other possible options include *avro_event* or the FQCN of an implementation of EventSerializer.Builder interface.                                                                                                          |
| sink.batchSize                              | 100     |                                                                                                                                                                                                                             |
| *sink.pathManager.useCreationTimestamp*     | false   | 1. 为真，正式文件名：$prefix-${**currentTimeMillis**: creationTimestampPattern}-$index.$extension、临时文件名：${正式文件名}.$inUseExtension；<br/>2. 否则，正式文件名：$prefix-${**startupTimeMillis**}-$index.$extension、临时文件名：${正式文件名}.$inUseExtension。 |
| *sink.pathManager.creationTimestampPattern* | -       |                                                                                                                                                                                                                             |
| *sink.pathManager.inUseExtension*           | tmp     |                                                                                                                                                                                                                             |

Example for agent named a1:

```properties
a1.channels=c1
a1.sinks=k1
a1.sinks.k1.type=xyz.kuilei.flume.sink.RollingFileSink
a1.sinks.k1.channel=c1
a1.sinks.k1.sink.directory=/testdata
a1.sinks.k1.pathManager=xyz.kuilei.flume.formatter.output.RollingPathManager$Builder
a1.sinks.k1.sink.pathManager.useCreationTimestamp=true
a1.sinks.k1.sink.pathManager.creationTimestampPattern=yyyyMMddHHmmssSSS
a1.sinks.k1.sink.pathManager.extension=txt
a1.sinks.k1.sink.pathManager.inUseExtension=tmp
```
