# kafka-project

用StructuredStreaming实现项目流程
共两个项目模块

tokafka项目模块是负责写数据

structured_streaming模块负责消费数据 

只有 微批处理和 持续处理 两个类

所有的逻辑都在MicroBatchProcessingApplicaitonRunnerImpl这个类里面，也就是微批处理

持续处理只是展示了怎么配置


