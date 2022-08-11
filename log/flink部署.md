## 1、flink部署

部署版本   flink1.13.2

## 2、flink部署模式

### 2.1  会话模式
启动集群，保持会话，客户端提交作业。资源有限，竞争资源。  
集群的生命周期超过任何作业之上。

taskSlots 固定  
作业小，执行时间短大量作业

### 2.2  单作业模式
为每一个作业启动一个集群，严格一对一，集群为作业工作。  
作业执行完成，集群就会关闭，资源就会释放。

### 2.3 应用模式
作业直接提交到JobManager，也就是创建一个集群。  
JobManager就为这个应用工作，执行任务之后就关闭。  

解决客户端占用带宽问题

### 2.3 独立模式
相当于会话模式


### 2.4 应用模式部署
jar包放在lib目录下，自动扫描jar包  
standalone-job.sh
taskManager.sh

### 2.5 YARN模式部署

客户端把Flink应用提交给YARN的ResourceManager,YARN的ResourceManage会想YARN的NOdeManager申容器，
在这些容器上，Flink会部署JobManager和TaskManager实例，从而启动集群。Flink会根据运行在JobManager上的任务自动分配TaskManager。



需要先启动Hadoop集群，1.8自动含有Hadoop，之后的版本需要手动下载Hadoop部署。



