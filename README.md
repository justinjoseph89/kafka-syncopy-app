![Maven Build](https://github.com/justinjoseph89/kafka-syncopy-app/workflows/Maven%20Build/badge.svg)
![Docker Build](https://github.com/justinjoseph89/kafka-syncopy-app/workflows/Docker%20Build/badge.svg)
# kafka-syncopy-app
This application will mirror topics from one cluster to another cluster in a time based synchronization.

## How to configure the application
1. Read throgh the [application yaml](https://github.com/justinjoseph89/kafka-syncopy-app/blob/master/application.yaml) file.
    * ``` bootstrapServers``` should be your source cluster, from you want to copy the topics.
    * ``` bootstrapServersTarget``` should be your target cluster, to copy the source topics .
    * ``` zookeeperHost``` your source cluster zookeeper host name. No need to provide the port unless it is other than ```2181```
    * ``` defaultKeySerde``` provide you key serde ```String, Integer, Avro``` etc. 
    * ``` defaultValueSerde``` provide you value serde. Currently it will only supports ```Avro```
    * ``` appVersion``` Your application version. Do not change this untill if you need to start the copy process from the beginning.
    * ``` zkNodeUpd``` Set it to true if you are running for the first time. if it is false this will use the previously commmited the records.
    * ``` appDeltaValue``` the value that needs to be added towards the maxTime of the records inorder to proceed the consumer further
    * ``` appSmallDeltaValue``` this is the value you want the performance of the application to be achieved. It should always less than ``` appDeltaValue```
    * ``` appSleepTimeMs``` this is the time in millie second it should wait for next retry to send the message.
    * ``` autoOffsetReset``` there will not be any necessary to change this from its default ```earliest```.
    * ``` schemaRegistyUrl``` schema registry url.
    * ``` numConsumerThreads``` number of threads you want to run in a single container.
    * ``` topics.input-topics``` comma seperated topic list, that needs to be mirrored from sorce topic.
    * ``` topicsFields.topic-name``` in this you should provide the all topic names that you need to mirror with its field name to be considered as the copy process base.
 ### Note
 ``` topicsFields.topic-name``` this field should be provided as I can find the field inside the record. For example if the record looks like this,
          ``` {data: 
                  {
                  field1:value1,
                  field2:value2
                  } 
                } ```
                then this fieled should be specified as ``` data, field1``` 
  
    
    
## How to run this
1. Create fork or clone the project
2. Run maven command ```mvn clean compile assembly:single```, this will create a jar file kafka-syncopy-app-jar-with-dependencies.jar in the target folder.
3. Build the docker image of this project by following steps.
    * login into your docker hub repo. ``` docker login --username=your_dockerhub_username``` 
    * build the docker image and tag ``` docker build -t reponame/kafka-scopy-app:tag .```
    * push the docker image to the container registry or docker hub repo``` docker push reponame/kafka-scopy-app:tag``` 
    * now you can run this docker image by giving  ```docker run -d  --name=kafka-scopy-app   reponame/kafka-scopy-app:tag```
