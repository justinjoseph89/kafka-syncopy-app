FROM openjdk:8
ENV APPLICATION_NAME=kafka-syncopy-app
ENV CONFIG="/config/application.yaml"
COPY ./target/kafka-syncopy-app-jar-with-dependencies.jar app/kafka-syncopy-app.jar
COPY ./application.yaml /config/application.yaml
WORKDIR /app
CMD java -jar ./${APPLICATION_NAME}.jar ${CONFIG}