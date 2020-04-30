FROM fabric8/java-centos-openjdk8-jdk:1.6

ADD target/kafka-phoenix-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar /deployments/app.jar

ENV JAVA_OPTIONS="-Dconf=/deployments/config.properties" \
    AB_OFF="disables"

ENTRYPOINT [ "/deployments/run-java.sh" ]