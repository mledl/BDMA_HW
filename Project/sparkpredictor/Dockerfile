FROM ubuntu:latest

RUN \
# Update
apt-get update -y && \
# Install Java
apt-get install default-jre -y

ADD ./target/spark-0.1-jar-with-dependencies.jar spark-app.jar

CMD java -jar spark-app.jar
