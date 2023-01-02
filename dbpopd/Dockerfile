FROM amazoncorretto:17-alpine
WORKDIR /home/app
COPY build/docker/main/layers/libs /home/app/libs
COPY build/docker/main/layers/classes /home/app/classes
COPY build/docker/main/layers/resources /home/app/resources
COPY build/docker/main/layers/application.jar /home/app/application.jar
EXPOSE 7104
#ENTRYPOINT ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-Dmicronaut.environments=docker", "-jar", "/home/app/application.jar"]
ENTRYPOINT ["java", "-Dmicronaut.environments=docker", "-jar", "/home/app/application.jar"]
