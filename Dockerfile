FROM amazoncorretto:17-alpine
WORKDIR /home/app
COPY dbpopd/build/docker/main/layers/libs /home/app/libs
COPY dbpopd/build/docker/main/layers/classes /home/app/classes
COPY dbpopd/build/docker/main/layers/resources /home/app/resources
COPY dbpopd/build/docker/main/layers/application.jar /home/app/application.jar
COPY dbpopr/build/ /home/app/resources/public
EXPOSE 7104
ENTRYPOINT ["java", "-Dmicronaut.environments=docker", "-jar", "/home/app/application.jar"]
#EXPOSE 5005 7104
#ENTRYPOINT ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005", "-Dmicronaut.environments=docker", "-jar", "/home/app/application.jar"]
