FROM gradle:5.5.1-jdk8 AS base
WORKDIR /usr/src/app
COPY . .

FROM base AS build
RUN gradle fatJar


FROM openjdk:9 as release
COPY --from=build /usr/src/app/build/libs/aggregator-all-0.0.0-SNAPSHOT.jar /usr/app/aggregator-with-sparkounet.jar
EXPOSE 4040
ENTRYPOINT ["java","-jar","/usr/app/aggregator-with-sparkounet.jar"]
