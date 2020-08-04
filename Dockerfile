# Copyright (c) HyperCloud Development Team.
# Distributed under the terms of the Modified BSD License.

# Global Arguments
ARG build_image=gradle:6.0.1-jdk8
ARG base_image=openjdk:8-jdk

FROM $build_image AS build

LABEL maintainer="Junxiang Wei <kevinprotoss.wei@gmail.com>"

COPY --chown=gradle:gradle . /home/gradle/workspace
WORKDIR /home/gradle/workspace
RUN gradle build copy bootJar --no-daemon

FROM $base_image

ARG postman_version=0.3.1

ENV LD_LIBRARY_PATH /app
ENV JASYPT_ENCRYPTOR_PASSWORD default

COPY --from=build /home/gradle/workspace/build/distributions/* /tmp/
RUN unzip /tmp/postman-boot-$postman_version.zip
RUN mkdir /app
RUN mv postman-boot-$postman_version/lib/postman-$postman_version.jar /app/postman.jar
RUN mv /tmp/*.so /app/

ENTRYPOINT ["java", "-jar", "/app/postman.jar"]
