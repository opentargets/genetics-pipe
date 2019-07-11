FROM openjdk:8u212-b04-jdk-stretch as build_section

ENV SBT_VERSION 1.2.8

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get -y install sbt && \
  sbt sbtVersion

# Pull sbt and all dependencies first
COPY project /pipe/project
COPY build.sbt /pipe/
WORKDIR /pipe
RUN sbt update

# Assemble the jar file
COPY ./ /pipe/
RUN sbt assembly

FROM openjdk:8u212-b04-jre-stretch
COPY --from=build_section /pipe/target/scala-*/ot-geckopipe-assembly-*.jar /ot-geckopipe.jar
CMD ["java", "-jar", "ot-geckopipe.jar"]
