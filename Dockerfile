FROM maven:3.9.11-eclipse-temurin-11 AS builder

WORKDIR /build

COPY pom.xml pom.xml
RUN mvn -q -DskipTests dependency:go-offline

COPY src src
COPY scripts scripts
RUN mvn -q -DskipTests package && \
    JAR_FILE="$(ls target/stockbot-*.jar | grep -v original | head -n 1)" && \
    cp "$JAR_FILE" /build/stockbot.jar

FROM eclipse-temurin:11-jre

WORKDIR /app

ENV TZ=Asia/Tokyo

RUN mkdir -p /app/outputs

COPY --from=builder /build/stockbot.jar /app/stockbot.jar

ENTRYPOINT ["java", "-jar", "/app/stockbot.jar"]
