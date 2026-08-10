# Dockerfile
FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /workspace
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN ./mvnw dependency:go-offline -B
COPY src src
RUN ./mvnw package -DskipTests -B

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN addgroup -S workflow && adduser -S workflow -G workflow
COPY --from=build /workspace/target/workflow-orchestrator-1.0.0.jar app.jar
USER workflow
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]