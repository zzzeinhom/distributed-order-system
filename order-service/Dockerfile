# Stage 1: Build
FROM maven:3.9.9-eclipse-temurin-21-alpine AS builder
WORKDIR /workspace

# Copy pom files first to leverage Docker layer caching
COPY pom.xml .
COPY order-service/pom.xml order-service/
COPY inventory-service/pom.xml inventory-service/
RUN mvn dependency:go-offline -pl order-service -B

# Copy source code and package JAR
COPY order-service/src order-service/src
RUN mvn clean package -pl order-service -DskipTests -B

# Stage 2: Runtime (lightweight & secure)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Run application as a non-root user
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

COPY --from=builder /workspace/order-service/target/*.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
