# Stage 1: Build application JAR
FROM maven:3.9.8-eclipse-temurin-21-alpine AS builder

WORKDIR /build

# Copy POM and resolve dependencies for layer caching
COPY pom.xml .
RUN mvn dependency:go-offline -B || true

# Copy application source code and package executable JAR
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Minimal runtime image
FROM eclipse-temurin:21-jre-alpine AS runtime

# Create unprivileged system user and group
RUN addgroup -S spring && adduser -S spring -G spring

WORKDIR /app

# Copy built executable JAR from builder stage with proper ownership
COPY --from=builder --chown=spring:spring /build/target/event-processor-*.jar /app/app.jar

# Run as non-root user
USER spring:spring

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=30s \
  CMD wget -q --spider http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-Dspring.profiles.active=docker", "-jar", "/app/app.jar"]
