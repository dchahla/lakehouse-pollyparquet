FROM maven:3.9.6-eclipse-temurin-11

WORKDIR /build

# Copy Java project
COPY java/ .

# Build fat JAR
RUN mvn clean package -DskipTests -q

# Runtime stage
FROM eclipse-temurin:11-jre

WORKDIR /app

# Copy built JAR (finalName=land-bronze in pom.xml; shade overwrites it in place)
COPY --from=0 /build/target/land-bronze.jar /app/app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
