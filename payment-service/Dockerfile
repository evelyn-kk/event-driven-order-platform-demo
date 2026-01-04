FROM eclipse-temurin:17-jre

# Create a non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

# Copy the fat jar produced by Maven
COPY target/*-SNAPSHOT.jar app.jar

# Set proper ownership
RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8083

ENTRYPOINT ["java", "-jar", "app.jar"]
