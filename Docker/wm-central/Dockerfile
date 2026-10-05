FROM eclipse-temurin:21-jdk-alpine

WORKDIR /app

COPY src/*.java ./src/
COPY lib/*.jar ./lib/


RUN javac -cp "lib/*" -d out src/*.java

ENTRYPOINT ["java", "-cp", "out:lib/*", "WM_Central"]