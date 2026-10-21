FROM eclipse-temurin:21-jdk-alpine

WORKDIR /app

COPY WM_Central/*.java ./src/
COPY WM_Central/lib/*.jar ./lib/

RUN javac -cp "lib/*" -d out src/*.java

ENTRYPOINT ["java", "-cp", "out:lib/*", "WM_Central"]