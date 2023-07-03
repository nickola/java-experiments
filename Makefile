# Do not print commands before executing
.SILENT:

# Targets are not files / directories ("all" - default target, invoked by simply executing "make")
.PHONY: all $(MAKECMDGOALS)

# Settings
URL = http://localhost:8080

# Maven
maven-clean:
	rm -rf ./target

maven-build:
	./mvnw compile package

maven-start:
	java -jar ./target/experiments-0.0.0.jar

# Gradle
gradle-clean:
	rm -rf ./.gradle ./build

gradle-build:
	./gradlew build

gradle-start:
	java -jar ./build/libs/experiments-0.0.0.jar

# Application calls
produce:
	curl ${URL}/produce/key1/value1

consume:
	curl ${URL}/consume
