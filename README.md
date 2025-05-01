# Flink Playgorund

This project is an Apache Flink application. It serves as a simple example of how to integrate Flink for data processing.

## Project Structure

```
Flink Playground
├── LICENSE
├── README.md
├── pom.xml
└── src
    ├── main
    │   ├── java
    │   │   └── com
    │   │       └── alex
    │   │           └── flink
    │   │               ├── FlinkPlayground.java
    │   │               ├── sinks
    │   │               │   ├── DiskSink.java
    │   │               │   └── DiskSinkWriter.java
    │   │               └── sources
    │   │                   ├── S3SimpleVersionedSerializer.java
    │   │                   ├── S3Source.java
    │   │                   ├── S3SourceReader.java
    │   │                   ├── S3SourceSplit.java
    │   │                   └── S3SplitEnumerator.java
    │   └── resources
    │       └── application.properties
    └── test
        ├── java
        │   └── com
        │       └── alex
        │           └── flink
        │               └── FlinkPlaygroundTest.java
        └── resources
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd flink-s3-to-disk
   ```

2. **Configure AWS Credentials:**
   Update the `src/main/resources/application.properties` file with your AWS credentials and S3 bucket information.

3. **Build the project:**
   Use Maven to build the project:
   ```
   mvn clean package
   ```

4. **Run the Flink job:**
   Submit the job to your Flink cluster using the following command:
   ```
   flink run target/flink-s3-to-disk-1.0-SNAPSHOT.jar
   ```

## Usage

You can customize the source and sink configurations in the `application.properties` file.

## Dependencies

This project uses the following dependencies:
- Apache Flink
- AWS SDK for Java
- Jackson Databind for JSON parsing

## License

This project is licensed under the Apache License. See the LICENSE file for more details.
