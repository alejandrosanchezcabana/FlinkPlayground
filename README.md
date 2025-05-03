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
    │   │               ├── mapper
    │   │               │   └── FieldRemoverMapper.java
    │   │               ├── sinks
    │   │               │   └── disk
    │   │               │       ├── DiskSink.java
    │   │               │       └── DiskSinkWriter.java
    │   │               ├── sources
    │   │               │   ├── disk
    │   │               │   │   ├── DiskSimpleVersionedSerializer.java
    │   │               │   │   ├── DiskSource.java
    │   │               │   │   ├── DiskSourceReader.java
    │   │               │   │   ├── DiskSourceSplit.java
    │   │               │   │   └── DiskSplitEnumerator.java
    │   │               │   └── s3
    │   │               │       ├── S3SimpleVersionedSerializer.java
    │   │               │       ├── S3Source.java
    │   │               │       ├── S3SourceReader.java
    │   │               │       ├── S3SourceSplit.java
    │   │               │       └── S3SplitEnumerator.java
    │   │               └── utils
    │   │                   ├── JSONFileParser.java
    │   │                   └── MarkdownProperties.java
    │   └── resources
    │       ├── application.md
    │       └── default_application.md
    └── test
        ├── java
        │   └── com
        │       └── alex
        │           └── flink
        │               ├── FlinkPlaygroundTest.java
        │               └── utils
        │                   └── MarkdownPropertiesTest.java
        └── resources
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd FlinkPlayground
   ```

2. **Configure AWS Credentials:**
   Update the `src/main/resources/application.md` file to set up all the information.

3. **Build the project:**
   Use Maven to build the project:
   ```
   mvn clean package
   ```

4. **Run the Flink job:**
   Submit the job to your Flink cluster using the following command:
   ```
   flink run target/FlinkPlayground-1.0-SNAPSHOT.jar
   ```

## Usage

You can customize the source and sink configurations in the `application.md` file.

## Dependencies

This project uses the following dependencies:
- Apache Flink
- AWS SDK for Java
- Jackson Databind for JSON parsing

## License

This project is licensed under the Apache License. See the LICENSE file for more details.
