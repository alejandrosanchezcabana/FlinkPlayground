# Flink S3 to Disk

This project is an Apache Flink application that reads files from an AWS S3 bucket and writes the contents to the local disk. It serves as a simple example of how to integrate Flink with AWS S3 for data processing.

## Features

- Reads files from AWS S3 using a custom Flink source.
- Supports JSON file parsing.
- Implements a `WatermarkStrategy` for event-time processing.
- Writes processed data to the local disk.

## Project Structure

```
flink-s3-to-disk
├── src
│   ├── main
│   │   ├── java
│   │   │   └── com
│   │   │       └── example
│   │   │           ├── FlinkS3ToDiskJob.java
│   │   │           ├── sources
│   │   │           │   └── S3Source.java
│   │   │           └── sinks
│   │   │               └── DiskSink.java
│   │   └── resources
│   │       └── application.properties
│   └── test
│       ├── java
│       │   └── com
│       │       └── example
│       │           └── FlinkS3ToDiskJobTest.java
│       └── resources
├── pom.xml
└── README.md
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

The application reads files from the specified S3 bucket and writes the contents to the local disk. You can customize the source and sink configurations in the `application.properties` file.

## Dependencies

This project uses the following dependencies:
- Apache Flink
- AWS SDK for Java
- Jackson Databind for JSON parsing

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
