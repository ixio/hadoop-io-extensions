# ODE Hadoop IO Extensions

Library providing Hadoop InputFormats for file types of interest
for the ODE project.

## Pre-requisites

Make sure you have java 8 and maven installed.

### Debian / Unbuntu

```sh
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install openjdk-8-jdk maven
```

## Usage

In the *hadoop-io-extensions* directory, use maven to compile and test:

```sh
mvn compile
mvn test
```

First run can be long as it will download all needed dependencies.
