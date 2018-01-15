# AWS IOT Kafka Sink Connector

The AWS IOT Kafka Sink Connector provides a simple link between a Kafka topic with JSON messages to the [Amazon IOT message broker](https://docs.aws.amazon.com/iot/latest/developerguide/iot-message-broker.html)

Messages must be of the following form:

    {
	  "DeviceId": TheDeviceId,
	  "Message": {  The JSON object }
    }

## Usage

* Setup your credentials according to the [Amazon documentation](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html)

* Run it

	    $ ./bin/connect-standalone.sh config/connect-standalone.properties connect-awsiot.properties


* Push some data to the topic of your choice

        $ kafkacat -P -t 'source.awsiot' -b 127.0.0.1:9092 <<< '{"DeviceId": "XXthisisatest", "Message": {"temperature":12, "humidity": 63}}'


## Build

* Compile

        $ ./gradlew fatJar

    The fat jar, with all the dependencies will be located in the `build/libs/` directory.
