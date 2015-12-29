# Kafka DSLink

Dart DSLink for Kafka. This link will connect with Kafka servers 0.8.2 and
up. There are no current plans to support Kafka 0.7.x or older.

## Requirements

This link requires Dart version 1.12.0 and up. This may require updating your
DG Lux server to a newer version by visiting the
[Download DSA](http://iot-dsa.org/get-started/download-dsa) page. (At this time,
updating via the DGLux Update Server will not upgrade the installed Dart SDK.)

### Kafka Server

For this link to work correctly, it is advised that you ensure the Kafka server
has the appropriate configuration for the `advertised.host.name` and
`advertised.port` values.

### Note

If subscribing to an existing topic whose log files have expired and been cleared,
the link will occasionally send an initial, empty message to the topic to
resolve and issue with the Kafka server being unable to locate the latest
offset point.