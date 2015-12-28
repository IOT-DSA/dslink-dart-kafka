library dslink.kafka;

import 'dart:async';

import 'package:dslink/client.dart';

import 'package:dslink_kafka/kafka_node.dart';

Future main(List<String> args) async {
  LinkProvider link;
  link = new LinkProvider(args, 'Kafka-', command: 'run',
      profiles: {
        AddConnection.isType : (String path) => new AddConnection(link, path),
        EditConnection.isType : (String path) => new EditConnection(path),
        RemoveConnection.isType : (String path) => new RemoveConnection(path),
        KafkaNode.isType : (String path) => new KafkaNode(link, path),
        AddTopic.isType : (String path) => new AddTopic(path),
        RemoveTopicNode.isType : (String path) => new RemoveTopicNode(path),
        TopicNode.isType : (String path) => new TopicNode(path),
      }, encodePrettyJson: true);

  link.addNode('/${AddConnection.pathName}', AddConnection.definition());
  link.init();
  await link.connect();
}