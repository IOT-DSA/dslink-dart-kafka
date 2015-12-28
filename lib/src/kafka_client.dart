library dslink.kafka.client;

import 'dart:async';
import 'dart:math' show Random;

import 'package:kafka/kafka.dart';

import 'package:dslink/utils.dart';

class KafkaClient {
  static final Map<String, KafkaClient> _cache = <String, KafkaClient>{};

  ContactPoint _host;
  KafkaSession _session;
  ConsumerGroup _cGroup;
  int _randNum;

  factory KafkaClient(String host, int port) =>
      _cache.putIfAbsent('$host$port', () => new KafkaClient._(host, port));

  KafkaClient._(String host, int port) {
    var seed = new DateTime.now().millisecond ~/ port;
    var rand = new Random(seed);
    _randNum = rand.nextInt(32768);
    _host = new ContactPoint(host, port);
    _session = new KafkaSession([_host]);
    _cGroup = new ConsumerGroup(_session, 'kafkaDSLink$_randNum');
    kafkaLogger.level = logger.level;
    kafkaLogger.onRecord.listen((log) {
      print(log.message);
    });
  }

  KafkaClient update(String host, int port) {
    if (host == _host.host && port == _host.port) return this;
    var curKey = '${_host.host}${_host.port}';
    _session.close();
    _cache.remove(curKey);
    return new KafkaClient(host, port);
  }

  void close() {
    _session.close();
  }

  Stream<String> subscribe(String topic, List partitions) async* {
    var topics = { topic : partitions};
    logger.finest('Subscribing to: $topic');

    print('About to try subscribbing');
    try {
      var consumer = new Consumer(_session, _cGroup, topics, 360, 1);
      await for (MessageEnvelope env in consumer.consume()) {
        var value = new String.fromCharCodes(env.message.value);
        print('Received value: $value');
        env.ack();
        yield value;
      }
    } catch (e) {
      logger.warning('Error subscribing', e);
      rethrow;
    }
  }
}