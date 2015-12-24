library dslink.kafka.client;

import 'dart:math' show Random;

import 'package:kafka/kafka.dart';

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
  }

  KafkaClient update(String host, int port) {
    if (host == _host.host && port == _host.port) return this;
    var curKey = '${_host.host}${_host.port}';
    _session.close();
    _cache.remove(curKey);
    return new KafkaClient(host, port);
  }
}