library dslink.kafka.client;

import 'dart:async';
import 'dart:math' show Random;

import 'package:kafka/kafka.dart';

import 'package:dslink/utils.dart' show logger;

class KafkaClient {
  static final Map<String, KafkaClient> _cache = <String, KafkaClient>{};

  ContactPoint _host;
  KafkaSession _session;
  ConsumerGroup _cGroup;
  int _randNum;
  Producer _producer;

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
      print('[Kafka Logger]: ${log.message}');
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

  Stream<String> subscribe(String topic, List partitions, {bool done: false}) async* {
    var topics = { topic : partitions};
    logger.finest('Subscribing to: $topic');

    try {
      var metadata = await _session.getMetadata(topicNames: [topic], invalidateCache: true);
      await new Future.delayed(new Duration(seconds: 3));
    } catch (e, s) {
      logger.fine('Error polling topic: $topic', e, s);
    }
    try {
      var consumer = new Consumer(_session, _cGroup, topics, 360, 1);
      await for (MessageEnvelope env in consumer.consume()) {
        var value = new String.fromCharCodes(env.message.value);
        print('Received value: $value');
        env.commit('dsLink');
        yield value;
      }
    } on StateError catch (e) {
      logger.warning('Error subscribing', e);
      if (!done) {
        await new Future.delayed(new Duration(seconds: 3));
        await for(var el in subscribe(topic, partitions, done: true)) {
          yield el;
        }
      } else {
        rethrow;
      }
    }
  }

  Future<Map> publish(String topic, int partition, String message, {bool done: false}) async {
    if (_producer == null) {
      _producer = new Producer(_session, 1, 360);
    }

    var ret = {};
    ProduceResult result;

    try {
      var metadata = await _session.getMetadata(
          topicNames: [topic], invalidateCache: true);
      await new Future.delayed(new Duration(seconds: 3));
    } catch (e, s) {
      logger.fine('Error polling metadata for topic: $topic', e, s);
    }

    try {
      result = await _producer.produce([
        new ProduceEnvelope(topic, partition, [new Message(message.codeUnits)])
      ]);
    } on StateError catch (e, s) {
      logger.fine('Error in producing:', e, s);
      if (!done) {
        await new Future.delayed(new Duration(seconds: 3));
        return publish(topic, partition, message, done: true);
      } else {
        result = null;
      }
    }

    if (result == null || result.hasErrors) {
      ret['success'] = false;
      ret['errMessage'] = 'Publishing encountered an error';
      logger.fine('Publish encountered errors');
      if(result != null ) {
        result.responses.forEach((pr) {
          logger.finest(pr.toString());
        });
      }
    } else {
      ret['success'] = true;
      ret['errMessage'] = 'Success';
    }
    return ret;
  }
}