library dslink.kafka.node;

import 'dart:async';

import 'package:dslink/client.dart';
import 'package:dslink/responder.dart';
import 'package:dslink/nodes.dart' show NodeNamer;
import 'package:dslink/utils.dart';
import 'package:kafka/kafka.dart';

import 'src/kafka_client.dart';

part 'src/topics_node.dart';

class AddConnection extends SimpleNode {
  static const String isType = 'addConnectionNode';
  static const String pathName = 'Add_Connection';
  static Map<String, dynamic> definition() => {
    r'$is' : isType,
    r'$invokable' : 'write',
    r'$name' : 'Add Connection',
    r'$result' : 'values',
    r'$params' : [
      {
        'name' : 'name',
        'description' : 'Connection Name',
        'type' : 'string'
      },
      {
        'name' : 'address',
        'type' : 'string',
        'description' : 'Server Address',
        'placeholder' : '127.0.0.1',
      },
      {
        'name' : 'port',
        'type' : 'int',
        'description' : 'Server Port',
        'default' : 9092,
        'min' : 0,
        'max' : 65535
      }
    ],
    r'$columns' : []
  };

  LinkProvider link;

  AddConnection(this.link, String path) : super(path);

  @override
  Future onInvoke(Map<String, dynamic> params) async {
    if (params['name'] == null || params['name'].isEmpty ||
        params['address'] == null || params['address'].isEmpty) {
      return;
    }

    var parPath = parent.path;
    var name = NodeNamer.createName(params['name']);
    provider.addNode('/$name', KafkaNode.definition(params));
    link.save();
  }
}

class EditConnection extends SimpleNode {
  static const String isType = 'editConnectionNode';
  static final String pathName = 'Edit_Connection';
  static Map<String, dynamic> definition(Map params) => {
    r'$is' : isType,
    r'$invokable' : 'write',
    r'$name' : 'Edit Connection',
    r'$result' : 'values',
    r'$params' : [
      {
        'name' : 'address',
        'type' : 'string',
        'description' : 'Server Address',
        'default' : params['address']
      },
      {
        'name' : 'port',
        'type' : 'int',
        'max' : 65535,
        'min' : 0,
        'description' : 'Server Port',
        'default' : params['port']
      }
    ],
    r'$columns' : []
  };

  EditConnection(String path) : super(path);

  @override
  dynamic onInvoke(Map<String, dynamic> params) {
    if (params['address'] == null || params['address'].isEmpty ||
        params['port'] == null) return;

    configs[r'$params'] = [
      {
        'name' : 'address',
        'type' : 'string',
        'description' : 'Server Address',
        'default' : params['address']
      },
      {
        'name' : 'port',
        'type' : 'int',
        'max' : 65535,
        'min' : 0,
        'description' : 'Server Port',
        'default' : params['port']
      }
    ];

    (parent as KafkaNode).updateConfig(params);
  }
}

class RemoveConnection extends SimpleNode {
  static const isType = 'removeConnectionNode';
  static const pathName = 'Remove_Connection';
  static Map<String, dynamic> definition() => {
    r'$is' : isType,
    r'$invokable' : 'write',
    r'$name' : 'Remove Connection',
    r'$params' : [],
    r'$columsn' : []
  };

  RemoveConnection(String path) : super(path);

  @override
  Map onInvoke(Map params) {
    provider.removeNode(parent.path);
    return {};
  }
}

class KafkaNode extends SimpleNode {
  static const String isType = 'kafkaNode';
  static Map<String, dynamic> definition(Map params) => {
    r'$is' : isType,
    r'$$kafka_address' : params['address'],
    r'$$kafka_port' : params['port'],
    EditConnection.pathName : EditConnection.definition(params),
    RemoveConnection.pathName : RemoveConnection.definition(),
    AddTopic.pathName : AddTopic.definition()
  };

  KafkaClient client;
  LinkProvider link;

  KafkaNode(this.link, String path) : super(path);

  @override
  void onCreated() {
    var addr = getConfig(r'$$kafka_address');
    var port = int.parse(getConfig(r'$$kafka_port'));

    client = new KafkaClient(addr, port);
  }

  void updateConfig(Map params) {
    configs[r'$$kafka_address'] = params['address'];
    configs[r'$$kafka_port'] = params['port'];

    client = client.update(params['address'], int.parse(params['port']));
    link.save();
  }

  @override
  void onRemoving() {
    client.close();
    link.save();
  }
}