part of dslink.kafka.node;

class AddTopic extends SimpleNode {
  static const String isType = 'addTopicNode';
  static const String pathName = 'Add_Topic';
  static Map<String, dynamic> definition() => {
    r'$is' : isType,
    r'$name' : 'Add Topic Subscription',
    r'$invokable' : 'write',
    r'$params' : [
      {
        'name' : 'topic',
        'type' : 'string',
        'placeholder' : 'topic',
      },
      {
        'name' : 'partitions',
        'type' : 'array',
        'default' : [0, 1]
      }
    ],
    r'$columns' : []
  };

  AddTopic(String path) : super(path);

  @override
  onInvoke(Map params) {
    if (params['topic'] == null || params['topic'].isEmpty) return;

    var partitions = [];
    if (params['partitions'] == null) {
      partitions.add(0);
    } else {
      for (var par in partitions) {
        if (par == null || par.trim().isEmpty) continue;
        var parNum = int.parse(par, onError: (_) => null);
        if (parNum == null) continue;
        partitions.add(num);
      }
      if (partitions.isEmpty) partitions.add(0);
    }

    var parPath = parent.path;
    var name = NodeNamer.createName(params['topic']);
    params['part'] = partitions;
    provider.addNode('$parPath/$name', TopicNode.definition(params));
  }
}

class RemoveTopicNode extends SimpleNode {
  static const isType = 'removeTopicNode';
  static const pathName = 'Remove_Subscription';
  static Map<String, dynamic> definition() => {
    r'$is' : isType,
    r'$name' : 'Remove Subscription',
    r'$invokable' : 'write',
    r'$params' : [],
    r'$columns' : []
  };

  RemoveTopicNode(String path) : super(path);

  @override
  dynamic onInvoke(Map params) {
    provider.removeNode(parent.path);
  }
}

class TopicNode extends SimpleNode {
  static const String isType = 'topicSubscriptionNode';
  static Map<String, dynamic> definition(Map params) => {
    r'$is' : isType,
    r'$$kafka_topic' : params['topic'],
    r'$$kafka_part' : params['part'],
    r'$type' : 'string',
    r'?value': '',
    RemoveTopicNode.pathName : RemoveTopicNode.definition()
  };

  KafkaClient _client;
  String _topic;
  List<int> _partitions;
  StreamSubscription _subscription;
  TopicNode(String path) : super(path);

  @override
  void onCreated() {
    _client = (parent as KafkaNode).client;

    _topic = getConfig(r'$$kafka_topic');
    _partitions = getConfig(r'$$kafka_part');

    _subscription = _client.subscribe(_topic, _partitions).listen((String val) {
      updateValue(val);
    }, onError: (e) {
    });
  }

  @override
  void onRemoving() {
    logger.finest('Removing subscriptiong $_topic');
    _subscription.cancel();
  }
}