import sys
import os

from confluent_kafka import Consumer, KafkaException, KafkaError

class consumer(threading.Thread):
    daemon=True
    topic = None
    c = None
    conf = {
        'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
        'group.id': "%s-consumer" % os.environ['CLOUDKARAFKA_USERNAME'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
    }
    def __init__(self,topic):
        self.topic = os.environ['CLOUDKARAFKA_TOPIC_PREFIX']+topic
        self.c = Consumer(**conf)

    
    def run(self):
        self.c.subscribe(self.topic)
        try:
            while True:
                msg = self.c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                # Error or event
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                    # Error
                        raise KafkaException(msg.error())
                else:
                # Proper message
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                    print(msg.value())

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

    # Close down consumer to commit final offsets.
        self.c.close()
