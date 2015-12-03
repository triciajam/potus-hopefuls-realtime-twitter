import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='tweets',
                         type='topic')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

binding_keys = sys.argv[1:]
if not binding_keys:
    print >> sys.stderr, "Usage: %s [binding_key]..." % (sys.argv[0],)
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange='tweets',
                       queue=queue_name,
                       routing_key=binding_key)


for method_frame, properties, body in channel.consume():

        tweets.append(json.loads(body))

        # Acknowledge the message
        channel.basic_ack(method_frame.delivery_tag)
        

# Cancel the consumer and return any pending messages
requeued_messages = channel.cancel()
print 'Requeued %i messages' % requeued_messages
    
    
#def callback(ch, method, properties, body):
#    print " [x] %r:%r" % (method.routing_key, body,)

#channel.basic_consume(callback,
#                      queue=queue_name,
#                      no_ack=True)

#channel.start_consuming()
