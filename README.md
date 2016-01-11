# potus-hopefuls-realtime-twitter

### collect
Python script to collect mentions of primary candidates on twitter.  Writes tweets to a RabbitMQ fanout queue.

### show
Web application in Python/FLask that waits for new queue entries and publishes them to all connected web clients. 
