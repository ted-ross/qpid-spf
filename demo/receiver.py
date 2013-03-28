
from qpid.messaging import Connection, Message
from time import sleep

conn = Connection("localhost:10003")
try:
  conn.open()
  sess = conn.session()
  rx = sess.receiver("spfdemo.com/mobile.45")
  rx.capacity = 16
  count = 0
  while(True):
    msg = rx.fetch()
    print "Content: %s Trace: %s" % (msg.content, msg.properties['x-qpid.trace'])
    if count % 16 == 0:
      sess.acknowledge()
    count += 1
except Exception, e:
  print "Exception: %r" % e
except KeyboardInterrupt:
  print

