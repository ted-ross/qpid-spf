
from qpid.messaging import Connection, Message
from time import sleep
import sys

host = "localhost:10009"
if len(sys.argv) > 1:
  host = sys.argv[1]

conn = Connection(host)
try:
  conn.open()
  sess = conn.session()
  tx = sess.sender("spfdemo.com/mobile.45")
  count = 0
  while(True):
    tx.send("Seq: %d" % count)
    print "Origin: %s Seq: %d" % (host, count)
    count += 1
    sleep(1)
except Exception, e:
  print "Exception: %r" % e
except KeyboardInterrupt:
  print
