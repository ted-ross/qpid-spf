import unittest
from router_engine import NeighborEngine, PathEngine, Configuration
from data import LinkState, MessageHELLO

class Adapter(object):
  def __init__(self, domain):
    self._domain = domain

  def log(self, level, text):
    print "Adapter.log(%d): domain=%s, text=%s" % (level, self._domain, text)

  def send(self, dest, opcode, body):
    print "Adapter.send: domain=%s, dest=%s, opcode=%s, body=%s" % (self._domain, dest, opcode, body)

  def local_bind(self, key):
    print "Adapter.local_bind: key=%s" % key

  def remote_bind(self, subject, peer):
    print "Adapter.remote_bind: subject=%s, peer=%s" % (subject, peer)

  def remote_unbind(self, subject, peer):
    print "Adapter.remote_unbind: subject=%s, peer=%s" % (subject, peer)

  def remote_rebind(self, subject, old_peer, new_peer):
    print "Adapter.remote_rebind: subject=%s, old_peer=%s, new_peer=%s" % (subject, old_peer, new_peer)


class DataTest(unittest.TestCase):
  def test_link_state(self):
    ls = LinkState(None, 'R1', 'area', 1, ['R2', 'R3'])
    self.assertEqual(ls.id, 'R1')
    self.assertEqual(ls.area, 'area')
    self.assertEqual(ls.ls_seq, 1)
    self.assertEqual(ls.peers, ['R2', 'R3'])
    ls.bump_sequence()
    self.assertEqual(ls.id, 'R1')
    self.assertEqual(ls.area, 'area')
    self.assertEqual(ls.ls_seq, 2)
    self.assertEqual(ls.peers, ['R2', 'R3'])

    result = ls.add_peer('R4')
    self.assertTrue(result)
    self.assertEqual(ls.peers, ['R2', 'R3', 'R4'])
    result = ls.add_peer('R2')
    self.assertFalse(result)
    self.assertEqual(ls.peers, ['R2', 'R3', 'R4'])

    result = ls.del_peer('R3')
    self.assertTrue(result)
    self.assertEqual(ls.peers, ['R2', 'R4'])
    result = ls.del_peer('R5')
    self.assertFalse(result)
    self.assertEqual(ls.peers, ['R2', 'R4'])

    encoded = ls.to_dict()
    new_ls = LinkState(encoded)
    self.assertEqual(new_ls.id, 'R1')
    self.assertEqual(new_ls.area, 'area')
    self.assertEqual(new_ls.ls_seq, 2)
    self.assertEqual(new_ls.peers, ['R2', 'R4'])


  def test_hello_message(self):
    msg1 = MessageHELLO(None, 'R1', 'area', ['R2', 'R3', 'R4'])
    self.assertEqual(msg1.get_opcode(), "HELLO")
    self.assertEqual(msg1.id, 'R1')
    self.assertEqual(msg1.area, 'area')
    self.assertEqual(msg1.seen_peers, ['R2', 'R3', 'R4'])
    encoded = msg1.to_dict()
    msg2 = MessageHELLO(encoded)
    self.assertEqual(msg2.get_opcode(), "HELLO")
    self.assertEqual(msg2.id, 'R1')
    self.assertEqual(msg2.area, 'area')
    self.assertEqual(msg2.seen_peers, ['R2', 'R3', 'R4'])
    self.assertTrue(msg2.is_seen('R3'))
    self.assertFalse(msg2.is_seen('R9'))



class NeighborTest(unittest.TestCase):
  def log(self, level, text):
    pass

  def send(self, dest, msg):
    self.sent.append((dest, msg))

  def local_link_state_changed(self, link_state):
    self.local_link_state = link_state

  def setUp(self):
    self.sent = []
    self.local_link_state = None
    self.id = "R1"
    self.area = "area"
    self.config = Configuration()

  def test_hello_sent(self):
    self.sent = []
    self.local_link_state = None
    self.engine = NeighborEngine(self)
    self.engine.tick(0.5)
    self.assertEqual(self.sent, [])
    self.engine.tick(1.5)
    self.assertEqual(len(self.sent), 1)
    dest, msg = self.sent.pop(0)
    self.assertEqual(dest, "_peer")
    self.assertEqual(msg.get_opcode(), "HELLO")
    self.assertEqual(msg.id, self.id)
    self.assertEqual(msg.area, self.area)
    self.assertEqual(msg.seen_peers, [])
    self.assertEqual(self.local_link_state, None)

  def test_sees_peer(self):
    self.sent = []
    self.local_link_state = None
    self.engine = NeighborEngine(self)
    self.engine.handle_hello(MessageHELLO(None, 'R2', 'area', []), 2.0)
    self.engine.tick(5.0)
    self.assertEqual(len(self.sent), 1)
    dest, msg = self.sent.pop(0)
    self.assertEqual(msg.seen_peers, ['R2'])

  def test_establish_peer(self):
    self.sent = []
    self.local_link_state = None
    self.engine = NeighborEngine(self)
    self.engine.handle_hello(MessageHELLO(None, 'R2', 'area', ['R1']), 0.5)
    self.engine.tick(1.0)
    self.engine.tick(2.0)
    self.engine.tick(3.0)
    self.assertEqual(self.local_link_state.id, 'R1')
    self.assertEqual(self.local_link_state.area, 'area')
    self.assertEqual(self.local_link_state.ls_seq, 1)
    self.assertEqual(self.local_link_state.peers, ['R2'])

  def test_establish_multiple_peers(self):
    self.sent = []
    self.local_link_state = None
    self.engine = NeighborEngine(self)
    self.engine.handle_hello(MessageHELLO(None, 'R2', 'area', ['R1']), 0.5)
    self.engine.tick(1.0)
    self.engine.handle_hello(MessageHELLO(None, 'R3', 'area', ['R1', 'R2']), 1.5)
    self.engine.tick(2.0)
    self.engine.handle_hello(MessageHELLO(None, 'R4', 'area', ['R1']), 2.5)
    self.engine.handle_hello(MessageHELLO(None, 'R5', 'area', ['R2']), 2.5)
    self.engine.handle_hello(MessageHELLO(None, 'R6', 'area', ['R1']), 2.5)
    self.engine.tick(3.0)
    self.assertEqual(self.local_link_state.id, 'R1')
    self.assertEqual(self.local_link_state.area, 'area')
    self.assertEqual(self.local_link_state.ls_seq, 3)
    self.local_link_state.peers.sort()
    self.assertEqual(self.local_link_state.peers, ['R2', 'R3', 'R4', 'R6'])

  def test_timeout_peer(self):
    self.sent = []
    self.local_link_state = None
    self.engine = NeighborEngine(self)
    self.engine.handle_hello(MessageHELLO(None, 'R2', 'area', ['R3', 'R1']), 2.0)
    self.engine.tick(5.0)
    self.engine.tick(17.1)
    self.assertEqual(self.local_link_state.id, 'R1')
    self.assertEqual(self.local_link_state.area, 'area')
    self.assertEqual(self.local_link_state.ls_seq, 2)
    self.assertEqual(self.local_link_state.peers, [])


class PathTest(unittest.TestCase):
  def setUp(self):
    self.id = 'R1'
    self.area = 'area'
    self.next_hops = None
    self.engine = PathEngine(self)

  def log(self, level, text):
    pass

  def next_hops_changed(self, nh):
    self.next_hops = nh

  def test_topology1(self): 
    """

    +====+      +----+      +----+
    | R1 |------| R2 |------| R3 |
    +====+      +----+      +----+

    """
    collection = { 'R1': LinkState(None, 'R1', 'area', 1, ['R2']),
                   'R2': LinkState(None, 'R2', 'area', 1, ['R1', 'R3']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R2']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 2)
    self.assertEqual(self.next_hops['R2'], 'R2')
    self.assertEqual(self.next_hops['R3'], 'R2')

  def test_topology2(self):
    """

    +====+      +----+      +----+
    | R1 |------| R2 |------| R4 |
    +====+      +----+      +----+
                   |           |
                +----+      +----+      +----+
                | R3 |------| R5 |------| R6 |
                +----+      +----+      +----+

    """
    collection = { 'R1': LinkState(None, 'R1', 'area', 1, ['R2']),
                   'R2': LinkState(None, 'R2', 'area', 1, ['R1', 'R3', 'R4']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R2', 'R5']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R2', 'R5']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R3', 'R4', 'R6']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 5)
    self.assertEqual(self.next_hops['R2'], 'R2')
    self.assertEqual(self.next_hops['R3'], 'R2')
    self.assertEqual(self.next_hops['R4'], 'R2')
    self.assertEqual(self.next_hops['R5'], 'R2')
    self.assertEqual(self.next_hops['R6'], 'R2')

  def test_topology3(self):
    """

    +----+      +----+      +----+
    | R2 |------| R3 |------| R4 |
    +----+      +----+      +----+
                   |           |
                +====+      +----+      +----+
                | R1 |------| R5 |------| R6 |
                +====+      +----+      +----+

    """
    collection = { 'R2': LinkState(None, 'R2', 'area', 1, ['R3']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R1', 'R2', 'R4']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R3', 'R5']),
                   'R1': LinkState(None, 'R1', 'area', 1, ['R3', 'R5']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R1', 'R4', 'R6']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 5)
    self.assertEqual(self.next_hops['R2'], 'R3')
    self.assertEqual(self.next_hops['R3'], 'R3')
    self.assertEqual(self.next_hops['R4'], 'R3')
    self.assertEqual(self.next_hops['R5'], 'R5')
    self.assertEqual(self.next_hops['R6'], 'R5')

  def test_topology4(self):
    """

    +----+      +----+      +----+
    | R2 |------| R3 |------| R4 |
    +----+      +----+      +----+
                   |           |
                +====+      +----+      +----+
                | R1 |------| R5 |------| R6 |------ R7 (no ls from R7)
                +====+      +----+      +----+

    """
    collection = { 'R2': LinkState(None, 'R2', 'area', 1, ['R3']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R1', 'R2', 'R4']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R3', 'R5']),
                   'R1': LinkState(None, 'R1', 'area', 1, ['R3', 'R5']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R1', 'R4', 'R6']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5', 'R7']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 6)
    self.assertEqual(self.next_hops['R2'], 'R3')
    self.assertEqual(self.next_hops['R3'], 'R3')
    self.assertEqual(self.next_hops['R4'], 'R3')
    self.assertEqual(self.next_hops['R5'], 'R5')
    self.assertEqual(self.next_hops['R6'], 'R5')
    self.assertEqual(self.next_hops['R7'], 'R5')

  def test_topology5(self):
    """

    +----+      +----+      +----+
    | R2 |------| R3 |------| R4 |
    +----+      +----+      +----+
       |           |           |
       |        +====+      +----+      +----+
       +--------| R1 |------| R5 |------| R6 |------ R7 (no ls from R7)
                +====+      +----+      +----+

    """
    collection = { 'R2': LinkState(None, 'R2', 'area', 1, ['R3', 'R1']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R1', 'R2', 'R4']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R3', 'R5']),
                   'R1': LinkState(None, 'R1', 'area', 1, ['R3', 'R5', 'R2']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R1', 'R4', 'R6']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5', 'R7']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 6)
    self.assertEqual(self.next_hops['R2'], 'R2')
    self.assertEqual(self.next_hops['R3'], 'R3')
    self.assertEqual(self.next_hops['R4'], 'R3')
    self.assertEqual(self.next_hops['R5'], 'R5')
    self.assertEqual(self.next_hops['R6'], 'R5')
    self.assertEqual(self.next_hops['R7'], 'R5')

  def test_topology5_with_asymmetry1(self):
    """

    +----+      +----+      +----+
    | R2 |------| R3 |------| R4 |
    +----+      +----+      +----+
       ^           |           |
       ^        +====+      +----+      +----+
       +-<-<-<--| R1 |------| R5 |------| R6 |------ R7 (no ls from R7)
                +====+      +----+      +----+

    """
    collection = { 'R2': LinkState(None, 'R2', 'area', 1, ['R3']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R1', 'R2', 'R4']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R3', 'R5']),
                   'R1': LinkState(None, 'R1', 'area', 1, ['R3', 'R5', 'R2']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R1', 'R4', 'R6']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5', 'R7']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 6)
    self.assertEqual(self.next_hops['R2'], 'R2')
    self.assertEqual(self.next_hops['R3'], 'R3')
    self.assertEqual(self.next_hops['R4'], 'R3')
    self.assertEqual(self.next_hops['R5'], 'R5')
    self.assertEqual(self.next_hops['R6'], 'R5')
    self.assertEqual(self.next_hops['R7'], 'R5')

  def test_topology5_with_asymmetry2(self):
    """

    +----+      +----+      +----+
    | R2 |------| R3 |------| R4 |
    +----+      +----+      +----+
       v           |           |
       v        +====+      +----+      +----+
       +->->->->| R1 |------| R5 |------| R6 |------ R7 (no ls from R7)
                +====+      +----+      +----+

    """
    collection = { 'R2': LinkState(None, 'R2', 'area', 1, ['R3', 'R1']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R1', 'R2', 'R4']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R3', 'R5']),
                   'R1': LinkState(None, 'R1', 'area', 1, ['R3', 'R5']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R1', 'R4', 'R6']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5', 'R7']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 6)
    self.assertEqual(self.next_hops['R2'], 'R3')
    self.assertEqual(self.next_hops['R3'], 'R3')
    self.assertEqual(self.next_hops['R4'], 'R3')
    self.assertEqual(self.next_hops['R5'], 'R5')
    self.assertEqual(self.next_hops['R6'], 'R5')
    self.assertEqual(self.next_hops['R7'], 'R5')

  def test_topology5_with_asymmetry3(self):
    """

    +----+      +----+      +----+
    | R2 |------| R3 |------| R4 |
    +----+      +----+      +----+
       v           |           |
       v        +====+      +----+      +----+
       +->->->->| R1 |------| R5 |<-<-<-| R6 |------ R7 (no ls from R7)
                +====+      +----+      +----+

    """
    collection = { 'R2': LinkState(None, 'R2', 'area', 1, ['R3', 'R1']),
                   'R3': LinkState(None, 'R3', 'area', 1, ['R1', 'R2', 'R4']),
                   'R4': LinkState(None, 'R4', 'area', 1, ['R3', 'R5']),
                   'R1': LinkState(None, 'R1', 'area', 1, ['R3', 'R5']),
                   'R5': LinkState(None, 'R5', 'area', 1, ['R1', 'R4']),
                   'R6': LinkState(None, 'R6', 'area', 1, ['R5', 'R7']) }
    self.engine.ls_collection_changed(collection)
    self.engine.tick(1.0)
    self.assertEqual(len(self.next_hops), 4)
    self.assertEqual(self.next_hops['R2'], 'R3')
    self.assertEqual(self.next_hops['R3'], 'R3')
    self.assertEqual(self.next_hops['R4'], 'R3')
    self.assertEqual(self.next_hops['R5'], 'R5')


if __name__ == '__main__':
  unittest.main()
