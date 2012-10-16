from twisted.internet import reactor, protocol
from twisted.protocols import basic
from twisted.internet.task import LoopingCall
from collections import defaultdict
import random

class PubProtocol(basic.LineReceiver):
    def __init__(self, factory):
        self.factory = factory

    def connectionMade(self):
        # self.factory.connections.add(self)

    def connectionLost(self, reason):
        self.factory.disconnect(self)

    def lineReceived(self, line):
        cmd, channel, data = line.split(' ')
        if cmd == 'sub':
            self.factory.subscribe_remote(channel, self)
        elif cmd == 'pub':
            self.factory.publish(channel, data)
        elif cmd == 'unsub':
            self.factory.unsubscribe_remote(channel, self)



class PubFactory(protocol.Factory):
    def __init__(self, name):
        self.connections = defaultdict(set)
        self.local_subscriber_map = defaultdict(set)
        self.remote_subscriber_map = defaultdict(set)

    def publish(self, channel, data):
        for s in self.local_subscriber_map[channel]:
            print "calling", s
            s(data)

        for s in self.remote_subscriber_map[channel]:
            print "sending to", s
            s.sendLine(' '.join(['pub', channel, str(data)]))

    def subscribe_local(self, channel, func):
        pass

    def subscribe_remote(self, channel, consumer):
        print self, "subscribed to", channel
        self.factory.connections[consumer].add(channel)
        self.factory.remote_subscriber_map[channel].add(consumer)

    def unsubscribe_local(self, channel, func):
        pass

    def unsubscribe_remote(self, channel, consumer):
        print self, "unsubscribed from", channel
        self.factory.connections[consumer].remove(channel)
        self.factory.remote_subscriber_map[channel].remove(consumer)

    def disconnect(consumer):
        for channel in self.factory.connections(consumer):
            self.factory.remote_subscriber_map[channel].remove(consumer)

        self.factory.connections.remove(self)

    def buildProtocol(self, addr):
        return PubProtocol(self)


f.local_subscriber_map['rand'] = set([printer])

reactor.listenTCP(1025, f)
reactor.run()

class App(object):
    def __init__(self, name):
        self.factory = PubFactory();

