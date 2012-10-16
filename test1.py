from stream import App

class RandPublisher():

def rand():
    f.publish('rand', random.random())

lc  = LoopingCall(rand)
lc.start(1.0)

def printer(data):
    print data

s = App('test1')

f = PubFactory()

