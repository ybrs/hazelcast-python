'''
an example for the listener interface,
on one terminal run:
python example/maplistener.py listen
on the other run 
python example/maplistener.py trigger
'''
from hazelcast.client import hazelcast
import sys
import time
if sys.argv[1] == 'listen':
    
    hc = hazelcast()
    hc.mput('mymap', 'foo', 'bar')
    def callback(eventtype, values, **kwargs):
        print ">>>>", eventtype, values
    print hc.maddlistener('mymap', callback)

else:
    hc = hazelcast()
    hc.mput('mymap', 'foo2', 'bar')
    time.sleep(0.1)
    hc.mput('mymap', 'foo3', 'bar')
    time.sleep(0.1)
    hc.mremovelistener('mymap')



