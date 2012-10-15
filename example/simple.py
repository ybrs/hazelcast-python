from hazelcast.client import hazelcast

hc = hazelcast()
hc.mput('mymap', 'foo', 'bar')
print hc.mget('mymap', 'foo')
