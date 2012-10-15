import unittest
from hazelcast.client import hazelcast

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.client = hazelcast()

    def test_map(self):
        hc = hazelcast()
        hc.destroy('map', 'mymap')
        hc.mput('mymap', 'foo', 'bar')
        assert 'bar' == hc.mget('mymap', 'foo')
        hc.mput('mymap', 'foo2', 'bar')
        assert hc.mgetall('mymap', ['foo', 'foo2', 'foo3']) == {'foo': 'bar', 'foo2': 'bar', 'foo3': ''}
        assert ['foo2', 'foo'] == hc.keyset('map', 'mymap')

if __name__ == '__main__':
    unittest.main()