# -*- coding: utf8 -*- 
import unittest
from hazelcast.client import hazelcast

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):        
        self.hc = hazelcast(debug=True)        

    def _test_unicode(self):
        self.hc.destroy('map', 'mymap')
        self.hc.mput('mymap', 'foo', u'çöğüşı')        
        assert u'çöğüşı' == self.hc.mget('mymap', 'foo')

    def _test_missing_methods(self):
        self.hc.destroy('map', 'mymap')
        self.hc.mput('mymap', 'foo', 'bar')
        assert self.hc.mcontainskey('mymap', 'foo') == True
        assert self.hc.mremove('mymap', 'foo') == ['bar']

    def _test_map(self):
        self.hc.destroy('map', 'mymap')
        self.hc.mput('mymap', 'foo', 'bar')
        assert 'bar' == self.hc.mget('mymap', 'foo')
        self.hc.mput('mymap', 'foo2', 'bar')
        assert self.hc.mgetall('mymap', ['foo', 'foo2', 'foo3']) == {'foo': 'bar', 'foo2': 'bar', 'foo3': ''}
        assert ['foo2', 'foo'] == self.hc.keyset('map', 'mymap')

    def test_map(self):
        self.hc.destroy('map', 'mymap')
        m = self.hc.Map('mymap')
        try:
            a = m['b']
            assert False
        except KeyError as e:
            assert True
        
        m['a'] = 'foo'
        m['b'] = 'bar'
        assert m['a'] == 'foo'
        assert m['b'] == 'bar'
        del m['b']
        assert 'a' in m
        assert not ('b' in m)
        m['c'] = '1'
        assert m.keys() == ['a', 'c']
        for i in m:            
            assert i in ['a', 'c']
            m[i] in ['foo', '1']

if __name__ == '__main__':
    unittest.main()