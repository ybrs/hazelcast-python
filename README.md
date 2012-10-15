this is a basic implementation of hazelcast protocol in python

you need to clone hazelcast/clientprotocol and compile for the server part.
--------------------
git clone https://github.com/hazelcast/hazelcast.git
cd hazelcast
git fetch
git checkout clientprotocol
ant

now run server,
---------------
java -server -Xms128M -Xmx128M -cp "hazelcast-2.2.SNAPSHOT.jar:hazelcast-client-2.2.SNAPSHOT.jar" com.hazelcast.examplestartServer

now for the python client,
---------------------------
git clone (this)
cd hazelcast-python
python setup.py install

