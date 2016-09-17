In this program, I have designed a simple DHT based on Chord implemented on upto 5 Mobile Devices forming the ring. Although the design is based on Chord, it is a simplified version of Chord; Three things are implemented here: 
1) ID based partitioning/re-partitioning of the mobile devices
2) Ring-based routing for inserting and querying data
3) Node joins in the ring (Node is one mobile instance of this application).

This app has an activity and a content provider. The main activity does not implement any DHT functionality. The content provider is implementing all DHT functionalities and supporting insert and query operations. Thus, if you run multiple instances of this app, all content provider instances forms a Chord ring and serve insert/query requests in a distributed fashion according to the Chord protocol.
