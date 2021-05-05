# axdb2
High performance key-value database

- Distributed: Raft consensus protocol
- Storage Engine: LSM-Tree / B+Tree
- Asynchronous IO: async/await based

# usage
start new instance:
```
fan axdb2_cluster testData/db/ http://localhost:8080
```

put data:
```
curl -v 'localhost:8080/execute?cmd=key_0%3Aval_0&sync=false'
```

get data:
```
curl -v 'localhost:8081/find?key=key_0'
```

add replicas:
```
curl -v 'localhost:8080/execute?cmd=http%3A%2F%2Flocalhost%3A8081&type=1'
```
