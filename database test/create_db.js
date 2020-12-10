var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/";

MongoClient.connect(url, function(err, db) {
  if (err) throw err;
  var dbo = db.db("mydb");
  dbo.collection("nodi").drop();
  var myobj = [
    { _id: 0, name: "Nodo1"},
    { _id: 1, name: "Nodo2"},
    { _id: 2, name: "Nodo3"},
    { _id: 3, name: "Nodo4"},
    { _id: 4, name: "Nodo5"}
  ];

  //insert all the element of the list
  dbo.collection("nodi").insertMany(myobj, function(err, res) {
    if (err) throw err;
    //result prints
    console.log("Number of documents inserted: " + res.insertedCount);
    console.log(res);
  });

  //close the connection
  db.close();
}); 

