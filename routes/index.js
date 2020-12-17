const express = require('express');
const cors = require('cors');
const router = express.Router();
// requiring the document from mongoose
const Nodo = require('../models/nodo');
//const assert = require('assert');
//const MongoClient = require('mongodb').MongoClient
const mongoose = require('mongoose');
const MONGODB_URL = 'mongodb://localhost:27017/db_nodi';
const fs = require('fs');

//Path della FIFO.
const FIFO_PATH = 'modbus_handler/write_pipe';

//Connecting to db
mongoose.connect(MONGODB_URL, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => {
  console.log("App is running... \n");
})
.catch((err) => {
  console.log(err);
  process.exit(1);
});

/**
 * GET: find all nodes by slave number with the latest date
 */
router.get('/', async (req,res) => {
  const nd = (await Nodo.aggregate([
              {$group: {
                      "_id": "$slave", //group by this id (the slave number)
                      date: {$first: '$date'}, //get only the last document (by field) in a ordered group 
                  }},
              
              {$sort: { //sort in descending order
                      "_id": 1
                  }}
          ])
).map((item) => ({_id:item._id, slave: item.slave}));
  //console.log("STAMPA NODI:"+nd);
  res.render('index',{items: nd});
});

/**
 * GET: find the node by slave passed in the url, always searching for the latest date
 */
router.get('/node/:id', async (req, res) => {
  //this is not the _id, it's the slave number
  console.log("id:"+req.params.id);
  let slave_id = req.params.id; 
  try{
    const data = (await Nodo.aggregate([
        {
          '$match': {
            'slave': Number(slave_id)
          }
        }, {
          '$sort': {
            'slave': -1,
            'date': -1
          }
        }, {
          '$limit': 1
        }
    ]))
    // non leggeva bene il documento
    //const data = nd.map((item) => ({...item, sensors: item.sensors.map(s => ({...s}))}));
    //console.log(data);
    if(data.length === 0) {
      //gestire errore
    }
    else res.json(data[0]);
  }catch(err){
    console.log("Errore nel get");
    res.json({message:err});
  }
});

/*
Scrive su una pipe per comunicare con backend python che gestisce
gli slave modbus.
*/
/*
Aggiungere le nuove notazioni per gli address
Modificare in modo da usare le nuove variabili
*/
router.post('/node/write/', async (req, res) => {
  console.log("POST COMANDI");
  let slave_address = req.body.slave_address;
  let register_address = req.body.register_address;
  console.log("register address: "+register_address);
  let value = req.body.value;
  let data = slave_address + ',' + register_address + ',' + value;
  fs.open(FIFO_PATH, 'w', (err, fd) => {
    if(err){
      console.log("Qualcosa è andato storto nell'apertura del file:" + err);
      return;
    }
    fs.writeSync(fd, data);
    fs.close(fd, () => {});
  });
  // mandare una risposta (?)
  res.json({msg: "Working"});
});


module.exports = router;
