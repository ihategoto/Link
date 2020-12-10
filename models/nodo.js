//importing mongoose
const mongoose = require('mongoose');
const { Schema } = mongoose;
// Creating a schema for the collection Nodi
/**
 * db_nodi
 * nodos is the collection
 * nodo is the document
 * */ 

//PS: Update the required to some data
/*const NodoSchema = new Schema({
    nome:{
        type: String,
        required: true
    },
    time:{
        type: String,
    },
    val_CO:{
        type: String,
        required: false,
        min: 0,
        max: 400
    },
    val_eCO2:{
        type: String,
        required: false,
        min: 450,
        max: 2000
    },
    val_TVCO:{
        type: String,
        required: false,
        min: 125,
        max: 600
    },
    val_PM1_0:{
        type: String,
        required: false,
        min: 0,
        max: 1000},
    val_PM2_5:{
        type: String,
        required: false,
        min: 0,
        max: 1000
    },
    val_PM4:{
        type: String,
        required: false,
        min: 0,
        max: 1000
    },
    val_PM10:{
        type: String,
        required: false,
        min: 0,
        max: 1000
    },
    val_PD:{
        type: String,
        required: false,
        min: 1,
        max: 30
    },
    val_T:{
        type: String,
        required: false,
        min: -40,
        max: 100
    },
    val_Umi:{
        type: String,
        required: false,
        min: 0,
        max: 100
    },
    val_H2O2:{
        type: String,
        required: false,
    },
    val_O3:{
        type: String,
        required: false,
        min: 0,
        max: 10
    }
});*/

const NodoSchema = new Schema({
    date: {
        type: Date,
        required: true
    },
    slave: { 
        type: Number,
        required: true
    },
    sensors: [{
        name: {
            type: String,
            required: true
        },
        address: {
            type: Number,
            required: true
        },
        value: {
            type: Number,
            required: true
        }
    }]
});


const Nodo = mongoose.model('Nodo', NodoSchema);

module.exports = Nodo;