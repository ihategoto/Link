//importing mongoose
const mongoose = require('mongoose');
const { Schema } = mongoose;
// Creating a schema for the collection Nodi
/**
 * db_nodi
 * nodos is the collection
 * nodo is the document
 * */ 

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
        type: {
            type: Number,
            required: true,
        },
        um : {
            type: String,
        },
        value: {
            type: Number,
            required: true
        }
    }]
});


const Nodo = mongoose.model('Nodo', NodoSchema);

module.exports = Nodo;