const functions = require('firebase-functions');
const express = require('express');
const app = express();
const mysql = require('./mysql');

app.use('./mysql', mysql)

exports.app = functions.https.onRequest(app);
