//
//  mysql connector
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
var express = require('express')
var router = express.Router()
const mysql = require('mysql')
const { encryptAsymmetric, saveCredentials } = require('./credentials')

router.post('/connectMySql', async (req, res) => {
	console.log(req.body)
	const con = mysql.createConnection({
		host: req.body.host,
		user: req.body.user,
		password: req.body.password,
		port: req.body.port,
	})

	con.connect(async err => {
		if (err) res.status(403).send(err);
		res.json({status: 'CONNECTED'})

		console.log('Connected.')

	// 	const projectId = 'tart-90ca2';
	// 	const region = 'us-central1';
	// 	const userId = req.body.uid
	// 	const keyRingId = 'cloudR-user-database';
	// 	const keyId = 'database-login';
	// 	const versionId = '1';
	// 	const plaintextBuffer = Buffer.from(req.body.password);
	//
	// 	const {KeyManagementServiceClient} = require('@google-cloud/kms');
	// 	const client = new KeyManagementServiceClient();
	// 	const versionName = client.cryptoKeyVersionPath(
	// 		projectId,
	// 		region,
	// 		keyRingId,
	// 		keyId,
	// 		versionId
	// 	);
	//
	// 	const encryptedPassword = await encryptAsymmetric(client, versionName, plaintextBuffer)
	//
	// 	const Firestore = require('@google-cloud/firestore');
	//
	// 	const firestore = new Firestore({
	// 		projectId: projectId,
	// 		keyFilename: './tart-90ca2-9d37c42ef480.json',
	// 	});
	//
	// 	const credentials = {
	// 		host: req.body.host,
	// 		user: req.body.user,
	// 		password: encryptedPassword,
	// 		port: req.body.port,
	// 		connector: req.body.connector,
	// 	}
	//
	// 	await saveCredentials(firestore, credentials, userId, req.body.host, req.body.port)
	})

	con.end()
})

// router.post('/listDatabasesMySql', (req, res) => {
// 	const con = mysql.createConnection({
// 		host: req.body.host,
// 		user: req.body.user,
// 		password: req.body.password,
// 		port: req.body.port,
// 	})
//
// 	const SQL_STATEMENT = "SHOW DATABASES"
//
// 	con.query(SQL_STATEMENT, (err, result, fields) => {
// 		if (err) res.json({status: 'ERROR'});
//
// 		const databases = result.map(database => database.Database)
// 		res.json(databases)
// 	})
//
// 	con.end()
// })
//
// router.post('/listTablesMySql', (req, res) => {
// 	const con = mysql.createConnection({
// 		host: req.body.host,
// 		user: req.body.user,
// 		password: req.body.password,
// 		database: req.body.database,
// 		port: req.body.port,
// 	})
//
// 	const SQL_STATEMENT = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + req.body.database + "'"
//
// 	con.query(SQL_STATEMENT, (err, result, fields) => {
// 		if (err) res.json({status: 'ERROR'});
//
// 		const tables = result.map(table => table.TABLE_NAME)
// 		res.json(tables)
// 	})
//
// 	con.end()
// })
//
// router.post('/getTableSampleMySql', (req,res) => {
// 	const con = mysql.createConnection({
// 		host: req.body.host,
// 		user: req.body.user,
// 		password: req.body.password,
// 		database: req.body.database,
// 		port: req.body.port,
// 	})
//
// 	const SQL_STATEMENT = "SELECT * FROM " + req.body.table + " LIMIT 200"
//
// 	con.query(SQL_STATEMENT, (err, result, fields) => {
// 		if (err) res.json({status: 'ERROR'});
// 		res.json(result)
// 	})
//
// 	con.end()
// })

module.exports = router
