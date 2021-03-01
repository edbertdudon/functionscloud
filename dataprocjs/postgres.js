//
//  SQL Server connector
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
const cors = require('cors')({origin: '*'});
const constants = require('./core/options')
const admin = require('./index');
const kms = require('./core/kms');

exports.connectPostgres = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const host = req.body.host;
		const user = req.body.user;
		const password = req.body.password;
		const port = req.body.port;
		const connector = req.body.connector;
    const ssl = req.body.ssl;
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const userId = req.body.uid;
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const { Client } = require('pg');

    const client = new Client({
      host: host,
      port: parseInt(port),
      user: user,
      password: password,
      ssl: ssl,
    });

    await client.connect(err => {
      if (err) {
        console.error('Connection error', err.stack);
        res.status(403).send(err);
        return;
      }

      console.log('Connected.');

			res.json({status: 'CONNECTED'})
    })

    const plaintextBuffer = Buffer.from(password);

    const {KeyManagementServiceClient} = require('@google-cloud/kms');

    const clientKMS = new KeyManagementServiceClient({
      keyFilename: './tart-90ca2-e8da41e06847.json',
    });

    const versionName = clientKMS.cryptoKeyVersionPath(
      projectId,
      region,
      keyRingId,
      keyId,
      versionId
    );

    const encryptedPassword = await kms.encryptAsymmetric(
      clientKMS,
      versionName,
      plaintextBuffer
    );

    console.log('Encrypted Password.');

    const Firestore = require('@google-cloud/firestore');

    const firestore = new Firestore({
      projectId: projectId,
      keyFilename: './tart-90ca2-e8da41e06847.json',
    });

    const credentials = {
      host: host,
      user: user,
      password: encryptedPassword,
      port: port,
      connector: connector,
      ssl: ssl,
    }

    await kms.saveCredentials(firestore, credentials, userId, host, port)

    await client.end();
	})
}

exports.listDatabasesPostgres = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const connection = req.body.connection
		const userId = req.body.uid
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const connections = await kms.getConnectionDocument(firestore, userId)

		const credentials = connections[connection]
		const host = credentials.host;
		const user = credentials.user;
		const encryptedPassword = credentials.password;
		const port = credentials.port;
		const ssl = credentials.ssl;
		const ciphertext = Buffer.from(encryptedPassword);

		const {KeyManagementServiceClient} = require('@google-cloud/kms');

		const clientKMS = new KeyManagementServiceClient({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const password = await kms.decryptAsymmetric(
			clientKMS,
			ciphertext,
			projectId,
			region,
			keyRingId,
			keyId,
			versionId
		);

		const { Client } = require('pg')

    const client = new Client({
      host: host,
      port: parseInt(port),
      user: user,
      password: password,
      ssl: ssl,
    })

		client
			.connect()
			.then(() => console.log('Connected.'))
			.catch(err => {
				console.error('Connection error', err.stack)
        res.status(403).send(err);
			})

    const SQL_STATEMENT = "SELECT datname FROM pg_database;"

		client
			.query(SQL_STATEMENT)
			.then((result) => {
				// rows: [
			  //   { datname: 'postgres' },
			  //   { datname: 'template1' },
			  //   { datname: 'template0' }
			  // ],
				const databases = result.rows.map(database => database.datname)
				// [ 'postgres', 'template1', 'template0' ]
				res.json(databases)
			})
			.catch((err) => {
				console.log(err)
				res.status(403).send(err);
			})
			.then(() => client.end())
	})
}

exports.listTablesPostgres = async (req, res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const connection = req.body.connection;
		const database = req.body.database;
		const userId = req.body.uid
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const connections = await kms.getConnectionDocument(firestore, userId)

		const credentials = connections[connection]
		const host = credentials.host;
		const user = credentials.user;
		const encryptedPassword = credentials.password;
		const port = credentials.port;
		const ssl = credentials.ssl;
		const ciphertext = Buffer.from(encryptedPassword);

		const {KeyManagementServiceClient} = require('@google-cloud/kms');

		const clientKMS = new KeyManagementServiceClient({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const password = await kms.decryptAsymmetric(
			clientKMS,
			ciphertext,
			projectId,
			region,
			keyRingId,
			keyId,
			versionId
		);

		const { Client } = require('pg')

    const client = new Client({
      host: host,
      port: parseInt(port),
      user: user,
      password: password,
			database: database,
      ssl: ssl,
    })

		client
			.connect()
			.then(() => console.log('Connected.'))
			.catch(err => {
				console.error('Connection error', err.stack)
				res.status(403).send(err);
			})

		const SQL_STATEMENT = `SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'public'
			ORDER BY table_name;`

		client
			.query(SQL_STATEMENT)
			.then((result) => {
				// rows: [ { table_name: 'mytble' } ],
				const tables = result.rows.map(table => table.table_name)
				// ["mytble"]
				res.json(tables)
			})
			.catch((err) => {
				console.log(err)
				res.status(403).send(err);
			})
			.then(() => client.end())
	});
}

exports.getTableSamplePostgres = async (req,res) => {
	cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

		const connection = req.body.connection
		const database = req.body.database;
		const table = req.body.table;
		const userId = req.body.uid
		const projectId = 'tart-90ca2';
		const region = 'us-central1';
		const keyRingId = 'cloudR-user-database';
		const keyId = 'database-login';
		const versionId = '1';

		const Firestore = require('@google-cloud/firestore');

		const firestore = new Firestore({
			projectId: projectId,
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const connections = await kms.getConnectionDocument(firestore, userId)

		const credentials = connections[connection]
		const host = credentials.host;
		const user = credentials.user;
		const encryptedPassword = credentials.password;
		const port = credentials.port;
		const ssl = credentials.ssl;
		const ciphertext = Buffer.from(encryptedPassword);

		const {KeyManagementServiceClient} = require('@google-cloud/kms');

		const clientKMS = new KeyManagementServiceClient({
			keyFilename: './tart-90ca2-e8da41e06847.json',
		});

		const password = await kms.decryptAsymmetric(
			clientKMS,
			ciphertext,
			projectId,
			region,
			keyRingId,
			keyId,
			versionId
		);

		const { Client } = require('pg')

    const client = new Client({
      host: host,
      port: parseInt(port),
      user: user,
      password: password,
			database: database,
      ssl: ssl,
    })

		client
			.connect()
			.then(() => console.log('Connected.'))
			.catch(err => {
				console.error('Connection error', err.stack)
				res.status(403).send(err);
			})

		const SQL_STATEMENT = `SELECT * FROM ${table} FETCH FIRST ${constants.maxRows} ROWS ONLY`

		client
			.query(SQL_STATEMENT)
			.then((result) => {
				// [
			  //   { air_flow: 80, water_temp: 27, acid_conc: 89, stack_loss: 42 },
			  //   { air_flow: 80, water_temp: 27, acid_conc: 88, stack_loss: 37 },
			  //   { air_flow: 75, water_temp: 25, acid_conc: 90, stack_loss: 37 },
			  //   { air_flow: 62, water_temp: 24, acid_conc: 87, stack_loss: 28 },
			  //   { air_flow: 62, water_temp: 22, acid_conc: 87, stack_loss: 18 },
			  //   { air_flow: 62, water_temp: 23, acid_conc: 87, stack_loss: 18 },
			  //   { air_flow: 62, water_temp: 24, acid_conc: 93, stack_loss: 19 },
			  //   { air_flow: 62, water_temp: 24, acid_conc: 93, stack_loss: 20 },
			  //   { air_flow: 58, water_temp: 23, acid_conc: 87, stack_loss: 15 },
			  //   { air_flow: 58, water_temp: 18, acid_conc: 80, stack_loss: 14 },
			  //   { air_flow: 58, water_temp: 18, acid_conc: 89, stack_loss: 14 },
			  //   { air_flow: 58, water_temp: 17, acid_conc: 88, stack_loss: 13 },
			  //   { air_flow: 58, water_temp: 18, acid_conc: 82, stack_loss: 11 },
			  //   { air_flow: 58, water_temp: 19, acid_conc: 93, stack_loss: 12 },
			  //   { air_flow: 50, water_temp: 18, acid_conc: 89, stack_loss: 8 },
			  //   { air_flow: 50, water_temp: 18, acid_conc: 86, stack_loss: 7 },
			  //   { air_flow: 50, water_temp: 19, acid_conc: 72, stack_loss: 8 },
			  //   { air_flow: 50, water_temp: 19, acid_conc: 79, stack_loss: 8 },
			  //   { air_flow: 50, water_temp: 20, acid_conc: 80, stack_loss: 9 },
			  //   { air_flow: 56, water_temp: 20, acid_conc: 82, stack_loss: 15 },
			  //   { air_flow: 70, water_temp: 20, acid_conc: 91, stack_loss: 15 }
			  // ]
				res.json(result.rows)
			})
			.catch((err) => {
				console.log(err)
				res.status(403).send(err);
			})
			.then(() => client.end())
	})
}
