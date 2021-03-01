// const oracledb = require('oracledb');
// const crypto = require('crypto')

async function test() {
	// Google Cloud KMS

	const projectId = 'tart-90ca2';
	const region = 'us-central1';
	const keyRingId = 'cloudR-user-database';
	const keyId = 'database-login';
	const versionId = '1';
	const userId = 'x9J4UnfNyDTfcPu4FilasMZdb1K3'
	//
	// const Firestore = require('@google-cloud/firestore');
	// const firestore = new Firestore({
	// 	projectId: projectId,
	// 	keyFilename: '/Users/eb/functionscloud/dataprocjs/tart-90ca2-e8da41e06847.json',
	// });
	//
	// async function getCredentials(firestore, uid) {
	// 	const document = firestore.collection('connections').doc(uid);
	// 	const doc = await document.get()
	//
	// 	return doc.data();
	// }
	//
	// const credentials = await getCredentials(firestore, userId)

	// const plaintextBuffer = Buffer.from('MyNewPass');
	// console.log(plaintextBuffer)
	//
	// const {KeyManagementServiceClient} = require('@google-cloud/kms');
	// const client = new KeyManagementServiceClient();
	// const versionName = client.cryptoKeyVersionPath(
	// 	projectId,
	// 	region,
	// 	keyRingId,
	// 	keyId,
	// 	versionId
	// );
	//
	// async function encryptAsymmetric() {
	//   // Get public key from Cloud KMS
	//   const [publicKey] = await client.getPublicKey({
	//     name: versionName,
	//   });
	//
	//   // Import and setup crypto
	//   const crypto = require('crypto');
	//   const ciphertextBuffer = crypto.publicEncrypt(
	//     {
	//       key: publicKey.pem,
	//       oaepHash: 'sha256',
	//       padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
	//     },
	//     plaintextBuffer
	//   );
	//
	//   console.log(`Ciphertext: ${ciphertextBuffer.toString('base64')}`);
	//   return ciphertextBuffer;
	// }
	//
	// const ciphertextBuffer = await encryptAsymmetric();
	//
	// console.log(ciphertextBuffer)
	//
	// async function decryptAsymmetric(client, versionName, ciphertext) {
	//   const [result] = await client.asymmetricDecrypt({
	// 		name: versionName,
	// 		ciphertext: ciphertext,
	//   });
	//   console.log(result.plaintext)
	//   const plaintext = result.plaintext.toString('utf8');
	//
	//   console.log(`Plaintext: ${plaintext}`);
	//   return plaintext;
	// }
	//
	// decryptAsymmetric(client, versionName, ciphertextBuffer)


	// Hash and salt

	// const salt = crypto.randomBytes(16)
	// var buf = Buffer.from(salter, 'hex');
	// crypto.scrypt('hGte87Vw4u', '18032e31b34ad2e290215b68d2c5cee4', 64, (err, derivedKey) => {
	// 	if (err) throw err
	// 	console.log(derivedKey.toString('hex'))
	// 	console.log(salt.toString('hex'))
	// })

	// let connection;
	//
	// const config = {
	// 	user:     'system',
	// 	password: 'oracle',
	// 	// connectString: 'localhost:1521/xe'
	// 	connectString: "(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST =" + "localhost" + ")(PORT =" + "1521" + "))(CONNECT_DATA =(SID=" + "xe" + ")))"
	// };
	//
	// try {
	// 	oracledb.initOracleClient({libDir: './lib/instantclient_19_3'})
	// 	connection = await oracledb.getConnection(config);
	//
	// 	// const SQL_STATEMENT = "SELECT table_name FROM user_tables ORDER BY table_name"
	// 	const SQL_STATEMENT = `SELECT * FROM ` + 'pit_pd'
	//
	// 	const result = await connection.execute(SQL_STATEMENT, [], {maxRows: 200});
	//
	// 	// const tables = result.rows.map(table => table[0])
	// 	const sample = {headers: result.metaData, rows:result.rows}
	//
	// 	console.log(JSON.stringify(sample))
	//
	// } catch(err) {
	// 	console.error(err)
	// } finally {
	// 	if (connection) {
	// 		try {
	// 			await connection.close();
	// 		} catch (err) {
	// 			console.error(err);
	// 		}
	// 	}
	// }

	// const mssql = require('mssql')
	// const config = {
	// 	server: 'localhost',
	// 	user: 'sa',
	// 	password: 'MyNewPass!',
	// 	database: 'SampleDB',
	// 	port: 1433,
	// };
  //
	// const SQL_STATEMENT = "CREATE TABLE ttc_pd (Rating_X NUMERIC(5,5), Rating_Y NUMERIC(5,5), Rating_Z NUMERIC(5,5))"
	// const SQL_STATEMENT = "SELECT name FROM master.sys.databases"
	// const SQL_STATEMENT = "Select name AS 'SampleDB' From sys.tables"
	// const SQL_STATEMENT = "SELECT a.TABLE_CATALOG as 'Database', a.TABLE_SCHEMA as 'Schema', a.TABLE_NAME as 'Table Name', a.TABLE_TYPE as 'Table Type' FROM INFORMATION_SCHEMA.TABLES a"
	// const SQL_STATEMENT = "SELECT TOP 200 * FROM " + "TestSchema.pit_pd"
	// const SQL_STATEMENT = "INSERT INTO ttc_pd (Rating_X, Rating_Y, Rating_Z) VALUES (0.0806,0.375,0.0526);"
  //
	// mssql.connect(config, function(err) {
	// 	if (err) {
	// 		console.log(err)
	// 	} else {
	// 		var request = new mssql.Request();
	// 		request.query(SQL_STATEMENT, function(err, result) {
	// 			if (err) {
	// 				console.log(err)
	// 			} else {
	// 				console.log(result.recordset)
	// 				// let tables = result.recordset.map(table => table['SampleDB'])
	// 				// console.log(tables)
	// 			}
	// 		});
	// 	}
	// });
  // const mysql = require('mysql')
  // const { URL } = require('url')
  //
  // const url = 'mysql://root:MyNewPass!@localhost:3306/dev2qa'
  // const ca = ''
  // // const ca = '/Users/eb/Library/Application Support/MySQL/Workbench/certificates/39CE5AF8-2B8A-45E2-ABC9-02B11DFBFF2E/ca-cert.pem'
  // const cert = '/Users/eb/Library/Application Support/MySQL/Workbench/certificates/39CE5AF8-2B8A-45E2-ABC9-02B11DFBFF2E/server-cert.pem'
  // const key = '/Users/eb/Library/Application Support/MySQL/Workbench/certificates/39CE5AF8-2B8A-45E2-ABC9-02B11DFBFF2E/server-key.pem'
  //
  // const getDatabaseConnectionConfig = () => {
  //   // Parse a database url, and destructure the result.
  //   // The database url should be similar to this:
  //   // mysql://root:somepassword@127.0.0.1:3306/database-name
  //   const {
  //     username: user,
  //     password,
  //     port,
  //     hostname: host,
  //     pathname = ''
  //   } = new URL(url)
  //   // Prepare connection configuration for mysql.
  //   return {
  //     user,
  //     password,
  //     host,
  //     port,
  //     database: pathname.replace('/', '')
  //   }
  // }
  //
  // const getSSLConfiguration = () => {
  //   if (!ca || !cert || !key) {
  //     return {}
  //   }
  //   return {
  //     ssl: {
  //       // This is an important step into making the keys work. When loaded into
  //       // the environment the \n characters will not be actual new-line characters.
  //       // so the .replace() calls fixes that.
  //       ca: ca.replace(/\\n/g, '\n'),
  //       cert: cert.replace(/\\n/g, '\n'),
  //       key: key.replace(/\\n/g, '\n')
  //     }
  //   }
  // }
  //
  // const con = mysql.createConnection({
  //   ...getDatabaseConnectionConfig(),
  //   ...getSSLConfiguration()
	// })
  //
  // con.connect(async err => {
  //   console.log('Connected.')
  // })
  //
	// con.end()
}

test()
