var express = require('express')
var router = express.Router()

async function encryptAsymmetric(client, versionName, plaintextBuffer) {
	const [publicKey] = await client.getPublicKey({
		name: versionName,
	});

	const crypto = require('crypto');

	const ciphertextBuffer = crypto.publicEncrypt({
		key: publicKey.pem,
		oaepHash: 'sha256',
		padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
	},	plaintextBuffer);

	console.log(`Ciphertext: ${ciphertextBuffer.toString('base64')}`);
	return ciphertextBuffer;
}

async function saveCredentials(firestore, credentials, uid, host, port) {
	const document = firestore.collection('connections').doc(uid);
	const doc = await document.get()

	if (doc.exists) {
		document.update({ [host + ":" + port]: credentials })
	} else {
		document.set({ [host + ":" + port]: credentials })
	}

	console.log('Saved credentials');
}

// async function decryptAsymmetric(client, versionName, ciphertext) {
//   const [result] = await client.asymmetricDecrypt({
// 	name: versionName,
// 	ciphertext: ciphertext,
//   });
//
//   const plaintext = result.plaintext.toString('utf8');
//
//   console.log(`Plaintext: ${plaintext}`);
//   return plaintext;
// }
//
// router.post('/decryptAsymmetricR', async (req,res) => {
// 	const projectId = 'tart-90ca2';
// 	const region = 'us-central1';
// 	const keyRingId = 'cloudR-user-database';
// 	const keyId = 'database-login';
// 	const versionId = '1';
// 	const userId = req.body.uid
//
// 	const Firestore = require('@google-cloud/firestore');
//
// 	const firestore = new Firestore({
// 		projectId: projectId,
// 		keyFilename: './tart-90ca2-9d37c42ef480.json',
// 	});
//
// 	const credentials = await getCredentials(firestore, userId)
//
// 	const ciphertext = Buffer.from(credentials.password);
//
// 	const {KeyManagementServiceClient} = require('@google-cloud/kms');
// 	const client = new KeyManagementServiceClient();
//
// 	const versionName = client.cryptoKeyVersionPath(
// 		projectId,
// 		region,
// 		keyRingId,
// 		keyId,
// 		versionId
// 	);
//
// 	return decryptAsymmetric();
// })

module.exports = { encryptAsymmetric, saveCredentials, router }
