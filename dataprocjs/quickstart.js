// const duration = require('google-protobuf/google/protobuf/duration_pb.js')
// const mysql = require('mysql')

function main(projectId, region, clusterName) {
	// const duration_message = new duration.Duration()
	// duration_message.fromSeconds()
	// console.log(typeof duration_message)

	const {OAuth2Client} = require('google-auth-library');
	const keys = require('./tart-90ca2-9d37c42ef480.json')
	const client = new OAuth2Client(CLIENT_ID);
	async function verify() {
		const ticket = await client.verifyIdToken({
			idToken: token,
			audience: keys.web.client_id
		});
		const payload = ticket.getPayload();
		const userid = payload['sub'];
		// If request specified a G Suite domain:
		// const domain = payload['hd'];
	}
	verify().catch(console.error);

}

main('tart-90ca2', 'us-central1', 'cluster-x9J4UnfNyDTfcPu4FilasMZdb1K3','cloudrun-r-sparkR-test' ,'gs://tart-90ca2.appspot.com/scripts/sparkR.R', "user/x9J4UnfNyDTfcPu4FilasMZdb1K3/Untitled Worksheet 2")

// const url = "jdbc:mysql://localhost:3306/dev2qa"
// const user = "asdf"
// const password = "1234"
// const database = "qwerty"
// const con = mysql.createConnection({
// 	host: url.host,
// 	user: user,
// 	password: password,
// 	database: database,
// 	port: url.port,
// })
