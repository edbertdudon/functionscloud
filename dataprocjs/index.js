//
//  dataprocjs.js
//  Tart
//
//  Created by Edbert Dudon on 7/8/19.
//  Copyright © 2019 Project Tart. All rights reserved.
//
//  Deploy:
//  gcloud functions deploy canceljob --runtime nodejs10 --trigger-http --allow-unauthenticated --timeout=540s
//
// const cors = require('cors')({origin: 'https://tart-90ca2.firebaseapp.com'});
// const cors = require('cors')({origin: 'https://www.tartcl.com'});
const cors = require('cors')({origin: '*'});
const admin = require('firebase-admin');
admin.initializeApp({
	credential: admin.credential.cert('./tart-90ca2-firebase-adminsdk-kf272-41cfb98e9e.json'),
	databaseURL: 'https://tart-90ca2.firebaseio.com'
})

exports.authenticate = async (req, res) => {
	let idToken;
	if (req.headers.authorization && req.headers.authorization.startsWith('Bearer ')) {
		console.log('Found "Authorization" header');
		// Read the ID Token from the Authorization header.
		idToken = req.headers.authorization.split('Bearer ')[1];
	} else if(req.cookies) {
		console.log('Found "__session" cookie');
		// Read the ID Token from cookie.
		idToken = req.cookies.__session;
	} else {
		// No cookie
		res.status(403).send('Unauthorized');
		return false;
	}

	try {
		const decodedIdToken = await admin.auth().verifyIdToken(idToken);
		// console.log('ID Token correctly decoded', decodedIdToken);
		req.user = decodedIdToken;
		return true;
	} catch (error) {
		console.error('Error while verifying Firebase ID token:', error);
		res.status(403).send('Unauthorized');
		return false;
	}
}

async function listJobs(jobClient, projectId, region, clusterName) {

  const request = {
    projectId: projectId,
    region: region,
    clusterName: clusterName,
  };

  const [operation] = await jobClient.listJobs(request)
  return operation
}

async function getCluster(clusterClient, projectId, region, clusterName) {
  const request = {
    projectId: projectId,
    region: region,
    clusterName: clusterName,
  };

  const [operation] = await clusterClient.getCluster(request)
  return operation
}

async function getJob(jobClient, projectId, region, jobId) {

  const request = {
    projectId: projectId,
    region: region,
    jobId: jobId,
  };

  const [operation] = await jobClient.getJob(request)
  return operation
}

async function cancelJob(jobClient, projectId, region, jobId) {

  const request = {
    projectId: projectId,
    region: region,
    jobId: jobId,
  };

  const operation = await jobClient.cancelJob(request)
  return operation
}

// goog.require('proto.google.protobuf.Duration')
// const Duration = proto.google.protobuf.Duration()
// const Duration = require('google-protobuf/google/protobuf/duration_pb')

// exports.createcluster = async (req, res) => {
//   cors(req, res, async () => {
//     const dataproc = require('@google-cloud/dataproc');
//     const projectId = 'tart-90ca2'
//     const region = 'us-central1'
//     const clusterName = 'cluster-' + req.body.authuser
//     // const duration_message = new duration.Duration()
//     const duration_message = Duration.setSeconds(10800)
//     const status = {status: 'job submitted'}

//     // Create a cluster client with the endpoint set to the desired cluster region
//     const clusterClient = new dataproc.v1beta2.ClusterControllerClient({
//       apiEndpoint: `${region}-dataproc.googleapis.com`,
//       keyFilename: './tart-90ca2-9d37c42ef480.json',
//     });

//     async function createcluster() {

//       const cluster = {
//         projectId: projectId,
//         region: region,
//         cluster: {
//           clusterName: clusterName,
//           config: {
//             masterConfig: {
//               numInstances: 1,
//               machineTypeUri: 'n1-highmem-2',
//               diskConfig: {
//                 bootDiskSizeGb: 20
//               },
//             },
//             workerConfig: {
//               numInstances: 2,
//               machineTypeUri: 'n1-highmem-2',
//               diskConfig: {
//                 bootDiskSizeGb: 20
//               },
//             },
//             lifecycleConfig: {
//               idleDeleteTtl: duration_message
//             },
//             initializationActions: [{
//               executableFile: 'gs://tart-90ca2.appspot.com/scripts/initializationScript.sh'
//             }]
//           },
//         },
//       };

//       const [operation] = await clusterClient.createCluster(cluster)
//       const [response] = await operation.promise()

//       console.log(`Cluster created successfully: ${response.clusterName}`);
//     }

//     await createcluster().catch(err => {
//       console.log(err)
//       status = {status: 'cluster is active'}
//     })

//     // job arguments
//     const worksheet = req.body.worksheet
//     const jobFileArgument = req.body.jobFileArgument
//     const jobFilePath = req.body.jobFilePath
//     const jobFileSave = req.body.jobFileSave

//     // Create a job client with the endpoint set to the desired cluster region
//     const jobClient = new dataproc.v1beta2.JobControllerClient({
//       apiEndpoint: `${region}-dataproc.googleapis.com`,
//       keyFilename: './tart-90ca2-9d37c42ef480.json',
//     });

//     async function submitjob() {

//       const job = {
//         projectId: projectId,
//         region: region,
//         job: {
//           placement: {
//             clusterName: clusterName,
//           },
//           sparkRJob: {
//             mainRFileUri: jobFilePath,
//             args: [
//               jobFileArgument,
//               jobFileSave
//             ]
//           },
//           labels: {
//             "worksheet": worksheet
//           }
//         },
//       };

//       let [jobResp] = await jobClient.submitJob(job);
//       const jobId = jobResp.reference.jobId;

//       console.log(`Submitted job "${jobId}".`);
//     }

//     submitjob().catch(err => {
//       console.log(err)
//       status = {status: 'failed job'}
//     })

//     res.status(200).send(status)
//   })
// };

exports.canceljob = async (req, res) => {
  cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

    const dataproc = require('@google-cloud/dataproc');
    const sleep = require('sleep')
    const projectId = 'tart-90ca2'
    const region = 'us-central1'
    const clusterName = 'cluster-' + req.body.authuser
    let jobId = req.body.jobId;
    const filename = req.body.filename

    // Create a job client with the endpoint set to the desired cluster region
    const jobClient = new dataproc.v1.JobControllerClient({
      apiEndpoint: `${region}-dataproc.googleapis.com`,
			keyFilename: './tart-90ca2-e8da41e06847.json',
    });

      // if job needs to be cancelled before listJobs replaces 'filler job'
    if (jobId.startsWith('filler job')) {

      const clusterClient = new dataproc.v1beta2.ClusterControllerClient({
        apiEndpoint: `${region}-dataproc.googleapis.com`,
				keyFilename: './tart-90ca2-e8da41e06847.json',
      });

      let clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
        .catch((err) => {
          console.log('Cluster not found.')

          return('failed retrieve cluster')
        })

      if (clusterResp !== 'failed retrieve cluster') {

        console.log('Waiting for cluster creation...')

        const terminalStates = new Set(['RUNNING', 'STOPPED', 'ERROR', 'DELETING', 'STOPPING'])

        while (!terminalStates.has(clusterResp.status.state)) {
          await sleep.sleep(15);
          clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
            .catch((err) => {
              return('failed retrieve cluster')
            })
        }

        console.log('Cluster created.')
      }

      // Wait for submitJob to execute before cancelling
      const queueStates = new Set(['PENDING', 'SETUP_DONE', 'RUNNING'])

      let jobList
      let filenameList = []
      let isJobInQueue = false

      while(!filenameList.includes(filename) || !isJobInQueue) {
        await sleep.sleep(1);
        jobList = await listJobs(jobClient, projectId, region, clusterName)
          .catch((err) => {
            return([{status: {state: 'failed list jobs'}}])
          })

        // Error because not failed list jobs but just empty []
        if (jobList.length != 0) {
          if (jobList[0].status.state !== 'failed list jobs') {
            filenameList = []

            jobList.forEach(job => {
              filenameList.push(job.labels.worksheet)

              if (queueStates.has(job.status.state)) {
                isJobInQueue = true

                if (job.labels.worksheet === filename) {
                  jobId = job.reference.jobId
                }
              }
            });
          }
        }
      }

      console.log(`Replaced filler job ID with "${jobId}".`);
    }

    let jobResp = await cancelJob(jobClient, projectId, region, jobId)
    .catch((err) => {
      return([{status: {state: 'job not found'}}])
    })

    // For cases where user instantly cancels job before createandsubmit gets to finish running
    if (jobResp[0].status.state === 'job not found') {

      jobResp = await getJob(jobClient, projectId, region, jobId)
        .catch((err) => {
          return([{status: {state: 'job not found'}}])
        })

      if (jobResp[0].status.state === 'job not found') {

        const terminalStates = new Set(['RUNNING', 'DONE', 'ERROR', 'CANCELLED'])

        while (!terminalStates.has(jobResp[0].status.state)) {
          await sleep.sleep(1);
          jobResp = await getJob(jobClient, projectId, region, jobId)
            .catch((err) => {
              return([{status: {state: 'job not found'}}])
            })
        }
      }

      await cancelJob(jobClient, projectId, region, jobId)
        .catch((err) => {
          console.log('Job not found')
        })
    }

    console.log(`Cancelled job "${jobId}".`);
  })
};

exports.listjobs = async (req,res) => {
  cors(req, res, async () => {
		const isAuthenticated = admin.authenticate(req, res);
		if (!isAuthenticated) return;

    const dataproc = require('@google-cloud/dataproc');
    const sleep = require('sleep')
    const projectId = 'tart-90ca2'
    const region = 'us-central1'
    const clusterName = 'cluster-' + req.body.authuser

    // Create a job client with the endpoint set to the desired cluster region
    const jobClient = new dataproc.v1beta2.JobControllerClient({
      apiEndpoint: `${region}-dataproc.googleapis.com`,
			keyFilename: './tart-90ca2-e8da41e06847.json',
    });

    let jobResp = await listJobs(jobClient, projectId, region, clusterName)
      .catch((err) => {
        return([{status:'failed list jobs'}])
      })

    if (jobResp[0].status === 'failed list jobs') {
      const clusterClient = new dataproc.v1beta2.ClusterControllerClient({
        apiEndpoint: `${region}-dataproc.googleapis.com`,
				keyFilename: './tart-90ca2-e8da41e06847.json',
      });

      let clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
        .catch((err) => {
          console.log('Cluster not found.')

          return('failed retrieve cluster')
        })

      if (clusterResp !== 'failed retrieve cluster') {

        console.log('Waiting for cluster creation...')

        const terminalStates = new Set(['RUNNING', 'STOPPED', 'ERROR', 'DELETING', 'STOPPING'])

        while (!terminalStates.has(clusterResp.status.state)) {
          await sleep.sleep(15);
          clusterResp = await getCluster(clusterClient, projectId, region, clusterName)
            .catch((err) => {
              return('failed retrieve cluster')
            })
        }

        console.log('Cluster created.')

        jobResp = await listJobs(jobClient, projectId, region, clusterName)
          .catch((err) => {
            return([{status:'failed list jobs'}])
          })
      }
    }

    res.setHeader('Content-Type', 'application/json');
    res.status(200).send(jobResp);
  })
}

const utility = require('./utility');

exports.moveToTrash = utility.moveToTrash;
exports.moveToWorksheets = utility.moveToWorksheets;
exports.renameWorksheet = utility.renameWorksheet;

const mysql = require('./mysql');

exports.connectMySql = mysql.connectMySql;
exports.listDatabasesMySql = mysql.listDatabasesMySql;
exports.listTablesMySql = mysql.listTablesMySql;
exports.getTableSampleMySql = mysql.getTableSampleMySql;

const mssql = require('./mssql');

exports.connectMsSql = mssql.connectMsSql;
exports.listDatabasesMsSql = mssql.listDatabasesMsSql;
exports.listTablesMsSql = mssql.listTablesMsSql;
exports.getTableSampleMsSql = mssql.getTableSampleMsSql;

const oracledb = require('./oracledb');

exports.connectOracledb = oracledb.connectOracledb;
exports.listDatabasesOracledb = oracledb.listDatabasesOracledb;
exports.listTablesOracledb = oracledb.listTablesOracledb;
exports.getTableSampleOracledb = oracledb.getTableSampleOracledb;

const postgres = require('./postgres');

exports.connectPostgres = postgres.connectPostgres;
exports.listDatabasesPostgres = postgres.listDatabasesPostgres;
exports.listTablesPostgres = postgres.listTablesPostgres;
exports.getTableSamplePostgres = postgres.getTableSamplePostgres;
