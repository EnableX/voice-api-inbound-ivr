// core modules
const fs = require('fs');
const http = require('http');
const https = require('https');
// modules installed from npm
const { EventEmitter } = require('events');
const express = require('express');
const bodyParser = require('body-parser');
const { createDecipher } = require('crypto');
const { connect } = require('ngrok');
require('dotenv').config();
const _ = require('lodash');
// application modules
const logger = require('./logger');
const {
  ivrVoiceCall, hangupCall, acceptCall,
} = require('./voiceapi');

// Express app setup
const app = express();
const eventEmitter = new EventEmitter();

let server;
let webHookUrl;
let activeCall = false;
let retrycount = 0;
let ttsPlayVoice = 'female';
let digitCollected = false;
const call = {};
const sseMsg = [];
const servicePort = process.env.SERVICE_PORT || 3000;
const redirect_number = process.env.REDIRECT_NUMBER;

// Handle error generated while creating / starting an http server
function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }

  switch (error.code) {
    case 'EACCES':
      logger.error(`Port ${servicePort} requires elevated privileges`);
      process.exit(1);
      break;
    case 'EADDRINUSE':
      logger.error(`Port ${servicePort} is already in use`);
      process.exit(1);
      break;
    default:
      throw error;
  }
}

// shutdown the node server forcefully
function shutdown() {
  server.close(() => {
    logger.error('Shutting down the server');
    process.exit(0);
  });
  setTimeout(() => {
    process.exit(1);
  }, 10000);
}

// exposes web server running on local machine to the internet
// @param - web server port
// @return - public URL of your tunnel
function createNgrokTunnel() {
  server = app.listen(servicePort, () => {
    console.log(`Server running on port ${servicePort}`);
    (async () => {
      try {
        webHookUrl = await connect({ proto: 'http', addr: servicePort });
        console.log('ngrok tunnel set up:', webHookUrl);
      } catch (error) {
        console.log(`Error happened while trying to connect via ngrok ${JSON.stringify(error)}`);
        shutdown();
        return;
      }
      webHookUrl += '/event';
      console.log(`To call webhook while inbound calls, Update this URL in portal: ${webHookUrl}`);
    })();
  });
}

// Set webhook event url
function setWebHookEventUrl() {
  logger.info(`Listening on Port ${servicePort}`);
  webHookUrl = `${process.env.PUBLIC_WEBHOOK_HOST}/event`;
  logger.info(`To call webhook while inbound calls, Update this URL in portal: ${webHookUrl}`);
}

// create and start an HTTPS node app server
// An SSL Certificate (Self Signed or Registered) is required
function createAppServer() {
  if (process.env.LISTEN_SSL) {
    const options = {
      key: fs.readFileSync(process.env.CERTIFICATE_SSL_KEY).toString(),
      cert: fs.readFileSync(process.env.CERTIFICATE_SSL_CERT).toString(),
    };
    if (process.env.CERTIFICATE_SSL_CACERTS) {
      options.ca = [];
      options.ca.push(fs.readFileSync(process.env.CERTIFICATE_SSL_CACERTS).toString());
    }
    // Create https express server
    server = https.createServer(options, app);
  } else {
    // Create http express server
    server = http.createServer(app);
  }
  app.set('port', servicePort);
  server.listen(servicePort);
  server.on('error', onError);
  server.on('listening', setWebHookEventUrl);
}

/* Initializing WebServer */
if (process.env.ENABLEX_APP_ID && process.env.ENABLEX_APP_KEY) {
  if (process.env.USE_NGROK_TUNNEL === 'true' && process.env.USE_PUBLIC_WEBHOOK === 'false') {
    createNgrokTunnel();
  } else if (process.env.USE_PUBLIC_WEBHOOK === 'true' && process.env.USE_NGROK_TUNNEL === 'false') {
    createAppServer();
  } else {
    logger.error('Incorrect configuration - either USE_NGROK_TUNNEL or USE_PUBLIC_WEBHOOK should be set to true');
  }
} else {
  logger.error('Please set env variables - ENABLEX_APP_ID, ENABLEX_APP_KEY');
}

process.on('SIGINT', () => {
  logger.info('Caught interrupt signal');
  shutdown();
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static('client'));

// It will send stream / events all the events received from webhook to the client
app.get('/event-stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  const id = (new Date()).toLocaleTimeString();

  setInterval(() => {
    if (!_.isEmpty(sseMsg[0])) {
      const data = `${sseMsg[0]}`;
      res.write(`id: ${id}\n`);
      res.write(`data: ${data}\n\n`);
      sseMsg.pop();
    }
  }, 100);
});

// Webhook event which will be called by EnableX server once an outbound call is made
// It should be publicly accessible. Please refer document for webhook security.
app.post('/event', (req, res) => {
  logger.info('called');
  let jsonObj;
  if (req.headers['x-algoritm'] !== undefined) {
    const key = createDecipher(req.headers['x-algoritm'], process.env.ENABLEX_APP_ID);
    let decryptedData = key.update(req.body.encrypted_data, req.headers['x-format'], req.headers['x-encoding']);
    decryptedData += key.final(req.headers['x-encoding']);
    jsonObj = JSON.parse(decryptedData);
    logger.info(JSON.stringify(jsonObj));
  } else {
    jsonObj = req.body;
    logger.info(JSON.stringify(jsonObj));
  }

  res.send();
  res.status(200);
  eventEmitter.emit('voicestateevent', jsonObj);
});

// Call is completed / disconneted, inform server to hangup the call
function timeOutHandler() {
  logger.info(`[${call.voice_id}] Disconnecting the call`);
  hangupCall(call.voice_id, () => {});
  shutdown();  
}

/* WebHook Event Handler function */
function voiceEventHandler(voiceEvent) {
  call.voice_id = voiceEvent.voice_id;
  if(voiceEvent.state != undefined) {
	if (voiceEvent.state === 'incomingcall') {
		activeCall = true;
		call.voice_id = voiceEvent.voice_id;
		call.to = voiceEvent.to;
		const eventMsg = `[${call.voice_id}] Received an inbound Call`;
		logger.info(eventMsg);
		sseMsg.push(eventMsg);
		acceptCall(call.voice_id, () => {});
	} else if (voiceEvent.state && voiceEvent.state === 'disconnected') {
		const eventMsg = `[${call.voice_id}] Call is disconnected`;
		logger.info(eventMsg);
		sseMsg.push(eventMsg);
	} else if (voiceEvent.state && voiceEvent.state === 'connected') {
		const eventMsg = `[${call.voice_id}] Call is connected`;
		logger.info(eventMsg);
		sseMsg.push(eventMsg);
	}
  }
  
  if (voiceEvent.playstate !== undefined) {
    if (voiceEvent.playstate === 'playfinished') {
       console.log ("["+call.voice_id+"] Playing voice menu");
       ivrVoiceCall(call.voice_id, ttsPlayVoice, "Please press one for sales, two for enquiry, three for accounts", 'voice_menu', () => {});
       ++retrycount;
    } else if (voiceEvent.playstate === 'menutimeout'){
       console.log ("["+call.voice_id+"]  menutimeout received");
      if (voiceEvent.prompt_ref === 'timeout') {
        if(retrycount === 3) {
          console.log("["+call.voice_id+"] max retry reached" );
          ivrVoiceCall(call.voice_id, ttsPlayVoice, "I am sorry you have reached maximum retries", 'maxretryreach', () => {});
        } else if(voiceEvent.prompt_ref === 'voice_menu') {
          console.log ("["+call.voice_id+"] Playing menu timeout prompt");
          ivrVoiceCall(call.voice_id, ttsPlayVoice, "Please press a digit to continue", 'timeout', () => {});
        } else {
            ++retrycount;
            console.log ("["+call.voice_id+"] Playing voice menu again");
            ivrVoiceCall(call.voice_id, ttsPlayVoice, "Please press one for sales, two for enquiry, three for accounts", 'voice_menu', () => {});
        }
      } else if(voiceEvent.prompt_ref ==='maxretryreach') {
        console.log("["+call.voice_id+"] max retry reached");
        setTimeout(timeOutHandler, 1000);
      } else {
        console.log ("["+call.voice_id+"] Playing menu timeout prompt");
        ivrVoiceCall(call.voice_id, ttsPlayVoice, "Please press a digit to continue", 'timeout', () => {});
      }
    } else if(voiceEvent.playstate === 'digitcollected' && voiceEvent.prompt_ref === 'voice_menu') {
      console.log ("["+call.voice_id+"] voice menu digit collected =>  " + voiceEvent.digit + " disconnecting the call");
      setTimeout(timeOutHandler, 1000);
    } else if(voiceEvent.playstate && voiceEvent.playstate === 'initiated') {
      console.log ("["+call.voice_id+"] Play initiated, [play_id] " + voiceEvent.play_id);
    } else {
      console.log ("["+call.voice_id+"] Unknown event received");
    }
  }
}

/* Registering WebHook Event Handler function */
eventEmitter.on('voicestateevent', voiceEventHandler);
