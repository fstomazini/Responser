require('dotenv').config();

const bodyParser = require('body-parser');
const express = require('express');

const health = require('@cloudnative/health-connect');
const fs = require("fs");
const https = require("https");
const http = require("http");
let healthChecker = new health.HealthChecker();

const app = express();

// Create express router
const router = express.Router();


const extratoController = require('./controllers/extrato.controller');
const dashboardController = require('./controllers/dashboard.controller');

app.get('/health', health.HealthEndpoint(healthChecker));

app.get('/verificar', dashboardController.procurador);


const port = process.env.PORT || 80;
app.set('port', port);

const server = http.createServer(app);

app.use(bodyParser.json());

app.use('/', router);

server.listen(port, () => {
  console.log(`Listening on port ${port}...`);
});
server.timeout = 1000000;
