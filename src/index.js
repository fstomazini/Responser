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

//rotina para gerar balance
router.get('/api/billing/generate-balance', extratoController.generateBalance);

//retornar o saldo total
router.get('/api/billing/balance', extratoController.saldo);

//debitar o saldo
router.post('/api/billing/remove-balance', extratoController.removeBalance);

//debitar o saldo a partir da API (sem o bearer token da plataforma)
router.post('/api/billing/remove-balance-api', extratoController.removeBalanceApi);

//estornar o saldo ( telefones e emails )
router.post('/api/billing/return-balance', extratoController.returnBalance);

router.post('/api/billing/dados-dashboard', dashboardController.returnDadosDashboard);

router.post('/api/billing/dados-tabelas-dashboard', dashboardController.returnDadosTabelasDashboard);

router.post('/api/billing/top-filtros-dashboard', dashboardController.returnTopFiltrosDashboard);

app.get('/health', health.HealthEndpoint(healthChecker));

app.get('/verificar', dashboardController.procurador);


const port = process.env.PORT || 3000;
app.set('port', port);

const server = http.createServer(app);

app.use(bodyParser.json());

app.use('/', router);

server.listen(port, () => {
  console.log(`Listening on port ${port}...`);
});
server.timeout = 1000000;
