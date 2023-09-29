const ecdtLibs = require('ecdt-libs');
const platAdminAccess = require('../services/adminDatabase/queries');
const jwt = require('jsonwebtoken');
const passwordService = require('../services/password');

const saldo = async (req, res) => {
  try {
    const cdCliente = jwt.decode(req.headers.authorization.split(' ')[1]).cd_cliente;
    const queryResult = await platAdminAccess.saldo(cdCliente);
    return res.status(200).send(queryResult);
  } catch (error) {
    console.log(error);
    return ecdtLibs.responseHelper.returnError(res, error);
  }
};

const generateBalance = async (req, res) => {
  try {
    const cdCliente = jwt.decode(req.headers.authorization.split(' ')[1]).cd_cliente;
    const queryResult = await platAdminAccess.generateBalance(cdCliente);
    return res.status(200).send(queryResult);
  } catch (error) {
    console.log(error);
    return ecdtLibs.responseHelper.returnError(res, error);

  }
}

const removeBalance = async (req, res) => {
  try {
    const cdCliente = jwt.decode(req.headers.authorization.split(' ')[1]).cd_cliente;
    const tpEvento = req.body.tpEvento;
    const amount = req.body.amount;
    const queryResult = await platAdminAccess.removeBalance(cdCliente, tpEvento, amount);
    return res.status(200).send(queryResult);
  } catch (error) {
    console.log(error);
    return ecdtLibs.responseHelper.returnError(res, error);
  }
}

const removeBalanceApi = async (req, res) => {
  try {
    const {
      cdCliente,
      password,
      amount
    } = req.body;
    if (!passwordService.isValidPassword(password, '/api/billing/remove-balance-api')) {
      console.log('- Password invalido enviado para /api/billing/remove-balance-api. Request body:', req.body);
      return res.status(401).send();
    }
    const queryResult = await platAdminAccess.removeBalance(cdCliente, 'export', amount);
    return res.status(200).send(queryResult);
  } catch (error) {
    console.log(error);
    return ecdtLibs.responseHelper.returnError(res, error);
  }
}

const returnBalance = async (req, res) => {
  try {
    const {
      cdCliente,
      tpEvento,
      amount,
      password
    } = req.body;
    if (!passwordService.isValidPassword(password, '/api/billing/return-balance')) {
      console.log('- Password invalido enviado para /api/billing/return-balance. Request body:', req.body);
      return res.status(401).send();
    }
    const queryResult = await platAdminAccess.returnBalance(cdCliente, tpEvento, amount);
    return res.status(200).send(queryResult);
  } catch (error) {
    console.log(error);
    return ecdtLibs.responseHelper.returnError(res, error);
  }
}

module.exports = {
  saldo,
  generateBalance,
  removeBalance,
  removeBalanceApi,
  returnBalance,
};
