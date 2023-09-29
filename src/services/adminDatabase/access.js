const pg = require('pg');

const DB_HOST = process.env.DB_HOST;
const DB_DATABASE = process.env.DB_DATABASE;
const DB_USER = process.env.DB_USER;
const DB_PASSWORD = process.env.DB_PASSWORD;
const DB_PORT = process.env.DB_PORT;

const poolDB = new pg.Pool({
  database: DB_DATABASE,
  user: DB_USER,
  password: DB_PASSWORD,
  host: DB_HOST,
  port: DB_PORT,
  max: 10,
  min: 0,
  idleTimeoutMillis: 120000,
  connectionTimeoutMillis: 10000,
});

pg.types.setTypeParser(pg.types.builtins.INT8, parseInt);

const executeQuery = (consulta, params = []) => {
  return poolDB.connect()
    .then(client => {
      return client.query(consulta, params)
        .then(result => {
          client.release(true);
          return result.rows;
        })
        .catch(error => {
          client.release(true);
          console.log('executeQuery safea.db', error);
          return Promise.reject(error);
        });
    });
};

const getConnection = () => {
  return poolDB.connect();
}

module.exports = {
  executeQuery, getConnection
};
