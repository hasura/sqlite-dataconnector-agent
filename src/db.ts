import { Config } from "./config";
import { Sequelize } from 'sequelize';

const SQLiteDDLParser = require('sqlite-ddl-parser');

export function connect(config: Config): Sequelize {
  const db = new Sequelize({
    dialect: 'sqlite',
    storage: config.db
  });

  return db;
};
