import { Config } from "./config";
import { Sequelize } from 'sequelize';
import { env } from "process";

const SQLiteDDLParser = require('sqlite-ddl-parser');

export function connect(config: Config): Sequelize {
  console.log('connect', env, config);
  if(env.DB_ALLOW_LIST != null) {
    if(!env.DB_ALLOW_LIST.split(',').includes(config.db)) {
      throw new Error(`Database ${config.db} is not present in DB_ALLOW_LIST 😭`);
    }
  }
  const db = new Sequelize({
    dialect: 'sqlite',
    storage: config.db
  });

  return db;
};
