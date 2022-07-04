import { SchemaResponse, ScalarType, Table, ColumnInfo } from "./types/schema"
import { Config } from "./config";

import { Sequelize } from 'sequelize';

const SQLiteDDLParser = require('sqlite-ddl-parser');

type columnT = {name: string, type: string, notNull: boolean, unique: boolean}

type columnOutT = {name: string, type: string, nullable: boolean, description?: string}

type ddlT = {tables: [
  { name: string,
    columns: [columnT],
    primaryKeys: [string]}]
  }

type resultTT = {name: string, type: string, tbl_name: string, rootpage: Number, sql: string}
type resultT = resultTT & {ddl: ddlT}

function getpks(x : ddlT) : ({ primary_keys: Array<string>}) {
  if(x.tables.length > 0) {
    const t = x.tables[0];
    if(t.primaryKeys.length > 0) {
      return {primary_keys: t.primaryKeys}
    }
  }
  return {primary_keys: []};
}

function columnCast(c: string): ScalarType {
  switch(c) {
    case "string":
    case "number":
    case "bool":
      return c as ScalarType;
    case "integer": return ScalarType.Number
    case "double": return ScalarType.Number
    case "float": return ScalarType.Number
    case "text": return ScalarType.String
    default:
      return ScalarType.String
      // throw new Error(`Couldn't decode SQLite column type 😭 Unexpected value: ${c}`) 
  }
}

function getcols(x : ddlT) : Array<ColumnInfo> {
  return x.tables.flatMap(t =>
    t.columns.map((c) => {
      console.log(t)
      console.log(c)
      return ({
        name: c.name,
        type: columnCast(c.type), // TODO: This cast is dubious
        nullable: (!c.notNull)
      })
    })
  )
}

function format(x : resultT): Table {
  return {
    name: x.name,
    ...getpks(x.ddl),
    description: x.sql,
    columns: getcols(x.ddl)
  }
}

function isMeta(n : string) {
  return /^(sqlite_|IFK_)/.test(n);
}

function includeTable(config: Config, table: Table): boolean {
  if(config.tables === null) {
    if(isMeta(table.name) && ! config.meta) {
      return false;
    }
    return true;
  } else {
    return config.tables.indexOf(table.name) >= 0
  }
}

export async function getSchema(config: Config): Promise<SchemaResponse> {
  const db = new Sequelize({
    dialect: 'sqlite',
    storage: config.db
  });

  const [results, metadata] = await db.query("SELECT * from sqlite_schema");
  const resultsT = results as unknown as Array<resultTT>;
  const withDDL = resultsT.map(e => ({ddl: SQLiteDDLParser.parse(e.sql) as ddlT, ...e}) );

  return {
    tables: withDDL.map(format).filter(table => includeTable(config,table))
  };
};
