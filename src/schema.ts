import { SchemaResponse, ScalarType, ColumnInfo, TableInfo } from "./types"
import { Config } from "./config";
import { connect } from './db';

const SQLiteDDLParser = require('sqlite-ddl-parser');

type ColumnInfoInternal = {
  name: string,
  type: string,
  notNull: boolean,
  unique: boolean
}

type DDL_Info = {
  tables: [
    { name: string,
      columns: [ColumnInfoInternal],
      primaryKeys: [string]
    }
  ]
}

type TableInfoInternal = {
  name: string,
  type: string,
  tbl_name: string,
  rootpage: Number,
  sql: string
}

type TableInfoInternalWithDDL = TableInfoInternal & { ddl: DDL_Info }

function getPKs(info : DDL_Info) : ({ primary_keys: Array<string>}) {
  if(info.tables.length > 0) {
    const t = info.tables[0];
    if(t.primaryKeys.length > 0) {
      return {primary_keys: t.primaryKeys}
    }
  }
  return {primary_keys: []};
}

/**
 * 
 * @param ColumnInfoInternalype as per HGE DataConnector IR
 * @returns SQLite's corresponding column type
 * 
 * Note: This defaults to "string" when a type is not anticipated
 *       in order to be as permissive as possible but logs when
 *       this happens.
 */
function columnCast(ColumnInfoInternalype: string): ScalarType {
  switch(ColumnInfoInternalype) {
    case "string":
    case "number":
    case "bool":    return ColumnInfoInternalype as ScalarType;
    case "boolean": return "bool";
    case "numeric": return "number";
    case "integer": return "number";
    case "double":  return "number";
    case "float":   return "number";
    case "text":    return "string";
    default:
      console.log(`Unknown SQLite column type: ${ColumnInfoInternalype}. Interpreting as string.`) 
      return "string";
  }
}

function getColumns(info : DDL_Info) : Array<ColumnInfo> {
  return info.tables.flatMap(t =>
    t.columns.map((c) => {
      return ({
        name: c.name,
        type: columnCast(c.type),
        nullable: (!c.notNull)
      })
    })
  )
}

function formatTableInfo(info : TableInfoInternalWithDDL): TableInfo {
  return {
    name: info.name,
    ...getPKs(info.ddl),
    description: info.sql,
    columns: getColumns(info.ddl)
  }
}

/** 
 * @param tableName
 * @returns true if the table is an SQLite meta table such as a sequence, or sqlite_info.
 * 
 * Note: This is currently tested for by the regex /^(sqlite_|IFK_)/ and may be brittle.
 */
function isMeta(tableName : string) {
  return /^(sqlite_|IFK_)/.test(tableName);
}

function includeTable(config: Config, table: TableInfo): boolean {
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
  const db                                        = connect(config);
  const [results, metadata]                       = await db.query("SELECT * from sqlite_schema");
  const resultsT: Array<TableInfoInternal>        = results as unknown as Array<TableInfoInternal>;
  const withDDL:  Array<TableInfoInternalWithDDL> = resultsT.map(e => ({ddl: SQLiteDDLParser.parse(e.sql) as DDL_Info, ...e}) );
  const result:   Array<TableInfo>                = withDDL.map(formatTableInfo).filter(table => includeTable(config,table))

  return {
    tables: result
  };
};
