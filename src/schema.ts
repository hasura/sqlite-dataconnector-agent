import { SchemaResponse, ScalarType, ColumnInfo, TableInfo } from "./types"
import { Config } from "./config";
import { connect } from './db';
import { logDeep } from "./util";

const SQLiteDDLParser = require('sqlite-ddl-parser');
var sqliteParser = require('sqlite-parser');

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

type TableInfoInternalWithDDL = TableInfoInternal & { type: string, ddl: DDL_Info }

function getPKs(info : DDL_Info) : ({ primary_key: Array<string>}) {
  if(info.tables.length > 0) {
    const t = info.tables[0];
    if(t.primaryKeys.length > 0) {
      return {primary_key: t.primaryKeys}
    }
  }
  return {primary_key: []};
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
    t.columns.map(c => {
      return ({
        name: c.name,
        type: columnCast(c.type),
        nullable: (!c.notNull)
      })
    })
  )
}

function getColumns2(ast : Array<any>) : Array<ColumnInfo> {
  return ast.map(c => {
    return ({
      name: c.name,
      type: columnCast(datatypeCast(c.datatype)),
      nullable: nullableCast(c.definition)
    })
  })
}

// Interpret the sqlite-parser datatype as a schema column response type.
function datatypeCast(d: any): any {
  switch(d.variant) {
    case "datetime": return 'string';
    default: return d.affinity;
  }
}

function nullableCast(ds: Array<any>): boolean {
  for(var d of ds) {
    if(d.type === 'constraint' && d.variant == 'not null') {
      return false;
    }
  }
  return true;
}

function formatTableInfo(info : TableInfoInternalWithDDL): TableInfo {
  const ast = sqliteParser(info.sql);
  const ddl = ddlColumns(ast);
  // const columns = getColumns(info.ddl);
  // const columns2 = getColumns2(ddl);
  // logDeep("columns", columns);
  // logDeep("columns2", columns2);

  return {
    name: info.name,
    ...getPKs(info.ddl),
    description: info.sql,
    columns: getColumns2(ddl)
  }
}

/** 
 * @param table
 * @returns true if the table is an SQLite meta table such as a sequence, index, etc.
 */
function isMeta(table : TableInfoInternal) {
  return table.type != 'table';
}

function includeTable(config: Config, table: TableInfoInternal): boolean {
  if(config.tables === null) {
    if(isMeta(table) && ! config.meta) {
      return false;
    }
    return true;
  } else {
    return config.tables.indexOf(table.name) >= 0
  }
}

function ddlColumns(ddl: any): Array<any> {
  if(ddl.type != 'statement' || ddl.variant != 'list') {
    throw new Error("Encountered a non-statement or non-list when parsing DDL for table.");
  }
  return ddl.statement.flatMap((t: any) => {
    if(t.type !=  'statement' || t.variant != 'create' || t.format != 'table') {
      return [];
    }
    return t.definition.flatMap((c: any) => {
      if(c.type != 'definition' || c.variant != 'column') {
        return [];
      }
      return [c];
    });
  })
}

function parseDDL(ddl: string): DDL_Info {
  return SQLiteDDLParser.parse(ddl) as DDL_Info;
}

export async function getSchema(config: Config): Promise<SchemaResponse> {
  const db                                        = connect(config);
  const [results, metadata]                       = await db.query("SELECT * from sqlite_schema");
  const resultsT: Array<TableInfoInternal>        = results as unknown as Array<TableInfoInternal>;
  const filtered: Array<TableInfoInternal>        = resultsT.filter(table => includeTable(config,table));
  const withDDL:  Array<TableInfoInternalWithDDL> = filtered.map(e => ({ddl: parseDDL(e.sql), ...e}) );
  const result:   Array<TableInfo>                = withDDL.map(formatTableInfo);

  return {
    tables: result
  };
};
