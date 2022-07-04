import { Config } from "./config";
import { connect } from "./db";
import { Expression, Fields, BinaryComparisonOperator, OrderBy, OrderType, ProjectedRow, Query, QueryResponse, RelationshipType, ScalarValue, UnaryComparisonOperator, ComparisonValue, BinaryArrayComparisonOperator, QueryRequest, TableName, ComparisonColumn, TableRelationships, Relationship, RelationshipName, Field } from "./types/query";
import { coerceUndefinedToNull, crossProduct, omap, unreachable, zip } from "./util";

function output(rs: any): Array<ProjectedRow> {
  console.log("rows",rs);
  return rs;
}

function field(k: string, v: Field): Array<string> {
  switch(v.type) {
    case "column":
      return [`${k} as ${v.column}`];
    case "relationship":
      console.log("relationships not supported yet", k, v);
      return [];
  }
}

function fields(r: QueryRequest): string {
  return omap(r.query.fields, field).flatMap(e => e).join(", ");
}

function where(r: QueryRequest): string {
  return ""; // TODO
}

function limit(r: QueryRequest): string {
  if(r.query.limit == null) {
    return "";
  } else {
    return `limit ${r.query.limit}`;
  }
}

function offset(r: QueryRequest): string {
  if(r.query.offset == null) {
    return "";
  } else {
    return `offset ${r.query.offset}`;
  }
}

function query(r: QueryRequest): string {
  return `select ${fields(r)} from ${r.table} ${where(r)} ${limit(r)} ${offset(r)}`; // TODO: Escaping
}

export async function queryData2(config: Config, queryRequest: QueryRequest): Promise<Array<ProjectedRow>> {
  console.log(queryRequest);
  const db     = connect(config); // TODO: Should this be cached?
  const q      = query(queryRequest);
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

