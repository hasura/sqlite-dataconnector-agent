import { Sequelize } from "sequelize/types";
import { Config } from "./config";
import { connect, escapeSQL } from "./db";
import { Expression, Fields, BinaryComparisonOperator, OrderBy, OrderType, ProjectedRow, Query, QueryResponse, RelationshipType, ScalarValue, UnaryComparisonOperator, ComparisonValue, BinaryArrayComparisonOperator, QueryRequest, TableName, ComparisonColumn, TableRelationships, Relationship, RelationshipName, Field, ApplyBinaryComparisonOperatorExpression } from "./types/query";
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

function bop_col(c: ComparisonColumn): string {
  if(c.path.length < 1) {
    return c.name;
  } else {
    return c.path.join(".") + "." + c.name;
  }
}

function bop_op(o: BinaryComparisonOperator): string {
  switch(o) {
    case BinaryComparisonOperator.Equal:              return "=";
    case BinaryComparisonOperator.GreaterThan:        return ">";
    case BinaryComparisonOperator.GreaterThanOrEqual: return ">=";
    case BinaryComparisonOperator.LessThan:           return "<";
    case BinaryComparisonOperator.LessThanOrEqual:    return "<=";
  }
}

function bop_val(v: ComparisonValue): string {
  switch(v.type) {
    case "column": return `${v.column}`;
    case "scalar": return `'${v.value}'`; // TODO: escape
  }
}

function binary_op(b: ApplyBinaryComparisonOperatorExpression): string {
  return `${bop_col(b.column)} ${bop_op(b.operator)} ${bop_val(b.value)}`; // TODO: Validate
}

function subexpressions(es: Array<Expression>): Array<string> {
  return es.map(where).filter(e => e !== "");
}

function junction(es: Array<Expression>, b: string): string {
  const ss = subexpressions(es);
  if(ss.length < 1) {
    return "";
  } else {
    return ss.join(b);
  }
}

function where(w:Expression): string {
  switch(w.type) {
    case "not": return `NOT (${where(w.expression)})`;
    case "and": return junction(w.expressions, " AND ");
    case "or": return junction(w.expressions, " OR ");
    case "binary_op": return binary_op(w);
    case "unary_op": // TODO
      return "TODO";
    case "binary_arr_op": // TODO
      return "TODO";
  }
}

function whereN(w: Expression | null | undefined): string {
  if(w == null) {
    return "";
  } else {
    const r = where(w);
    if(r === "") {
      return "";
    } else {
      return `where ${r}`;
    }
  }
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

type EscapeSQL = (s: string) => string

function query(escapeSQL: EscapeSQL, r: QueryRequest): string {
  return `select ${fields(r)} from ${escapeSQL(r.table)} ${whereN(r.query.where)} ${limit(r)} ${offset(r)}`;
}

export async function queryData2(config: Config, queryRequest: QueryRequest): Promise<Array<ProjectedRow>> {
  console.log(queryRequest);
  const db     = connect(config); // TODO: Should this be cached?
  const esc    = (s: string) => db.escape(s); // TODO: Thread escaper to other functions
  const q      = query(esc, queryRequest);
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

