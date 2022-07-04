import { Sequelize } from "sequelize/types";
import { Config } from "./config";
import { connect, escapeSQL } from "./db";
import { Expression, Fields, BinaryComparisonOperator, OrderBy, OrderType, ProjectedRow, Query, QueryResponse, RelationshipType, ScalarValue, UnaryComparisonOperator, ComparisonValue, BinaryArrayComparisonOperator, QueryRequest, TableName, ComparisonColumn, TableRelationships, Relationship, RelationshipName, Field, ApplyBinaryComparisonOperatorExpression, ApplyUnaryComparisonOperatorExpression } from "./types/query";
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

function bop_col(escapeSQL: EscapeSQL, c: ComparisonColumn): string {
  if(c.path.length < 1) {
    return c.name;
  } else {
    return c.path.map(escapeSQL).join(".") + "." + escapeSQL(c.name);
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

function bop_val(escapeSQL: EscapeSQL, v: ComparisonValue): string {
  switch(v.type) {
    case "column": return `${bop_col(escapeSQL, v.column)}`;
    case "scalar": return `${escapeSQL(`${v.value}`)}`;
  }
}

function binary_op(escapeSQL: EscapeSQL, b: ApplyBinaryComparisonOperatorExpression): string {
  return `${bop_col(escapeSQL, b.column)} ${bop_op(b.operator)} ${bop_val(escapeSQL, b.value)}`; // TODO: Validate
}

function subexpressions(escapeSQL: EscapeSQL, es: Array<Expression>): Array<string> {
  return es.map((e) => where(escapeSQL, e)).filter(e => e !== "");
}

function junction(escapeSQL: EscapeSQL, es: Array<Expression>, b: string): string {
  const ss = subexpressions(escapeSQL, es);
  if(ss.length < 1) {
    return "";
  } else {
    return ss.join(b);
  }
}

function unary_op(escapeSQL: EscapeSQL, u: ApplyUnaryComparisonOperatorExpression): string {
  switch(u.type) {
    case "unary_op":
      switch(u.operator) {
        case UnaryComparisonOperator.IsNull:
          return `${bop_col(escapeSQL, u.column)} IS NULL`;
      }
  }
}

function where(escapeSQL: EscapeSQL, w:Expression): string {
  switch(w.type) {
    case "not": return `NOT (${where(escapeSQL, w.expression)})`;
    case "and": return junction(escapeSQL, w.expressions, " AND ");
    case "or": return junction(escapeSQL, w.expressions, " OR ");
    case "binary_op": return binary_op(escapeSQL, w);
    case "binary_arr_op": // TODO
      return "TODO";
    case "unary_op": // TODO
      return unary_op(escapeSQL, w);
  }
}

function whereN(escapeSQL: EscapeSQL, w: Expression | null | undefined): string {
  if(w == null) {
    return "";
  } else {
    const r = where(escapeSQL, w);
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
  return `select ${fields(r)} from ${escapeSQL(r.table)} ${whereN(escapeSQL, r.query.where)} ${limit(r)} ${offset(r)}`;
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

