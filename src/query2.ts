import { Config }  from "./config";
import { connect } from "./db";
import { omap }    from "./util";
import { Expression, BinaryComparisonOperator, ProjectedRow, UnaryComparisonOperator, ComparisonValue, QueryRequest, ComparisonColumn, Field, ApplyBinaryComparisonOperatorExpression, ApplyUnaryComparisonOperatorExpression } from "./types/query";
import { Fields, OrderBy, OrderType, Query, QueryResponse, RelationshipType, ScalarValue, BinaryArrayComparisonOperator, TableName, TableRelationships, Relationship, RelationshipName, } from "./types/query"; // TODO: Remove maybe~

function output(rs: any): Array<ProjectedRow> {
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
  return es.map((e) => where(escapeSQL, e)).filter(e => e !== ""); // NOTE: This seems fragile.
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
  // Note: Nested switches could be an issue, but since there is only one unary op,
  // it should be ok.
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
    case "not":           return `NOT (${where(escapeSQL, w.expression)})`;
    case "and":           return junction(escapeSQL, w.expressions, " AND ");
    case "or":            return junction(escapeSQL, w.expressions, " OR ");
    case "unary_op":      return unary_op(escapeSQL, w);
    case "binary_op":     return binary_op(escapeSQL, w);
    case "binary_arr_op": // TODO
      return "TODO";
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

/** Performs a query and returns results
 * 
 * Limitations:
 * - Binary Array Operations not currently supported.
 * - Relationship fields not currently supported.
 * 
 * The current algorithm is to first create a query, then execute it, returning results.
 * 
 * Potential ideas for adding relationship fields:
 * - Some kind of JSON aggregation similar to Postgres' approach. This doesn't seem to be available in SQLite.
 *     - 4.13. The json_group_array() and json_group_object() aggregate SQL functions
 *     - https://www.sqlite.org/json1.html#jgrouparray
 * - Process the partial results and enrich them by performing further queries.
 * - Figure out the full set of tables and joins and execute a minimal set of queries then stitch the results together.
 * 
 * The second approach could lead to the classic n+1 problem with many queries being executed, although that
 * may not be a big problem for a reference implementation, but it would be good to have a strategy to address
 * this regardless.
 * 
 * The third approach is similar to the TS XML Reference implementation and could potentially reuse its algorithm
 * if desired.
 */
export async function queryData2(config: Config, queryRequest: QueryRequest): Promise<Array<ProjectedRow>> {
  console.log(queryRequest);
  const db     = connect(config);             // TODO: Should this be cached?
  const esc    = (s: string) => db.escape(s); // TODO: Thread escaper to other functions
  const q      = query(esc, queryRequest);    // TODO: Could the depth of recursion be a problem?
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

