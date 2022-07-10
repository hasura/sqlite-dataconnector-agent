import { Config }  from "./config";
import { connect } from "./db";
import { coerceUndefinedOrNullToEmptyArray, coerceUndefinedToNull, omap }    from "./util";
import {
    Expression,
    BinaryComparisonOperator,
    ComparisonValue,
    QueryRequest,
    ComparisonColumn,
    TableRelationships,
    Relationship,
    RelationshipField,
    BinaryArrayComparisonOperator,
    OrderBy,
    QueryResponse,
    Field, 
  } from "./types";

/** Helper type for convenience.
 */
type Fields = Record<string, Field>

let escapeString: (s: string) => string // This is set globally when running queryData;

/**
 * 
 * @param c: Unescaped name. E.g. 'Alb"um'
 * @returns Escaped name. E.g. '"Alb\"um"'
 */ 
function escapeColumn(c: string): string {
  // TODO: Review this function since the current implementation is off the cuff.
  const result = c.replace(/\\/g,"\\\\").replace(/"/g,'\\"');
  return `"${result}"`;
}

function json_object(rs: Array<TableRelationships>, fs: Fields, t: string): string {
  return tag('json_object', omap(fs, (k,v) => {
    switch(v.type) {
      case "column":
        return [`${escapeString(k)}, ${escapeColumn(v.column)}`];
      case "relationship":
        // TODO: Use a for insteand of a map?
        const result = rs.flatMap((x) => {
          if(x.source_table === t) {
            const rel = x.relationships[v.relationship];
            if(rel) {
              return [`'${k}', ${relationship(rs, rel, v, t)}`];
            }
          }
          return [];
        });
        if(result.length < 1) {
          console.log("Couldn't find relationship for field", k, v, rs);
        }
        return result;
    }
  }).flatMap((e) => e).join(", "));
}

function relationship_where(w: Expression | null): Array<string> {
  if(w == null) {
    return [];
  } else {
    switch(w.type) {
      case "not":
        const aNot = relationship_where(w.expression);
        if(aNot.length > 0) {
          return [`(NOT ${aNot})`];
        }
        break;
      case "and":
        const aAnd = w.expressions.flatMap(relationship_where);
        if(aAnd.length > 0) {
          return [`(${aAnd.join(" AND ")})`];
        }
        break;
      case "or":
        const aOr = w.expressions.flatMap(relationship_where);
        if(aOr.length > 0) {
          return [`(${aOr.join(" OR ")})`];
        }
        break;
      case "unary_op":
        switch(w.operator) {
          case 'is_null':
            return [`(${bop_col(w.column)} IS NULL)`]; // TODO: Could escape usnig bop_col if escape is threaded through.
        }
      case "binary_op":
        const bop = bop_op(w.operator);
        return [`${bop_col(w.column)} ${bop} ${bop_val(w.value)}`];
      case "binary_arr_op":
        const bopA = bop_array(w.operator);
        return [`(${bop_col(w.column)} ${bopA} (${w.values.map(v => `'${v}'`).join(", ")}))`];
    }
    return [];
  }
}

function array_relationship(
    ts: Array<TableRelationships>,
    table: string,
    wJoin: Array<string>,
    fields: Fields,
    wWhere: Expression | null,
    wLimit: number | null,
    wOffset: number | null,
    wOrder: Array<OrderBy>,
  ): string {
      // NOTE: The order of table prefixes are currently assumed to be from "parent" to "child".
      // NOTE: The reuse of the 'j' identifier should be safe due to scoping. This is confirmed in testing.
      if(wOrder.length < 1) {
        return tag('array_relationship',`(
          SELECT JSON_GROUP_ARRAY(j)
          FROM (
            SELECT JSON_OBJECT(${json_object(ts, fields, table)}) AS j
            FROM ${escapeColumn(table)}
            ${where(wWhere, wJoin)}
            ${limit(wLimit)}
            ${offset(wOffset)}
          ))`);
      } else {
        // NOTE: Rationale for subselect in FROM clause:
        // 
        // There seems to be a bug in SQLite where an ORDER clause in this position causes ARRAY_RELATIONSHIP
        // to return rows as JSON strings instead of JSON objects. This is worked around by using a subselect.
        return tag('array_relationship',`(
          SELECT JSON_GROUP_ARRAY(j)
          FROM (
            SELECT JSON_OBJECT(${json_object(ts, fields, table)}) AS j
            FROM (
              SELECT *
              FROM ${escapeColumn(table)}
              ${where(wWhere, wJoin)}
              ${order(wOrder)}
              ${limit(wLimit)}
              ${offset(wOffset)}
            ) AS ${table}
          ))`);
      }
}

function object_relationship(
    ts: Array<TableRelationships>,
    table: string,
    wJoin: Array<string>,
    fields: Fields,
  ): string {
      // NOTE: The order of table prefixes are currently assumed to be from "parent" to "child".
      return tag('object_relationship',`(
        SELECT JSON_OBJECT(${json_object(ts, fields, table)}) AS j
        FROM ${table}
        ${where(null, wJoin)}
      )`);
}

function relationship(ts: Array<TableRelationships>, r: Relationship, f: RelationshipField, t: string): string {
  const wJoin = omap(
    r.column_mapping,
    (k,v) => `${escapeColumn(t)}.${escapeColumn(k)} = ${escapeColumn(r.target_table)}.${escapeColumn(v)}`
  );

  switch(r.relationship_type) {
    case 'object':
      return tag('relationship', object_relationship(
        ts,
        r.target_table,
        wJoin,
        f.query.fields,
      ));

    case 'array':
      return tag('relationship', array_relationship(
        ts,
        r.target_table,
        wJoin,
        f.query.fields,
        coerceUndefinedToNull(f.query.where),
        coerceUndefinedToNull(f.query.limit),
        coerceUndefinedToNull(f.query.offset),
        coerceUndefinedOrNullToEmptyArray(f.query.order_by),
      ));
  }
}

function bop_col(c: ComparisonColumn): string {
  if(c.path.length < 1) {
    return tag('bop_col',escapeColumn(c.name));
  } else {
    return tag('bop_col',c.path.map(escapeColumn).join(".") + "." + escapeColumn(c.name));
  }
}

function bop_array(o: BinaryArrayComparisonOperator): string {
  switch(o) {
    case 'in': return tag('bop_array','IN');
  }
}

function bop_op(o: BinaryComparisonOperator): string {
  let result;
  switch(o) {
    // 'less_than' | 'less_than_or_equal' | 'greater_than' | 'greater_than_or_equal' | 'equal';
    case 'equal':                 result = "="; break;
    case 'greater_than':          result = ">"; break;
    case 'greater_than_or_equal': result = ">="; break;
    case 'less_than':             result = "<"; break;
    case 'less_than_or_equal':    result = "<="; break;
  }
  return tag('bop_op',result);
}

function bop_val(v: ComparisonValue): string {
  switch(v.type) {
    case "column": return tag('bop_val',`${bop_col(v.column)}`);
    case "scalar": return tag('bop_val',`${escapeString(`${v.value}`)}`);
  }
}

function order(o: Array<OrderBy>): string {
  if(o.length < 1) {
    return "";
  }
  const result = o.map(e => `${e.column} ${e.ordering}`).join(', ');
  return tag('order',`ORDER BY ${result}`);
}

function where(w: Expression | null, j: Array<string>,): string {
  const r = [...relationship_where(w), ...j];
  if(r.length < 1) {
    return "";
  } else {
    return tag('where',`WHERE ${r.join(" AND ")}`);
  }
}

function limit(l: number | null): string {
  if(l === null) {
    return "";
  } else {
    return tag('limit',`LIMIT ${l}`);
  }
}

function offset(o: number | null): string {
  if(o == null) {
    return "";
  } else {
    return tag('offset', `OFFSET ${o}`);
  }
}

// TODO: Could the depth of recursion be a problem?
function query(t: Array<TableRelationships>, r: QueryRequest): string {
  const q = array_relationship(
    r.table_relationships,
    r.table,
    [],
    r.query.fields,
    coerceUndefinedToNull(r.query.where),
    coerceUndefinedToNull(r.query.limit),
    coerceUndefinedToNull(r.query.offset),
    coerceUndefinedOrNullToEmptyArray(r.query.order_by),
    );
  return tag('query', `SELECT ${q} as data`);
}

/** Format the DB response into a /query response.
 * 
 * Note: There should always be one result since 0 rows still generates an empty JSON array.
 */
function output(r: any): QueryResponse {
  return JSON.parse(r[0].data);
}

/** Function to add SQL comments to the generated SQL to tag which procedures generated what text.
 * 
 * comment('a','b') => '/*\<a>\*\/ b /*\</a>*\/'
 */
function tag(t: string, s: string): string {
  return `/*<${t}>*/ ${s} /*</${t}>*/`;
}

/** Performs a query and returns results
 * 
 * Limitations:
 * 
 * - Binary Array Operations not currently supported.
 * 
 * The current algorithm is to first create a query, then execute it, returning results.
 * 
 * Method for adding relationship fields:
 * 
 * - JSON aggregation similar to Postgres' approach.
 *     - 4.13. The json_group_array() and json_group_object() aggregate SQL functions
 *     - https://www.sqlite.org/json1.html#jgrouparray
 * 


 * Example of a test query:
 * 
 * ```
 * query MyQuery {
 *   Artist(limit: 5, order_by: {ArtistId: asc}, where: {Name: {_neq: "Accept"}, _and: {Name: {_is_null: false}}}, offset: 3) {
 *     ArtistId
 *     Name
 *     Albums(where: {Title: {_is_null: false, _gt: "A", _nin: "foo"}}, limit: 2) {
 *       AlbumId
 *       Title
 *       ArtistId
 *       Tracks(limit: 1) {
 *         Name
 *         TrackId
 *       }
 *       Artist {
 *         ArtistId
 *       }
 *     }
 *   }
 *   Track(limit: 3) {
 *     Name
 *     Album {
 *       Title
 *     }
 *   }
 * }
 * ```
 * 
 */
export async function queryData(config: Config, queryRequest: QueryRequest): Promise<QueryResponse> {
  const db     = connect(config); // TODO: Should this be cached?
  escapeString    = (s: string) => db.escape(s); // Set globally for this module - TODO: Should this have options for what to escape? E.g. table-name, string, etc.
  const q      = query(queryRequest.table_relationships, queryRequest);
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

