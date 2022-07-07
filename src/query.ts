import { Config }  from "./config";
import { connect } from "./db";
import { coerceUndefinedToNull, omap }    from "./util";
import {
    Expression,
    BinaryComparisonOperator,
    ProjectedRow,
    UnaryComparisonOperator,
    ComparisonValue,
    QueryRequest,
    ComparisonColumn,
    TableRelationships,
    Relationship,
    RelationshipField,
    RelationshipType,
    BinaryArrayComparisonOperator,
    Fields, 
  } from "./types/query";

let escapeSQL: (s: string) => string // This is set globally when running queryData;

function json_object(rs: Array<TableRelationships>, fs: Fields, t: string): string {
  return tag('json_object', omap(fs, (k,v) => {
    switch(v.type) {
      case "column":
        return [`'${k}', ${v.column}`];
      case "relationship":
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
          case UnaryComparisonOperator.IsNull:
            return [`(${bop_col(w.column)} IS NULL)`]; // TODO: Could escape usnig bop_col if escape is threaded through.
        }
      case "binary_op":
        const bop = bop_op(w.operator);
        return [`${bop_col(w.column)} ${bop} ${bop_val(w.value)}`];
      case "binary_arr_op":
        console.log("binary_op",w)
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
  ): string {
      // NOTE: The order of table prefixes are currently assumed to be from "parent" to "child".
      return tag('array_relationship',`(
        SELECT json_group_array(j)
        FROM (
          SELECT json_object(${json_object(ts, fields, table)}) as j
          FROM ${table}
          ${where(wWhere, wJoin)}
          ${limit(wLimit)}
          ${offset(wOffset)}
        ))`);
}

function object_relationship(
    ts: Array<TableRelationships>,
    table: string,
    wJoin: Array<string>,
    fields: Fields,
    wWhere: Expression | null,
    wLimit: number | null,
    wOffset: number | null,
  ): string {
      // NOTE: The order of table prefixes are currently assumed to be from "parent" to "child".
      return tag('object_relationship',`(
        SELECT json_object(${json_object(ts, fields, table)}) as j
        FROM ${table}
        ${where(wWhere, wJoin)}
        ${limit(wLimit)}
        ${offset(wOffset)}
      )`);
}

function relationship(ts: Array<TableRelationships>, r: Relationship, f: RelationshipField, t: string): string {
  const wJoin = omap(r.column_mapping, (k,v) => `${t}.${k} = ${r.target_table}.${v}`);

  switch(r.relationship_type) {
    // TODO: Query where clause etc.
    case RelationshipType.Object:
      return tag('relationship', object_relationship(
        ts,
        r.target_table,
        wJoin,
        f.query.fields,
        coerceUndefinedToNull(f.query.where),
        coerceUndefinedToNull(f.query.limit),
        coerceUndefinedToNull(f.query.offset),
      ));
      // return `(select json_object(${object_relationship()}))`;

    case RelationshipType.Array:
      // const wFilter = relationship_where(f.query.where);
      return tag('relationship', array_relationship(
        ts,
        r.target_table,
        wJoin,
        f.query.fields,
        coerceUndefinedToNull(f.query.where),
        coerceUndefinedToNull(f.query.limit),
        coerceUndefinedToNull(f.query.offset),
      ));
  }
}

function bop_col(c: ComparisonColumn): string {
  if(c.path.length < 1) {
    return tag('bop_col',c.name);
  } else {
    return tag('bop_col',c.path.map(escapeSQL).join(".") + "." + escapeSQL(c.name));
  }
}

function bop_array(o: BinaryArrayComparisonOperator): string {
  switch(o) {
    case BinaryArrayComparisonOperator.In:
      return tag('bop_array','IN');
  }
}

function bop_op(o: BinaryComparisonOperator): string {
  let result;
  switch(o) {
    case BinaryComparisonOperator.Equal:              result = "="; break;
    case BinaryComparisonOperator.GreaterThan:        result = ">"; break;
    case BinaryComparisonOperator.GreaterThanOrEqual: result = ">="; break;
    case BinaryComparisonOperator.LessThan:           result = "<"; break;
    case BinaryComparisonOperator.LessThanOrEqual:    result = "<="; break;
  }
  return tag('bop_op',result);
}

function bop_val(v: ComparisonValue): string {
  switch(v.type) {
    case "column": return tag('bop_val',`${bop_col(v.column)}`);
    case "scalar": return tag('bop_val',`${escapeSQL(`${v.value}`)}`);
  }
}

function where(w: Expression | null, j: Array<string>,): string {
  if(w == null) {
    return "";
  } else {
    const r = relationship_where(w);
    if(r.length < 1) {
      return "";
    } else {
      return tag('where',`WHERE ${[...r, ...j].join(" AND ")}`);
    }
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
    );
  return tag('query', `SELECT ${q} as data`);
}

function output(r: any): Array<ProjectedRow> {
  return JSON.parse(r[0].data); // TODO: What to do if there are no results? (Should be impossible.)
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
 * Example of Raw SQLite queries and results for Reference:
 * 
 * sqlite> select json_group_array(j) from (select json_object('who', name) as j from artist limit 5);
 *   [{"who":"AC/DC"},{"who":"Accept"},{"who":"Aerosmith"},{"who":"Alanis Morissette"},{"who":"Alice In Chains"}]
 * 
 * sqlite> select json_group_array(j) from (select json_object('who', name) as j from artist where name not like '%Aero%' limit 5 offset 1);
 *   [{"who":"Accept"},{"who":"Alanis Morissette"},{"who":"Alice In Chains"},{"who":"Antônio Carlos Jobim"},{"who":"Apocalyptica"}]
 * 
 * sqlite> select json_group_array(j) from (select json_object('who', name, 'album', (select json_group_array(k) from (select json_object('t', Title) as k from Album where Album.artistId = Artist.artistId))) as j from artist where name not like '%Aero%' limit 5 offset 1);
 *   [{"who":"Accept","album":[{"t":"Balls to the Wall"},{"t":"Restless and Wild"}]},{"who":"Alanis Morissette","album":[{"t":"Jagged Little Pill"}]},{"who":"Alice In Chains","album":[{"t":"Facelift"}]},{"who":"Antônio Carlos Jobim","album":[{"t":"Warner 25 Anos"},{"t":"Chill: Brazil (Disc 2)"}]},{"who":"Apocalyptica","album":[{"t":"Plays Metallica By Four Cellos"}]}]
 */
export async function queryData(config: Config, queryRequest: QueryRequest): Promise<Array<ProjectedRow>> {
  console.log(queryRequest);
  const db     = connect(config); // TODO: Should this be cached?
  escapeSQL    = (s: string) => db.escape(s); // Set globally for this module
  const q      = query(queryRequest.table_relationships, queryRequest);
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

