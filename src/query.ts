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
    ApplyBinaryComparisonOperatorExpression,
    ApplyUnaryComparisonOperatorExpression,
    TableRelationships,
    Relationship,
    RelationshipField,
    RelationshipType,
    BinaryArrayComparisonOperator,
    Fields, 
  } from "./types/query";

function relationship_object(escapeSQL: EscapeSQL, rs: Array<TableRelationships>, fs: Fields, t: string): string {
  return omap(fs, (k,v) => {
    switch(v.type) {
      case "column":
        return [`'${k}', ${v.column}`];
      case "relationship":
        return rs.flatMap((x) => {
          if(x.source_table === t) {
            const rel = x.relationships[v.relationship];
            if(rel) {
              return [`'${k}', ${relationship(escapeSQL, rs, rel, v, t)}`];
            }
          }
          console.log("Couldn't find relationship for field", k, v, rs);
          return [];
        })
    }
  }).flatMap((e) => e).join(", ");
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
            return [`(${bop_col2(w.column)} IS NULL)`]; // TODO: Could escape usnig bop_col if escape is threaded through.
        }
      case "binary_op":
        const bop = bop_op(w.operator);
        return [`${bop_col2(w.column)} ${bop} ${bop_val2(w.value)}`];
      case "binary_arr_op":
        console.log("binary_op",w)
        const bopA = bop_array(w.operator);
        return [`(${bop_col2(w.column)} ${bopA} (${w.values.map(v => `'${v}'`).join(", ")}))`];
    }
    return [];
  }
}

// TODO: Use wWhere?: instead of | null | undefined
function array_relationship(
    escapeSQL: EscapeSQL,
    ts: Array<TableRelationships>,
    wTable: string,
    wJoin: Array<string>,
    wFields: Fields,
    wWhere: Expression | null,
    wLimit: number | null,
    wOffset: number | null,
  ): string {
      const wFilter = relationship_where(wWhere);
      // TODO: Ensure that the table prefixes are correct - currently assuming it's from "parent" to "child"
      return `(
        SELECT json_group_array(j)
        FROM (
          SELECT json_object(${relationship_object(escapeSQL, ts, wFields, wTable)}) as j
          FROM ${wTable}
          ${whereN(escapeSQL, wWhere, wJoin)}
          ${limit(wLimit)}
          ${offset(wOffset)}
        ))`;
}

function relationship(escapeSQL: EscapeSQL, ts: Array<TableRelationships>, r: Relationship, f: RelationshipField, t: string): string {
  // (select json_group_array(json_object('Title', Album.Title)) from Album where Album.ArtistId = Artist.ArtistId) as J

  console.log("relationship", f, r);

  switch(r.relationship_type) {
    // TODO: Query where clause etc.
    case RelationshipType.Object:
      console.log("Object relationships not supported yet", r, f, t);
      return "oops";
      // return `(select json_object(${object_relationship()}))`;

    case RelationshipType.Array:
      const wJoin   = omap(r.column_mapping, (k,v) => `${t}.${k} = ${r.target_table}.${v}`);
      // const wFilter = relationship_where(f.query.where);
      return array_relationship(
        escapeSQL,
        ts,
        r.target_table,
        wJoin,
        f.query.fields,
        coerceUndefinedToNull(f.query.where),
        coerceUndefinedToNull(f.query.limit),
        coerceUndefinedToNull(f.query.offset),
        );
      // return `
      //   (select json_group_array(json_object(${relationship_object(ts, f.query.fields, r.target_table)}))
      //     from ${r.target_table}
      //     where ${[...wJoin, ...wFilter].join(" AND ")})
      // `;
  }
}

function fields(escapeSQL: EscapeSQL, rs: Array<TableRelationships>, r: QueryRequest): string {
  return omap(r.query.fields, (k,v) => {
    switch(v.type) {
      case "column":
        return [`${v.column} as ${k}`];
      case "relationship": // TODO: What if there's more than one table relationship? Currently just includes all of them!
        return rs.flatMap((x) => {
          if(x.source_table === r.table) {
            const rel = x.relationships[v.relationship];
            if(rel) {
              return [`${relationship(escapeSQL, rs, rel, v, r.table)} as ${k}`];
            }
          }
          console.log("Couldn't find relationship for field", k, v, rs);
          return [];
        })
    }
  }).flatMap((e) => e).join(", ");
}

function bop_col2(c: ComparisonColumn): string {
  if(c.path.length < 1) {
    return c.name;
  } else {
    return c.path.join(".") + "." + c.name;
  }
}

function bop_col(escapeSQL: EscapeSQL, c: ComparisonColumn): string {
  if(c.path.length < 1) {
    return c.name;
  } else {
    return c.path.map(escapeSQL).join(".") + "." + escapeSQL(c.name);
  }
}

function bop_array(o: BinaryArrayComparisonOperator): string {
  switch(o) {
    case BinaryArrayComparisonOperator.In:
      return "IN";
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

function bop_val2(v: ComparisonValue): string {
  switch(v.type) {
    case "column": return `${bop_col2(v.column)}`;
    case "scalar":
      if(typeof v.value == "number") {
        return `${v.value}`;
      } else {
        return `'${v.value}'`;
      }
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

function whereN(escapeSQL: EscapeSQL, w: Expression | null, j: Array<string>,): string {
  if(w == null) {
    return "";
  } else {
    const r = relationship_where(w);
    if(r.length < 1) {
      return "";
    } else {
      return `WHERE ${[...r, ...j].join(" AND ")}`;
    }
  }
}

function limit(l: number | null): string {
  if(l === null) {
    return "";
  } else {
    return `LIMIT ${l}`;
  }
}

function offset(o: number | null): string {
  if(o == null) {
    return "";
  } else {
    return `OFFSET ${o}`;
  }
}

type EscapeSQL = (s: string) => string

function object_relationship(escapeSQL: EscapeSQL, ts: Array<TableRelationships>, q: QueryRequest ): string {
  const wFilter = relationship_where(coerceUndefinedToNull(q.query.where));
  return `
    (select json_object(${relationship_object(escapeSQL, q.table_relationships, q.query.fields, q.table)})
      FROM ${q.table}
      WHERE ${wFilter.join(" AND ")})
  `;
}

// function query(t: Array<TableRelationships>, escapeSQL: EscapeSQL, q: QueryRequest): string {
//   // return `select ${fields(t, r)} from ${escapeSQL(r.table)} ${whereN(escapeSQL, r.query.where)} ${limit(r)} ${offset(r)}`;
//   return `
//     SELECT ${object_relationship(escapeSQL, t, q)}
//     FROM ${escapeSQL(q.table)}
//     ${whereN(escapeSQL, q.query.where)}
//     ${limit(q)}
//     ${offset(q)}`;
// }

function query(t: Array<TableRelationships>, escapeSQL: EscapeSQL, r: QueryRequest): string {
  // return `select ${fields(t, r)} from ${escapeSQL(r.table)} ${whereN(escapeSQL, r.query.where)} ${limit(r)} ${offset(r)}`;
  const q = array_relationship(
    escapeSQL,
    r.table_relationships,
    r.table,
    [],
    r.query.fields,
    coerceUndefinedToNull(r.query.where),
    coerceUndefinedToNull(r.query.limit),
    coerceUndefinedToNull(r.query.offset),
    );
  return `SELECT ${q} as data`;
}

function output(r: any): Array<ProjectedRow> {
  return JSON.parse(r[0].data); // TODO: What to do if there are no results? (Should be impossible.)
}

/** Performs a query and returns results
 * 
 * Limitations:
 * 
 * - Binary Array Operations not currently supported.
 * - Relationship fields not currently supported.
 * 
 * The current algorithm is to first create a query, then execute it, returning results.
 * 
 * Method for adding relationship fields:
 * 
 * - Some kind of JSON aggregation similar to Postgres' approach. This doesn't seem to be available in SQLite.
 *     - 4.13. The json_group_array() and json_group_object() aggregate SQL functions
 *     - https://www.sqlite.org/json1.html#jgrouparray
 * 
 * Example of a test query:
 * 
 * ```
 * query MyQuery {
 *   Artist(limit: 5, order_by: {ArtistId: asc, Albums_aggregate: {}}, where: {Name: {_neq: "Accept"}, _and: {Name: {_is_null: false}}}) {
 *       ArtistId
 *       Name
 *       Albums(where: {Title: {_is_null: false, _gt: "A", _nin: "foo"}}, limit: 2) {
 *         AlbumId
 *         Title
 *         ArtistId
 *         Tracks(limit: 1) {
 *           Name
 *           TrackId
 *         }
 *       }
 *     }
 *   }
 * ```
 * 
 * Example of Raw Queries:
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
  const db     = connect(config);             // TODO: Should this be cached?
  const esc    = (s: string) => db.escape(s); // TODO: Thread escaper to other functions
  // TODO: Could the depth of recursion be a problem?
  const q      = query(queryRequest.table_relationships, esc, queryRequest);    // TODO: Could the depth of recursion be a problem?
  // const q      = array_relationship(queryRequest.table_relationships, queryRequest.table, [], queryRequest.query.fields, queryRequest.query.where);
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

