import { Config }  from "./config";
import { connect } from "./db";
import { omap }    from "./util";
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
    Field,
    BinaryArrayComparisonOperator,
    Fields, 
  } from "./types/query";

// function array_relationship_object(rs: Array<TableRelationships>, k: string, v: Field): string {
//   console.log("array_relationship_object", k, v);

//   switch(v.type) {
//     case "column":
//       return `'${k}', ${v.column}`;
//     case "relationship":
//       console.log("Sub-field relationships not supported yet", k, v, v.query.fields);
//       return `'${k}', ${fields2(rs, v.query.field)}`;
//   }
// }

function array_relationship_object(rs: Array<TableRelationships>, fs: Fields, t: string): string {
  return omap(fs, (k,v) => {
    switch(v.type) {
      case "column":
        return [`'${k}', ${v.column}`];
      case "relationship":
        return rs.flatMap((x) => {
          if(x.source_table === t) {
            const rel = x.relationships[v.relationship];
            if(rel) {
              return [`'${k}', ${relationship(rs, rel, v, t)}`];
            }
          }
          console.log("Couldn't find relationship for field", k, v, rs);
          return [];
        })
    }
  }).flatMap((e) => e).join(", ");
}

function array_relationship_where(w: Expression | null | undefined): Array<string> {
  if(w == null) {
    return [];
  } else {
    switch(w.type) {
      case "not":
        const aNot = array_relationship_where(w.expression);
        if(aNot.length > 0) {
          return [`(NOT ${aNot})`];
        }
        break;
      case "and":
        const aAnd = w.expressions.flatMap(array_relationship_where);
        if(aAnd.length > 0) {
          return [`(${aAnd.join(" AND ")})`];
        }
        break;
      case "or":
        const aOr = w.expressions.flatMap(array_relationship_where);
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

function relationship(rs: Array<TableRelationships>, r: Relationship, f: RelationshipField, t: string): string {
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
      const wFilter = array_relationship_where(f.query.where);
      // TODO: Ensure that the table prefixes are correct - currently assuming it's from "parent" to "child"
      return `
        (select json_group_array(json_object(${array_relationship_object(rs, f.query.fields, r.target_table)}))
          from ${r.target_table}
          where ${[...wJoin, ...wFilter].join(" AND ")})
      `;
      // return `
      //   (select json_group_array(json_object(${omap(f.query.fields, (k,v) => array_relationship_object(rs,k,v)).join(", ")}))
      //     from ${r.target_table}
      //     where ${[...wJoin, ...wFilter].join(" AND ")})
      // `;
  }
}

function fields(rs: Array<TableRelationships>, r: QueryRequest): string {
  return omap(r.query.fields, (k,v) => {
    switch(v.type) {
      case "column":
        return [`${v.column} as ${k}`];
      case "relationship": // TODO: What if there's more than one table relationship? Currently just includes all of them!
        return rs.flatMap((x) => {
          if(x.source_table === r.table) {
            const rel = x.relationships[v.relationship];
            if(rel) {
              return [`${relationship(rs, rel, v, r.table)} as ${k}`];
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

function query(t: Array<TableRelationships>, escapeSQL: EscapeSQL, r: QueryRequest): string {
  return `select ${fields(t, r)} from ${escapeSQL(r.table)} ${whereN(escapeSQL, r.query.where)} ${limit(r)} ${offset(r)}`;
}

function output(rs: any): Array<ProjectedRow> {
  return rs;
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
 *   Artist(limit: 5, order_by: {ArtistId: asc}, where: {Name: {_neq: "Accept"}, _and: {Name: {_is_null: false}}}) {
 *     ArtistId
 *     Name
 *     Albums(where: {Title: {_is_null: false, _gt: "A", _nin: "poo"}}) {
 *       AlbumId
 *       Title
 *       ArtistId
 *       Tracks {
 *         Name
 *         TrackId
 *       }
 *     }
 *   }
 * }
 * ```
 */
export async function queryData2(config: Config, queryRequest: QueryRequest): Promise<Array<ProjectedRow>> {
  console.log(queryRequest);
  const db     = connect(config);             // TODO: Should this be cached?
  const esc    = (s: string) => db.escape(s); // TODO: Thread escaper to other functions
  const q      = query(queryRequest.table_relationships, esc, queryRequest);    // TODO: Could the depth of recursion be a problem?
  const [r, m] = await db.query(q);
  const o      = output(r);
  return o;
}

