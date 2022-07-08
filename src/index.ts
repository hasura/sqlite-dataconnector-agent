import Fastify                                         from 'fastify';
import FastifyCors                                     from '@fastify/cors';
import { getSchema }                                   from './schema';
import { queryData }                                   from './query';
import { getConfig }                                   from './config';
import { CapabilitiesResponse, capabilitiesResponse}   from './capabilities';
import { connect }                                     from './db';
import { stringToBool }                                from './util';
import { QueryResponse, SchemaResponse, QueryRequest } from './types';

const port = Number(process.env.PORT) || 8100;
const server = Fastify({ logger: { prettyPrint: true } });

if(stringToBool(process.env['PERMISSIVE_CORS'])) {
  // See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
  server.register(FastifyCors, {
    origin: true,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["X-Hasura-DataConnector-Config", "X-Hasura-DataConnector-SourceName"]
  });
}

server.get<{ Reply: CapabilitiesResponse }>("/capabilities", async (request, _response) => {
  server.log.info({ headers: request.headers, query: request.body, }, "capabilities.request");
  return capabilitiesResponse;
});

server.get<{ Reply: SchemaResponse }>("/schema", async (request, _response) => {
  server.log.info({ headers: request.headers, query: request.body, }, "schema.request");
  const config = getConfig(request);
  return getSchema(config);
});

server.post<{ Body: QueryRequest, Reply: QueryResponse }>("/query", async (request, _response) => {
  server.log.info({ headers: request.headers, query: request.body, }, "query.request");
  const config = getConfig(request);
  return queryData(config, request.body);
});

server.get("/health", async (request, response) => {
  const config = getConfig(request);

  if(config.db == null) {
    server.log.info({ headers: request.headers, query: request.body, }, "health.request");
    response.statusCode = 204;
  } else {
    server.log.info({ headers: request.headers, query: request.body, }, "health.db.request");
    const db = connect(config);
    const [r, m] = await db.query('select 1 where 1 = 1');
    if(r && JSON.stringify(r) == '[{"1":1}]') {
      response.statusCode = 204;
      return { "status": "ok" };
    } else {
      response.statusCode = 500;
      return { "error": "problem executing query", "query_result": r };
    }
  }
});

process.on('SIGINT', () => {
  server.log.info("interrupted");
  process.exit(0);
});

const start = async () => {
  try {
    await server.listen(port, "0.0.0.0");
  }
  catch (err) {
    server.log.fatal(err);
    process.exit(1);
  }
};
start();
