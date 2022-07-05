import Fastify                                       from 'fastify';
import { SchemaResponse }                            from './types/schema';
import { ProjectedRow, QueryRequest }                from './types/query';
import { getSchema }                                 from './schema';
import { queryData }                                 from './query';
import { getConfig }                                 from './config';
import { CapabilitiesResponse, capabilitiesResponse} from './capabilities';

const port = Number(process.env.PORT) || 8100;
const server = Fastify({ logger: { prettyPrint: true } });

server.get<{ Reply: CapabilitiesResponse }>("/capabilities", async (request, _response) => {
  server.log.info({ headers: request.headers, query: request.body, }, "capabilities.request");
  return capabilitiesResponse;
});

server.get<{ Reply: SchemaResponse }>("/schema", async (request, _response) => {
  server.log.info({ headers: request.headers, query: request.body, }, "schema.request");
  const config = getConfig(request);
  return getSchema(config);
});

server.post<{ Body: QueryRequest, Reply: ProjectedRow[] }>("/query", async (request, _response) => {
  server.log.info({ headers: request.headers, query: request.body, }, "query.request");
  const config = getConfig(request);
  return queryData(config, request.body);
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
