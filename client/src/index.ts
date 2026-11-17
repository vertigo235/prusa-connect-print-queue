// The hand-written half of this package. generate-client-sdk -module wrote it
// on the first run and leaves it alone: add exports here, and never under
// src/generated/.

import type { Transport } from "@prusa/connect";
import { bindOperations, type Operations } from "./generated/operations.js";

/** What bind takes from a client: the transport that sends its requests. A PrusaConnect is one. */
export interface Client {
  readonly transport: Transport;
}

/**
 * bind attaches every operation of module com.github.aux.vertigo235.prusa-connect-print-queue to the client's
 * transport. A call carries the client's URL and credential, and a refusal
 * is the same ApiError the core's operations throw, with the module's own
 * code as a string.
 */
export function bind(client: Client): Operations {
  return bindOperations(client.transport);
}

export { bindOperations, operationTable, type Operations } from "./generated/operations.js";
export type { components, operations, paths } from "./generated/module-api.js";
