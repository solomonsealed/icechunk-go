// Entry point of the Worker: instantiates the Go WebAssembly module on the
// first request (Workers forbid the randomness and timers the Go runtime
// needs at startup in global scope) and forwards requests to the handler
// registered by cfworker.Serve.
import "./build/wasm_exec.js";
import goModule from "./build/app.wasm";

let ready;

function startGo() {
  if (!ready) {
    ready = (async () => {
      const go = new Go();
      const instance = await WebAssembly.instantiate(goModule, go.importObject);
      // Runs main() until it blocks inside cfworker.Serve.
      go.run(instance);
      if (typeof globalThis.__goWorkerFetch !== "function") {
        throw new Error("the Go program did not call cfworker.Serve");
      }
    })();
    ready.catch(() => {
      ready = undefined;
    });
  }
  return ready;
}

export default {
  async fetch(request, env, ctx) {
    await startGo();
    return globalThis.__goWorkerFetch(request, env, ctx);
  },
};
