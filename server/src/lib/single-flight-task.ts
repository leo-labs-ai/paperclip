/** Skip duplicate scheduler ticks; only a later tick may start the next pass. */
export function createSingleFlightTask() {
  let running = false;
  return function run<T>(work: () => Promise<T>): Promise<T> | undefined {
    if (running) return undefined;
    running = true;
    // Reserve before invocation, and turn synchronous throws into tracked rejections.
    return Promise.resolve().then(work).finally(() => {
      running = false;
    });
  };
}
