export const log = (...args: any[]) => {
  const ts = new Date().toISOString();
  console.log(`[${ts}]`, ...args);
};

export const error = (...args: any[]) => {
  const ts = new Date().toISOString();
  console.error(`[${ts}] ERROR:`, ...args);
};
