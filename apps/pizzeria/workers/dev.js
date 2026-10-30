const { spawn } = require("node:child_process");

const workers = [
  {
    name: "Waiter",
    file: "waiter.js",
  },
  {
    name: "Mario",
    file: "chef.js",
    env: { CHEF_NAME: "Mario" },
  },
  {
    name: "Luigi",
    file: "chef.js",
    env: { CHEF_NAME: "Luigi" },
  },
  {
    name: "Retry",
    file: "retry.js",
  },
];

const processes = [];

for (const worker of workers) {
  const child = spawn(process.execPath, ["--watch", worker.file], {
    cwd: __dirname,
    stdio: "inherit",
    env: {
      ...process.env,
      ...worker.env,
    },
  });

  processes.push(child);

  child.on("error", (error) => {
    console.error(`${worker.name} failed:`, error.message);
  });

  child.on("exit", (code) => {
    console.log(`${worker.name} exited with code ${code}`);
  });
}

function shutdown() {
  for (const child of processes) {
    if (child.exitCode === null) {
      child.kill("SIGTERM");
    }
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
