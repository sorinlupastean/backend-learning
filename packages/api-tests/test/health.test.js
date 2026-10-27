const {test} = require("node:test");
const assert = require("node:assert/strict");

const BASE_URL = process.env.BASE_URL ?? "http://localhost:3000";

test("GET /health returns 200 and status ok", async () => {
    const res = await fetch(`${BASE_URL}/health`);

    assert.equal(res.status, 200);
    assert.deepEqual(await res.json(), { status: "ok"});
});
