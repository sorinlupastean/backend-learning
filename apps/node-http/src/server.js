
const http = require("node:http");
const server = http.createServer((req, res) => {
    if(req.method === "GET" && req.url === "/health") {
        res.writeHead(200, {
            "Content-Type": "application/json",
        });

        res.end(JSON.stringify({status: "ok"}));
        return;
    }

    res.writeHead(404, {
        "Content-Type": "application/json",
    });

    res.end(JSON.stringify({error: "Not Found"}));
});

server.listen(3000, () => {
    console.log("Server running at http://localhost:3000");
});

