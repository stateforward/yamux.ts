import assert from "node:assert/strict";
import { chromium } from "playwright";
import { createServer } from "vite";

const vite = await createServer({
  root: process.cwd(),
  logLevel: "error",
  server: {
    host: "127.0.0.1",
    port: 0,
    strictPort: false,
  },
});

let browser;
try {
  await vite.listen();
  const baseUrl = vite.resolvedUrls?.local[0];
  assert(baseUrl, "Vite did not expose a local browser URL");

  browser = await chromium.launch();
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(error));
  page.on("console", (message) => {
    if (message.type() === "error") {
      errors.push(new Error(message.text()));
    }
  });

  await page.goto(new URL("/browser/index.html", baseUrl).toString(), {
    waitUntil: "networkidle",
  });
  const result = await page.evaluate(() => window.__yamuxBrowserProbe);

  assert.equal(result.ok, true);
  assert.match(result.userAgent, /Chrome|Chromium/);
  assert.equal(result.transferredBytes, 256 * 1024 + 1024);
  assert.equal(errors.length, 0, errors.map((error) => error.stack ?? error.message).join("\n"));

  console.log(`browser conformance passed: Chromium transferred ${result.transferredBytes} bytes`);
} finally {
  await browser?.close();
  await vite.close();
}
