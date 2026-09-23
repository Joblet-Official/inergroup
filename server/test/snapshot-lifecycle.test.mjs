import test, { afterEach } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = path.resolve(TEST_DIR, "..", "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server", "src", "index.ts");
const APPLY_HOST = "tnl2.jometer.com";

const children = new Set();
const feedServers = new Set();
const temporaryDirectories = new Set();

afterEach(async () => {
  await Promise.all([...children].map(stopChild));
  await Promise.all([...feedServers].map(closeServer));
  for (const directory of temporaryDirectories) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
  temporaryDirectories.clear();
});

function createTemporaryDirectory() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "async-snapshot-test-"));
  temporaryDirectories.add(directory);
  return directory;
}

function createLegacySnapshot(filePath, modifiedAtMs = Date.now(), jobCount = 25) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const database = new DatabaseSync(filePath);
  database.exec(`
    PRAGMA journal_mode = DELETE;
    CREATE TABLE jobs (
      id TEXT PRIMARY KEY, title TEXT, company TEXT, workplace TEXT,
      city TEXT, state TEXT, country TEXT, postcode TEXT, type TEXT,
      contractType TEXT, salary TEXT, hours TEXT, summary TEXT,
      description_search TEXT, url TEXT, category TEXT, location TEXT,
      search_blob TEXT, loc_blob TEXT
    );
    CREATE VIRTUAL TABLE jobs_fts
    USING fts5(title, company, category, description_search, location, content='jobs', content_rowid='rowid');
    PRAGMA user_version = 2;
    BEGIN;
  `);
  const insert = database.prepare(`
    INSERT INTO jobs (
      id, title, company, workplace, city, state, country, postcode, type,
      contractType, salary, hours, summary, description_search, url,
      category, location, search_blob, loc_blob
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  for (let index = 0; index < jobCount; index += 1) {
    const title = index === 0
      ? "Statistics Expert - AI Trainer"
      : index === 1
        ? "AI Language Evaluator (German)"
        : `Fixture Role ${index}`;
    const summary = index === 0
      ? "A saved listing used to verify restart-safe search behavior."
      : `Fixture description ${index}`;
    const isGermanListing = index === 1;
    insert.run(
      `legacy-${index}`, title, "Scale AI", "",
      isGermanListing ? "Mannheim" : "Seattle",
      isGermanListing ? "Baden-Wurttemberg" : "WA",
      isGermanListing ? "Germany" : "United States",
      isGermanListing ? "" : "98101", "Contract", "Contract", "", "",
      summary, `${summary} Python JavaScript later searchable content`,
      `https://${APPLY_HOST}/apply/${index}`, "AI Training",
      isGermanListing ? "Mannheim, Baden-Wurttemberg, Germany" : "Seattle, WA, United States",
      `${title} Scale AI AI Training`.toLowerCase(),
      isGermanListing
        ? "mannheim baden wurttemberg germany de"
        : "remote seattle wa washington us united states 98101",
    );
  }
  database.exec("COMMIT; INSERT INTO jobs_fts(jobs_fts) VALUES('rebuild');");
  database.close();
  const time = new Date(modifiedAtMs);
  fs.utimesSync(filePath, time, time);
}

function createVersionZeroLegacySnapshot(filePath, modifiedAtMs = Date.now()) {
  createLegacySnapshot(filePath, modifiedAtMs);
  const database = new DatabaseSync(filePath);
  database.exec(`
    DROP TABLE jobs_fts;
    ALTER TABLE jobs RENAME TO jobs_v2;
    CREATE TABLE jobs (
      id TEXT PRIMARY KEY, title TEXT, company TEXT, workplace TEXT,
      city TEXT, state TEXT, country TEXT, postcode TEXT, type TEXT,
      contractType TEXT, salary TEXT, hours TEXT, summary TEXT, url TEXT,
      category TEXT, location TEXT, search_blob TEXT, loc_blob TEXT
    );
    INSERT INTO jobs (
      rowid, id, title, company, workplace, city, state, country, postcode,
      type, contractType, salary, hours, summary, url, category, location,
      search_blob, loc_blob
    )
    SELECT
      rowid, id, title, company, workplace, city, state, country, postcode,
      type, contractType, salary, hours, summary, url, category, location,
      search_blob, loc_blob
    FROM jobs_v2;
    DROP TABLE jobs_v2;
    CREATE VIRTUAL TABLE jobs_fts
    USING fts5(title, company, category, summary, location, content='jobs', content_rowid='rowid');
    INSERT INTO jobs_fts(jobs_fts) VALUES('rebuild');
    PRAGMA user_version = 0;
  `);
  database.close();
  const time = new Date(modifiedAtMs);
  fs.utimesSync(filePath, time, time);
}

function xmlEscape(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function makeFeed(jobCount, titlePrefix = "Promoted Snapshot Engineer", overrides = {}) {
  const jobs = [];
  for (let index = 0; index < jobCount; index += 1) {
    const title = index === 0 ? titlePrefix : `Promoted Fixture Role ${index}`;
    const fields = {
      referencenumber: `feed-${index}-expVer-${index}`,
      title,
      company: "Scale AI",
      advertiser: "Scale AI",
      category: "AI Training",
      location: "Remote",
      city: "Seattle",
      state: "WA",
      country: "US",
      postalcode: "98101",
      url: `https://${APPLY_HOST}/apply/feed-${index}?market=US`,
      type: "CONTRACT",
      contractType: "Contract",
      salary: "",
      hours: "Flexible",
      description: `Current ${title} listing with full Python and JavaScript details.`,
      ...overrides,
    };
    jobs.push(`<job>${Object.entries(fields).map(([key, value]) => `<${key}>${xmlEscape(value)}</${key}>`).join("")}</job>`);
  }
  return `<?xml version="1.0" encoding="UTF-8"?><source>${jobs.join("")}</source>`;
}

async function reservePort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  await closeServer(server);
  return port;
}

async function startFeedServer(xml, { status = 200, delayMs = 0 } = {}) {
  const server = http.createServer((request, response) => {
    const reply = () => {
      response.statusCode = status;
      response.setHeader("content-type", "application/xml");
      response.end(status === 200 ? xml : "feed unavailable");
    };
    if (delayMs > 0) setTimeout(reply, delayMs);
    else reply();
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  feedServers.add(server);
  const address = server.address();
  return `http://127.0.0.1:${address.port}/feed.xml`;
}

async function closeServer(server) {
  if (!server?.listening) {
    feedServers.delete(server);
    return;
  }
  await new Promise((resolve) => server.close(() => resolve()));
  feedServers.delete(server);
}

async function startApp({ dbPath, snapshotDir, feedUrl, env = {} }) {
  const port = await reservePort();
  const child = spawn(process.execPath, ["--import", "tsx", SERVER_ENTRY], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      PORT: String(port),
      SQLITE_DB_PATH: dbPath,
      SQLITE_SNAPSHOT_DIR: snapshotDir,
      INERGROUP_FEED_URL: feedUrl,
      INERGROUP_APPLY_HOST: APPLY_HOST,
      SYNC_INTERVAL_MS: "60000",
      FEED_FETCH_TIMEOUT_MS: "1500",
      INERGROUP_REFRESH_WORKER_TIMEOUT_MS: "5000",
      INERGROUP_STALE_AFTER_MS: "3600000",
      INERGROUP_MAX_STALE_MS: "86400000",
      INERGROUP_MIN_PROMOTED_JOBS: "25",
      ...env,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.add(child);
  let logs = "";
  child.stdout.on("data", (chunk) => { logs += String(chunk); });
  child.stderr.on("data", (chunk) => { logs += String(chunk); });
  child.logs = () => logs;
  child.baseUrl = `http://127.0.0.1:${port}`;

  await waitFor(async () => {
    try {
      await fetch(`${child.baseUrl}/health`);
      return true;
    } catch {
      if (child.exitCode !== null) throw new Error(`Server exited early.\n${logs}`);
      return false;
    }
  }, 10000);
  return child;
}

async function stopChild(child) {
  if (!child || child.exitCode !== null) {
    children.delete(child);
    return;
  }
  child.kill("SIGTERM");
  await Promise.race([
    new Promise((resolve) => child.once("exit", resolve)),
    new Promise((resolve) => setTimeout(resolve, 3000)),
  ]);
  if (child.exitCode === null) child.kill("SIGKILL");
  children.delete(child);
}

async function waitFor(check, timeoutMs = 10000, intervalMs = 75) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw lastError || new Error(`Condition was not met within ${timeoutMs}ms.`);
}

async function health(child) {
  const response = await fetch(`${child.baseUrl}/health`);
  return { statusCode: response.status, body: await response.json() };
}

async function mcpRequest(child, method, params) {
  const response = await fetch(`${child.baseUrl}/mcp`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
    },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  assert.equal(response.status, 200);
  return response.json();
}

async function readWidget(child) {
  return mcpRequest(child, "resources/read", { uri: "ui://inergroup/job-cards-v1.html" });
}

async function search(child, query, limit = 6) {
  return callSearch(child, { query, limit });
}

async function callSearch(child, args, meta) {
  return mcpRequest(child, "tools/call", {
    name: "search_inergroup_job_listings",
    arguments: args,
    ...(meta ? { _meta: meta } : {}),
  });
}

test("serves and reuses the last-good snapshot when refresh is unavailable", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  const snapshotDir = path.join(directory, "snapshots");
  const savedAt = Date.now() - 2 * 60 * 1000;
  createLegacySnapshot(dbPath, savedAt);
  const unavailableFeed = await startFeedServer("", { status: 503, delayMs: 1200 });

  const app = await startApp({
    dbPath,
    snapshotDir,
    feedUrl: unavailableFeed,
  });

  const startedAt = Date.now();
  const repeatedRequests = await Promise.all(Array.from({ length: 10 }, async () => {
    const [widget, tool] = await Promise.all([readWidget(app), search(app, "Statistics Expert")]);
    return { widget, tool };
  }));
  assert.ok(Date.now() - startedAt < 2500, "resource and search requests should not wait for refresh");
  const { widget, tool } = repeatedRequests[0];
  assert.equal(repeatedRequests.length, 10);
  assert.ok(repeatedRequests.every((result) => result.tool.result.structuredContent.data.totalResults === 1));
  assert.equal(widget.result.contents[0].mimeType, "text/html;profile=mcp-app");
  assert.equal(tool.result.structuredContent.data.totalResults, 1);
  assert.equal(tool.result.structuredContent.data.jobs[0].title, "Statistics Expert - AI Trainer");

  const degraded = await waitFor(async () => {
    const result = await health(app);
    return result.body.reason === "latest_refresh_failed" ? result : null;
  });
  assert.equal(degraded.statusCode, 200);
  assert.equal(degraded.body.status, "degraded");
  assert.equal(degraded.body.ready, true);
  assert.equal(degraded.body.jobs, 25);

  const persisted = JSON.parse(fs.readFileSync(path.join(snapshotDir, "active-snapshot.json"), "utf8"));
  assert.equal(persisted.activeKind, "legacy");
  assert.equal(persisted.jobCount, 25);
  assert.ok(persisted.lastFailureMs > 0);
  const persistedLastSync = persisted.lastSuccessfulSyncMs;

  await stopChild(app);
  const restarted = await startApp({
    dbPath,
    snapshotDir,
    feedUrl: unavailableFeed,
  });
  const restartHealth = await health(restarted);
  assert.equal(restartHealth.statusCode, 200);
  assert.equal(restartHealth.body.lastSync, persistedLastSync);
  const restartSearch = await search(restarted, "Statistics Expert");
  assert.equal(restartSearch.result.structuredContent.data.totalResults, 1);
});

test("keeps resources available and returns a structured error with no valid snapshot", async () => {
  const directory = createTemporaryDirectory();
  const app = await startApp({
    dbPath: path.join(directory, "missing.db"),
    snapshotDir: path.join(directory, "snapshots"),
    feedUrl: "http://127.0.0.1:9/unavailable.xml",
  });

  const initialHealth = await health(app);
  assert.equal(initialHealth.statusCode, 503);
  assert.equal(initialHealth.body.status, "unavailable");
  const widget = await readWidget(app);
  assert.match(widget.result.contents[0].text, /<!doctype html>/i);
  const tool = await search(app, "Statistics Expert");
  assert.equal(tool.result.isError, true);
  assert.deepEqual(tool.result.structuredContent.data.jobs, []);
  assert.equal(tool.result.structuredContent.data.totalResults, 0);
});

test("serves the deployed version-zero legacy database while upgrading in the worker", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  const snapshotDir = path.join(directory, "snapshots");
  createVersionZeroLegacySnapshot(dbPath, Date.now());
  const unavailableFeed = await startFeedServer("", { status: 503, delayMs: 1200 });
  const app = await startApp({ dbPath, snapshotDir, feedUrl: unavailableFeed });

  const result = await health(app);
  assert.equal(result.statusCode, 200);
  assert.equal(result.body.status, "degraded");
  assert.equal(result.body.reason, "legacy_snapshot_pending_upgrade");
  const tool = await search(app, "Statistics Expert");
  assert.equal(tool.result.structuredContent.data.totalResults, 1);

  const afterFailure = await waitFor(async () => {
    const current = await health(app);
    return current.body.reason === "latest_refresh_failed" ? current : null;
  });
  assert.equal(afterFailure.statusCode, 200);
  assert.equal((await search(app, "Statistics Expert")).result.structuredContent.data.totalResults, 1);
});

test("atomically promotes a validated worker-built snapshot", async () => {
  const directory = createTemporaryDirectory();
  const snapshotDir = path.join(directory, "snapshots");
  const feedUrl = await startFeedServer(makeFeed(25), { delayMs: 750 });
  const app = await startApp({
    dbPath: path.join(directory, "missing.db"),
    snapshotDir,
    feedUrl,
  });

  // The HTTP/MCP surface is alive while the separate process is still waiting
  // on the feed; no template request is coupled to database construction.
  const widget = await readWidget(app);
  assert.equal(widget.result.contents[0].mimeType, "text/html;profile=mcp-app");

  const ready = await waitFor(async () => {
    const result = await health(app);
    return result.statusCode === 200 && result.body.status === "ok" ? result : null;
  }, 15000);
  assert.equal(ready.body.jobs, 25);
  assert.equal(ready.body.timestampSource, "feed");

  const manifest = JSON.parse(fs.readFileSync(path.join(snapshotDir, "active-snapshot.json"), "utf8"));
  assert.equal(manifest.activeKind, "generated");
  assert.match(manifest.activeFile, /^jobs-\d{13}-[a-f0-9]{12}\.db$/);
  const snapshotPath = path.join(snapshotDir, manifest.activeFile);
  assert.ok(fs.existsSync(snapshotPath));
  assert.equal(fs.readdirSync(snapshotDir).some((name) => name.endsWith(".tmp")), false);

  const database = new DatabaseSync(snapshotPath, { readOnly: true });
  assert.equal(database.prepare("PRAGMA quick_check").get().quick_check, "ok");
  assert.equal(database.prepare("SELECT COUNT(*) AS n FROM jobs").get().n, 25);
  assert.equal(database.prepare("SELECT COUNT(*) AS n FROM jobs_fts").get().n, 25);
  assert.equal(database.prepare("SELECT job_count FROM snapshot_metadata WHERE id = 1").get().job_count, 25);
  database.close();

  const tool = await search(app, "Promoted Snapshot Engineer");
  assert.equal(tool.result.structuredContent.data.totalResults, 1);
  assert.equal(tool.result.structuredContent.data.jobs[0].title, "Promoted Snapshot Engineer");

  // A corrupt pointer must not discard a valid last-good database. Recovery
  // scans only strict generated filenames, rejects a newer invalid candidate,
  // falls back to the retained valid snapshot, and rewrites the manifest.
  await stopChild(app);
  const invalidCandidate = path.join(snapshotDir, `jobs-${Date.now() + 1}-deadbeefcafe.db`);
  fs.copyFileSync(snapshotPath, invalidCandidate);
  const invalidDatabase = new DatabaseSync(invalidCandidate);
  invalidDatabase.exec("DROP TABLE jobs_fts");
  invalidDatabase.close();
  fs.writeFileSync(path.join(snapshotDir, "active-snapshot.json"), "not-json", "utf8");
  const recovered = await startApp({
    dbPath: path.join(directory, "missing.db"),
    snapshotDir,
    feedUrl: "http://127.0.0.1:9/unavailable.xml",
  });
  const recoveredHealth = await health(recovered);
  assert.equal(recoveredHealth.statusCode, 200);
  assert.equal(recoveredHealth.body.snapshot, manifest.activeFile);
  assert.equal((await search(recovered, "Promoted Snapshot Engineer")).result.structuredContent.data.totalResults, 1);
  const repairedManifest = JSON.parse(fs.readFileSync(path.join(snapshotDir, "active-snapshot.json"), "utf8"));
  assert.equal(repairedManifest.activeFile, manifest.activeFile);
});

test("rejects a bad refresh and leaves the previous snapshot untouched", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  const snapshotDir = path.join(directory, "snapshots");
  createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000);
  const feedUrl = await startFeedServer(makeFeed(1, "Incomplete Feed Role"));
  const app = await startApp({ dbPath, snapshotDir, feedUrl });

  const degraded = await waitFor(async () => {
    const result = await health(app);
    return result.body.reason === "latest_refresh_failed" ? result : null;
  }, 10000);
  assert.equal(degraded.statusCode, 200);
  assert.equal(degraded.body.jobs, 25);
  const manifest = JSON.parse(fs.readFileSync(path.join(snapshotDir, "active-snapshot.json"), "utf8"));
  assert.equal(manifest.activeKind, "legacy");
  assert.equal(manifest.activeFile, null);
  assert.equal(fs.readdirSync(snapshotDir).some((name) => /^jobs-.*\.db$/.test(name)), false);

  const oldResult = await search(app, "Statistics Expert");
  assert.equal(oldResult.result.structuredContent.data.totalResults, 1);
  const rejectedResult = await search(app, "Incomplete Feed Role");
  assert.equal(rejectedResult.result.structuredContent.data.totalResults, 0);
});

test("rejects a small first snapshot below the absolute promotion floor", async () => {
  const directory = createTemporaryDirectory();
  const snapshotDir = path.join(directory, "snapshots");
  const feedUrl = await startFeedServer(makeFeed(25, "Partial Initial Feed"));
  const app = await startApp({
    dbPath: path.join(directory, "missing.db"),
    snapshotDir,
    feedUrl,
    env: { INERGROUP_MIN_PROMOTED_JOBS: "3000" },
  });

  const unavailable = await waitFor(async () => {
    const result = await health(app);
    return result.body.reason === "refresh_failed_no_valid_snapshot" && result.body.lastRefreshFailureMs ? result : null;
  }, 10000);
  assert.equal(unavailable.statusCode, 503);
  assert.equal(unavailable.body.jobs, 0);
  assert.equal(fs.readdirSync(snapshotDir).some((name) => /^jobs-.*\.db$/.test(name)), false);
});

test("reports stale snapshots as degraded and expires them only at the configured maximum", async (t) => {
  await t.test("stale but usable", async () => {
    const directory = createTemporaryDirectory();
    const dbPath = path.join(directory, "jobs.db");
    createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 60 * 1000);
    const app = await startApp({
      dbPath,
      snapshotDir: path.join(directory, "snapshots"),
      feedUrl: "http://127.0.0.1:9/unavailable.xml",
      env: {
        SYNC_INTERVAL_MS: "86400000",
        INERGROUP_STALE_AFTER_MS: "3600000",
        INERGROUP_MAX_STALE_MS: "10800000",
      },
    });
    const result = await health(app);
    assert.equal(result.statusCode, 200);
    assert.equal(result.body.status, "degraded");
    assert.equal(result.body.reason, "snapshot_is_stale");
    assert.equal((await search(app, "Statistics Expert")).result.structuredContent.data.totalResults, 1);
    await stopChild(app);
  });

  await t.test("expired and not used for search", async () => {
    const directory = createTemporaryDirectory();
    const dbPath = path.join(directory, "jobs.db");
    createLegacySnapshot(dbPath, Date.now() - 4 * 60 * 60 * 1000);
    const app = await startApp({
      dbPath,
      snapshotDir: path.join(directory, "snapshots"),
      feedUrl: "http://127.0.0.1:9/unavailable.xml",
      env: {
        SYNC_INTERVAL_MS: "86400000",
        INERGROUP_STALE_AFTER_MS: "3600000",
        INERGROUP_MAX_STALE_MS: "10800000",
      },
    });
    const result = await health(app);
    assert.equal(result.statusCode, 503);
    assert.equal(result.body.status, "expired");
    const tool = await search(app, "Statistics Expert");
    assert.equal(tool.result.isError, true);
    assert.equal(tool.result.structuredContent.data.totalResults, 0);
    assert.equal((await readWidget(app)).result.contents[0].mimeType, "text/html;profile=mcp-app");
  });
});

test("rejects truncated XML and preserves the last-good snapshot", async (t) => {
  const completeFeed = makeFeed(25, "Replacement Engineer");
  const cases = [
    { name: "missing root close", xml: completeFeed.replace(/<\/source>$/, "") },
    { name: "partial final job", xml: completeFeed.slice(0, -12) },
  ];

  for (const fixture of cases) {
    await t.test(fixture.name, async () => {
      const directory = createTemporaryDirectory();
      const dbPath = path.join(directory, "jobs.db");
      const snapshotDir = path.join(directory, "snapshots");
      createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000);
      const feedUrl = await startFeedServer(fixture.xml);
      const app = await startApp({ dbPath, snapshotDir, feedUrl });

      const degraded = await waitFor(async () => {
        const result = await health(app);
        return result.body.reason === "latest_refresh_failed" ? result : null;
      }, 10000);
      assert.equal(degraded.statusCode, 200);
      assert.equal(degraded.body.jobs, 25);
      assert.equal((await search(app, "Statistics Expert")).result.structuredContent.data.totalResults, 1);
      assert.equal((await search(app, "Replacement Engineer")).result.structuredContent.data.totalResults, 0);
      await stopChild(app);
    });
  }
});

test("parses expected source/job XML without raw closing-tag slicing", async () => {
  const directory = createTemporaryDirectory();
  const snapshotDir = path.join(directory, "snapshots");
  const description = "Current CDATA Safe Engineer listing with full Python and JavaScript details.";
  const xml = makeFeed(25, "CDATA Safe Engineer")
    .replace("<source>", "<source><!-- a literal <job></job> in a valid comment is not a listing -->")
    .replace(
      "<title>CDATA Safe Engineer</title>",
      "<title><![CDATA[CDATA Safe Engineer]]></title>",
    )
    .replace(
      `<description>${description}</description>`,
      `<description><![CDATA[<p>${description} A literal </job> here is text.</p>]]><!-- valid field comment --></description>`,
    );
  const feedUrl = await startFeedServer(xml);
  const app = await startApp({
    dbPath: path.join(directory, "missing.db"),
    snapshotDir,
    feedUrl,
  });

  const ready = await waitFor(async () => {
    const result = await health(app);
    return result.body.status === "ok" ? result : null;
  }, 15000);
  assert.equal(ready.body.jobs, 25);
  const result = await search(app, "CDATA Safe Engineer");
  assert.equal(result.result.structuredContent.data.status, "ok");
  assert.equal(result.result.structuredContent.data.totalResults, 1);
  assert.equal(result.result.structuredContent.data.jobs[0].title, "CDATA Safe Engineer");
});

test("rejects unexpected feed roots and non-direct job elements", async (t) => {
  const fixtures = [
    {
      name: "unexpected root",
      xml: makeFeed(25, "Wrong Root Engineer")
        .replace("<source>", "<jobs>")
        .replace("</source>", "</jobs>"),
    },
    {
      name: "nested jobs",
      xml: makeFeed(25, "Nested Job Engineer")
        .replace("<source>", "<source><wrapper>")
        .replace("</source>", "</wrapper></source>"),
    },
  ];

  for (const fixture of fixtures) {
    await t.test(fixture.name, async () => {
      const directory = createTemporaryDirectory();
      const dbPath = path.join(directory, "jobs.db");
      const snapshotDir = path.join(directory, "snapshots");
      createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000);
      const feedUrl = await startFeedServer(fixture.xml);
      const app = await startApp({ dbPath, snapshotDir, feedUrl });

      const degraded = await waitFor(async () => {
        const result = await health(app);
        return result.body.reason === "latest_refresh_failed" ? result : null;
      }, 10000);
      assert.equal(degraded.body.jobs, 25);
      assert.equal((await search(app, "Statistics Expert")).result.structuredContent.data.totalResults, 1);
      assert.equal((await search(app, "Engineer")).result.structuredContent.data.totalResults, 0);
      await stopChild(app);
    });
  }
});

test("fails the entire refresh for an oversized job regardless of invalid-row allowance", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  const snapshotDir = path.join(directory, "snapshots");
  createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000);
  const ordinaryDescription = "Current Oversized Feed Engineer listing with full Python and JavaScript details.";
  const xml = makeFeed(26, "Oversized Feed Engineer").replace(
    `<description>${ordinaryDescription}</description>`,
    `<description>${"x".repeat(270_000)}</description>`,
  );
  const feedUrl = await startFeedServer(xml);
  const app = await startApp({
    dbPath,
    snapshotDir,
    feedUrl,
    env: { INERGROUP_MAX_INVALID_JOB_PERCENT: "100" },
  });

  const degraded = await waitFor(async () => {
    const result = await health(app);
    return result.body.reason === "latest_refresh_failed" ? result : null;
  }, 10000);
  assert.equal(degraded.body.jobs, 25);
  assert.equal((await search(app, "Statistics Expert")).result.structuredContent.data.totalResults, 1);
  assert.equal((await search(app, "Oversized Feed Engineer")).result.structuredContent.data.totalResults, 0);
});

test("counts empty required fields as invalid before snapshot promotion", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  const snapshotDir = path.join(directory, "snapshots");
  createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000);
  const xml = makeFeed(26, "Missing Required Title").replace(
    "<title>Missing Required Title</title>",
    "<title></title>",
  );
  const feedUrl = await startFeedServer(xml);
  const app = await startApp({ dbPath, snapshotDir, feedUrl });

  const degraded = await waitFor(async () => {
    const result = await health(app);
    return result.body.reason === "latest_refresh_failed" ? result : null;
  }, 10000);
  assert.equal(degraded.body.jobs, 25);
  assert.equal((await search(app, "Statistics Expert")).result.structuredContent.data.totalResults, 1);
  assert.equal((await search(app, "Promoted Fixture Role")).result.structuredContent.data.totalResults, 0);
});

test("enforces the configured last-good snapshot retention threshold", async (t) => {
  await t.test("rejects a refresh below 80 percent", async () => {
    const directory = createTemporaryDirectory();
    const dbPath = path.join(directory, "jobs.db");
    const snapshotDir = path.join(directory, "snapshots");
    createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000, 40);
    const feedUrl = await startFeedServer(makeFeed(31, "Below Threshold Engineer"));
    const app = await startApp({
      dbPath,
      snapshotDir,
      feedUrl,
      env: { INERGROUP_MIN_SNAPSHOT_RETENTION_PERCENT: "80" },
    });

    const degraded = await waitFor(async () => {
      const result = await health(app);
      return result.body.reason === "latest_refresh_failed" ? result : null;
    }, 10000);
    assert.equal(degraded.body.jobs, 40);
    assert.equal((await search(app, "Below Threshold Engineer")).result.structuredContent.data.totalResults, 0);
    await stopChild(app);
  });

  await t.test("accepts a refresh at 80 percent", async () => {
    const directory = createTemporaryDirectory();
    const dbPath = path.join(directory, "jobs.db");
    const snapshotDir = path.join(directory, "snapshots");
    createLegacySnapshot(dbPath, Date.now() - 2 * 60 * 1000, 40);
    const feedUrl = await startFeedServer(makeFeed(32, "At Threshold Engineer"));
    const app = await startApp({
      dbPath,
      snapshotDir,
      feedUrl,
      env: { INERGROUP_MIN_SNAPSHOT_RETENTION_PERCENT: "80" },
    });

    const ready = await waitFor(async () => {
      const result = await health(app);
      return result.body.status === "ok" && result.body.jobs === 32 ? result : null;
    }, 15000);
    assert.equal(ready.statusCode, 200);
    assert.equal((await search(app, "At Threshold Engineer")).result.structuredContent.data.totalResults, 1);
    await stopChild(app);
  });
});

test("normalizes feed and requested US subdivisions symmetrically", async () => {
  const directory = createTemporaryDirectory();
  const snapshotDir = path.join(directory, "snapshots");
  const feedUrl = await startFeedServer(makeFeed(25, "Princeton AI Engineer", {
    city: "Princeton",
    state: "New Jersey",
    country: "US",
  }));
  const app = await startApp({
    dbPath: path.join(directory, "missing.db"),
    snapshotDir,
    feedUrl,
  });

  await waitFor(async () => {
    const result = await health(app);
    return result.body.status === "ok" ? result : null;
  }, 15000);

  for (const region of ["NJ", "New Jersey"]) {
    const response = await callSearch(app, {
      query: "Princeton AI Engineer",
      market: { countryCode: "US", region },
      limit: 8,
    });
    assert.equal(response.result.structuredContent.data.status, "ok");
    assert.equal(response.result.structuredContent.data.totalResults, 1);
    assert.ok(response.result.structuredContent.data.jobs.every((job) => job.location === "Princeton, NJ, United States"));
  }
});

test("enforces the scanned tool contract and distinct result states", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  createLegacySnapshot(dbPath);
  const app = await startApp({
    dbPath,
    snapshotDir: path.join(directory, "snapshots"),
    feedUrl: "http://127.0.0.1:9/unavailable.xml",
    env: { SYNC_INTERVAL_MS: "86400000" },
  });

  const tools = await mcpRequest(app, "tools/list", {});
  const descriptor = tools.result.tools.find((tool) => tool.name === "search_inergroup_job_listings");
  assert.equal(descriptor.inputSchema.additionalProperties, false);
  assert.equal(descriptor.inputSchema.properties.market.additionalProperties, false);
  assert.deepEqual(Object.keys(descriptor.inputSchema.properties).sort(), ["limit", "market", "query"]);
  assert.equal(descriptor.inputSchema.properties.limit.maximum, 50);
  assert.match(descriptor.inputSchema.properties.limit.description, /at most eight/i);
  assert.match(descriptor.description, /one tool call per explicit search request/i);
  assert.match(descriptor.description, /do not broaden the query, substitute synonyms, or retry/i);
  assert.deepEqual(
    descriptor.outputSchema.properties.data.properties.status.enum,
    ["ok", "no_results", "invalid_request", "unavailable"],
  );

  const invalidArguments = [
    { query: "engineer", extra: true },
    { query: "engineer", limit: "6" },
    { query: "engineer", limit: 51 },
    { query: "engineer", limit: 0 },
    { query: "engineer", market: { countryCode: "US", extra: true } },
    { query: "engineer", market: { countryCode: "US" }, extra: true },
    { query: " " },
    { query: "jobs" },
    { query: "roles", market: { countryCode: "US" } },
  ];
  for (const args of invalidArguments) {
    const response = await callSearch(app, args);
    assert.equal(response.result.isError, true);
    assert.equal(response.result.structuredContent.data.status, "invalid_request");
    assert.equal(response.result.structuredContent.data.totalResults, 0);
    assert.deepEqual(response.result.structuredContent.data.jobs, []);
  }

  const oversizedLimit = await callSearch(app, { query: "Fixture jobs", limit: 10 });
  assert.equal(oversizedLimit.result.isError, undefined);
  assert.equal(oversizedLimit.result.structuredContent.data.status, "ok");
  assert.equal(oversizedLimit.result.structuredContent.data.appliedFilters.query, "Fixture");
  assert.equal(oversizedLimit.result.structuredContent.data.appliedFilters.limit, 8);
  assert.ok(oversizedLimit.result.structuredContent.data.totalResults > 8);
  assert.equal(oversizedLimit.result.structuredContent.data.jobs.length, 8);

  const remoteGermany = await callSearch(app, {
    query: "remote AI",
    market: { countryCode: "DE" },
    limit: 10,
  });
  assert.equal(remoteGermany.result.isError, undefined);
  assert.equal(remoteGermany.result.structuredContent.data.status, "no_results");
  assert.equal(remoteGermany.result.structuredContent.data.totalResults, 0);
  assert.equal(remoteGermany.result.structuredContent.data.appliedFilters.query, "remote AI");
  assert.equal(remoteGermany.result.structuredContent.data.appliedFilters.limit, 8);
  assert.deepEqual(remoteGermany.result.structuredContent.data.jobs, []);
  assert.match(remoteGermany.result.content[0].text, /do not retry/i);

  const noBroadRemoteRetry = await callSearch(app, { query: "remote product designer" });
  assert.equal(noBroadRemoteRetry.result.isError, undefined);
  assert.equal(noBroadRemoteRetry.result.structuredContent.data.status, "no_results");
  assert.equal(noBroadRemoteRetry.result.structuredContent.data.totalResults, 0);

  const empty = await callSearch(app, { query: "Nonexistent Quantum Banana Role" });
  assert.equal(empty.result.isError, undefined);
  assert.equal(empty.result.structuredContent.data.status, "no_results");

  const successful = await callSearch(app, { query: "Statistics Expert" });
  assert.equal(successful.result.structuredContent.data.status, "ok");
  assert.equal(successful.result.structuredContent.data.totalResults, 1);

  // Identical search arguments return the same catalog results regardless of
  // client location metadata, including hints outside the fixture's market.
  for (const hint of [
    { city: "Bengaluru", region: "Karnataka", country: "IN" },
    { city: "Sydney", region: "New South Wales", country: "AU" },
  ]) {
    const response = await callSearch(app, { query: "Statistics Expert" }, {
      "openai/userLocation": hint,
    });
    assert.deepEqual(response.result.structuredContent, successful.result.structuredContent);
  }

  // Explicit markets still work even if the host hint names another country.
  const filtered = await callSearch(app, {
    query: "Statistics Expert", market: { countryCode: "DE" },
  }, { "openai/userLocation": { city: "Seattle", region: "WA", country: "US" } });
  assert.equal(filtered.result.structuredContent.data.status, "no_results");
  assert.deepEqual(filtered.result.structuredContent.data.appliedFilters.location, {
    source: "market", country: "Germany",
  });
  const afterFiltered = await callSearch(app, { query: "Statistics Expert" });
  assert.deepEqual(afterFiltered.result.structuredContent, successful.result.structuredContent);

  for (const method of ["GET", "DELETE", "PUT", "PATCH"]) {
    const response = await fetch(`${app.baseUrl}/mcp`, { method });
    assert.equal(response.status, 405);
    assert.equal(response.headers.get("allow"), "POST");
  }

  const preflight = await fetch(`${app.baseUrl}/mcp`, {
    method: "OPTIONS",
    headers: {
      origin: "https://chatgpt.com",
      "access-control-request-method": "POST",
      "access-control-request-headers": "content-type,mcp-protocol-version",
    },
  });
  assert.equal(preflight.status, 204);
  assert.equal(preflight.headers.get("access-control-allow-methods"), "POST,OPTIONS");
  assert.match(preflight.headers.get("access-control-allow-headers") || "", /MCP-Protocol-Version/i);

  const oversized = await fetch(`${app.baseUrl}/mcp`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: 99,
      method: "tools/call",
      params: { name: "search_inergroup_job_listings", arguments: { query: "x".repeat(70_000) } },
    }),
  });
  assert.equal(oversized.status, 413);
  assert.equal((await oversized.json()).error.code, -32600);

  const afterOversized = await callSearch(app, { query: "Statistics Expert" });
  assert.equal(afterOversized.result.structuredContent.data.status, "ok");
});

test("rate-limits abusive MCP clients without changing ordinary requests", async () => {
  const directory = createTemporaryDirectory();
  const dbPath = path.join(directory, "jobs.db");
  createLegacySnapshot(dbPath);
  const app = await startApp({
    dbPath,
    snapshotDir: path.join(directory, "snapshots"),
    feedUrl: "http://127.0.0.1:9/unavailable.xml",
    env: {
      SYNC_INTERVAL_MS: "86400000",
      INERGROUP_MCP_RATE_LIMIT_MAX_REQUESTS: "1",
    },
  });

  const first = await mcpRequest(app, "tools/list", {});
  assert.ok(first.result.tools.some((tool) => tool.name === "search_inergroup_job_listings"));
  const second = await fetch(`${app.baseUrl}/mcp`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }),
  });
  assert.equal(second.status, 429);
  assert.equal(second.headers.get("retry-after"), "60");
  assert.equal((await second.json()).error.code, -32000);
});
