import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const WIDGET_PATH = path.resolve(TEST_DIR, "..", "public", "widget", "job-cards.html");

function loadWidgetHarness() {
  const html = fs.readFileSync(WIDGET_PATH, "utf8");
  const script = html.match(/<script type="module">([\s\S]*?)<\/script>/)?.[1];
  assert.ok(script, "widget module script should exist");

  const root = {
    innerHTML: "",
    hidden: false,
    attributes: new Map(),
    addEventListener() {},
    querySelector() { return null; },
    setAttribute(name, value) { this.attributes.set(name, value); },
  };
  let closeRequests = 0;
  const context = {
    console,
    TextEncoder,
    URL,
    Element: class Element {},
    HTMLButtonElement: class HTMLButtonElement {},
    document: {
      body: { style: {} },
      documentElement: { style: {} },
      getElementById: () => root,
    },
    window: {
      addEventListener() {},
      matchMedia: () => ({ matches: true }),
      openai: {
        requestClose() {
          closeRequests += 1;
          return Promise.resolve();
        },
      },
    },
  };
  vm.runInNewContext(`${script}\nglobalThis.__renderJobs = renderJobs;`, context);
  return { root, renderJobs: context.__renderJobs, closeRequestCount: () => closeRequests };
}

test("shows an empty-result message and stays available for later results", () => {
  const { root, renderJobs, closeRequestCount } = loadWidgetHarness();
  renderJobs({
    structuredContent: {
      type: "application/json",
      data: { status: "no_results", appliedFilters: { query: "engineer", limit: 6 }, totalResults: 0, jobs: [] },
    },
  });

  assert.match(root.innerHTML, /No output found/);
  assert.doesNotMatch(root.innerHTML, /class="job-card"/);
  assert.equal(root.hidden, false);
  assert.equal(root.attributes.get("aria-busy"), "false");
  assert.equal(closeRequestCount(), 0);

  renderJobs({
    type: "application/json",
    data: {
      status: "ok", totalResults: 1,
      jobs: [{ title: "Python Engineer", employer: "Example", location: "United States" }],
    },
  });
  assert.match(root.innerHTML, /Python Engineer/);
  assert.match(root.innerHTML, /class="job-card"/);
  assert.doesNotMatch(root.innerHTML, /No output found/);
  assert.equal(root.hidden, false);

  renderJobs({
    type: "application/json",
    data: { status: "no_results", totalResults: 0, jobs: [] },
  });
  assert.match(root.innerHTML, /No output found/);
  assert.doesNotMatch(root.innerHTML, /class="job-card"/);
  assert.equal(closeRequestCount(), 0);
});

test("renders distinct error states without presenting them as no results", () => {
  const cases = [
    ["invalid_request", "could not be processed"],
    ["unavailable", "temporarily unavailable"],
  ];

  for (const [status, expectedText] of cases) {
    const { root, renderJobs } = loadWidgetHarness();
    renderJobs({
      structuredContent: {
        type: "application/json",
        data: { status, appliedFilters: { query: "engineer", limit: 6 }, totalResults: 0, jobs: [] },
      },
    });
    assert.match(root.innerHTML, new RegExp(expectedText, "i"));
    assert.doesNotMatch(root.innerHTML, /No output found/i);
    assert.equal(root.hidden, false);
    assert.equal(root.attributes.get("aria-busy"), "false");
  }
});

test("does not disguise malformed or contradictory successful output", () => {
  for (const payload of [
    null,
    { type: "application/json", data: { jobs: [] } },
    {
      type: "application/json",
      data: { status: "ok", appliedFilters: { query: "engineer", limit: 6 }, totalResults: 0, jobs: [] },
    },
  ]) {
    const { root, renderJobs } = loadWidgetHarness();
    renderJobs(payload);
    assert.match(root.innerHTML, /Unable to display job listings right now/i);
    assert.doesNotMatch(root.innerHTML, /No output found/i);
  }
});
