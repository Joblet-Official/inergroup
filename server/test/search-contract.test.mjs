import test from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";

import {
  normalizeSearchDocument,
  normalizeSubdivision,
  searchDb,
} from "../src/index.ts";

const APPLY_HOST = "tnl2.jometer.com";

function createSearchDatabase(fixtures) {
  const database = new DatabaseSync(":memory:");
  database.exec(`
    CREATE TABLE jobs (
      id TEXT PRIMARY KEY, title TEXT, company TEXT, workplace TEXT,
      city TEXT, state TEXT, country TEXT, postcode TEXT, type TEXT,
      contractType TEXT, salary TEXT, hours TEXT, summary TEXT,
      description_search TEXT, url TEXT, category TEXT, location TEXT,
      search_blob TEXT, loc_blob TEXT
    );
    CREATE VIRTUAL TABLE jobs_fts
    USING fts5(title, company, category, description_search, location, content='jobs', content_rowid='rowid');
    BEGIN;
  `);

  const insert = database.prepare(`
    INSERT INTO jobs (
      id, title, company, workplace, city, state, country, postcode, type,
      contractType, salary, hours, summary, description_search, url,
      category, location, search_blob, loc_blob
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  fixtures.forEach((fixture, index) => {
    const country = fixture.country || "United States";
    const state = fixture.state || "WA";
    const city = fixture.city || "Seattle";
    const description = fixture.description || "Current role description.";
    insert.run(
      fixture.id,
      fixture.title,
      fixture.company || "Scale AI",
      fixture.workplace || "Remote",
      city,
      state,
      country,
      fixture.postcode || "98101",
      fixture.type || "Contract",
      fixture.contractType || "Contract",
      "",
      "Flexible",
      description.slice(0, 220),
      normalizeSearchDocument(description),
      `https://${APPLY_HOST}/apply/search-${index}`,
      fixture.category || "AI Training",
      [city, state, country].filter(Boolean).join(", "),
      "",
      "",
    );
  });
  database.exec("COMMIT; INSERT INTO jobs_fts(jobs_fts) VALUES('rebuild');");
  return database;
}

function titles(result) {
  return result.jobs.map((job) => job.title);
}

test("returns exact role and full-description matches without broad prefix leakage", () => {
  const database = createSearchDatabase([
    { id: "cpp", title: "LLM Engineer", description: "Requires C++ and Python." },
    { id: "python-title", title: "Python Specialist", description: "Builds evaluation workflows." },
    { id: "csharp", title: "Backend Engineer", description: "Requires C# and .NET." },
    { id: "r-language", title: "Data Analyst", description: "Uses R, SQL, and statistics." },
    { id: "research", title: "Research Counsel", description: "Works with R&D governance." },
    { id: "qa", title: "QA Engineer", description: "Owns software quality assurance." },
    {
      id: "product-owner",
      title: "Director, Commercial AI Product Owner",
      description: "Sets engineering standards for deployment and QA/testing.",
    },
    { id: "account-manager", title: "Enterprise Account Manager", description: "Supports customer accounts." },
    {
      id: "strategy-manager",
      title: "Senior Manager, Global Strategy",
      description: "Owns accountability measures and accounts reporting.",
    },
    { id: "credit", title: "AI Trainer", description: "A credit check may be required." },
    { id: "payout", title: "Flexible AI Trainer", description: "Includes weekly payouts." },
    {
      id: "remote-ai-germany",
      title: "AI Language Evaluator (German)",
      description: "Evaluate German-language AI responses.",
      city: "Remote",
      state: "",
      country: "Germany",
    },
  ]);

  const expectations = [
    ["C++", ["LLM Engineer"]],
    ["C#", ["Backend Engineer"]],
    [".NET", ["Backend Engineer"]],
    ["R", ["Data Analyst"]],
    ["QA engineer", ["QA Engineer"]],
    ["account manager", ["Enterprise Account Manager"]],
    ["credit check", ["AI Trainer"]],
    ["weekly payouts", ["Flexible AI Trainer"]],
    ["Python", ["Python Specialist", "LLM Engineer"]],
    ["senior nurse", []],
  ];

  for (const [query, expectedTitles] of expectations) {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const result = searchDb(database, query, null, 8);
      assert.equal(result.total, expectedTitles.length, query);
      assert.deepEqual(titles(result), expectedTitles, query);
    }
  }

  const remoteAi = searchDb(database, "remote AI", {
    country: "Germany",
    label: "Germany",
  }, 8);
  assert.equal(remoteAi.total, 0);
  assert.deepEqual(titles(remoteAi), []);

  const noBroadOrRetry = searchDb(database, "product designer", null, 8);
  assert.equal(noBroadOrRetry.total, 0);
  assert.deepEqual(titles(noBroadOrRetry), []);

  for (const query of ['"', "*", "OR", "NEAR(", "R&D"]) {
    assert.doesNotThrow(() => searchDb(database, query, null, 8));
  }
  database.close();
});

test("matches canonical and legacy country-scoped subdivision values", () => {
  const database = createSearchDatabase([
    { id: "nj-code", title: "Princeton Engineer", city: "Princeton", state: "NJ" },
    { id: "nj-name", title: "Princeton Engineer", city: "Princeton", state: "New Jersey" },
    { id: "qc-code", title: "Montreal Engineer", city: "Montreal", state: "QC", country: "Canada" },
    { id: "qc-name", title: "Montreal Engineer", city: "Montreal", state: "Quebec", country: "Canada" },
  ]);

  for (const region of ["NJ", "New Jersey"]) {
    const result = searchDb(database, "Princeton Engineer", {
      country: "United States",
      region,
      label: `${region}, United States`,
    }, 8);
    assert.equal(result.total, 2);
  }

  for (const region of ["QC", "Quebec"]) {
    const result = searchDb(database, "Montreal Engineer", {
      country: "Canada",
      region,
      label: `${region}, Canada`,
    }, 8);
    assert.equal(result.total, 2);
  }

  assert.equal(normalizeSubdivision("New Jersey", "United States"), "NJ");
  assert.equal(normalizeSubdivision("Quebec", "Canada"), "QC");
  assert.equal(normalizeSubdivision("Remote", "United States"), "");
  database.close();
});

test("filters a role search to a specific city", () => {
  const database = createSearchDatabase([
    { id: "elgin-wh", title: "Warehouse Associate", city: "Elgin", state: "IL" },
    { id: "dallas-wh", title: "Warehouse Associate", city: "Dallas", state: "TX" },
    { id: "elgin-elec", title: "Electrician", city: "Elgin", state: "IL" },
  ]);

  const inCity = searchDb(database, "warehouse", { country: "United States", city: "Elgin", label: "Elgin" }, 8);
  assert.equal(inCity.total, 1);
  assert.deepEqual(inCity.jobs.map((job) => job.city), ["Elgin"]);

  // City match is case-insensitive.
  const lower = searchDb(database, "warehouse", { country: "United States", city: "elgin", label: "elgin" }, 8);
  assert.equal(lower.total, 1);

  // A role that is not present in the requested city returns nothing.
  const wrongRole = searchDb(database, "electrician", { country: "United States", city: "Dallas", label: "Dallas" }, 8);
  assert.equal(wrongRole.total, 0);

  // An unknown city returns nothing rather than leaking other cities.
  const unknownCity = searchDb(database, "warehouse", { country: "United States", city: "Nowhere", label: "Nowhere" }, 8);
  assert.equal(unknownCity.total, 0);

  database.close();
});
