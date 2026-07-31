import { describe, expect, it } from "vitest";
import { script } from "../src/ui/script";
import { adminCard, browseTab } from "../src/ui/markup";

/**
 * De clientcode is één grote template-string, dus de compiler kijkt er niet in:
 * een ontbrekende komma of een backtick te veel merk je pas in de browser. Deze
 * twee tests vangen precies dat af — of het JavaScript parseert, en of elk
 * element waar een knop aan hangt ook echt in de markup staat.
 */

describe("gegenereerde clientcode", () => {
  it("is geldig JavaScript", () => {
    // new Function parseert wel, maar voert niets uit: er is hier geen document.
    expect(() => new Function(script)).not.toThrow();
  });

  it("bevat geen backtick of ${, want de string is zelf een template", () => {
    expect(script).not.toContain("`");
    expect(script).not.toContain("${");
  });

  it("spreekt alleen elementen aan die in de markup staan", () => {
    const markup = adminCard() + browseTab();
    const ids = [...script.matchAll(/\$\("([a-zA-Z0-9_-]+)"\)/g)].map((m) => m[1]!);
    // Alleen de nieuwe log- en wisknoppen; de rest van de UI zit in andere
    // tabbladen die deze test niet inleest.
    const own = [
      "logCopy", "logRefresh", "logClear", "logLevel", "appLogs",
      "wipe", "wipeAll", "autoPause",
    ];
    for (const id of own) {
      expect(ids, `${id} wordt aangesproken`).toContain(id);
      expect(markup, `${id} staat in de markup`).toContain(`id="${id}"`);
    }
  });
});
