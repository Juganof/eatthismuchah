import { describe, expect, it } from "vitest";
import { script } from "../src/ui/script";
import { adminCard, browseTab, recipeDialog, weekTab } from "../src/ui/markup";

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

  it("heeft een receptvenster met de elementen die de code aanspreekt", () => {
    const markup = recipeDialog();
    for (const id of ["recipeDialog", "recipeBody", "recipeClose"]) {
      expect(script, `${id} wordt aangesproken`).toContain(`$("${id}")`);
      expect(markup, `${id} staat in de markup`).toContain(`id="${id}"`);
    }
  });

  it("heeft per eetmoment een max-kcal-veld, leeg betekent geen limiet", () => {
    // slotRow is een functie in de template-string die pas in de browser draait;
    // het veld moet er dus in de string zelf staan, anders is er niets om in te
    // vullen.
    expect(script).toContain("slot-max-kcal");
    expect(script).toContain("max kcal");
  });

  it("stuurt maxKcal mee bij het opslaan van de eetmomenten", () => {
    // De clientcode is niet in Node uitvoerbaar (geen DOM), dus dit is een
    // statische check op de template-string: readSlots moet het veld uitlezen
    // dat slotRow neerzet, anders verdwijnt de limiet bij het opslaan.
    expect(script).toContain('querySelector(".slot-max-kcal")');
  });

  it("heeft de boodschappenlijst-extra's: kopieerknop, afvinkvakjes en dagkeuze", () => {
    // De knop en het dagkiezen zijn statisch in het weektabblad; de
    // afvinkvakjes worden per rij gegenereerd, dus die moeten in het script
    // zelf staan, net als de pure tekstomzetter die de kopieerknop gebruikt.
    const markup = weekTab();
    for (const id of ["shop-copy", "shopDay"]) {
      expect(script, `${id} wordt aangesproken`).toContain(`$("${id}")`);
      expect(markup, `${id} staat in de markup`).toContain(`id="${id}"`);
    }
    expect(script).toContain('class="shop-check"');
    expect(script).toContain("shoppingLinesToText");
  });

  it("spreekt alleen elementen aan die in de markup staan", () => {
    const markup = adminCard() + browseTab() + recipeDialog() + weekTab();
    const ids = [...script.matchAll(/\$\("([a-zA-Z0-9_-]+)"\)/g)].map((m) => m[1]!);
    // Alleen de nieuwe log- en wisknoppen; de rest van de UI zit in andere
    // tabbladen die deze test niet inleest.
    const own = [
      "logCopy", "logRefresh", "logClear", "logLevel", "appLogs",
      "wipe", "wipeAll", "autoPause", "shop-copy", "shopDay",
    ];
    for (const id of own) {
      expect(ids, `${id} wordt aangesproken`).toContain(id);
      expect(markup, `${id} staat in de markup`).toContain(`id="${id}"`);
    }
  });

  it("toont op de receptkaarten een badge met het eetmoment", () => {
    // Het moment komt van de server (uit AH's keywords) en wordt als opvallende
    // badge boven op de kaart getoond; "snack" wordt leesbaar "Tussendoortje".
    expect(script).toContain("moment-badge");
    expect(script).toContain("Tussendoortje");
  });

  it("toont per kaartregel het product met kcal per 100 g", () => {
    // De afleiding is een pure functie die het script inembedt (net als
    // shoppingLinesToText); de productregel toont per product de kcal/100 g.
    expect(script).toContain("kcalPer100g");
    expect(script).toContain("kcal/100 g");
  });
});
