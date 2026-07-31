import type { Nutrients } from "../ah/types";

/**
 * Rekent een profiel om naar dagdoelen. Alles hier is pure rekenkunde zonder D1
 * of netwerk, zodat de UI dezelfde getallen live kan tonen als de planner gebruikt.
 */

export type Sex = "man" | "vrouw";
export type ActivityLevel = "sedentary" | "light" | "moderate" | "active" | "very_active";
export type Goal = "cut" | "maintain" | "bulk";

export interface Profile {
  age: number | null;
  sex: Sex | null;
  heightCm: number | null;
  weightKg: number | null;
  activityLevel: ActivityLevel;
  goal: Goal;
  /** Snelheid in kg per week, altijd als positief getal; `goal` bepaalt de richting. */
  rateKgPerWeek: number;
  proteinPerKg: number;
  fatPerKg: number;
  /** Gezet? Dan wint dit van de berekende TDEE. */
  kcalOverride: number | null;
  /** Sleutels uit DIETS in ./diet.ts. */
  diet: string[];
}

export const DEFAULT_PROFILE: Profile = {
  age: null,
  sex: null,
  heightCm: null,
  weightKg: null,
  activityLevel: "moderate",
  goal: "maintain",
  rateKgPerWeek: 0.5,
  proteinPerKg: 1.8,
  fatPerKg: 0.8,
  kcalOverride: null,
  diet: [],
};

/** Standaard PAL-factoren; "moderate" is 3-5 keer per week sporten. */
export const ACTIVITY_FACTORS: Record<ActivityLevel, number> = {
  sedentary: 1.2,
  light: 1.375,
  moderate: 1.55,
  active: 1.725,
  very_active: 1.9,
};

/** Eén kilo lichaamsvet is ruwweg 7700 kcal. */
const KCAL_PER_KG = 7700;

/** Onder deze grens is een dagdoel niet meer verantwoord, hoe agressief het doel ook is. */
const KCAL_FLOOR = 1200;

/**
 * Mifflin-St Jeor: de rustverbranding. Geeft null zolang het profiel nog niet
 * genoeg gegevens heeft — beter geen getal dan een verzonnen getal.
 */
export function bmr(profile: Profile): number | null {
  const { age, sex, heightCm, weightKg } = profile;
  if (!age || !sex || !heightCm || !weightKg) return null;
  const base = 10 * weightKg + 6.25 * heightCm - 5 * age;
  return sex === "man" ? base + 5 : base - 161;
}

/** Dagverbruik: rustverbranding maal de activiteitsfactor. */
export function tdee(profile: Profile): number | null {
  const rest = bmr(profile);
  if (rest === null) return null;
  return rest * (ACTIVITY_FACTORS[profile.activityLevel] ?? 1.55);
}

/** Het kcal-doel: TDEE met het tekort of overschot dat bij het doel hoort. */
export function kcalTarget(profile: Profile): number | null {
  if (profile.kcalOverride && profile.kcalOverride > 0) return Math.round(profile.kcalOverride);
  const maintenance = tdee(profile);
  if (maintenance === null) return null;

  const daily = (Math.abs(profile.rateKgPerWeek) * KCAL_PER_KG) / 7;
  const adjusted =
    profile.goal === "cut"
      ? maintenance - daily
      : profile.goal === "bulk"
        ? maintenance + daily
        : maintenance;
  return Math.round(Math.max(KCAL_FLOOR, adjusted));
}

export interface DailyTargets extends Nutrients {
  kcal: number;
  protein: number;
  carbs: number;
  fat: number;
  fiber: number;
}

/**
 * De volledige dagdoelen. Eiwit en vet worden op lichaamsgewicht gezet omdat dat
 * de twee zijn met een fysiologische ondergrens; koolhydraten vullen de rest van
 * de calorieën op. Zit er na eiwit en vet geen ruimte meer, dan worden die
 * naar beneden bijgesteld in plaats van de koolhydraten negatief te laten worden.
 */
export function dailyTargets(profile: Profile): DailyTargets | null {
  const kcal = kcalTarget(profile);
  const weight = profile.weightKg;
  if (kcal === null || !weight) return null;

  let protein = profile.proteinPerKg * weight;
  let fat = profile.fatPerKg * weight;

  // 4 kcal per gram eiwit en koolhydraten, 9 per gram vet.
  let remaining = kcal - protein * 4 - fat * 9;
  if (remaining < 0) {
    // Schaal eiwit en vet samen terug tot ze precies in het budget passen; de
    // verhouding tussen die twee blijft dan wel staan.
    const shrink = kcal / (protein * 4 + fat * 9);
    protein *= shrink;
    fat *= shrink;
    remaining = 0;
  }

  return {
    kcal,
    protein: Math.round(protein),
    fat: Math.round(fat),
    carbs: Math.round(remaining / 4),
    // Voedingsrichtlijn: ~14 g vezels per 1000 kcal.
    fiber: Math.round((kcal / 1000) * 14),
  };
}

/** Vult ontbrekende velden aan en knipt onzinwaarden weg voor opslag. */
export function sanitiseProfile(input: Partial<Profile>): Profile {
  const clamp = (v: unknown, min: number, max: number, fallback: number | null) => {
    // Leeg is geen nul: een leeg invoerveld hoort de standaard te geven, niet het
    // minimum. Number(null) en Number("") zijn allebei 0, dus vang die eerst af.
    if (v === null || v === undefined || v === "") return fallback;
    const n = typeof v === "number" ? v : Number(v);
    if (!Number.isFinite(n)) return fallback;
    return Math.min(max, Math.max(min, n));
  };

  return {
    age: clamp(input.age, 10, 120, null),
    sex: input.sex === "man" || input.sex === "vrouw" ? input.sex : null,
    heightCm: clamp(input.heightCm, 100, 250, null),
    weightKg: clamp(input.weightKg, 30, 300, null),
    activityLevel: input.activityLevel && input.activityLevel in ACTIVITY_FACTORS
      ? input.activityLevel
      : DEFAULT_PROFILE.activityLevel,
    goal:
      input.goal === "cut" || input.goal === "bulk" || input.goal === "maintain"
        ? input.goal
        : DEFAULT_PROFILE.goal,
    rateKgPerWeek: clamp(input.rateKgPerWeek, 0, 1.5, DEFAULT_PROFILE.rateKgPerWeek) ?? 0,
    proteinPerKg: clamp(input.proteinPerKg, 0.5, 4, DEFAULT_PROFILE.proteinPerKg) ?? 1.8,
    fatPerKg: clamp(input.fatPerKg, 0.3, 2, DEFAULT_PROFILE.fatPerKg) ?? 0.8,
    kcalOverride: clamp(input.kcalOverride, 800, 8000, null),
    diet: Array.isArray(input.diet) ? input.diet.filter((d) => typeof d === "string") : [],
  };
}
