# Identity Resolution Fixture Dataset
## Ticket 4.1 — Pre-Implementation Spec

> ⚠️ STATUS: COMPLETE
> This document defines the ground truth for identity resolution behavior.
> All matching logic in Ticket 4.2 must produce outputs consistent with
> this specification.

---

## Purpose

This fixture dataset is the answer key for identity resolution.

It was created BEFORE any matching logic was written — following TDD
principles applied to data. Each record defines an input and its
expected output. The identity resolution SQL must pass all scenarios.

---

## Matching Rules (Reference)

Identity resolution applies two deterministic rules in order:

**Rule 1:** Exact match on `donor_name_normalized` + `zip_normalized`
**Rule 2:** Exact match on `donor_name_normalized` + `donor_address_normalized`

If no match → new `donor_id` created
If same canonical key → same `donor_id` (same donor assumed)
`identity_conflict` is always `FALSE` in current implementation
`dim_donors_unresolved` is reserved for post-MVP collision handling

## Address Data Limitation

The FEC individual contributions file does not contain street-level
address data. Available address fields are CITY, STATE, and ZIP_CODE only.

Therefore "full address" in this pipeline is defined as:
    donor_address_normalized = normalize_address(CITY + " " + STATE)

Example: "ATLANTA" + "GA" → "atlanta ga"

Rule 2 matches on normalized name + city + state.
This is the maximum address specificity available in the source data.
Street-level address abbreviation testing (St vs Street) is not
applicable to this dataset.

### Rule 2 False Merge Risk

Rule 2 matches on `donor_name_normalized + donor_address_normalized`
where `donor_address_normalized` is only `city + state`. This means
two genuinely different donors with the same name in the same city
will be merged if either record has no ZIP.

Example: Two different people named "JONES MARY" in "boston ma" —
one with ZIP, one without — will share a donor_id if the no-ZIP
record triggers Rule 2 and finds the ZIP record via name+address.

This is a known limitation of the available FEC address data.
Street-level address data is not available to disambiguate.
Flagging and resolution is deferred to post-MVP.

---

## Processing Mode

**Matching: Batch** — all records for a given execution date are
processed simultaneously in a single SQL query. Processing order
within a batch does not affect outcomes.

**Loading: Incremental** — results are MERGEd into `dim_donors`
on `sub_id`. Each execution date adds or updates records without
rebuilding the full table. Running the same date twice produces
identical results.

---

## Scenario 1 — Rule 1 Exact Match (Name + ZIP)

**Records:** R001, R002, R003, R004, R021, R022

**Description:**
These records represent donors who appear multiple times across
contributions. Their normalized names and ZIP codes are identical,
triggering a Rule 1 match.

| Records | Normalized Name | ZIP | Expected |
|---|---|---|---|
| R001, R002 | `smith john` | `30301` | Same donor_id |
| R003, R004 | `jones mary a` | `02101` | Same donor_id |
| R021, R022 | `harris janet p` | `10001` | Same donor_id |

**Why R021 and R022 match:**
`"HARRIS JANET P"` and `"HARRIS JANET P."` both normalize to
`"harris janet p"` — the period is removed by `normalize_name`.
This is a critical test of normalization correctness.

**Expected behavior:**
- First record creates a new `donor_id`
- Subsequent matching records resolve to the same `donor_id`
- `identity_conflict = FALSE`
- Records appear in `dim_donors` only

---

## Scenario 2 — Rule 2 Exact Match (Name + Full Address)

**Records:** R005, R006

**Description:**
These records have matching normalized names and full addresses
but R006 has a missing ZIP code. Rule 1 cannot fire without a ZIP.
Rule 2 fires on name + address and resolves them to the same donor.

| Records | Normalized Name | ZIP | Address | Expected |
|---|---|---|---|---|
| R005, R006 | `obrien patrick` | R005=`60601`, R006=empty | `chicago il` | Same donor_id |

**Why R005 and R006 match:**
R006 has no ZIP — Rule 1 cannot fire. The normalized address
`"chicago il"` matches exactly between both records.
Rule 2 resolves them to the same donor_id.

**Expected behavior:**
- R005 creates a new `donor_id`
- R006 resolves to R005's `donor_id` via Rule 2
- `identity_conflict = FALSE` for both
- Both records appear in `dim_donors` only

---

## Scenario 3 — No Match (New Donor)

**Records:** R007, R008, R009, R010, R011, R012, R020, R023

**Description:**
These records represent genuinely unique donors who have no
matching records in the system. Each should receive a new,
unique `donor_id`.

| Record | Normalized Name | ZIP | Expected |
|---|---|---|---|
| R007 | `davis carol ms` | `98101` | New donor_id |
| R008 | `davis carol` | empty | New donor_id |
| R009 | `johnson robert` | `80201` | New donor_id |
| R010 | `williams sarah` | `33101` | New donor_id |
| R011 | `brown michael` | `85001` | New donor_id |
| R012 | `garcia linda` | `75201` | New donor_id |
| R020 | `anderson patricia` | `94101` | New donor_id |
| R023 | `patel anita` | empty | New donor_id |

**Expected behavior:**
- Each record receives a unique `donor_id`
- No `identity_conflict` flag
- All records appear in `dim_donors` only
- None appear in `dim_donors_unresolved`

**Note on R007 and R008 — Name Suffix Edge Case:**
`"DAVIS CAROL MS."` normalizes to `"davis carol ms"` and
`"Davis Carol"` normalizes to `"davis carol"`. These are different
normalized strings — the `ms` suffix is preserved by `normalize_name`.
R008 has no raw ZIP so `zip_normalized` is empty — Rule 1 cannot fire.
Rule 2 also fails because the normalized names differ.
Both receive new donor_ids. This tests that titles and suffixes
affect matching — partial name matches are not allowed.

**Note on R023 — Null ZIP and Null Address:**
When both `zip_normalized` and `donor_address_normalized` are empty:
- Rule 1 cannot fire — no ZIP to match on
- Rule 2 cannot fire — no address to match on
- Result: new `donor_id` created regardless of name
- `identity_conflict = FALSE`
- Record appears in `dim_donors` only

This means two donors with identical names but no address data
will each receive separate `donor_id` values. This is the safest
default behavior — never assume a match without supporting evidence.

---

## Scenario 4 — Same Name + ZIP (Resolved as Same Donor)

**Records:** R013, R014, R015, R016

**Description:**
These records share identical normalized names and ZIP codes and resolve
to the same donor_id. In a deterministic batch hash system, same
canonical key always means same donor — the system cannot distinguish
"same person, two contributions" from "two different people with
identical name and ZIP" without additional disambiguating data.

| Records | Normalized Name | ZIP | Expected |
|---|---|---|---|
| R013, R014 | `smith james` | `97201` | Same donor_id |
| R015, R016 | `lee jennifer` | `78701` | Same donor_id |

**Expected behavior:**
- R013 and R014 resolve to the same donor_id
- R015 and R016 resolve to the same donor_id
- `identity_conflict = FALSE` for all
- All records appear in `dim_donors` only

**Known limitation:**
True collision detection (flagging genuinely different people who share
a name and ZIP) requires incremental matching against a known-good
donor registry, or additional disambiguating fields not present in
this dataset. Deferred to post-MVP.

---

## Scenario 5 — Same Name, Different ZIP (Separate Donors)

**Records:** R017, R018, R019

**Description:**
R017 and R018 share the same name and ZIP (`37201`) — they resolve to
the same donor_id. R019 has the same name but a different ZIP (`37202`)
— it gets a separate donor_id.

| Records | Normalized Name | ZIP | Expected |
|---|---|---|---|
| R017, R018 | `taylor william` | `37201` | Same donor_id |
| R019 | `taylor william` | `37202` | New donor_id |

**Expected behavior:**
- R017 and R018 resolve to the same donor_id via Rule 1
- R019 gets a new donor_id (different ZIP = different canonical key)
- `identity_conflict = FALSE` for all
- All records appear in `dim_donors` only

---

## Scenario 6 — Rule 2 Multi-ZIP Lookup (MIN Behavior)

**Records:** R024, R025, R026

**Description:**
R024 and R025 are two Rule 1 records sharing the same name and city
but with different ZIP codes. R026 has no ZIP — Rule 1 cannot fire.
Rule 2 joins on name + address and finds BOTH R024 and R025.

`MIN(canonical_key)` selects the lexicographically smallest key:
- `"chen robert|zip:02138"` < `"chen robert|zip:02139"`
- R026 inherits R024's canonical key → same donor_id as R024

**Expected behavior:**
- R024: new donor_id via Rule 1 (e.g., MD5("chen robert|zip:02138"))
- R025: new donor_id via Rule 1 (e.g., MD5("chen robert|zip:02139"))
- R026: resolves to R024's donor_id via Rule 2 MIN

**Why MIN is acceptable for MVP:**
The selection is deterministic — same input always produces same output.
The semantic question of which ZIP is "correct" for R026 is deferred
to post-MVP fuzzy matching with confidence scoring.

**Note:** This behavior is confirmed to occur in real FEC data.
Example: `goldberg paul` in `needham ma` has ZIPs `02149` and `02492`.

---

## Summary Table

| Records | Scenario | Expected Outcome | dim_donors | dim_donors_unresolved |
|---|---|---|---|---|
| R001, R002 | Rule 1 match | Same donor_id | ✅ | ❌ |
| R003, R004 | Rule 1 match | Same donor_id | ✅ | ❌ |
| R021, R022 | Rule 1 match (punctuation) | Same donor_id | ✅ | ❌ |
| R005, R006 | Rule 2 match | Same donor_id | ✅ | ❌ |
| R007 | No match (name suffix differs) | New donor_id | ✅ | ❌ |
| R008 | No match (no ZIP, name mismatch) | New donor_id | ✅ | ❌ |
| R009-R012, R020 | No match | New donor_id each | ✅ | ❌ |
| R013, R014 | Rule 1 match (same name+ZIP) | Same donor_id | ✅ | ❌ |
| R015, R016 | Rule 1 match (same name+ZIP) | Same donor_id | ✅ | ❌ |
| R017, R018 | Rule 1 match (same name+ZIP) | Same donor_id | ✅ | ❌ |
| R019 | No match (different ZIP) | New donor_id | ✅ | ❌ |
| R023 | No match (null ZIP + null address) | New donor_id | ✅ | ❌ |
| R024 | Rule 2 multi-ZIP — Rule 1 record (ZIP 02138) | New donor_id | ✅ | ❌ |
| R025 | Rule 2 multi-ZIP — Rule 1 record (ZIP 02139) | New donor_id | ✅ | ❌ |
| R026 | Rule 2 multi-ZIP — inherits R024 via MIN | Same donor_id as R024 | ✅ | ❌ |

---

## Notes for Implementation

1. **Normalization must run before matching** — the fixture provides
   both raw and normalized fields to make this explicit

2. **R007/R008 is an intentional edge case** — name suffix differences
   (`ms` vs no suffix) affect matching. This tests that the
   normalization utility preserves meaningful tokens.

3. **All records enter `dim_donors`** — no records are rejected or dropped.
   Records sharing a canonical key resolve to the same donor_id.
   `dim_donors_unresolved` exists as a reserved table for post-MVP
   collision handling — it is not populated in the current implementation.

4. **Rule order matters** — Rule 1 is always attempted first.
   Rule 2 only fires if Rule 1 finds no match.

5. **Single-word names are a known gap** — FEC records occasionally
   contain only a last name or first name (e.g., `"SMITH"`). These
   would produce false positives under Rule 1 if another donor shares
   the same single-word name in the same ZIP. This edge case is
   intentionally deferred to post-MVP fuzzy matching enhancements.
   No fixture record covers this scenario.

6. **Run this spot-check after each real data run** to verify
   match_rule distribution looks reasonable:

```sql
   SELECT
       match_rule,
       COUNT(*) as cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
   FROM `project-a10238bd-a355-474b-b6a.core.dim_donors`
   WHERE _load_date = DATE('2025-01-01')
   GROUP BY match_rule
   ORDER BY cnt DESC
```

   Expected: rule1 should account for the vast majority of records.
   A high no_match rate indicates a normalization or data quality issue.

7. **True collision detection is deferred to post-MVP** — the current
   batch hash system cannot distinguish "same donor, two contributions"
   from "two different people with identical name and ZIP." Same
   canonical key always resolves to the same donor_id. Collision
   detection requires either incremental matching against a known-good
   donor registry or additional disambiguating fields not present in
   FEC individual contribution data.
