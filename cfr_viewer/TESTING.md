# CFR Viewer - Functionality & Test Plan

This document describes all functionality in the CFR Viewer web application and how to verify it works correctly.

## Quick Start

```bash
# Start the server
cfr-viewer

# Run automated tests
pytest cfr_viewer/tests/
pytest cfr_viewer/tests/test_user_stories.py  # Playwright E2E (requires running server)
```

---

## Pages & Routes

### Homepage (`/`)

**Functionality:**
- Displays aggregate statistics: total words, sections, titles, agencies, change % since baseline
- Shows top 5 titles by word count with links
- Shows top 5 agencies by word count with links
- Animated count-up effect on statistics cards

**Test:**
1. Navigate to `/`
2. Verify 5 stat cards display with values > 0
3. Verify "Browse Titles" table has 5 rows with links
4. Verify "Top Agencies" table has 5 rows with links
5. Verify stat card animations play on page load

---

### Titles Index (`/titles`)

**Functionality:**
- Sortable table: Title #, Name, Word Count, Change %
- Filter input searches title number and name
- Year selector changes data to historical year
- Reserved titles shown with `[Reserved]` tag

**Test:**
1. Navigate to `/titles`
2. Click "Word Count" header - verify sorts descending then ascending
3. Click "Title" header - verify sorts by number
4. Type "environment" in filter - verify only matching titles shown
5. Change year selector to 2010 - verify URL updates and data changes
6. Verify Title 35 shows as reserved (if present)

---

### Title Detail (`/title/<num>`)

**Functionality:**
- Shows title name, total word count, section count
- Breadcrumb navigation (All Titles → Title X)
- Structure table with chapters/subchapters/parts
- Each row links to deeper structure or sections

**Test:**
1. Navigate to `/title/47`
2. Verify breadcrumb shows "All Titles → Title 47"
3. Verify structure table shows chapters (e.g., "Chapter I")
4. Click a chapter row - verify navigation to `/title/47/chapter/I`
5. Verify word counts and change % display correctly

---

### Structure Navigation (`/title/<num>/<path>`)

**Functionality:**
- Displays children of any structure node (chapter, subchapter, part, subpart)
- Breadcrumb shows full path
- Links to sections when at lowest level

**Test:**
1. Navigate to `/title/47/chapter/I`
2. Verify breadcrumb: All Titles → Title 47 → Chapter I
3. Click through to a part (e.g., Part 73)
4. Verify sections are listed with § symbol
5. Click a section - verify navigation to section detail

---

### Section Detail (`/title/<num>/section/<path>`)

**Functionality:**
- Full section text display
- Section heading with citation
- Previous/Next navigation buttons
- Copy citation button (copies "47 CFR § 73.609")
- "Compare Years" link
- Year selector
- Similar Sections sidebar (lazy-loaded via HTMX)

**Test:**
1. Navigate to `/title/47/section/73.609`
2. Verify section heading displays
3. Verify section text content is present
4. Click "Copy" button - verify toast shows "Copied: 47 CFR § 73.609"
5. Click Previous/Next - verify navigation works
6. Verify Similar Sections panel loads (may show "100% distinct")
7. Click "Compare Years" - verify redirect to compare page

---

### Agencies Index (`/agencies`)

**Functionality:**
- Table of all agencies: Abbreviation, Name, Word Count, Change %
- Sortable by any column
- Filter by abbreviation or name
- Year selector

**Test:**
1. Navigate to `/agencies`
2. Verify table shows agencies (EPA, IRS, etc.)
3. Sort by Word Count - verify EPA near top
4. Filter by "Treasury" - verify matching agencies shown
5. Click agency row - verify navigation to detail page

---

### Agency Detail (`/agencies/<slug>`)

**Functionality:**
- Agency name and abbreviation
- Table of CFR chapters the agency administers
- Links to title/chapter combinations

**Test:**
1. Navigate to `/agencies/environmental-protection-agency`
2. Verify agency name "Environmental Protection Agency" displays
3. Verify chapters table shows Title 40 chapters
4. Click a chapter - verify navigation to browse structure

---

### Historical Compare (`/compare`)

**Functionality:**
- Landing page with citation input
- Example links for quick access
- Accepts formats: "47 CFR 73.609", "47 C.F.R. § 73.609"

**Test:**
1. Navigate to `/compare`
2. Verify citation input field is present
3. Enter "29 CFR 1910.134" and click Go
4. Verify redirect to `/compare/title/29/section/1910.134`
5. Click example link - verify redirect works

---

### Historical Diff (`/compare/title/<num>/section/<path>`)

**Functionality:**
- Two year selectors (year1 → year2)
- Side-by-side diff with word-level highlighting
- Handles edge cases:
  - No changes: single column with "unchanged" banner
  - Section added: shows when section first appeared
  - Section removed: shows when section was removed
  - Not found: lists available years

**Test:**
1. Navigate to `/compare/title/29/section/1910.134?year1=2010&year2=0`
2. Verify two year dropdowns display
3. Verify diff highlights in red (added) and green (removed)
4. Change year1 to same as year2 - verify "No changes" banner
5. Try a section that doesn't exist in old year - verify partial data handling

---

### Cross-Section Compare (`/compare/sections`)

**Functionality:**
- Two citation inputs for comparing different sections
- Side-by-side display of both sections

**Test:**
1. Navigate to `/compare/sections`
2. Enter "29 CFR 1910.134" in Section 1
3. Enter "29 CFR 1910.135" in Section 2
4. Click Compare - verify both sections display
5. Verify word counts shown for each

---

### Trends/Chart (`/chart`)

**Functionality:**
- Line chart showing word count over time
- Cascading selectors: Title → Chapter → Part → Section
- "All CFR Titles" shows total regulatory growth
- Statistics card: first year, last year, total change, CAGR
- Zoom toggle (fit data vs start at 0)
- Download PNG button
- URL state persistence

**Test:**
1. Navigate to `/chart`
2. Verify chart loads with "All CFR Titles" data
3. Select Title 40 - verify chart updates
4. Select a Chapter - verify selector cascade works
5. Verify statistics card shows change % and CAGR
6. Toggle "Zoom to data" - verify Y-axis changes
7. Click Download - verify PNG downloads
8. Refresh page - verify selections persist via URL

---

## Interactive Features

### Table Sorting

**Test:**
1. On any table page, click a column header
2. Verify sort indicator (↑/↓) appears
3. Click again - verify sort reverses
4. Verify numeric columns sort numerically (not alphabetically)

### Table Filtering

**Test:**
1. On `/titles`, type in filter input
2. Verify rows filter in real-time
3. Verify filter is case-insensitive
4. Clear filter - verify all rows return

### Year Selector

**Test:**
1. On any page with year selector, change the value
2. Verify page reloads with new year in URL
3. Verify data reflects the selected year
4. Verify "Current" label on year 0

### Navigation Dropdown

**Test:**
1. Hover over "Compare" in nav bar
2. Verify dropdown appears with "Historical" and "Cross-Section"
3. Click "Historical" - verify navigation
4. On mobile: tap Compare - verify dropdown toggles

### Copy Citation

**Test:**
1. On section page, click Copy button
2. Verify toast notification appears
3. Paste in another app - verify citation format correct

### Similar Sections Panel

**Test:**
1. On section page, verify panel loads
2. If similar sections exist:
   - Verify similarity % shown
   - Click "Preview" - verify text loads
   - Click "Compare" - verify navigation to compare page
3. If no similar sections:
   - Verify "100% distinct" message

### Mobile Responsiveness

**Test:**
1. Resize browser to < 768px width
2. Verify hamburger menu appears
3. Tap hamburger - verify nav menu opens
4. Verify tables scroll horizontally
5. Verify Similar Sections panel moves below content

---

## Edge Cases

### Empty/Reserved Sections

**Test:**
1. Find a reserved section (search for "reserved" in structure)
2. Verify `[Reserved]` badge displays
3. Verify section shows "no regulatory text" message

### Missing Historical Data

**Test:**
1. Navigate to compare with a very old year (e.g., 2000)
2. Find a section that didn't exist then
3. Verify "Section not found in [year]" message
4. Verify available years are listed

### Invalid Citations

**Test:**
1. On compare page, enter invalid citation "999 CFR 999.999"
2. Verify error handling (404 or error message)

### Large Word Counts

**Test:**
1. Navigate to Title 40 (largest title)
2. Verify word counts display with commas (e.g., "13,943,856")
3. Verify page loads in reasonable time

---

## API Endpoints

### Similar Sections API

```
GET /api/similar/<title>/<section>
```

**Test:**
```bash
curl http://localhost:5000/api/similar/47/73.609
```
Verify JSON response with similarity data.

### Section Preview API

```
GET /api/preview/<title>/<section>
```

**Test:**
```bash
curl http://localhost:5000/api/preview/47/73.609
```
Verify plain text response (truncated).

### Chart Data API

```
GET /chart/data/total
GET /chart/data/<title>
GET /chart/data/<title>/<path>
```

**Test:**
```bash
curl http://localhost:5000/chart/data/total
curl http://localhost:5000/chart/data/47
```
Verify JSON with year → word_count mapping.

### Structure API

```
GET /chart/structure/<title>
GET /chart/structure/<title>/<path>
```

**Test:**
```bash
curl http://localhost:5000/chart/structure/47
```
Verify JSON array of children with type, identifier, label.

---

## Automated Test Coverage

### Unit Tests (`cfr_viewer/tests/`)

| File | Coverage |
|------|----------|
| `test_routes_browse.py` | Homepage, titles, title detail, sections |
| `test_routes_agencies.py` | Agency index and detail |
| `test_routes_compare.py` | Compare landing, diff, sections |
| `test_routes_chart.py` | Chart page and data endpoints |
| `test_routes_api.py` | Similar sections, preview APIs |

### E2E Tests (`cfr_viewer/tests/test_user_stories.py`)

Playwright-based browser tests covering:
- Navigation flows
- Interactive features
- Form submissions
- Visual verification

### Running Tests

```bash
# All viewer tests
pytest cfr_viewer/tests/ -v

# Specific test file
pytest cfr_viewer/tests/test_routes_browse.py -v

# E2E tests (requires server running)
pytest cfr_viewer/tests/test_user_stories.py -v

# With coverage
pytest cfr_viewer/tests/ --cov=cfr_viewer
```

---

## Performance Expectations

| Page | Expected Load Time |
|------|-------------------|
| Homepage | < 500ms |
| Titles index | < 500ms |
| Title detail | < 1s |
| Section detail | < 1s |
| Similar sections | < 2s (async) |
| Chart page | < 1s |
| Compare diff | < 2s |

---

## Browser Compatibility

Tested on:
- Chrome/Chromium (latest)
- Firefox (latest)
- Safari (latest)
- Mobile Safari (iOS)
- Chrome Mobile (Android)

Required features:
- ES6+ JavaScript
- CSS Grid/Flexbox
- Fetch API
- localStorage
