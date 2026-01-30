# eCFR API Documentation Review

## Endpoint: `/api/versioner/v1/full/{date}/title-{title}.xml`

**Review Date:** 2026-01-30
**Source:** ecfr.gov API documentation

---

## Endpoint Summary

Retrieves source XML for a CFR title or subset. Returns downloadable XML for full titles or processed XML for partial requests (part, subpart, section, appendix).

---

## Current Documentation Analysis

### What's Documented

| Aspect | Documentation Status |
|--------|---------------------|
| Base endpoint URL | ✅ Documented |
| Required parameters (date, title) | ✅ Documented |
| Subset retrieval behavior | ✅ Documented |
| Section context behavior | ✅ Documented |
| File size warning | ✅ Documented |
| External XML guide reference | ✅ Documented |

### What's Missing

| Aspect | Documentation Status | Priority |
|--------|---------------------|----------|
| Subset URL pattern (part/section/appendix) | ❌ Missing | High |
| Example requests | ❌ Missing | High |
| Example responses | ❌ Missing | High |
| HTTP status codes | ❌ Missing | High |
| Error response format | ❌ Missing | High |
| Authentication requirements | ❌ Missing | Medium |
| Rate limiting | ❌ Missing | Medium |
| Valid date range | ❌ Missing | Medium |
| Response headers | ❌ Missing | Low |
| XML schema/DTD reference | ❌ Missing | Low |

---

## Detailed Findings

### 1. Incomplete URL Pattern

**Issue:** The documentation states that requests can target part, subpart, section, or appendix levels, but the URL pattern only shows:

```
/api/versioner/v1/full/{date}/title-{title}.xml
```

**Question:** How are subset identifiers (chapter, part, subpart, section, appendix) specified?

**Possible patterns to investigate:**
```
# Query parameters
/api/versioner/v1/full/2024-01-15/title-42.xml?part=1

# Path segments
/api/versioner/v1/full/2024-01-15/title-42/chapter-I/part-1.xml

# Combined identifiers
/api/versioner/v1/full/2024-01-15/title-42/part-1/section-1.1.xml
```

### 2. No Response Documentation

**Issue:** No example XML responses or schema references provided.

**Recommendation:** Include:
- Sample XML structure for title-level response
- Sample XML structure for section-level response (showing parent Part inclusion)
- Reference to XML schema/DTD
- Content-Type header value (`application/xml` or `text/xml`)

### 3. No Error Handling Documentation

**Issue:** HTTP status codes and error response formats not specified.

**Expected status codes to document:**
| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Invalid parameters (bad date format, invalid title) |
| 404 | Title/section not found for date |
| 429 | Rate limit exceeded |
| 500 | Server error |

### 4. Date Parameter Ambiguity

**Issue:** Date format specified as `YYYY-MM-DD` but constraints unclear.

**Questions:**
- What is the earliest available date?
- Are future dates rejected or return "current" data?
- What happens for dates before a title existed?
- Are weekends/holidays handled differently?

### 5. Title Number Validation

**Issue:** Example shows `'1', '2', '50'` but complete range not specified.

**Clarifications needed:**
- Valid title range (1-50?)
- Whether leading zeros are accepted (`01` vs `1`)
- Reserved or non-existent title numbers

---

## Recommendations

### High Priority

1. **Document complete URL patterns** for all supported subset levels
2. **Add example requests** for each access pattern:
   - Full title
   - Chapter subset
   - Part subset
   - Section subset
   - Appendix subset
3. **Document HTTP responses** with status codes and error formats
4. **Provide authentication requirements** (API key, rate limits)

### Medium Priority

5. **Clarify date constraints** (historical range, edge cases)
6. **Add XML response examples** with schema references
7. **Document response headers** and caching behavior

### Low Priority

8. **Add code examples** in common languages (Python, JavaScript, curl)
9. **Cross-reference related endpoints** in the versioner API
10. **Include performance recommendations** for large title downloads

---

## External Resources

- [GPO eCFR XML User Guide](https://www.ecfr.gov/) (referenced in docs)
- eCFR API base: `https://www.ecfr.gov/api/`

---

## Next Steps

1. Verify subset URL patterns through API testing
2. Document actual error responses
3. Determine valid date ranges
4. Create example client code
