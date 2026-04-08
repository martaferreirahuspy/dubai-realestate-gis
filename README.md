# Dubai Real Estate GIS

GeoJSON boundaries for Dubai Municipality's 222 official communities, used to power the Dubai real estate transactions map in Omni Analytics.

## Files

### `dubai_communities.geojson`
- **222 polygon features** — one per Dubai Municipality administrative community
- **Source:** Dubai Municipality via ArcGIS (authoritative boundaries, same taxonomy used by Dubai Land Department)
- **Updated:** April 2026

**Properties per feature:**

| Property | Description | Example |
|----------|-------------|---------|
| `shapeName` | Title-case community name — used as the join key in Omni | `"Business Bay"` |
| `CNAME_E` | Original DM name (uppercase) | `"BUSINESS BAY"` |
| `CNAME_A` | Arabic name | `"الخليج التجاري"` |
| `COMM_NUM` | Community number | `361` |
| `SHAPE_AREA` | Area in square metres | `5183420.6` |

## Using in Omni Analytics

1. Chart type: **Region map**
2. Region: **Custom**
3. Shape source: **GeoJSON URL**
4. Source URL: `https://raw.githubusercontent.com/martaferreirahuspy/dubai-realestate-gis/main/dubai_communities.geojson`
5. Region property: `shapeName`
6. Custom regions field: your `COMMUNITY` column in Snowflake

## Name mapping

DLD transaction data uses marketing/brand names (e.g. `"Dubai Hills"`, `"Dubai Creek Harbour"`) that do not exist as official DM communities. These are mapped to their parent DM community in the pipeline:

| DLD AREA_EN | Maps to shapeName |
|-------------|-----------------|
| Dubai Hills | Hadaeq Sheikh Mohammed bin Rashid |
| Dubai Creek Harbour | Al Kheeran First |
| Dubai Festival City | Nadd al Hamar |
| Dubai Marina | Marsa Dubai |
| Palm Jumeirah | Nakhlat Jumeira |
| JVC | Al Barsha South Fourth |
| Bluewaters | Marsa Dubai |

The full mapping is maintained in the pipeline's `geocode_pip_cache.json`.

## License

Source data: Dubai Municipality (open data under Dubai Law 26/2015).
