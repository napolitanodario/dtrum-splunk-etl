# Splunk Dashboards Package (Flusso v3)

This directory contains import-ready Splunk Dashboard Studio JSON files based on `:flusso` events (schema v3).

## Files

- `dashboard_overview.json`  
  Italian UI overview dashboard (kpi, trends, funnel depth, category/browser mix).
- `dashboard_performance_dropout.json`  
  Italian UI performance and dropout analytics dashboard.
- `dashboard_detail_drilldown.json`  
  Italian UI detail dashboard for flusso -> step -> action drilldown.
- `searches_reference.spl`  
  English, snake_case SPL reference with comments and reusable base/detail patterns.

## Expected event source

- `sourcetype`: `dtrum:funnel:flusso` (or your configured prefix + `:flusso`)
- `schema`: version `3`
- nested fields: `steps[]` and `steps[].actions[]`
- action fields: `seq`, `sessionId`, `startTime`, `actionKey`, `actionType`, `duration`, plus Dynatrace timing in ms (`frontendTime`, `networkTime`, `serverTime`)

## Import instructions (Dashboard Studio)

1. Open Splunk -> **Dashboards** -> **Create New Dashboard** -> **Dashboard Studio**.
2. Select **Source** mode.
3. Paste one JSON file content at a time and save.
4. Repeat for all three dashboard files.

## Dashboard definition conventions

The JSON files follow the current Dashboard Studio schema:

- visualization `type` values use the `splunk.*` namespace (`splunk.singlevalue`, `splunk.line`,
  `splunk.column`, `splunk.pie`, `splunk.table`). The legacy `viz.*` namespace is rejected by
  the source editor validator.
- input tokens are declared inside the input `options` object (`"options": {"token": "tok_x"}`).
- inputs rendered above the canvas are listed in `layout.globalInputs`, not in
  `layout.layoutDefinitions.*.structure`.
- panel placement lives in `layout.layoutDefinitions.<layout_id>.structure`, and every layout id
  must be referenced by an entry in `layout.tabs.items`.
- the root `layout` object accepts only `globalInputs`, `tabs`, `layoutDefinitions` and a restricted
  `options` (`submitButton`, `showTitleAndDescription`, `submitOnDashboardLoad`). Canvas settings such
  as `type`, `width`, `height` and `display` belong to each `layoutDefinitions` entry, otherwise the
  validator reports `must NOT have additional properties`.
- single value panels resolve their number with dynamic options syntax, for example
  `"majorValue": "> primary | seriesByName('total_flussi') | lastPoint()"`.
- click-through filtering uses `eventHandlers` with `drilldown.setToken`, where `key` points to the
  clicked row field (for example `row.flussoId.value`).

## Token conventions

All tokens use snake_case and start with `tok_`, for example:

- `tok_timerange`
- `tok_user_id`
- `tok_browser_type`
- `tok_category`
- `tok_has_error`
- `tok_flusso_id`
- `tok_step_index`

## Base + detail strategy

Search design follows a layered approach:

- base flusso search for global filtering
- step expansion (`mvexpand steps{}`)
- action expansion (`mvexpand steps{}.actions{}`)

See `searches_reference.spl` sections:

- `00) base macros`
- `10) overview`
- `20) performance and dropout`
- `30) detail and drilldown`

## Legacy parity and gaps

The dashboards replicate the key legacy analytics intent (overview, dropout, performance, detail).
Some Mongo-specific business fields are not available in current flusso v3 payload (for example legacy document composition fields), so equivalent business panels are approximated with available `:flusso` metrics.

## Optional external drilldown to raw dynatrace indexes

`searches_reference.spl` includes a commented `raw_dtrum_investigation_stub` query.
Replace `<raw_index_name>` and adapt field names to your raw ingestion model.
