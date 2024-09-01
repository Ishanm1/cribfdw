use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, 
        types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
        utils,
    },
};

#[derive(Debug, Default)]
struct GoogleSheetsFdw {
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

static mut INSTANCE: *mut GoogleSheetsFdw = std::ptr::null_mut::<GoogleSheetsFdw>();

impl GoogleSheetsFdw {
    fn init_instance() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }
}

impl Guest for GoogleSheetsFdw {
    fn host_version_requirement() -> String {
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.get("api_url").unwrap_or("https://docs.google.com/spreadsheets/d").to_string();

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        let opts = ctx.get_options(OptionsType::Table);
        let sheet_id = opts.require("sheet_id").map_err(|e| format!("Missing required option: 'sheet_id': {}", e))?;
        let object = opts.require("object").map_err(|e| format!("Missing required option: 'object': {}", e))?;
        let url = format!("{}/{}/gviz/tq?tqx=out:json&sheet={}", this.base_url, sheet_id, object);

        utils::report_info(&format!("Requesting URL: {}", url));

        let headers: Vec<(String, String)> = vec![("user-agent".to_owned(), "GoogleSheetsFDW".to_owned())];

        let req = http::Request {
            method: http::Method::Get,
            url: url.clone(),
            headers,
            body: String::default(),
        };

        let resp = http::get(&req).map_err(|e| format!("HTTP request failed: {}", e))?;

        utils::report_info(&format!("Received response: {}", resp.body));

        // Remove the prefix and suffix from the response body
        let json_str = resp.body.trim_start_matches(")]}'\n").trim();
        let resp_json: JsonValue = serde_json::from_str(json_str)
            .map_err(|e| format!("Failed to parse JSON: {}", e))?;

        this.src_rows = resp_json
            .pointer("/table/rows")
            .and_then(|v| v.as_array().cloned())
            .ok_or_else(|| "Response does not contain expected 'table/rows' structure".to_string())?;

        utils::report_info(&format!("Fetched {} rows from Google Sheets", this.src_rows.len()));

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }

        let src_row = &this.src_rows[this.src_idx];
        for (i, tgt_col) in ctx.get_columns().iter().enumerate() {
            let src_value = src_row
                .pointer(&format!("/c/{}/v", i))
                .ok_or_else(|| format!("Source column '{}' not found", tgt_col.name()))?;

            let cell = match src_value {
                JsonValue::String(s) => Cell::String(s.clone()),
                JsonValue::Number(n) => Cell::String(n.to_string()),
                JsonValue::Bool(b) => Cell::String(b.to_string()),
                JsonValue::Null => Cell::Null,
                _ => Cell::String(src_value.to_string()),
            };

            row.push(cell.as_ref());
        }

        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        Err("Re-scan on foreign table is not supported".to_owned())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0;
        Ok(())
    }

    // Implement other required methods...
}

bindings::export!(GoogleSheetsFdw with_types_in bindings);
