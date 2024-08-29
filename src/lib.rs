#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, time,
        types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
        utils,
    },
};

#[derive(Debug, Default)]
struct SheetsFdw {
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

// Pointer for the static FDW instance
static mut INSTANCE: *mut SheetsFdw = std::ptr::null_mut::<SheetsFdw>();

impl SheetsFdw {
    // Initialize FDW instance
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

impl Guest for SheetsFdw {
    fn host_version_requirement() -> String {
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        // Get API URL from foreign server options if it is specified
        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://docs.google.com/spreadsheets/d");

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Get sheet id from foreign table options and make the request URL
        let opts = ctx.get_options(OptionsType::Table);
        let sheet_id = opts.require("sheet_id")?;
        let url = format!("{}/{}/gviz/tq?tqx=out:json", this.base_url, sheet_id);

        // Make up request headers
        let headers: Vec<(String, String)> = vec![
            ("user-agent".to_owned(), "Sheets FDW".to_owned()),
            ("x-datasource-auth".to_owned(), "true".to_owned()),
        ];

        // Make a request to Google API and parse response as JSON
        let req = http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        };
        let resp = http::get(&req).map_err(|e| format!("Failed to fetch data from Google Sheets: {}", e))?;
        let body = resp.body.strip_prefix(")]}'\n").ok_or("Invalid response format from Google Sheets API")?;
        let resp_json: JsonValue = serde_json::from_str(body).map_err(|e| format!("Failed to parse JSON response: {}", e))?;

        // Extract source rows from response
        this.src_rows = resp_json
            .pointer("/table/rows")
            .ok_or("Cannot find 'rows' in JSON response. The structure might have changed.")?
            .as_array()
            .ok_or("Expected 'rows' to be an array")?
            .to_owned();

        // Log for debugging
        utils::report_info(&format!("Received {} rows from Google Sheets", this.src_rows.len()));

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }

        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            let src = src_row
                .pointer(&format!("/c/{}/v", tgt_col.num() - 1))
                .ok_or_else(|| format!("Source column '{}' not found or has no value", tgt_col_name))?;
            let cell = match tgt_col.type_oid() {
                TypeOid::Bool => src.as_bool().map(Cell::Bool),
                TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
                TypeOid::Timestamp => {
                    if let Some(s) = src.as_str() {
                        let ts = time::parse_from_rfc3339(s)?;
                        Some(Cell::Timestamp(ts))
                    } else {
                        None
                    }
                }
                TypeOid::Json => src.as_object().map(|_| Cell::Json(src.to_string())),
                _ => {
                    return Err(format!(
                        "Unsupported data type for column '{}'",
                        tgt_col_name
                    ));
                }
            };

            row.push(cell.as_ref());
        }

        this.src_idx += 1;

        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        Err("re_scan on foreign table is not supported".to_owned())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("modify on foreign table is not supported".to_owned())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Ok(())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Ok(())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Ok(())
    }
}

bindings::export!(SheetsFdw with_types_in bindings);


