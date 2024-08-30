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
struct ExampleFdw {
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
}

// Static instance pointer for the FDW
static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();

impl ExampleFdw {
    // Initialize the FDW instance
    fn init_instance() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    // Get a mutable reference to the FDW instance
    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }
}

impl Guest for ExampleFdw {
    fn host_version_requirement() -> String {
        // SemVer requirement for the FDW host version
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        // Retrieve the base URL from the server options
        let opts = ctx.get_options(OptionsType::Server);
        this.base_url = opts.require_or("api_url", "https://docs.google.com/spreadsheets/d");

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Get the object (Google Sheet or range) from table options
        let opts = ctx.get_options(OptionsType::Table);
        let object = opts.require("object").map_err(|_| "Missing required option: 'object'")?;
        let url = format!("{}/{}/gviz/tq?tqx=out:json", this.base_url, object);

        // Define request headers
        let headers: Vec<(String, String)> = vec![("user-agent".to_owned(), "Example FDW".to_owned())];

        // Make an HTTP GET request to the API
        let req = http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        };
        
        // Fetch data from the URL and parse it as JSON
        let resp = http::get(&req)?;
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        // Ensure the response is a JSON array
        this.src_rows = resp_json
            .pointer("/table/rows")
            .and_then(|v| v.as_array().cloned())
            .ok_or_else(|| "Response is not a JSON array".to_string())?;

        utils::report_info(&format!("Fetched {} rows from Google Sheets", this.src_rows.len()));

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        // Check if all source rows are consumed
        if this.src_idx >= this.src_rows.len() {
            return Ok(None);
        }

        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            let src_value = src_row
                .pointer(&format!("/c/{}/v", tgt_col.num() - 1))
                .ok_or_else(|| format!("Source column '{}' not found", tgt_col_name))?;

            // Map source value to the appropriate cell type
            let cell = match tgt_col.type_oid() {
                TypeOid::I64 => src_value.as_f64().map(|v| Cell::I64(v as i64)),
                TypeOid::String => src_value.as_str().map(|v| Cell::String(v.to_owned())),
                TypeOid::Numeric => src_value.as_str().map(|v| Cell::Numeric(v.parse().unwrap_or_default())),
                _ => {
                    return Err(format!("Unsupported column data type for '{}'", tgt_col_name).into());
                }
            };

            row.push(cell.as_ref());
        }

        this.src_idx += 1; // Move to the next row

        Ok(Some(0))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        Err("Re-scan on foreign table is not supported".to_owned())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("Modify on foreign table is not supported".to_owned())
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

// Export the FDW with type information
bindings::export!(ExampleFdw with_types_in bindings);

