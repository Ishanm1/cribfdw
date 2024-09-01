use postgres::{Client, NoTls, Row};
use semver::{Version, Op};

fn main() {
    let mut client = Client::connect("host=localhost user=postgres password=postgres dbname=postgres", NoTls).unwrap();

    // Clean up existing objects to avoid conflicts
    clean_up_existing_objects(&mut client);

    // Install necessary extension for FDWs
    install_fdw_extension(&mut client);

    // Create the foreign data wrapper for Wasm
    create_fdw(&mut client);

    // Define the server for the Google Sheets FDW with the updated checksum
    create_server(&mut client);

    // Create schema to isolate Google Sheets data
    create_schema(&mut client);

    // Define the foreign table linked to Google Sheets, all columns as TEXT
    create_foreign_table(&mut client);
}

fn clean_up_existing_objects(client: &mut Client) {
    // Check and drop existing FDW if it exists
    let fdw_exists = client.query_one("SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'wasm_wrapper'", &[]).is_ok();
    if fdw_exists {
        client.execute("DROP FOREIGN DATA WRAPPER IF EXISTS wasm_wrapper CASCADE", &[]).unwrap();
    }

    // Check and drop existing server if it exists
    let server_exists = client.query_one("SELECT 1 FROM pg_foreign_server WHERE srvname = 'google_sheets_server'", &[]).is_ok();
    if server_exists {
        client.execute("DROP SERVER IF EXISTS google_sheets_server CASCADE", &[]).unwrap();
    }

    // Check and drop existing schema if it exists
    let schema_exists = client.query_one("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'google'", &[]).is_ok();
    if schema_exists {
        client.execute("DROP SCHEMA IF EXISTS google CASCADE", &[]).unwrap();
    }
}

fn install_fdw_extension(client: &mut Client) {
    client.execute("CREATE EXTENSION IF NOT EXISTS wrappers WITH SCHEMA extensions", &[]).unwrap();
}

fn create_fdw(client: &mut Client) {
    client.execute("CREATE FOREIGN DATA WRAPPER wasm_wrapper HANDLER wasm_fdw_handler VALIDATOR wasm_fdw_validator", &[]).unwrap();
}

fn create_server(client: &mut Client) {
    let fdw_package_url = "https://github.com/ishanm1/cribfdw/releases/download/v0.2.0/wasm_fdw_example.wasm";
    let fdw_package_name = "my-company:example-fdw";
    let fdw_package_version = "0.2.0";
    let fdw_package_checksum = "88781ca13e15c368c9b5be09c6032b641479b85d05d4624ed92ed9a3af0bd290";

    client.execute("CREATE SERVER google_sheets_server FOREIGN DATA WRAPPER wasm_wrapper OPTIONS (fdw_package_url $1, fdw_package_name $2, fdw_package_version $3, fdw_package_checksum $4)", &[&fdw_package_url, &fdw_package_name, &fdw_package_version, &fdw_package_checksum]).unwrap();
}

fn create_schema(client: &mut Client) {
    client.execute("CREATE SCHEMA IF NOT EXISTS google", &[]).unwrap();
}

fn create_foreign_table(client: &mut Client) {
    let sheet_id = "1bw3CDIlIDHwo0y6U5fV7cTv_UsGJ9Tf5Xan7h0eNNPY";
    let object = "CribCDN";

    client.execute("CREATE FOREIGN TABLE IF NOT EXISTS google.sheets_data (\"Price\" TEXT, \"Bedrooms\" TEXT, \"Bathrooms\" TEXT, \"Square Footage\" TEXT, \"Listing Area\" TEXT, \"Downpayment or Income Requirement\" TEXT, \"Description\" TEXT) SERVER google_sheets_server OPTIONS (sheet_id $1, object $2)", &[&sheet_id, &object]).unwrap();
}
