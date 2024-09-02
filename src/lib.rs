// Import necessary crates and modules
use std::collections::HashMap;
use serde_json;
use reqwest;
use std::error::Error;

// Define the FDW state struct to hold necessary data and state information
struct MyFDWState {
    base_url: String,                // Base URL for Google Sheets API request
    options: HashMap<String, String>, // Options provided to the FDW
    parsed_data: Vec<MyFDWRow>,      // Parsed rows from Google Sheets
}

// Define the row struct to represent each row of data from Google Sheets
struct MyFDWRow {
    price: String,
    bedrooms: String,
}

// Initialize the FDW state with options provided by PostgreSQL
fn init(options: &HashMap<String, String>) -> Result<MyFDWState, Box<dyn Error>> {
    // Retrieve the Google Sheet ID from options and handle missing ID gracefully
    let sheet_id = options.get("sheet_id").ok_or("Missing sheet_id option")?;
    
    // Construct the base URL for the Google Sheets API request
    let base_url = format!(
        "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:json",
        sheet_id
    );

    // Initialize and return the FDW state
    Ok(MyFDWState {
        base_url,
        options: options.clone(),
        parsed_data: Vec::new(),
    })
}

// Function to begin scanning the data from Google Sheets
fn begin_scan(state: &mut MyFDWState) -> Result<(), Box<dyn Error>> {
    // Make an HTTP GET request to fetch data from Google Sheets
    let response = reqwest::blocking::get(&state.base_url)
        .map_err(|e| format!("Failed to fetch data from Google Sheets: {}", e))?;
    let json_data = response.text()
        .map_err(|e| format!("Failed to read response text: {}", e))?;
    
    // Parse the JSON response into rows
    state.parsed_data = parse_json_to_rows(&json_data)?;

    Ok(())
}

// Helper function to parse JSON data into a vector of rows
fn parse_json_to_rows(json_data: &str) -> Result<Vec<MyFDWRow>, Box<dyn Error>> {
    let mut rows = Vec::new();
    
    // Parse JSON using serde_json
    let parsed: serde_json::Value = serde_json::from_str(json_data)
        .map_err(|e| format!("Failed to parse JSON: {}", e))?;

    // Ensure the structure is as expected and extract data
    if let Some(entries) = parsed["table"]["rows"].as_array() {
        for entry in entries {
            let price = entry["c"][0]["v"].as_str().unwrap_or("").to_string();
            let bedrooms = entry["c"][1]["v"].as_str().unwrap_or("").to_string();
            rows.push(MyFDWRow { price, bedrooms });
        }
    } else {
        return Err("Unexpected JSON structure".into());
    }

    Ok(rows)
}

// Function to iterate over the parsed data and return one row at a time to PostgreSQL
fn iter_scan(state: &mut MyFDWState) -> Result<Option<MyFDWRow>, Box<dyn Error>> {
    // Pop a row from the parsed data vector
    if let Some(row) = state.parsed_data.pop() {
        Ok(Some(row))
    } else {
        Ok(None) // No more rows to return
    }
}

// This main function might set up and register your FDW in a real environment
// It will depend on your FDW framework and PostgreSQL setup
fn main() {
    // Register FDW, if necessary
    // This is placeholder code and will need to be adapted to your specific setup
    println!("Google Sheets FDW initialized and ready.");
}

