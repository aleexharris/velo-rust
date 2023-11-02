use std::io::ErrorKind;
use std::io::ErrorKind::{InvalidInput, NotConnected, InvalidData};
use reqwest;
use std::time::SystemTime;
use base64::{Engine as _, engine::general_purpose};
use polars::prelude::*;
use std::io::Cursor;

const BASEURL: &str = "https://velodata.app/api/v1/";
const FUTURES_EXCHANGES: [&str; 4] = ["binance-futures", "bybit", "deribit", "okex-swap"];
const SPOT_EXCHANGES: [&str; 2] = ["binance", "coinbase"];
const OPTIONS_EXCHANGES: [&str; 1] = ["deribit"];
const FUTURES_COLUMNS: [&str; 29] = ["open_price", "high_price", "low_price", "close_price", "coin_volume", "dollar_volume", "buy_trades", "sell_trades", "total_trades", "buy_coin_volume", "sell_coin_volume", "buy_dollar_volume", "sell_dollar_volume", "coin_open_interest_high", "coin_open_interest_low", "coin_open_interest_close", "dollar_open_interest_high", "dollar_open_interest_low", "dollar_open_interest_close", "funding_rate", "premium", "buy_liquidations", "sell_liquidations", "buy_liquidations_coin_volume", "sell_liquidations_coin_volume", "liquidations_coin_volume", "buy_liquidations_dollar_volume", "sell_liquidations_dollar_volume", "liquidations_dollar_volume"];
const SPOT_COLUMNS: [&str; 28] = ["iv_1w", "iv_1m", "iv_3m", "iv_6m","skew_1w", "skew_1m", "skew_3m", "skew_6m", "vega_coins","vega_dollars", "call_delta_coins", "call_delta_dollars", "put_delta_coins", "put_delta_dollars","gamma_coins", "gamma_dollars", "call_volume", "call_premium", "call_notional","put_volume", "put_premium", "put_notional", "dollar_volume", "dvol_open", "dvol_high", "dvol_low", "dvol_close", "index_price"];
const OPTIONS_COLUMNS: [&str; 13] = ["open_price", "high_price", "low_price", "close_price", "coin_volume", "dollar_volume", "buy_trades", "sell_trades","total_trades", "buy_coin_volume", "sell_coin_volume", "buy_dollar_volume", "sell_dollar_volume"];

#[derive(Debug, Clone)]
pub enum Params {
    RowsCoins{
        product_type: String,
        exchanges: Vec<String>,
        coins: Vec<String>,
        columns: Vec<String>,
        start_timestamp_millis: u128,
        end_timestamp_millis: u128,
        resolution_mins: u32
    },
    RowsProducts{
        product_type: String,
        exchanges: Vec<String>,
        products: Vec<String>,
        columns: Vec<String>,
        start_timestamp_millis: u128,
        end_timestamp_millis: u128,
        resolution_mins: u32
    },
    Terms{ coins: Vec<String> },
    Caps{ coins: Vec<String> },
}

impl Params {
    pub fn new(endpoint: String,
               product_type: Option<String>,
               exchanges: Option<Vec<String>>,
               products: Option<Vec<String>>,
               coins: Option<Vec<String>>,
               columns: Option<Vec<String>>,
               start_timestamp_millis: Option<u128>,
               end_timestamp_millis: Option<u128>,
               resolution_mins: Option<u32>) -> Result<Params, ErrorKind> {
        let e = InvalidInput;
        match (endpoint.as_str(), &coins) {
            ("terms", Some(c)) => Ok(Self::new_terms_params(c)?),
            ("caps", Some(c)) => Ok(Self::new_caps_params(c)?),
            ("rows", _) => Ok(Self::new_rows_params(product_type.ok_or(e)?, exchanges.ok_or(e)?, products, coins, columns.ok_or(e)?, start_timestamp_millis.ok_or(e)?, end_timestamp_millis.ok_or(e)?, resolution_mins.ok_or(e)?)?),
            _ => Err(InvalidInput),
        }
    }
    pub fn new_rows_params(product_type: String,
                       exchanges: Vec<String>,
                       products: Option<Vec<String>>,
                       coins: Option<Vec<String>>,
                       columns: Vec<String>,
                       start_timestamp_millis: u128,
                       end_timestamp_millis: u128,
                       resolution_mins: u32) -> Result<Params, ErrorKind> {
        let valid_product_type_columns: bool = match &product_type.as_str() {
            &"futures" => columns.iter().any(|item| FUTURES_COLUMNS.iter().any(|&col| col == item.as_str())),
            &"spot" => columns.iter().any(|item| SPOT_COLUMNS.iter().any(|&col| col == item.as_str())),
            &"options" => columns.iter().any(|item| OPTIONS_COLUMNS.iter().any(|&col| col == item.as_str())),
            _ => false,
        };
        let valid_exchange_product_type_pair: bool = match &product_type.as_str() {
            &"futures" => exchanges.iter().any(|item| FUTURES_EXCHANGES.iter().any(|&exc| exc == item.as_str())),
            &"spot" => exchanges.iter().any(|item| SPOT_EXCHANGES.iter().any(|&exc| exc == item.as_str())),
            &"options" => exchanges.iter().any(|item| OPTIONS_EXCHANGES.iter().any(|&exc| exc == item.as_str())),
            _ => false,
        };
        match (valid_product_type_columns, valid_exchange_product_type_pair, &coins, &products) {
            (.. , None, None) => Err(InvalidInput),
            (true, true, _, None) => Ok(Params::RowsCoins{ product_type, exchanges, coins: coins.unwrap(), columns, start_timestamp_millis, end_timestamp_millis, resolution_mins }),
            (true, true, _, _) => Ok(Params::RowsProducts{ product_type, exchanges, products: products.unwrap(), columns, start_timestamp_millis, end_timestamp_millis, resolution_mins }),
            _ => Err(InvalidInput),
        }
    }
    pub fn new_terms_params(coins: &Vec<String>) -> Result<Params, ErrorKind> {
        Ok(Params::Terms{ coins: coins.to_vec() })
    }
    pub fn new_caps_params(coins: &Vec<String>) -> Result<Params, ErrorKind> {
        Ok(Params::Caps{ coins: coins.to_vec() })
    }
}

#[derive(Debug, Clone)]
pub enum Client {
    Offline{ api_key: String },
    Online{ api_key: String, req_client: reqwest::blocking::Client },
}

impl Client {
    pub fn timestamp_ms(&self) -> u128 {
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()
    }
    pub fn api_key(&self) -> &String {
        match self {
            Client::Offline {api_key} => api_key,
            Client::Online {api_key, ..} => api_key,
        }
    }
    pub fn new(api_key: String) -> Result<Client, ErrorKind> {
        if api_key.len() > 0 {
            Ok(Client::Offline{api_key,})
        } else {
            Err(InvalidInput)
        }
    }
    pub fn connect(&self) -> Client {  // maybe this should return Result<Client, Client> where Ok is Online and Err is Offline?
        let new_client = Client::Online { api_key: self.api_key().clone(), req_client: self.build_request_client(self.api_key()), };
        if new_client.get_status().unwrap_or("not ok".to_string()) == "ok" {
            new_client
        } else {
            Client::Offline {api_key: self.api_key().clone()}
        }
    }
    fn build_request_client(&self, api_key: &String) -> reqwest::blocking::Client {
        let mut headers = reqwest::header::HeaderMap::new();
        let auth = format!("Basic {}", general_purpose::STANDARD.encode(format!("api:{}", api_key).as_bytes())); // api key reformatting mess
        headers.insert(reqwest::header::AUTHORIZATION, auth.parse().unwrap());
        reqwest::blocking::Client::builder().default_headers(headers).build().unwrap()
    }
    fn get_request(&self, endpoint: &str, params: Option<Params>) -> Result<String, ErrorKind> {
        if let Client::Online {req_client, .. } = self {
            let query_vec: Vec<(&str, String)> = self.parse_params(params);
            match req_client.get(BASEURL.to_owned() + endpoint).query(&query_vec).send() {
                Ok(res) => Ok(res.text().unwrap()),
                Err(_) => Err(InvalidInput),
            }
        } else {
            Err(NotConnected)
        }
    }
    fn parse_params(&self, params: Option<Params>) -> Vec<(&str, String)> {
        let mut param_vec: Vec<(&str, String)> = Vec::new();
        match params {
            Some(Params::Terms{coins}) => {
                param_vec.push(("coins", coins.join(",")));
            },
            Some(Params::Caps{coins}) => {
                param_vec.push(("coins", coins.join(",")));
            },
            Some(Params::RowsCoins{product_type, exchanges, coins, columns, start_timestamp_millis, end_timestamp_millis, resolution_mins }) => {
                param_vec = vec![("type", product_type), ("exchanges", exchanges.join(",")), ("coins", coins.join(",")), ("columns", columns.join(",")), ("begin", start_timestamp_millis.to_string()), ("end", end_timestamp_millis.to_string()), ("resolution", resolution_mins.to_string())];
            },
            Some(Params::RowsProducts{product_type, exchanges, products, columns, start_timestamp_millis, end_timestamp_millis, resolution_mins }) => {
                param_vec = vec![("type", product_type), ("exchanges", exchanges.join(",")), ("products", products.join(",")), ("columns", columns.join(",")), ("begin", start_timestamp_millis.to_string()), ("end", end_timestamp_millis.to_string()), ("resolution", resolution_mins.to_string())];
            },
            None => (),
        };
        param_vec
    }
    fn parse_csv(&self, csv: String) -> Result<DataFrame, ErrorKind> {
        let c = Cursor::new(csv);
        let df = CsvReader::new(c).has_header(true).finish().map_err(|e| InvalidData)?;
        Ok(df)
    }
    pub fn get_status(&self) -> Result<String, ErrorKind> {
        self.get_request("status", None)
    }
    pub fn get_futures(&self) -> Result<DataFrame, ErrorKind> {
        let req = self.get_request("futures", None)?;
        self.parse_csv(req)
    }
    pub fn get_options(&self) -> Result<DataFrame, ErrorKind> {
        let req = self.get_request("options", None)?;
        self.parse_csv(req)

    }
    pub fn get_spot(&self) -> Result<DataFrame, ErrorKind> {
        let req = self.get_request("spot", None)?;
        self.parse_csv(req)
    }
    pub fn get_rows(&self, params: Params) -> Result<DataFrame, ErrorKind> {
        match params {
            Params::RowsProducts{..} | Params::RowsCoins{..} => {
                let req = self.get_request("rows", Some(params.clone()))?;
                self.parse_csv(req)
            }
            _ => Err(InvalidInput)
        }
    }
    pub fn get_term_structure(&self, params: Params) -> Result<DataFrame, ErrorKind> {
        match params {
            Params::Terms{..} => {
                let req = self.get_request("terms", Some(params.clone()))?;
                self.parse_csv(req)
            }
            _ => Err(InvalidInput)
        }
    }
    pub fn get_market_caps(&self, params: Params) -> Result<DataFrame, ErrorKind> {
        match params {
            Params::Caps{..} => {
                let req = self.get_request("caps", Some(params.clone()))?;
                self.parse_csv(req)
            }
            _ => Err(InvalidInput)
        }
    }
}


