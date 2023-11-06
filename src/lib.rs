mod client;
const API_KEY: &str = "";

#[cfg(test)]
mod tests {
    use crate::API_KEY;
    use crate::client::{Client, Params};
    use polars::prelude::*;

    #[test]
    fn client_build() {
        let api_key = String::from(API_KEY.to_string());
        let offline_client = Client::new(api_key.clone());
        assert_eq!(offline_client.expect("client failed to build").api_key(), &api_key);
    }
    #[test]
    fn timestamp_ms() {
        let api_key = String::from(API_KEY.to_string());
        let offline_client = Client::new(api_key.clone());
        assert!(offline_client.unwrap().timestamp_ms() > 1698794827415);
    }
    #[test]
    fn online_client() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        match online_client {
            Client::Online{..} => assert!(true),
            Client::Offline{..} => panic!("client did not connect despite .connect() being called"),
        }
    }
    #[test]
    fn status() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let res = online_client.get_status();
        assert_eq!(res.unwrap(), "ok");
    }
    #[test]
    fn futures() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let res = online_client.get_futures();
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in futures");
    }
    #[test]
    fn options() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let res = online_client.get_options();
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in options");
    }
    #[test]
    fn spot() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let res = online_client.get_spot();
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in spot");
    }
    #[test]
    fn terms() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::Terms {coins: vec!["btc".to_string(), "eth".to_string()]};
        let res = online_client.get_term_structure(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in terms");
    }
    #[test]
    fn caps() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::Caps {coins: vec!["btc".to_string(), "eth".to_string()]};
        let res = online_client.get_market_caps(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in caps");
    }
    #[test]
    fn rows_coins() {
        let hour_in_ms = 1000 * 60 * 60;
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::RowsCoins {
            product_type: "futures".to_string(),
            exchanges: vec!["binance-futures".to_string(), "okex-swap".to_string()],
            coins: vec!["btc".to_string(), "eth".to_string()],
            columns: vec!["funding_rate".to_string(), "coin_open_interest_close".to_string()],
            start_timestamp_millis: online_client.timestamp_ms() - hour_in_ms,
            end_timestamp_millis: online_client.timestamp_ms(),
            resolution_mins: 1
        };
        let res = online_client.get_rows(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in rows from coins");
    }
    #[test]
    fn rows_products() {
        let hour_in_ms = 1000 * 60 * 60;
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::RowsProducts {
            product_type: "futures".to_string(),
            exchanges: vec!["binance-futures".to_string(), "okex-swap".to_string()],
            products: vec!["btcusdt".to_string(), "ethusdt".to_string()],
            columns: vec!["funding_rate".to_string(), "coin_open_interest_close".to_string()],
            start_timestamp_millis: online_client.timestamp_ms() - hour_in_ms,
            end_timestamp_millis: online_client.timestamp_ms(),
            resolution_mins: 1
        };
        let res = online_client.get_rows(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in rows from products");
    }
    #[test]
    fn params_new_terms() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::new(
            "terms".to_string(),
            None,
            None,
            None,
            Some(vec!["btc".to_string(), "eth".to_string()]),
            None,
            None,
            None,
            None,
        ).unwrap();
        let res = online_client.get_term_structure(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in terms");
    }
    #[test]
    fn params_new_caps() {
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::new(
            "caps".to_string(),
            None,
            None,
            None,
            Some(vec!["btc".to_string(), "eth".to_string()]),
            None,
            None,
            None,
            None,
        ).unwrap();
        let res = online_client.get_market_caps(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in terms");
    }
    #[test]
    fn params_new_rows_coins() {
        let hour_in_ms = 1000 * 60 * 60;
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::new(
            "rows".to_string(),
            Some("futures".to_string()),
            Some(vec!["okex-swap".to_string()]),
            None,
            Some(vec!["btc".to_string(), "eth".to_string(), "sol".to_string()]),
            Some(vec!["funding_rate".to_string(), "coin_open_interest_close".to_string()]),
            Some(online_client.timestamp_ms() - hour_in_ms),
            Some(online_client.timestamp_ms()),
            Some(1),
        ).unwrap();
        let res = online_client.get_rows(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in terms");
    }
    #[test]
    fn params_new_rows_products() {
        let hour_in_ms = 1000 * 60 * 60;
        let online_client = Client::new(API_KEY.to_string()).unwrap().connect();
        let params = Params::new(
            "rows".to_string(),
            Some("futures".to_string()),
            Some(vec!["binance-futures".to_string()]),
            Some(vec!["btcusdt".to_string(), "ethusdt".to_string(), "solusdt".to_string()]),
            None,
            Some(vec!["funding_rate".to_string(), "coin_open_interest_close".to_string()]),
            Some(online_client.timestamp_ms() - hour_in_ms),
            Some(online_client.timestamp_ms()),
            Some(1),
        ).unwrap();
        let res = online_client.get_rows(params);
        let all_zero = res.unwrap().get_columns().iter().all(|col| col.null_count() == 0);
        assert!(all_zero, "No missing values in terms");
    }
}


