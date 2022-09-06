#[macro_use]
extern crate lazy_static;

use std::{collections::HashMap, time::Duration};


use rdkafka::{ClientConfig, consumer::{Consumer, BaseConsumer}, Message};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use tracing_subscriber::fmt;
use uuid::Uuid;
use std::sync::Mutex;

use warp::{self, Filter, Rejection};
type Result<T> = std::result::Result<T, Rejection>;

mod handlers;


lazy_static! {
    static ref KEYSTORE: Mutex<HashMap<Vec<u8>,Vec<u8>>> = {
        Mutex::new(HashMap::new())
    };
    static ref DATASTORE: Mutex<HashMap<Vec<u8>,EncryptedCustomer>> = {
        Mutex::new(HashMap::new())
    };
    static ref MASTER_KEY: [u8; 32] = {
        [0x42; 32]
    };
}

#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() {

    tracing_subscriber::fmt()
    .event_format(fmt::format()
        .compact()
        .with_level(true) 
        .with_target(false) 
        .with_thread_ids(false)
        .with_thread_names(false)
    )
    .init();

    let mut tasks = vec![];
    tasks.push(tokio::spawn(async move { fill_keystore(&KEYSTORE).await }));
    tasks.push(tokio::spawn(async move { fill_datastore(&DATASTORE).await }));
    tasks.push(tokio::spawn(async { webserver(&DATASTORE, &KEYSTORE, MASTER_KEY.as_ref()).await }));


    futures::future::join_all(tasks).await;
    info!("all tasks completed");


}

pub async fn fill_keystore(keystore: &Mutex<HashMap<Vec<u8>, Vec<u8>>>) {
    info!("Initialize Keystore stream");

    let consumer: BaseConsumer = local_kafka_client_config()
        .set("group.id", format!{"keystore_cg_{}", Uuid::new_v4()})
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create keystore consumer");

    consumer.subscribe(&vec!["customer_data_keys"]).expect("Failed to subscribe");

    loop {
        match consumer.poll(Duration::from_secs(10)) {
            None => {},
            Some(res) => {
                match res {
                    Err(e) => warn!("Kafka error: {}", e),
                    Ok(m) => {
                        let key = m.key().expect("Key is mandatory").to_owned();
                        match m.payload() {
                            Some(value) => {
                                if value.len() > 0 {
                                    info!("Added key");
                                    keystore.lock().unwrap().insert(key, value.to_owned())
                                } else {
                                    info!("Removed key");
                                    keystore.lock().unwrap().remove(&key)
                                }
                               
                            },
                            None => {
                                info!("Removed key");
                                keystore.lock().unwrap().remove(&key)
                            },
                        };
                    }
                }
            }
        };
    };
}

pub async fn fill_datastore(datastore: &Mutex<HashMap<Vec<u8>, EncryptedCustomer>>) {
    info!("Initialize Datastore stream");
    let consumer: BaseConsumer = local_kafka_client_config()
        .set("group.id", format!{"customer_cg_{}", Uuid::new_v4()})
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create customer_cg consumer");

    consumer.subscribe(&vec!["customer_data"]).expect("Failed to subscribe");

    loop {
        match consumer.poll(Duration::from_secs(10)) {
            None => {},
            Some(res) => {
                match res {
                    Err(e) => warn!("Kafka error: {}", e),
                    Ok(m) => {
                        let key = m.key().expect("Key is mandatory").to_owned();
                        match m.payload() {
                            Some(value) => {
                                datastore.lock().unwrap().insert(key, serde_json::from_slice(value).expect("Invalid payload json format"));
                                info!("InsertedCustomer");
                            },
                            None => warn!("Tombstoning data not allowed"),
                        };
                    }
                }
            }
        };
    };
}

pub async fn webserver(datastore: &'static  Mutex<HashMap<Vec<u8>, EncryptedCustomer>>, keystore:  &'static  Mutex<HashMap<Vec<u8>, Vec<u8>>>, master_key: &'static [u8]){
    info!("Initialize Webserver");
    let root = warp::path::end().map(|| "Lets Scram!");

    let get_customer = warp::path!("customer" / String / "decrypted")
    .and(warp::get())
    .and_then(move |id| handlers::get_decrypted_customer(id, &datastore, &keystore, master_key));

    let get_raw_customer = warp::path!("customer" / String / "raw")
    .and(warp::get())
    .and_then(move |id| handlers::get_encrypted_customer(id, &datastore));

    let post_customer = warp::path("customer")
    .and(warp::post())
    .and(warp::body::json())
    .and_then(|new_customer| handlers::create_customer(new_customer, master_key));

    let delete_customer = warp::path!("customer"/ String)
    .and(warp::delete())
    .and_then(handlers::delete_customer);

    let routes = root
    .or(get_customer)
    .or(get_raw_customer)
    .or(post_customer)
    .or(delete_customer)
    .with(warp::cors().allow_any_origin());
    // .with(warp::log("support::api"));

    warp::serve(routes).run(([127, 0, 0, 1], 5000)).await;
}

pub fn local_kafka_client_config() -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", "localhost:9092");

    cfg
}



#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EncryptedCustomer {
    pub customer_id: String,
    pub personal_data: EncryptedCustomerData
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EncryptedCustomerData {
    pub first_name: Vec<u8>, 
    pub last_name: Vec<u8>
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DecryptedCustomer {
    pub customer_id: String,
    pub first_name: String,
    pub last_name: String
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NewCustomer {
    pub first_name: String,
    pub last_name: String
}