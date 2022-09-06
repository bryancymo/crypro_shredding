use std::collections::HashMap;

use crate::{Result, EncryptedCustomer, DecryptedCustomer, NewCustomer, EncryptedCustomerData};
use chacha20::ChaCha20;
use chacha20::cipher::{KeyIvInit, StreamCipher};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use warp::{http::StatusCode, reply, Reply};
use rand::{distributions::Alphanumeric, Rng};


pub async fn get_decrypted_customer(id: String, datastore: &HashMap<Vec<u8>, EncryptedCustomer>, keystore: &HashMap<Vec<u8>, Vec<u8>>, master_key: &Vec<u8>) -> Result<impl Reply> {

    match keystore.get(id.as_bytes()){
        Some(nonce) => {
            let mut cipher = ChaCha20::new(master_key.as_slice().into(), nonce.as_slice().into());
            match datastore.get(id.as_bytes()){
                Some(enc_customer) => {
                    let mut first_name_buffer = enc_customer.personal_data.first_name.clone();
                    cipher.apply_keystream(&mut first_name_buffer);
                    let mut lastname_name_buffer = enc_customer.personal_data.last_name.clone();
                    cipher.apply_keystream(&mut lastname_name_buffer);

                    let res = DecryptedCustomer {
                        customer_id: enc_customer.customer_id.to_owned(),
                        first_name: String::from_utf8(first_name_buffer).unwrap(),
                        last_name: String::from_utf8(lastname_name_buffer).unwrap()
                    };
                    return Ok(reply::with_status(reply::json(&res), StatusCode::OK))
                },
                None => {
                    return Ok(reply::with_status(reply::json(&"Key without data..."), StatusCode::IM_A_TEAPOT))
                },
            }
        },
        None => {
            return Ok(reply::with_status(reply::json(&"ID unknown"), StatusCode::GONE))
        }
    }
}

pub async fn get_encrypted_customer(id: String, datastore: &HashMap<Vec<u8>, EncryptedCustomer>) -> Result<impl Reply> {
    match datastore.get(id.as_bytes()){
        Some(customer) => {
            return Ok(reply::with_status(reply::json(customer), StatusCode::OK))
        },
        None => return Ok(reply::with_status(reply::json(&"ID unknown"), StatusCode::GONE)),
    }
}

pub async fn create_customer(customer: NewCustomer, master_key: &Vec<u8>) -> Result<impl Reply> {
    let customer_key: String = rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(12)
    .map(char::from)
    .collect();
    let customer_key_copy = customer_key.to_owned();

    let nonce: String = rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(12)
    .map(char::from)
    .collect();
    let nonce_copy = nonce.to_owned();

    let mut cipher = ChaCha20::new(master_key.as_slice().into(), nonce.as_bytes().into());
    let producer: BaseProducer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .create()
    .expect("Unable to create producer");

    let mut first_name_buffer = customer.first_name.as_bytes().to_owned();
    cipher.apply_keystream(&mut first_name_buffer);
    let mut lastname_name_buffer = customer.last_name.as_bytes().to_owned();
    cipher.apply_keystream(&mut lastname_name_buffer);

    let cust = EncryptedCustomer{        
        customer_id: customer_key,
        personal_data: EncryptedCustomerData {
            first_name: first_name_buffer.to_vec(),
            last_name: lastname_name_buffer.to_vec(),
        }
    };

    producer.send(
        BaseRecord::to("customer_data")
            .payload(serde_json::to_string(&cust).unwrap().as_bytes())
            .key(customer_key_copy.as_str()),
    ).expect("Failed to enqueue");

    producer.send(
        BaseRecord::to("customer_data_keys")
            .payload(nonce_copy.as_bytes())
            .key(customer_key_copy.as_str()),
    ).expect("Failed to enqueue");

    Ok(reply::with_status(reply::reply(), StatusCode::CREATED))
}

pub async fn delete_customer(id: String) -> Result<impl Reply> {
    let producer: BaseProducer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .create()
    .expect("Unable to create producer");

    producer.send(
        BaseRecord::to("customer_data_keys")
            .payload(b"")
            .key(id.as_str()),
    ).expect("Failed to enqueue");

    
    Ok(reply::with_status(reply::reply(), StatusCode::ACCEPTED))
}
