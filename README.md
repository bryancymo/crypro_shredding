# crypro_shredding
Demo crypto shredding implementation in Rust

# Usage
## Create a customer
curl -i -H 'Content-type: application/json' -d '{"first_name":"bryan","last_name":"De Smaele"}' -X POST localhost:5000/customer

## View created user without decrypt(get ID from create response)
curl -i localhost:5000/customer/IDHERE/raw

## View created user with decrypt(get ID from create response)
curl -i localhost:5000/customer/IDHERE/decrypted

## Forget the user (actually, delete his/her key)
curl -i -X DELETE localhost:5000/customer/IDHERE