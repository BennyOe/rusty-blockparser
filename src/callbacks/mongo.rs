use std::{collections::HashMap, slice::SliceIndex};

use mongodb::{
    bson::{doc, Document},
    sync::Client,
    sync::Collection,
    sync::Database,
};

use crate::blockchain::parser::types::CoinType;
use crate::blockchain::proto::block::Block;
use crate::blockchain::proto::tx::{EvaluatedTx, EvaluatedTxOut, TxInput};
use crate::blockchain::proto::Hashed;
use crate::callbacks::Callback;
use crate::common::utils;
use crate::errors::OpResult;
use clap::{App, ArgMatches, SubCommand};

/// Dumps the whole blockchain into csv files
pub struct Mongo {
    // Each structure gets stored in a separate csv file
    client: Client,
    db: Database,
    block_collection: Collection<Document>,
    tx_collection: Collection<Document>,

    start_height: u64,
    end_height: u64,
    tx_count: u64,
}

impl Callback for Mongo {
    fn build_subcommand<'a, 'b>() -> App<'a, 'b>
    where
        Self: Sized,
    {
        SubCommand::with_name("mongo")
            .about("Dumps the whole blockchain into a monogdb")
            .version("0.1")
            .author("WWCTW")
    }

    fn new(_matches: &ArgMatches) -> OpResult<Self>
    where
        Self: Sized,
    {
        let client = Client::with_uri_str("mongodb://localhost:27017")?;
        let db = client.database("data");
        let block_collection = db.collection::<Document>("blocks");
        let tx_collection = db.collection::<Document>("transactions");

        let mongo = Mongo {
            client,
            db,
            block_collection,
            tx_collection,

            start_height: 0,
            end_height: 0,
            tx_count: 0,
        };
        Ok(mongo)
    }

    fn on_start(&mut self, _: &CoinType, block_height: u64) -> OpResult<()> {
        self.start_height = block_height;
        info!(target: "callback", "Using `mongo`");
        // Ping the server to see if you can connect to the cluster
        self.db.run_command(doc! {"ping": 1}, None)?;
        println!("Connected successfully.");
        Ok(())
    }

    fn on_block(&mut self, block: &Block, block_height: u64) -> OpResult<()> {
        self.block_collection
            .insert_one(block.as_doc(block_height), None)?;

        let block_hash = utils::arr_to_hex_swapped(&block.header.hash);
        let mut transactions: Vec<Document> = Vec::new();
        let mut tx_map = HashMap::new();

        for tx in &block.txs {
            transactions.push(tx.as_doc(&block_hash, &self.tx_collection, &tx_map));
            let (tx_hash, output_map) = tx.as_map_tuple();
            tx_map.insert(tx_hash, output_map);
        }
        self.tx_collection.insert_many(transactions, None)?;
        self.tx_count += block.tx_count.value;
        Ok(())
    }

    fn on_complete(&mut self, block_height: u64) -> OpResult<()> {
        self.end_height = block_height;

        println!("done");
        // Keep in sync with c'tor

        info!(target: "callback", "Done.\nDumped all {} blocks:\n\
                                            \t-> transactions: {:9}",
                      self.end_height, self.tx_count);
        Ok(())
    }
}

impl Block {
    #[inline]
    fn as_doc(&self, block_height: u64) -> Document {
        doc! {
            "hash": &utils::arr_to_hex_swapped(&self.header.hash),
            "blockHeight": *&block_height as i64,
            "version": &self.header.value.version,
            "size": &self.size,
            "previousHash": &utils::arr_to_hex_swapped(&self.header.value.prev_hash),
            "merkleRootHash": &utils::arr_to_hex_swapped(&self.header.value.merkle_root),
            "timestamp": &self.header.value.timestamp,
            "nBits": &self.header.value.bits,
            "txCount": *&self.tx_count.value as i64,
            "nNonce": &self.header.value.nonce
        }
    }
}

impl Hashed<EvaluatedTx> {
    #[inline]
    fn as_doc(
        &self,
        block_hash: &str,
        collection: &Collection<Document>,
        tx_map: &HashMap<String, HashMap<i32, (i64, String)>>,
    ) -> Document {
        let mut inputs: Vec<Document> = Vec::new();
        let mut outputs: Vec<Document> = Vec::new();
        let txid_str = &utils::arr_to_hex_swapped(&self.hash);
        for (i, output) in self.value.outputs.iter().enumerate() {
            outputs.push(output.as_doc(&txid_str, i as i32))
        }
        for (i, input) in self.value.inputs.iter().enumerate() {
            inputs.push(input.as_doc(&txid_str, i as i32, collection, tx_map))
        }
        doc! {
                    "txHash": &txid_str,
                    "blockHash": &block_hash,
                    "version": &self.value.version,
                    "lockTime": &self.value.locktime,
                    "inputCount": *&self.value.in_count.value as i64,
                    "txInputs": inputs,
                    "outputCount": *&self.value.out_count.value as i64,
                    "txOutputs": outputs
        }
    }

    #[inline]
    fn as_map_tuple(&self) -> (String, HashMap<i32, (i64, String)>) {
        let mut output_map = HashMap::new();
        let txid_str = &utils::arr_to_hex_swapped(&self.hash);
        for (i, output) in self.value.outputs.iter().enumerate() {
            output_map.extend(output.as_map(i as i32));
        }
        return (txid_str.to_string(), output_map);
    }
}

impl TxInput {
    #[inline]
    fn as_doc(
        &self,
        txid: &str,
        index: i32,
        collection: &Collection<Document>,
        tx_map: &HashMap<String, HashMap<i32, (i64, String)>>,
    ) -> Document {
        let hash_prev_out = &utils::arr_to_hex_swapped(&self.outpoint.txid);
        let index_prev_out = &self.outpoint.index;
        let mut value = 0;
        let mut address: String =
            "0000000000000000000000000000000000000000000000000000000000000000".to_string();

        // if the previous tx is no doge reward set value and address
        if hash_prev_out != "0000000000000000000000000000000000000000000000000000000000000000" {
            if let Some(output_map) = tx_map.get(hash_prev_out) {
                if let Some((new_value, new_address)) = output_map.get(&(*index_prev_out as i32)) {
                    value = *new_value as i64;
                    address = new_address.to_string();
                } else if let Some(prev_out_tx) = collection
                    .find_one(doc! {"txHash": hash_prev_out}, None)
                    .unwrap()
                {
                    let tx_outputs = &prev_out_tx.get_array("txOutputs").unwrap()
                        [*index_prev_out as usize]
                        .as_document()
                        .unwrap();

                    value = tx_outputs.get_i64("value").unwrap();

                    address = tx_outputs.get_str("address").unwrap().to_string();
                    // println!("Adress: {} Value: {}", address, value);
                } else {
                    println!(
                        "No Transaction for the Input found for TX: {} with PrevOut: {}",
                        txid, hash_prev_out
                    );
                };
            }
            // if previous out tx exists
        }

        doc!(
            "txHash": &txid,
            "hashPrevOut": hash_prev_out,
            "indexPrevOut": index_prev_out,
            "indexIn": *&index,
            "scriptSig": &utils::arr_to_hex(&self.script_sig),
            "sequenceNumber": &self.seq_no,
            "value": value,
            "address": address
        )
    }
}

impl EvaluatedTxOut {
    #[inline]
    fn as_doc(&self, txid: &str, index: i32) -> Document {
        let address = match self.script.address.clone() {
            Some(address) => address,
            None => {
                debug!(target: "mongo", "Unable to evaluate address for utxo in txid: {} ({})", txid, self.script.pattern);
                String::new()
            }
        };

        // (@txid, indexOut, value, @scriptPubKey, address)
        doc!(
            "txHash": &txid,
            "indexOut": *&index,
            "value": *&self.out.value as i64,
            "scriptPubKey": &utils::arr_to_hex(&self.out.script_pubkey),
            "address": &address
        )
    }

    #[inline]
    fn as_map(&self, index: i32) -> HashMap<i32, (i64, String)> {
        let address = match self.script.address.clone() {
            Some(address) => address,
            None => String::new(),
        };
        let mut map = HashMap::new();
        // index, value, address
        map.insert(index, (*&self.out.value as i64, address));
        return map;
    }
}
