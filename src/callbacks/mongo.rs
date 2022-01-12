use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::{App, Arg, ArgMatches, SubCommand};

use crate::blockchain::parser::types::CoinType;
use crate::blockchain::proto::block::Block;
use crate::blockchain::proto::tx::{EvaluatedTx, EvaluatedTxOut, TxInput};
use crate::blockchain::proto::Hashed;
use crate::callbacks::Callback;
use crate::common::utils;
use crate::errors::OpResult;

use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client,
};

/// Dumps the whole blockchain into csv files
pub struct Mongo {
    // Each structure gets stored in a separate csv file
    client: Client,
    db: Database,
    block_collection: Collection,

    start_height: u64,
    end_height: u64,
    tx_count: u64,
    in_count: u64,
    out_count: u64,
}

impl CsvDump {
    fn create_writer(cap: usize, path: PathBuf) -> OpResult<BufWriter<File>> {
        Ok(BufWriter::with_capacity(cap, File::create(&path)?))
    }
}

impl Callback for CsvDump {
    fn build_subcommand<'a, 'b>() -> App<'a, 'b>
    where
        Self: Sized,
    {
        SubCommand::with_name("mongo")
            .about("Dumps the whole blockchain into a monogdb")
            .version("0.1")
            .author("WWCTW")
        // .arg(
        //     Arg::with_name("mongo-url")
        //         .help("URL to Mongo")
        //         .index(1)
        //         .required(true),
        // )
    }

    fn new(matches: &ArgMatches) -> OpResult<Self>
    where
        Self: Sized,
    {
        let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
        let client = Client::with_options(client_options)?;
        let db = client.database("db");
        let block_collection = db.collection::<Document>("blocks");

        let mongo = mongo {
            client,
            db,
            block_collection,

            start_height: 0,
            end_height: 0,
            tx_count: 0,
            in_count: 0,
            out_count: 0,
        };
        Ok(mongo)
    }

    fn on_start(&mut self, _: &CoinType, block_height: u64) -> OpResult<()> {
        self.start_height = block_height;
        info!(target: "callback", "Using `mongo`");
        // Ping the server to see if you can connect to the cluster
        self.db.run_command(doc! {"ping": 1}, None).await?;
        println!("Connected successfully.");
        Ok(())
    }

    fn on_block(&mut self, block: &Block, block_height: u64) -> OpResult<()> {
        self.block_collection.insert_one(block.as_doc, None).await?;
        println!("inserted block into db");
        // serialize block
        // self.block_writer
        // .write_all(block.as_csv(block_height).as_bytes())?;

        // serialize transaction
        //         let block_hash = utils::arr_to_hex_swapped(&block.header.hash);
        //         for tx in &block.txs {
        //             self.tx_writer
        //                 .write_all(tx.as_csv(&block_hash).as_bytes())?;
        //             let txid_str = utils::arr_to_hex_swapped(&tx.hash);
        //
        //             // serialize inputs
        //             for input in &tx.value.inputs {
        //                 self.txin_writer
        //                     .write_all(input.as_csv(&txid_str).as_bytes())?;
        //             }
        //             self.in_count += tx.value.in_count.value;
        //
        //             // serialize outputs
        //             for (i, output) in tx.value.outputs.iter().enumerate() {
        //                 self.txout_writer
        //                     .write_all(output.as_csv(&txid_str, i as u32).as_bytes())?;
        //             }
        //             self.out_count += tx.value.out_count.value;
        //         }
        //         self.tx_count += block.tx_count.value;
        Ok(())
    }

    fn on_complete(&mut self, block_height: u64) -> OpResult<()> {
        self.end_height = block_height;

        println!("done");
        // Keep in sync with c'tor
        //         for f in &["blocks", "transactions", "tx_in", "tx_out"] {
        //             // Rename temp files
        //             fs::rename(
        //                 self.dump_folder.as_path().join(format!("{}.csv.tmp", f)),
        //                 self.dump_folder.as_path().join(format!(
        //                     "{}-{}-{}.csv",
        //                     f, self.start_height, self.end_height
        //                 )),
        //             )?;
        //         }
        //
        //         info!(target: "callback", "Done.\nDumped all {} blocks:\n\
        //                                    \t-> transactions: {:9}\n\
        //                                    \t-> inputs:       {:9}\n\
        //                                    \t-> outputs:      {:9}",
        //              self.end_height, self.tx_count, self.in_count, self.out_count);
        Ok(())
    }
}

impl Block {
    #[inline]
    fn as_csv(&self, block_height: u64) -> String {
        // (@hash, height, version, blocksize, @hashPrev, @hashMerkleRoot, nTime, nBits, nNonce)
        format!(
            "{};{};{};{};{};{};{};{};{}\n",
            &utils::arr_to_hex_swapped(&self.header.hash),
            &block_height,
            &self.header.value.version,
            &self.size,
            &utils::arr_to_hex_swapped(&self.header.value.prev_hash),
            &utils::arr_to_hex_swapped(&self.header.value.merkle_root),
            &self.header.value.timestamp,
            &self.header.value.bits,
            &self.header.value.nonce
        )
    }
    #[inline]
    fn as_doc(&self, block_height: u64) -> Document {
        doc! {"hash": &utils::arr_to_hex_swapped(&self.header.hash), "blockHeight": &block_height}
    }
}

impl Hashed<EvaluatedTx> {
    #[inline]
    fn as_csv(&self, block_hash: &str) -> String {
        // (@txid, @hashBlock, version, lockTime)
        format!(
            "{};{};{};{}\n",
            &utils::arr_to_hex_swapped(&self.hash),
            &block_hash,
            &self.value.version,
            &self.value.locktime
        )
    }
}

impl TxInput {
    #[inline]
    fn as_csv(&self, txid: &str) -> String {
        // (@txid, @hashPrevOut, indexPrevOut, scriptSig, sequence)
        format!(
            "{};{};{};{};{}\n",
            &txid,
            &utils::arr_to_hex_swapped(&self.outpoint.txid),
            &self.outpoint.index,
            &utils::arr_to_hex(&self.script_sig),
            &self.seq_no
        )
    }
}

impl EvaluatedTxOut {
    #[inline]
    fn as_csv(&self, txid: &str, index: u32) -> String {
        let address = match self.script.address.clone() {
            Some(address) => address,
            None => {
                debug!(target: "csvdump", "Unable to evaluate address for utxo in txid: {} ({})", txid, self.script.pattern);
                String::new()
            }
        };

        // (@txid, indexOut, value, @scriptPubKey, address)
        format!(
            "{};{};{};{};{}\n",
            &txid,
            &index,
            &self.out.value,
            &utils::arr_to_hex(&self.out.script_pubkey),
            &address
        )
    }
}
