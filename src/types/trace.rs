use std::fmt::{Display, Formatter};

use ethers::types::{
    Action, Address, Bytes, Call, CallResult, Res, Trace as EtherTrace, H256, U256,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{to_string, to_string_pretty, Number};

use super::InnerCallFrame;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub transaction_index: Option<u32>,
    pub from_address: Option<Address>,
    pub to_address: Option<Address>,
    #[serde(
        deserialize_with = "value_from_string",
        serialize_with = "value_as_string"
    )]
    pub value: Option<U256>,
    pub input: Option<Bytes>,
    pub output: Option<Bytes>,
    pub trace_type: Option<String>,
    pub call_type: Option<String>,
    pub reward_type: Option<String>,
    pub gas: Option<u64>,
    pub gas_used: Option<u64>,
    pub subtraces: u32,
    pub trace_address: Vec<u32>,
    pub error: Option<String>,
    pub transaction_hash: Option<H256>,
    pub block_number: u64,
    pub block_timestamp: Option<u64>,
    pub block_hash: Option<H256>,
    //#[serde(rename = "type")]
    //pub ty: String,
    //pub status: u64,
    //pub trace_id: String,
    //pub trace_index: u64,
    //pub item_id: String,
    //pub item_timestamp: Option<String>,
}

fn value_as_string<S>(v: &Option<U256>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    v.as_ref()
        .map(|v| v.to_string())
        .as_ref()
        .serialize(serializer)
}

pub fn value_from_string<'de, D>(deserializer: D) -> Result<Option<U256>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    Option::<Number>::deserialize(deserializer).and_then(|o| {
        o.map(|n| U256::from_dec_str(&n.to_string()).map_err(|err| Error::custom(err.to_string())))
            .transpose()
    })
}

impl Display for Trace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            to_string_pretty(self).expect("Failed to serialize trace")
        )
    }
}

impl AsRef<Trace> for Trace {
    fn as_ref(&self) -> &Trace {
        self
    }
}

impl Trace {
    pub fn from_call_frame(
        InnerCallFrame {
            frame,
            subtraces,
            trace_address,
        }: InnerCallFrame,
        transaction_index: u32,
        transaction_hash: H256,
        block_number: u64,
    ) -> Option<Self> {
        Some(Self {
            transaction_index: Some(transaction_index),
            from_address: Some(frame.from),
            to_address: frame.to.and_then(|x| x.as_address().copied()),
            value: frame.value,
            input: Some(frame.input),
            output: frame.output,
            trace_type: Some("call".to_string()),
            call_type: Some(frame.typ.to_lowercase()),
            reward_type: None,
            gas: Some(frame.gas.as_u64()),
            gas_used: Some(frame.gas_used.as_u64()),
            subtraces,
            trace_address,
            error: frame.error,
            transaction_hash: Some(transaction_hash),
            block_number,
            block_timestamp: None,
            block_hash: None,
        })
    }
    pub fn from_ethers(trace: EtherTrace) -> Option<Self> {
        match trace {
            EtherTrace {
                action:
                    Action::Call(Call {
                        from,
                        to,
                        value,
                        gas,
                        input,
                        call_type,
                    }),
                result: Some(Res::Call(CallResult { gas_used, output })),
                trace_address,
                subtraces,
                transaction_position: Some(tx_idx),
                transaction_hash,
                block_number,
                block_hash,
                action_type,
                error,
            } => Some(Self {
                transaction_index: Some(tx_idx as u32),
                from_address: Some(from),
                to_address: Some(to),
                value: Some(value),
                input: Some(input),
                output: Some(output),
                trace_type: Some(
                    to_string(&action_type)
                        .expect("Failed to serialize action type")
                        .replace('\"', ""),
                ),
                call_type: Some(
                    to_string(&call_type)
                        .expect("Failed to serialize call type")
                        .replace('\"', ""),
                ),
                reward_type: None,
                gas: Some(gas.as_u64()),
                gas_used: Some(gas_used.as_u64()),
                subtraces: subtraces as u32,
                trace_address: trace_address.iter().map(|x| *x as u32).collect(),
                error,
                transaction_hash,
                block_number,
                block_timestamp: None,
                block_hash: Some(block_hash),
            }),
            _ => None,
        }
    }
}
