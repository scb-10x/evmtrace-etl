use std::fmt::Display;

use ethers::types::{
    CallConfig, CallFrame, GethDebugBuiltInTracerConfig, GethDebugBuiltInTracerType,
    GethDebugTracerConfig, GethDebugTracerType, GethDebugTracingOptions, GethTrace, GethTraceFrame,
};

pub struct GethTraceCall(pub Vec<InnerCallFrame>);

impl From<CallFrame> for GethTraceCall {
    fn from(call_frame: CallFrame) -> Self {
        Self(InnerCallFrame::flatten_frame(call_frame))
    }
}

impl GethTraceCall {
    pub fn option() -> GethDebugTracingOptions {
        GethDebugTracingOptions {
            tracer: Some(GethDebugTracerType::BuiltInTracer(
                GethDebugBuiltInTracerType::CallTracer,
            )),
            tracer_config: Some(GethDebugTracerConfig::BuiltInTracer(
                GethDebugBuiltInTracerConfig::CallTracer(CallConfig {
                    only_top_call: Some(false),
                    ..Default::default()
                }),
            )),
            ..Default::default()
        }
    }

    pub fn from_geth_trace(trace: GethTrace) -> Option<Self> {
        match trace {
            GethTrace::Known(GethTraceFrame::CallTracer(call)) => Some(call.into()),
            _ => None,
        }
    }
}

pub struct InnerCallFrame {
    pub frame: CallFrame,
    pub subtraces: u32,
    pub trace_address: Vec<u32>,
}

impl InnerCallFrame {
    fn recursive_push_children(
        frame: CallFrame,
        trace_address: Vec<u32>,
        results: &mut Vec<InnerCallFrame>,
    ) {
        if let Some(calls) = frame.calls {
            for (i, call) in calls.iter().enumerate() {
                let mut trace_address = trace_address.clone();
                // current trace address is the parent's trace address with the current index
                trace_address.push(i as u32);
                results.push(Self {
                    frame: call.clone(),
                    // subtraces is the length of the calls in current level
                    subtraces: call.calls.as_ref().map(|e| e.len()).unwrap_or_default() as u32,
                    trace_address: trace_address.clone(),
                });
                Self::recursive_push_children(call.clone(), trace_address, results);
            }
        }
    }

    pub fn flatten_frame(frame: CallFrame) -> Vec<Self> {
        let mut results = vec![];
        let trace_address = vec![];

        // first level
        results.push(Self {
            frame: frame.clone(),
            subtraces: frame.calls.as_ref().map(|e| e.len()).unwrap_or_default() as u32,
            trace_address: trace_address.clone(),
        });

        // then recursively push children
        Self::recursive_push_children(frame, trace_address, &mut results);

        results
    }
}

impl Display for InnerCallFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} -> {}, subtrace: {}, addr: {:?}",
            self.frame.typ,
            self.frame.from,
            self.frame
                .to
                .as_ref()
                .and_then(|f| f.as_address().copied())
                .unwrap_or_default(),
            self.subtraces,
            self.trace_address
        )
    }
}
