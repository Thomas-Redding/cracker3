// src/strategy/gamma_scalp.rs

use crate::traits::{MarketStream, ExecutionClient};

pub struct GammaScalp<S, E> {
    stream: S,
    exec: E,
}

impl<S: MarketStream, E: ExecutionClient> GammaScalp<S, E> {
    pub fn new(stream: S, exec: E) -> Self {
        Self { stream, exec }
    }

    pub async fn run(&mut self) {
        while let Some(event) = self.stream.next().await {
            if let Some(delta) = event.delta {
                if delta.abs() > 0.5 {
                    println!("High Delta detected: {}", delta);
                }
            }
        }
    }
}
