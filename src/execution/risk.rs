use crate::config::Config;
use crate::types::events::{TradeSignal, TradeTarget};
use crate::types::order::Position;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use tracing::warn;

pub struct RiskManager {
    max_position_usd: Decimal,
    max_total_exposure_usd: Decimal,
    max_drawdown_pct: Decimal,
    max_orders_per_minute: u32,

    positions: HashMap<String, Position>,
    order_timestamps: VecDeque<DateTime<Utc>>,
    peak_equity: Decimal,
    current_equity: Decimal,
}

#[derive(Debug)]
pub enum RiskRejection {
    PositionLimitExceeded {
        current: Decimal,
        limit: Decimal,
    },
    ExposureLimitExceeded {
        current: Decimal,
        limit: Decimal,
    },
    DrawdownLimitExceeded {
        drawdown_pct: Decimal,
        limit: Decimal,
    },
    RateLimitExceeded {
        orders_last_minute: u32,
        limit: u32,
    },
}

impl std::fmt::Display for RiskRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskRejection::PositionLimitExceeded { current, limit } => {
                write!(f, "position limit exceeded: {} > {}", current, limit)
            }
            RiskRejection::ExposureLimitExceeded { current, limit } => {
                write!(f, "exposure limit exceeded: {} > {}", current, limit)
            }
            RiskRejection::DrawdownLimitExceeded {
                drawdown_pct,
                limit,
            } => {
                write!(f, "drawdown limit exceeded: {}% > {}%", drawdown_pct, limit)
            }
            RiskRejection::RateLimitExceeded {
                orders_last_minute,
                limit,
            } => {
                write!(
                    f,
                    "rate limit exceeded: {} orders/min > {}",
                    orders_last_minute, limit
                )
            }
        }
    }
}

/// Derive a unique position key from a TradeTarget.
fn position_id(target: &TradeTarget) -> String {
    match target {
        TradeTarget::Polymarket(m) => m.condition_id.clone(),
        TradeTarget::Spot { symbol, .. } => format!("spot:{symbol}"),
    }
}

impl RiskManager {
    pub fn new(config: &Config) -> Self {
        Self {
            max_position_usd: config.max_position_usd,
            max_total_exposure_usd: config.max_total_exposure_usd,
            max_drawdown_pct: config.max_drawdown_pct,
            max_orders_per_minute: config.max_orders_per_minute,
            positions: HashMap::new(),
            order_timestamps: VecDeque::new(),
            peak_equity: config.max_total_exposure_usd, // initial equity = max exposure
            current_equity: config.max_total_exposure_usd,
        }
    }

    /// Check if a trade signal passes all risk checks.
    pub fn check(&mut self, signal: &TradeSignal) -> Result<(), RiskRejection> {
        self.check_position_limit(signal)?;
        self.check_exposure_limit(signal)?;
        self.check_drawdown()?;
        self.check_rate_limit()?;
        Ok(())
    }

    /// Record that an order was placed (for rate limiting).
    pub fn record_order(&mut self) {
        self.order_timestamps.push_back(Utc::now());
    }

    /// Update position tracking.
    pub fn update_position(&mut self, position: Position) {
        self.positions
            .insert(position.condition_id.clone(), position);
        self.recalculate_equity();
    }

    fn check_position_limit(&self, signal: &TradeSignal) -> Result<(), RiskRejection> {
        let pid = position_id(&signal.target);
        let current = self
            .positions
            .get(&pid)
            .map(|p| p.notional())
            .unwrap_or(Decimal::ZERO);

        if current + signal.size_usd > self.max_position_usd {
            warn!(current = %current, size = %signal.size_usd, limit = %self.max_position_usd, "position limit");
            return Err(RiskRejection::PositionLimitExceeded {
                current: current + signal.size_usd,
                limit: self.max_position_usd,
            });
        }
        Ok(())
    }

    fn check_exposure_limit(&self, signal: &TradeSignal) -> Result<(), RiskRejection> {
        let total_exposure: Decimal = self.positions.values().map(|p| p.notional()).sum();

        if total_exposure + signal.size_usd > self.max_total_exposure_usd {
            return Err(RiskRejection::ExposureLimitExceeded {
                current: total_exposure + signal.size_usd,
                limit: self.max_total_exposure_usd,
            });
        }
        Ok(())
    }

    fn check_drawdown(&self) -> Result<(), RiskRejection> {
        if self.peak_equity == Decimal::ZERO {
            return Ok(());
        }

        let drawdown = (self.peak_equity - self.current_equity) / self.peak_equity;
        if drawdown > self.max_drawdown_pct {
            return Err(RiskRejection::DrawdownLimitExceeded {
                drawdown_pct: drawdown * Decimal::from(100),
                limit: self.max_drawdown_pct * Decimal::from(100),
            });
        }
        Ok(())
    }

    fn check_rate_limit(&mut self) -> Result<(), RiskRejection> {
        let one_minute_ago = Utc::now() - chrono::Duration::seconds(60);

        // Trim old timestamps
        while let Some(ts) = self.order_timestamps.front() {
            if *ts < one_minute_ago {
                self.order_timestamps.pop_front();
            } else {
                break;
            }
        }

        let count = self.order_timestamps.len() as u32;
        if count >= self.max_orders_per_minute {
            return Err(RiskRejection::RateLimitExceeded {
                orders_last_minute: count,
                limit: self.max_orders_per_minute,
            });
        }
        Ok(())
    }

    fn recalculate_equity(&mut self) {
        let total_pnl: Decimal = self
            .positions
            .values()
            .map(|p| p.realized_pnl + p.unrealized_pnl)
            .sum();

        self.current_equity = self.max_total_exposure_usd + total_pnl;
        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }
    }
}
