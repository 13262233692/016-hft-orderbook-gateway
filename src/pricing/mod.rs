use std::f64::consts::{FRAC_1_SQRT_2, PI};

const NEG_INF_THRESHOLD: f64 = -6.0;
const POS_INF_THRESHOLD: f64 = 6.0;
const MAX_IV_ITERATIONS: usize = 32;
const IV_EPSILON: f64 = 1e-8;
const MIN_IV: f64 = 1e-4;
const MAX_IV: f64 = 10.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BsmInputs {
    pub spot_price: f64,
    pub strike_price: f64,
    pub risk_free_rate: f64,
    pub time_to_maturity: f64,
    pub volatility: f64,
    pub option_type: OptionType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BsmResult {
    pub price: f64,
    pub delta: f64,
    pub gamma: f64,
    pub vega: f64,
    pub theta: f64,
    pub rho: f64,
    pub implied_volatility: f64,
}

#[inline(always)]
pub fn norm_pdf(x: f64) -> f64 {
    if x < NEG_INF_THRESHOLD || x > POS_INF_THRESHOLD {
        return 0.0;
    }
    (-0.5 * x * x).exp() * FRAC_1_SQRT_2 / PI.sqrt()
}

#[inline(always)]
pub fn norm_cdf(x: f64) -> f64 {
    if x < NEG_INF_THRESHOLD {
        return 0.0;
    }
    if x > POS_INF_THRESHOLD {
        return 1.0;
    }

    let sign = if x >= 0.0 { 1.0 } else { -1.0 };
    let x_abs = x.abs();

    let t = 1.0 / (1.0 + 0.2316419 * x_abs);
    let d = 0.3989423 * (-x_abs * x_abs / 2.0).exp();
    let prob = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));

    if sign > 0.0 {
        1.0 - prob
    } else {
        prob
    }
}

#[inline(always)]
fn calc_d1_d2(
    spot: f64,
    strike: f64,
    rate: f64,
    vol: f64,
    ttm: f64,
) -> (f64, f64, f64, f64) {
    let ln_sk = (spot / strike).ln();
    let sqrt_ttm = ttm.sqrt();
    let vol_sqrt_ttm = vol * sqrt_ttm;
    let d1 = (ln_sk + (rate + 0.5 * vol * vol) * ttm) / vol_sqrt_ttm;
    let d2 = d1 - vol_sqrt_ttm;
    (d1, d2, sqrt_ttm, vol_sqrt_ttm)
}

#[inline(always)]
pub fn bsm_price(inputs: &BsmInputs) -> f64 {
    if inputs.time_to_maturity <= 0.0 {
        let intrinsic = match inputs.option_type {
            OptionType::Call => (inputs.spot_price - inputs.strike_price).max(0.0),
            OptionType::Put => (inputs.strike_price - inputs.spot_price).max(0.0),
        };
        return intrinsic;
    }

    let (d1, d2, _, _) = calc_d1_d2(
        inputs.spot_price,
        inputs.strike_price,
        inputs.risk_free_rate,
        inputs.volatility,
        inputs.time_to_maturity,
    );

    let k_exp = inputs.strike_price * (-inputs.risk_free_rate * inputs.time_to_maturity).exp();
    let nd1 = norm_cdf(d1);
    let nd2 = norm_cdf(d2);

    match inputs.option_type {
        OptionType::Call => inputs.spot_price * nd1 - k_exp * nd2,
        OptionType::Put => k_exp * (1.0 - nd2) - inputs.spot_price * (1.0 - nd1),
    }
}

#[inline(always)]
pub fn bsm_greeks(inputs: &BsmInputs) -> BsmResult {
    let price = bsm_price(inputs);

    if inputs.time_to_maturity <= 0.0 {
        let delta = match inputs.option_type {
            OptionType::Call => {
                if inputs.spot_price > inputs.strike_price {
                    1.0
                } else {
                    0.0
                }
            }
            OptionType::Put => {
                if inputs.spot_price < inputs.strike_price {
                    -1.0
                } else {
                    0.0
                }
            }
        };
        return BsmResult {
            price,
            delta,
            gamma: 0.0,
            vega: 0.0,
            theta: 0.0,
            rho: 0.0,
            implied_volatility: inputs.volatility,
        };
    }

    let (d1, d2, sqrt_ttm, vol_sqrt_ttm) = calc_d1_d2(
        inputs.spot_price,
        inputs.strike_price,
        inputs.risk_free_rate,
        inputs.volatility,
        inputs.time_to_maturity,
    );

    let nd1 = norm_cdf(d1);
    let nd2 = norm_cdf(d2);
    let _nd1_neg = norm_cdf(-d1);
    let nd2_neg = norm_cdf(-d2);
    let phi_d1 = norm_pdf(d1);
    let k_exp = inputs.strike_price * (-inputs.risk_free_rate * inputs.time_to_maturity).exp();

    let delta = match inputs.option_type {
        OptionType::Call => nd1,
        OptionType::Put => nd1 - 1.0,
    };

    let gamma = phi_d1 / (inputs.spot_price * vol_sqrt_ttm);
    let vega = inputs.spot_price * phi_d1 * sqrt_ttm;
    let theta = match inputs.option_type {
        OptionType::Call => {
            -inputs.spot_price * phi_d1 * inputs.volatility / (2.0 * sqrt_ttm)
                - inputs.risk_free_rate * k_exp * nd2
        }
        OptionType::Put => {
            -inputs.spot_price * phi_d1 * inputs.volatility / (2.0 * sqrt_ttm)
                + inputs.risk_free_rate * k_exp * nd2_neg
        }
    };
    let rho = match inputs.option_type {
        OptionType::Call => inputs.strike_price * inputs.time_to_maturity * k_exp * nd2,
        OptionType::Put => -inputs.strike_price * inputs.time_to_maturity * k_exp * nd2_neg,
    };

    BsmResult {
        price,
        delta,
        gamma,
        vega: vega / 100.0,
        theta: theta / 365.0,
        rho: rho / 100.0,
        implied_volatility: inputs.volatility,
    }
}

pub fn solve_implied_volatility(
    market_price: f64,
    spot: f64,
    strike: f64,
    rate: f64,
    ttm: f64,
    opt_type: OptionType,
) -> Option<f64> {
    if ttm <= 0.0 || spot <= 0.0 || strike <= 0.0 || market_price <= 0.0 {
        return None;
    }

    let intrinsic = match opt_type {
        OptionType::Call => (spot - strike).max(0.0),
        OptionType::Put => (strike - spot).max(0.0),
    };

    if market_price < intrinsic - 1e-8 {
        return None;
    }

    if market_price <= intrinsic + 1e-8 {
        return Some(0.0);
    }

    let mut vol = 0.3;
    let mut best_vol = vol;
    let mut best_error = f64::INFINITY;

    for _ in 0..MAX_IV_ITERATIONS {
        let inputs = BsmInputs {
            spot_price: spot,
            strike_price: strike,
            risk_free_rate: rate,
            time_to_maturity: ttm,
            volatility: vol,
            option_type: opt_type,
        };

        let price = bsm_price(&inputs);
        let error = price - market_price;
        let abs_error = error.abs();

        if abs_error < best_error {
            best_error = abs_error;
            best_vol = vol;
        }

        if abs_error < IV_EPSILON {
            return Some(vol);
        }

        let (d1, _, sqrt_ttm, _) = calc_d1_d2(spot, strike, rate, vol, ttm);
        let vega = spot * norm_pdf(d1) * sqrt_ttm;

        if vega < 1e-12 {
            break;
        }

        vol -= error / vega;

        vol = vol.clamp(MIN_IV, MAX_IV);
    }

    if best_error < 0.01 * market_price {
        Some(best_vol)
    } else {
        None
    }
}

pub fn solve_iv_and_greeks(
    market_price: f64,
    spot: f64,
    strike: f64,
    rate: f64,
    ttm: f64,
    opt_type: OptionType,
) -> Option<BsmResult> {
    solve_implied_volatility(market_price, spot, strike, rate, ttm, opt_type).map(|iv| {
        let inputs = BsmInputs {
            spot_price: spot,
            strike_price: strike,
            risk_free_rate: rate,
            time_to_maturity: ttm,
            volatility: iv,
            option_type: opt_type,
        };
        let mut result = bsm_greeks(&inputs);
        result.implied_volatility = iv;
        result
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn test_norm_cdf() {
        assert_relative_eq!(norm_cdf(0.0), 0.5, epsilon = 1e-6);
        assert_relative_eq!(norm_cdf(1.0), 0.841344746, epsilon = 1e-6);
        assert_relative_eq!(norm_cdf(-1.0), 0.158655254, epsilon = 1e-6);
        assert_relative_eq!(norm_cdf(2.0), 0.977249868, epsilon = 1e-6);
    }

    #[test]
    fn test_bsm_atm_call() {
        let inputs = BsmInputs {
            spot_price: 100.0,
            strike_price: 100.0,
            risk_free_rate: 0.05,
            time_to_maturity: 1.0,
            volatility: 0.2,
            option_type: OptionType::Call,
        };

        let price = bsm_price(&inputs);
        assert_relative_eq!(price, 10.4506, epsilon = 1e-2);
    }

    #[test]
    fn test_bsm_atm_put() {
        let inputs = BsmInputs {
            spot_price: 100.0,
            strike_price: 100.0,
            risk_free_rate: 0.05,
            time_to_maturity: 1.0,
            volatility: 0.2,
            option_type: OptionType::Put,
        };

        let price = bsm_price(&inputs);
        assert_relative_eq!(price, 5.5735, epsilon = 1e-2);
    }

    #[test]
    fn test_put_call_parity() {
        let inputs_call = BsmInputs {
            spot_price: 100.0,
            strike_price: 105.0,
            risk_free_rate: 0.03,
            time_to_maturity: 0.5,
            volatility: 0.25,
            option_type: OptionType::Call,
        };

        let inputs_put = BsmInputs {
            option_type: OptionType::Put,
            ..inputs_call
        };

        let call_price = bsm_price(&inputs_call);
        let put_price = bsm_price(&inputs_put);
        let k_exp = 105.0_f64 * (-0.03_f64 * 0.5_f64).exp();

        assert_relative_eq!(call_price - put_price, 100.0 - k_exp, epsilon = 1e-6);
    }

    #[test]
    fn test_greeks_delta_call() {
        let inputs = BsmInputs {
            spot_price: 100.0,
            strike_price: 100.0,
            risk_free_rate: 0.05,
            time_to_maturity: 1.0,
            volatility: 0.2,
            option_type: OptionType::Call,
        };

        let result = bsm_greeks(&inputs);
        assert_relative_eq!(result.delta, 0.6368, epsilon = 1e-2);
        assert!(result.gamma > 0.0);
        assert!(result.vega > 0.0);
        assert!(result.theta < 0.0);
    }

    #[test]
    fn test_greeks_delta_put() {
        let inputs = BsmInputs {
            spot_price: 100.0,
            strike_price: 100.0,
            risk_free_rate: 0.05,
            time_to_maturity: 1.0,
            volatility: 0.2,
            option_type: OptionType::Put,
        };

        let result = bsm_greeks(&inputs);
        assert_relative_eq!(result.delta, -0.3632, epsilon = 1e-2);
    }

    #[test]
    fn test_implied_volatility() {
        let spot = 100.0;
        let strike = 100.0;
        let rate = 0.05;
        let ttm = 1.0;
        let vol = 0.25;

        let inputs = BsmInputs {
            spot_price: spot,
            strike_price: strike,
            risk_free_rate: rate,
            time_to_maturity: ttm,
            volatility: vol,
            option_type: OptionType::Call,
        };

        let market_price = bsm_price(&inputs);

        let iv = solve_implied_volatility(market_price, spot, strike, rate, ttm, OptionType::Call)
            .unwrap();

        assert_relative_eq!(iv, vol, epsilon = 1e-4);
    }

    #[test]
    fn test_iv_solve_with_market_data() {
        let iv = solve_implied_volatility(10.45, 100.0, 100.0, 0.05, 1.0, OptionType::Call).unwrap();
        assert_relative_eq!(iv, 0.2, epsilon = 1e-2);
    }
}
