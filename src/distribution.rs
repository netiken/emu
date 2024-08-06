use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::Context;
use ordered_float::OrderedFloat;
use rand::prelude::*;
use rand_distr::{Exp, LogNormal, Uniform};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistKind {
    Constant { value: f64 },
    Uniform { lo: f64, hi: f64 },
    Exponential { lambda: f64 },
    LogNormal { mu: f64, sigma: f64 },
    Empirical { ecdf: Ecdf },
}

impl DistKind {
    pub fn mean(&self) -> f64 {
        match self {
            DistKind::Constant { value } => *value,
            DistKind::Uniform { lo, hi } => lo + (hi - lo) / 2.0,
            DistKind::Exponential { lambda } => lambda.recip(),
            DistKind::LogNormal { mu, sigma } => (mu + (sigma.powi(2) / 2_f64)).exp(),
            DistKind::Empirical { ecdf } => ecdf.mean(),
        }
    }

    pub fn to_gen(&self) -> anyhow::Result<Box<dyn GenF64 + Send>> {
        Ok(match self {
            DistKind::Constant { value } => Box::new(Const::new(*value)),
            DistKind::Uniform { lo, hi } => Box::new(Uniform::new(lo, hi)),
            DistKind::Exponential { lambda } => Box::new(Exp::new(*lambda)?),
            DistKind::LogNormal { mu, sigma } => Box::new(LogNormal::new(*mu, *sigma)?),
            DistKind::Empirical { ecdf } => Box::new(ecdf.clone()),
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DistShape {
    Constant,
    Uniform { pct_delta_about_mean: f64 },
    Exponential,
    LogNormal { sigma: f64 },
}

impl DistShape {
    pub fn to_kind(&self, mean: f64) -> DistKind {
        match *self {
            DistShape::Constant => DistKind::Constant { value: mean },
            DistShape::Uniform {
                pct_delta_about_mean,
            } => {
                // `pct_delta` is in [0, 1.0), so `mean * pct_delta_about_mean < mean`
                let delta_about_mean = mean * pct_delta_about_mean;
                DistKind::Uniform {
                    lo: mean - delta_about_mean,
                    hi: mean + delta_about_mean,
                }
            }
            DistShape::Exponential => DistKind::Exponential {
                lambda: mean.recip(),
            },
            DistShape::LogNormal { sigma } => {
                let mu = mean.ln() - (sigma.powi(2) / 2_f64);
                DistKind::LogNormal { mu, sigma }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistSpec {
    Constant { value: f64 },
    Uniform { lo: f64, hi: f64 },
    Exponential { lambda: f64 },
    LogNormal { mu: f64, sigma: f64 },
    Empirical { path: PathBuf },
}

impl DistSpec {
    pub fn into_dist_kind(self) -> anyhow::Result<DistKind> {
        let kind = match self {
            DistSpec::Constant { value } => DistKind::Constant { value },
            DistSpec::Uniform { lo, hi } => DistKind::Uniform { lo, hi },
            DistSpec::Exponential { lambda } => DistKind::Exponential { lambda },
            DistSpec::LogNormal { mu, sigma } => DistKind::LogNormal { mu, sigma },
            DistSpec::Empirical { path } => DistKind::Empirical {
                ecdf: read_ecdf(path)?,
            },
        };
        Ok(kind)
    }
}

pub trait GenF64 {
    fn gen(&self, rng: &mut StdRng) -> f64;

    fn box_clone(&self) -> Box<dyn GenF64>;
}

impl Clone for Box<dyn GenF64> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

impl<T> GenF64 for T
where
    T: Distribution<f64> + Clone + 'static,
{
    fn gen(&self, rng: &mut StdRng) -> f64 {
        self.sample(rng).round()
    }

    fn box_clone(&self) -> Box<dyn GenF64> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, Copy, derive_new::new)]
pub struct Const {
    value: f64,
}

impl Distribution<f64> for Const {
    fn sample<R: Rng + ?Sized>(&self, _: &mut R) -> f64 {
        self.value
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ecdf {
    ecdf: Vec<(f64, f64)>,
}

impl Ecdf {
    pub fn from_ecdf(ecdf: Vec<(f64, f64)>) -> Result<Self, EcdfError> {
        if ecdf.is_empty() {
            return Err(EcdfError::InvalidEcdf);
        }
        let len = ecdf.len();
        if (ecdf[len - 1].1 - 100.0).abs() > f64::EPSILON {
            return Err(EcdfError::InvalidEcdf);
        }
        for i in 1..len {
            if ecdf[i].1 <= ecdf[i - 1].1 || ecdf[i].0 <= ecdf[i - 1].0 {
                return Err(EcdfError::InvalidEcdf);
            }
        }
        Ok(Self { ecdf })
    }

    pub fn from_values(values: &[f64]) -> Result<Self, EcdfError> {
        if values.is_empty() {
            return Err(EcdfError::NoValues);
        }
        let mut values = values
            .iter()
            .map(|&val| OrderedFloat(val))
            .collect::<Vec<_>>();
        values.sort();
        let points = values
            .iter()
            .enumerate()
            .map(|(i, &size)| (size, (i + 1) as f64 / values.len() as f64))
            .collect::<Vec<_>>();
        let mut map = BTreeMap::new();
        for (x, y) in points {
            // Updates if key exists, kicking out the old value
            map.insert(x, y);
        }
        let ecdf = map
            .into_iter()
            .map(|(x, y)| (x.into_inner(), y * 100.0))
            .collect();
        Self::from_ecdf(ecdf)
    }

    pub fn mean(&self) -> f64 {
        let mut s = 0.0;
        let (mut last_x, mut last_y) = self.ecdf[0];
        for &(x, y) in self.ecdf.iter().skip(1) {
            s += (x + last_x) / 2.0 * (y - last_y);
            last_x = x;
            last_y = y;
        }
        s / 100.0
    }

    pub fn points(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.ecdf.iter().copied()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EcdfError {
    #[error("EDist is invalid")]
    InvalidEcdf,

    #[error("No values provided")]
    NoValues,
}

impl Distribution<f64> for Ecdf {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> f64 {
        let y = rng.gen_range(0.0..=100.0);
        let mut i = 0;
        while y > self.ecdf[i].1 {
            i += 1;
        }
        match i {
            0 => self.ecdf[0].0,
            _ => {
                let (x0, y0) = self.ecdf[i - 1];
                let (x1, y1) = self.ecdf[i];
                x0 + (x1 - x0) / (y1 - y0) * (y - y0)
            }
        }
    }
}

pub fn read_ecdf(path: impl AsRef<Path>) -> anyhow::Result<Ecdf> {
    let s = std::fs::read_to_string(path).context("failed to read CDF file")?;
    let v = s
        .lines()
        .map(|line| {
            let words = line.split_whitespace().collect::<Vec<_>>();
            anyhow::ensure!(words.len() == 2, "invalid CDF file");
            let x = words[0].parse::<f64>().context("invalid CDF x-val")?;
            let y = words[1].parse::<f64>().context("invalid CDF y-val")?;
            Ok((x, y))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Ecdf::from_ecdf(v)?)
}
