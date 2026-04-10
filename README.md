# Cross-Market Car Price Analysis: EU vs. Turkey

> **Project Link:** [github.com/oykutugana/Cross-Market-Car-Price-Analysis](https://github.com/oykutugana/Cross-Market-Car-Price-Analysis)

---

## Problem

> *What factors explain the second-hand car price gap between the EU and Turkish markets after controlling for currency?*

The same car model can cost 1.5–2.6× more in Turkey than in Europe — even after converting to USD. This project investigates whether the gap is explainable by vehicle features alone, or whether market-level structural forces (taxation, inflation, import restrictions) are the real drivers.

---

## Dataset
Data Collection Note: The datasets used in this project were custom-built by crawling thousands of live listings from AutoScout24 (representing the EU market) and arabam.com (representing the TR market) using Python-based web scrapers.

| | AutoScout24 (EU) | arabam.com (TR) |
|---|---|---|
| **Rows (raw)** | 7,240 | 4,519 |
| **Currency** | EUR | TRY |
| **brand, model, year** | ✓ | ✓ |
| **price** | ✓ | ✓ |
| **mileage** | ✓ | ✓ |
| **fuel_type** | ✓ | ✓ |
| **transmission** | ✓ | ✓ |
| **hp** | ✓ | ✗ |
| **body_type** | ✓ | ✗ |
| **country** | ✓ BE / DE / IT / NL / AT | ✓ TR |

**Currency conversion:** Fixed reference date 2024-01-01  
`1 EUR = 1.10 USD` · `1 TRY = 0.033 USD`

---

## Project Structure

```
Cross-Market-Car-Price-Analysis/
│
├── data/
│   ├── raw/
│   └── clean/
│    
│
├── notebooks/
│   ├── P1_EDA.ipynb                # Problem formulation, EDA, feature engineering
│   ├── P2_Regression.ipynb         # (upcoming) 
│   └── P3_Classification.ipynb     # (upcoming) 
│
├── scripts/
│   ├── data_scraper_eu.py           # AutoScout24 scraper
│   └── data_scraper_tr.py           # arabam.com scraper
│
├── outputs/                        # All generated figures (fig_00 … fig_16)
│
└── README.md
```

---

## P1 — Problem Formulation & EDA

**Notebook:** `notebooks/P1_EDA.ipynb`

### Pipeline

| Step | Description |
|------|-------------|
| Data Cleaning | Dedup, type coercion, range filters, label standardization (TR→EN) |
| Feature Engineering | `age`, `km_per_year`, `price_per_km`, `log_price_usd`, `hp_per_price`, `powertrain_class`, `age_bin` |
| Outlier Removal | IQR fencing (k=2.5) on `price_usd` and `mileage` per market |
| Visualization | 11 figures covering distribution, depreciation, correlation, brand comparison |

### Key Findings

**Price gap is large and statistically significant**  
Mann-Whitney U test: p < 0.0001. Turkish cars cost a median of **$42,900** vs **$25,850** in the EU — a **1.66× premium** in USD terms.

**Cars depreciate slower in Turkey**  
EU cars lose ~63% of their value over 9 years. Turkish cars lose only ~41% over the same period. Cars in Turkey function as inflation hedges, not depreciating tools.

**HP is the strongest price predictor in EU (r = +0.76)**  
Age (r = −0.40) and mileage (r = −0.33) have moderate negative effects. Age and mileage are correlated with each other (r = 0.71) — multicollinearity must be handled in P2.

**Brand-level price ratios (TR / EU median USD)**

| Brand | Ratio |
|-------|-------|
| Honda | 2.61× |
| Opel | 2.31× |
| Renault | 2.21× |
| Volkswagen | 2.04× |
| BMW | 1.84× |
| Volvo | 1.41× |

Economy brands show *larger* gaps than premium brands — ÖTV rates hit lower-displacement engines harder.

**Fuel type divergence**  
EU: Gasoline 46%, Hybrid growing post-2020.  
TR: Diesel 68%, LPG visible — reflecting fuel cost sensitivity and slower EV adoption.

### Limitations

- TR `hp` and `body_type` entirely missing — limits feature parity with EU
- Exchange rate fixed at 2024-01-01 — does not capture TRY volatility within 2015–2025
- No macroeconomic features yet (ÖTV brackets, fuel prices, EUR/TRY monthly rates) — planned for P2

---

## P2 — Regression *(upcoming — Week 10)*

Target: `price_usd`

Planned workflow (following course Model Selection guidelines):
1. Train / test split — 80/20, `random_state=42`, held-out test set untouched until final evaluation
2. Pipeline: `StandardScaler` → optional `PolynomialFeatures` → model
3. Models: `ElasticNetCV` (primary) · `RidgeCV` · `LassoCV`
4. Diagnostics: learning curves, validation curves, regularization path, residual plots
5. Final report: test-set metrics + cross-validation mean ± std

---

## P3 — Classification *(upcoming — Week 15)*

Target: `market` (EU vs TR)

Same train/test split as P2. Planned models: Logistic Regression · Decision Tree · Random Forest · SVM.

---

## Requirements

```
pandas
numpy
matplotlib
seaborn
scipy
scikit-learn
cloudscraper
beautifulsoup4
```

Install:
```bash
pip install pandas numpy matplotlib seaborn scipy scikit-learn cloudscraper beautifulsoup4
```

---

## Reproducibility

All notebooks are self-contained. Run in order:

```bash
# 1. Collect data (optional — raw CSVs already in data/)
python scripts/data_scraper.py
python scripts/arabam_scraper.py

# 2. Open notebooks
jupyter notebook notebooks/P1_EDA.ipynb
```

Figures are saved automatically to `outputs/` when each notebook is run.
