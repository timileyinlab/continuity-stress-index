# continuity-stress-index
Reproducible code + outputs for “Capability Bundles, Policy Frictions, and Sales–Liquidity Divergence in Crisis” using World Bank Business Pulse Surveys (2020–2021). Builds Adaptation Capability Index (ACI) and Continuity Stress Index (CSI), estimates fixed-effects models, and produces tables/figures.

# Continuity Capability Stack (ACI/CSI) — World Bank Business Pulse Surveys (2020–2021)

**Paper:** *Capability Bundles, Policy Frictions, and Sales–Liquidity Divergence in Crisis: Evidence from the World Bank Business Pulse Surveys, 2020–21*  
**Repo:** `timileyinlab/continuity-stress-index`

This repository contains code, analysis-ready panels, model objects, and reproducible outputs (tables/figures) for a cross-economy study of **sales recovery vs liquidity survival** during COVID-era shocks using the **World Bank Business Pulse Surveys (WB_BPS)**.

## Study in one paragraph
We develop a decision-oriented “continuity capability stack” separating **market continuity** (sales recovery) from **financial continuity** (liquidity survival). We construct two composite indices: **Adaptation Capability Index (ACI)** (digital/market-channel adaptation + operational reconfiguration) and **Continuity Stress Index (CSI)** (arrears + sales/labor distress). Using economy–time–stratum panels (size/sector) from WB_BPS (2020–2021), we estimate **two-way fixed-effects models with lags** and derive **resilience archetypes** via clustering. Core result: capability bundles align more consistently with **sales recovery** than with **arrears/liquidity relief**, consistent with binding **policy access frictions**.

---

## Data source (World Bank)
This analysis uses **aggregated, harmonized indicator series** from the World Bank COVID-19 **Business Pulse Surveys**:

- **WB_BPS dataset (Data360):** https://data360.worldbank.org/en/dataset/WB_BPS  
- **BPS interactive dashboard:** https://www.worldbank.org/en/data/interactive/2021/01/19/covid-19-business-pulse-survey-dashboard

Please follow the World Bank’s access and reuse terms when redistributing indicators or derivatives.

---

## Repository structure
Top-level outputs are organized for “grab-and-use” replication:


data/ Analysis-ready panels and data dictionary (CSV/JSON/PDF)
figures_png/ Publication-ready figures (PNG)
tables/ Publication-ready tables (CSV + HTML)
models/ Fitted model objects (RDS)
logs/ Run logs (e.g., run_log.txt)
manifest.json Run manifest / provenance metadata
sessionInfo.txt R session information for reproducibility


**Key files (typical):**
- `data/bps_long_clean.csv` — cleaned long panel
- `data/panel_total.csv`, `data/panel_size.csv`, `data/panel_sector.csv` — analysis panels
- `tables/Table*_*.csv` and `tables/Table*_*.html` — manuscript tables
- `figures_png/Figure*.png` — manuscript figures
- `models/model_*.rds` — fixed-effects model objects

---

## Reproducibility (minimal)
1. **Clone** the repository.
2. Open R (or RStudio) and confirm your environment using `sessionInfo.txt`.
3. If you are re-running the pipeline, ensure required packages are installed (see `sessionInfo.txt` for exact versions).
4. Outputs should reproduce into:
   - `tables/` (Table 1–7)
   - `figures_png/` (Figure 1–7)
   - `models/` (saved `.rds` model objects)
   - `logs/` (run log)

> Tip: If you are reviewing results without rerunning anything, start with `tables/Table*_*.html` and `figures_png/`.

---

## Outputs (what to cite)
- **Figures:** `figures_png/Figure1_*.png` … `Figure7_*.png`  
- **Tables:** `tables/Table1_*.html` … `Table7_*.html`  
- **Models:** `models/model_*.rds` (for exact estimates)

---

## Citation
If you use this repository, please cite:

**Manuscript (preprint/submitted):**  
Adetunji OC, Adetunji SA, Aromolaran O, Ajayi P, Afolabi O. *Capability Bundles, Policy Frictions, and Sales–Liquidity Divergence in Crisis: Evidence from the World Bank Business Pulse Surveys, 2020–21.* (Preprint/submitted).

**Data:**  
World Bank. *COVID-19 Business Pulse Surveys (WB_BPS) harmonized indicators (2020–2021).* Data360. https://data360.worldbank.org/en/dataset/WB_BPS

---

## Authors
- **Corresponding:** Oluwatimileyin C. Adetunji — eniola16.to@gmail.com (ORCID: 0009-0003-4779-9286)  
- Sunday A. Adetunji — adetunjs@oregonstate.edu  
- Opeyemi Aromolaran — aromoo@unisa.ac.za  
- Paul Ajayi — paul.ajayi@queensu.ca  
- Oluwadamilare Afolabi — damilareafolabi7@gmail.com  

---

## Funding, ethics, competing interests
- **Funding:** none.  
- **Ethics:** not applicable (secondary, aggregated indicators; no individual identifiers).  
- **Competing interests:** none declared.

---

## License
Choose and add a license file (recommended: **MIT** for code). Data remain subject to World Bank terms.
