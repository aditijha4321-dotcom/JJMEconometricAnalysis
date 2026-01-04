# Econometric Analysis of Jal Jeevan Mission (JJM)

## Project Overview
This repository contains a quantitative assessment of the Jal Jeevan Mission's impact on rural health outcomes, specifically examining the relationship between Functional Household Tap Connection (FHTC) coverage and diarrhoea cases.

## Methodology
- **Model:** Panel Fixed Effects Regression (Two-Way Fixed Effects)
- **Time Period:** 2019-2020 (Baseline Period)
- **Key Variables:**
  - Independent: Functional Household Tap Connections (FHTC) coverage percentage
  - Dependent: Log of diarrhoea inpatient cases

## Prerequisites
- Python 3.8 or higher
- All required packages are listed in `requirements.txt` (including `linearmodels` and `seaborn`)

## Data Requirements

### Health Data
Place state-wise Excel files (`.xls` format) in `data/raw/health_2019_20/`:
- Files should be named by state (e.g., `Assam.xls`, `West Bengal.xls`)
- Files should contain MultiIndex headers with:
  - Column 0: District Name
  - Column 1: Indicator Code
  - Column 2: Indicator Name (containing "Diarrhoea" and "Inpatient")
  - Columns 4-63: Monthly data (April 2019 to March 2020)

### Water Data
Synthetic water coverage data will be generated automatically from health data districts.

## Structure
```
├── notebooks/          # Step-by-step analysis notebooks
│   ├── 03_Data_Preparation_2019.ipynb    # Merge water and health data
│   └── 04_Econometric_Analysis.ipynb     # Run fixed effects regression
├── src/               # Helper Python scripts
│   ├── process_health_2019_final.py      # Process health Excel files
│   ├── generate_synthetic_jjm.py         # Generate synthetic water data
│   └── utils.py                           # Utility functions
├── data/
│   ├── raw/           # Raw input data
│   │   └── health_2019_20/  # Place Excel files here
│   └── processed/     # Cleaned and merged datasets
└── output/            # Regression results and visualizations
```

## How to Run

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```
This installs all required packages including `pandas`, `numpy`, `linearmodels`, `seaborn`, `matplotlib`, and `xlrd`.

### Step 2: Prepare Health Data
1. Place state-wise Excel files in `data/raw/health_2019_20/`
2. Run health data processing:
   ```bash
   python src/process_health_2019_final.py
   ```
   This creates `data/processed/health_2019_cleaned.csv`

### Step 3: Generate Synthetic Water Data
```bash
python src/generate_synthetic_jjm.py
```
This creates `data/raw/jjm_raw_2019.csv` based on districts from health data.

### Step 4: Merge Datasets
Run the data preparation notebook:
- `notebooks/03_Data_Preparation_2019.ipynb`
- This creates `data/processed/final_panel_2019.csv`

### Step 5: Run Econometric Analysis
Run the econometric analysis notebook:
- `notebooks/04_Econometric_Analysis.ipynb`
- This performs the fixed effects regression and compares Model A vs Model B

### Alternative: Run Scripts Directly
```bash
# State-wise analysis
python src/analyze_by_state.py

# Star states analysis
python src/analyze_star_states.py

# Visualize results
python src/visualize_results.py
```

## Expected Outputs
- `data/processed/health_2019_cleaned.csv`: Processed health data
- `data/processed/final_panel_2019.csv`: Merged panel dataset
- `data/processed/state_wise_results.csv`: State-wise regression results
- `output/figures/coefficient_comparison.png`: Visualization of results

## Troubleshooting

### Common Issues

1. **"linearmodels not found"**
   ```bash
   pip install linearmodels
   ```

2. **"State column not found"**
   - The script automatically searches for columns starting with 'State'
   - Check that your data has a state identifier column

3. **"File not found" errors**
   - Ensure Excel files are in `data/raw/health_2019_20/`
   - Run data processing scripts in order

4. **Empty results after filtering**
   - Check that health data contains "Inpatient" indicators
   - Verify district name matching is working

## Key Findings
The analysis compares two models:
- **Model A (Official View):** Full dataset including potential data inflation
- **Model B (Sanitized View):** Filtered dataset excluding suspicious coverage spikes (>10% month-on-month increase)

This comparison helps detect self-reporting bias in water coverage data and reveals the true health benefits of tap water access.

## Notes
- Health data files are HTML-based Excel files (`.xls` extension)
- District names are matched using fuzzy matching (difflib)
- The model uses clustered standard errors (clustered by district)

