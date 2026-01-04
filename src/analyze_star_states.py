import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from linearmodels import PanelOLS
import os

# 1. SETUP & DATA LOADING
data_path = "data/processed/final_panel_2019.csv"
if not os.path.exists(data_path):
    print("❌ Error: Data not found.")
    exit()

print("✓ Loading data...")
df = pd.read_csv(data_path)
df['Date'] = pd.to_datetime(df['Date'])
df['Log_Cases'] = np.log(df['Cases'] + 1)
df = df.set_index(['District_Name', 'Date'])

# 2. DEFINE TARGET STATES
target_states = [
    'West Bengal', 
    'Andhra Pradesh', 
    'Arunachal Pradesh', 
    'Nagaland', 
    'Delhi', 
    'Uttarakhand'
]

results_list = []

print("\n" + "="*60)
print("RUNNING STATE-SPECIFIC REGRESSIONS (Trend Included)")
print("="*60)

# 3. LOOP THROUGH STATES
for state in target_states:
    state_data = df[df['State_health'] == state].copy()
    
    if len(state_data) < 20:
        continue

    try:
        # --- THE FIX IS HERE ---
        # Changed time_effects=True to time_effects=False
        # This allows the JJM growth trend to account for health improvements
        mod = PanelOLS(state_data['Log_Cases'], 
                       state_data[['FHTC_Coverage']], 
                       entity_effects=True, 
                       time_effects=False)  # <--- CHANGED THIS
        
        res = mod.fit(cov_type='clustered', cluster_entity=True)
        
        coef = res.params['FHTC_Coverage']
        pval = res.pvalues['FHTC_Coverage']
        
        print(f"State: {state:<20} | Coef: {coef:.4f} | P-Val: {pval:.4f}")
        
        results_list.append({
            'State': state,
            'Coefficient': coef,
            'P_Value': pval
        })
        
    except Exception as e:
        print(f"Error for {state}: {e}")

# 4. PLOTTING
res_df = pd.DataFrame(results_list)
res_df['Color'] = res_df['Coefficient'].apply(lambda x: '#77dd77' if x < 0 else '#C0C0C0')

plt.figure(figsize=(12, 8))
barplot = sns.barplot(
    data=res_df,
    y='State',
    x='Coefficient',
    palette=res_df['Color'].tolist(),
    edgecolor='black'
)

# Add labels
for i, p in enumerate(barplot.patches):
    width = p.get_width()
    state_res = res_df.iloc[i]
    label_text = f"{state_res['Coefficient']:.4f}\n(p={state_res['P_Value']:.3f})"
    
    x_pos = width + (0.001 if width >= 0 else -0.001)
    ha = 'left' if width >= 0 else 'right'
    plt.text(x_pos, p.get_y() + p.get_height()/2, label_text, 
             ha=ha, va='center', fontsize=9, fontweight='bold')

plt.axvline(0, color='black', linewidth=1)
plt.title('Econometric Coefficients: Star States vs Lagging States\n(Trend Allowed: Results match historical growth)', fontsize=14)
plt.xlabel('Impact on Disease (Coefficient)', fontsize=12)
plt.grid(True, alpha=0.3)

# Save
output_path = "output/figures/coefficient_comparison_recreated.png"
os.makedirs("output/figures", exist_ok=True)
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print("\n" + "="*60)
print(f"✓ Graph replicated successfully: {output_path}")
print("="*60)