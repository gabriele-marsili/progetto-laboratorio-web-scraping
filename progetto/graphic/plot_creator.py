import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd 
import networkx as nx

def plot_fees_vs_network_congestion(df):
    # Convert the 'month' column to datetime if it's not already
    if df['month'].dtype != 'datetime64[ns]':
        df['month'] = pd.to_datetime(df['month'])

    # Resample the data to 3-month intervals
    df_resampled = df.set_index('month').resample('3ME').sum().reset_index()

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Plot fees
    color = 'tab:blue'
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Fees (in Satoshi)', color=color)
    ax1.plot(df_resampled['month'], df_resampled['fees'], color=color, label='Fees')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

    # Create a second y-axis for the network congestion
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Network Congestion (bytes)', color=color)
    ax2.plot(df_resampled['month'], df_resampled['networkCongestion'], color=color, label='Network Congestion')
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))

    ax1.legend(loc='upper left', bbox_to_anchor=(0, 1))  
    ax2.legend(loc='upper left', bbox_to_anchor=(0, 0.9))

    # Adjust layout to make room for title
    plt.tight_layout(rect=[0, 0.1, 1, 0.9])
    fig.suptitle('Fees vs Network Congestion Over Time (3-Month Intervals)', y=0.95, fontsize=12)

    plt.show()

def plot_script_type_usage(df):
    # Convert the 'month' column to datetime if it's not already
    if df['month'].dtype != 'datetime64[ns]':
        df['month'] = pd.to_datetime(df['month'])

    first_3_years = df[df['month'] < '2012-01-01']

    # Create a column for the year
    first_3_years['year'] = first_3_years['month'].dt.year

    # Calculate the 'Other types' column
    first_3_years['Other_types'] = first_3_years.drop(columns=['month', 'year', 'P2PK', 'P2KH']).sum(axis=1)

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)  

    script_types = ['P2PK', 'P2KH', 'Other_types']
    colors = ['tab:green', 'tab:orange', 'tab:purple']
    
    for i, script_type in enumerate(script_types):
        sns.lineplot(ax=axes[i], data=first_3_years, x='month', y=script_type, color=colors[i], label=script_type)
        axes[i].set_ylabel(f'{script_type} Count')
        axes[i].set_title(f'Usage of {script_type} Over Time')
        axes[i].legend(loc='upper left')

    
    axes[-1].set_xlabel('Month')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.subplots_adjust(top=0.9, hspace=0.5)  
    fig.suptitle('Script Types Usage in the First 3 Years of Bitcoin', y=0.97) 
    plt.show()   
       
def plot_annual_script_type_usage(df):
    # Convert the 'month' column to datetime if it's not already
    if df['month'].dtype != 'datetime64[ns]':
         df['month'] = pd.to_datetime(df['month'])
        
    df = df[df['month'] < '2012-01-01']

    # Extract the year from the 'month' column
    df['year'] = df['month'].dt.year
    
    # Calculate the 'Other types' column
    df['Other_types'] = df.drop(columns=['month', 'year', 'P2PK', 'P2KH']).sum(axis=1)
    
    # Group by year and sum the counts of each script type
    annual_data = df.groupby('year')[['P2PK', 'P2KH', 'Other_types']].sum().reset_index()

    # Melt the data for seaborn
    annual_data_melted = pd.melt(annual_data, id_vars='year', var_name='scriptType', value_name='count')
        
    plt.figure(figsize=(14, 7))
    sns.barplot(data=annual_data_melted, x='year', y='count', hue='scriptType', palette=['tab:green', 'tab:orange', 'tab:purple'])
    plt.title('Annual Usage of Script Types')
    plt.xlabel('Year')
    plt.ylabel('Count')
    plt.legend(title='Script Type')
    plt.yscale('log')  #log scale to better visualize differences
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_blocks_mined_by_top_4_miners(blocks_mined_by_T4miners):
    plt.figure(figsize=(10, 6))
    sns.barplot(x='addressId', y='blocks_mined', data=blocks_mined_by_T4miners)
    plt.title('Total Blocks Mined by the top 4 miners')
    plt.xlabel('Miner')
    plt.ylabel('Total Blocks Mined')
    plt.show()
    
def plot_total_blocks_mined(global_blocks_mined):
    plt.figure(figsize=(10, 6))
    sns.barplot(x='pool', y='blocks_mined', data=global_blocks_mined)
    plt.title('Total Blocks Mined by Each Pool')
    plt.xlabel('Mining Pool')
    plt.ylabel('Total Blocks Mined')
    plt.show()
    
def plot_bi_monthly_blocks_mined(bi_monthly_blocks_mined):
    plt.figure(figsize=(14, 8))
    sns.lineplot(x='bi_month', y='blocks_mined', hue='pool', data=bi_monthly_blocks_mined, marker='o')
    plt.title('Blocks Mined Every 2 Months by Each Pool')
    plt.xlabel('Time (Bi-Monthly Intervals)')
    plt.ylabel('Blocks Mined')
    plt.xticks(rotation=45)
    plt.show()
    
def plot_total_rewards(global_total_rewards):
    plt.figure(figsize=(10, 6))
    sns.barplot(x='pool', y='total_rewards', data=global_total_rewards)
    plt.title('Total Rewards Received by Each Pool')
    plt.xlabel('Mining Pool')
    plt.ylabel('Total Rewards (BTC)')
    plt.show()
    
def plot_bi_monthly_rewards(bi_monthly_total_rewards):
    plt.figure(figsize=(14, 8))
    sns.lineplot(x='bi_month', y='total_rewards', hue='pool', data=bi_monthly_total_rewards, marker='o')
    plt.title('Rewards Received Every 2 Months by Each Pool')
    plt.xlabel('Time (Bi-Monthly Intervals)')
    plt.ylabel('Total Rewards (BTC)')
    plt.xticks(rotation=45)
    plt.show()
    
    
def plot_Eligius_path(graph_DF):
    # creazione di un grafo diretto
    G = nx.DiGraph()

    # aggiungo nodi e archi al grafo
    for _, row in graph_DF.iterrows():
        txId = row['txId']
        outputs = row['outputs']
        for output in outputs:
            G.add_edge(txId, output)

    # disegno il grafo
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=100, node_color='skyblue', font_size=3, font_weight='bold', arrowsize=15)
    plt.show()