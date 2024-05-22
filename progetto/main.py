import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os
import time
from graphic import plot_creator
from utilities import calculate_chunk_size
from utilities import unix_to_date
from dataset_analysis import analizer
from scraping import scraper
from utilities import LOG_LEVELS, SETTINGS

pd.set_option("mode.copy_on_write", True)

#csv file paths : 
CURRENT_PATH = os.path.dirname(__file__)
DATASET_ANALYSIS_PATH = os.path.join(CURRENT_PATH, "dataset_analysis")
DATASET_PATH = os.path.join(DATASET_ANALYSIS_PATH, "datasetCSV")

INPUTS_CSV_PATH = os.path.join(DATASET_PATH, "inputs.csv")
MAP_CSV_PATH = os.path.join(DATASET_PATH, "map.csv")
OUTPUTS_CSV_PATH = os.path.join(DATASET_PATH, "outputs.csv")
TRANSACTIONS_CSV_PATH = os.path.join(DATASET_PATH, "transactions.csv")

MAX_THREAD_QUANTITY = SETTINGS['MAX_THREAD_QUANTITY']
CHUNK_SIZE = 10000


inputs_columns = ["txId", "prevTxId", "prevTxpos"]
map_columns = ["hash", "addressId"]
outputs_columns = ["txId", "position", "addressId", "amount", "scripttype"]
transactions_columns = ["timestamp", "blockId", "txId", "isCoinbase", "fee"]



# --- read datas by csv files (for calculation of network congestion and script type data) :
 
def read_csv_chunk(schema, columns, usecols, filePath, chunkSize = CHUNK_SIZE, parseDate = False):                    
    """Read a CSV by chunks and returns chunks
    @param schema : schema of data types (to reduce dimension)
    @param columns : column'n names 
    @param usecols : columns indexes of csv file
    @param filePath : path of the CSV file
    @param [optional] chunkSize : size of 1 chunk (default = CHUNK_SIZE)
    @param [optional] parseDate : boolean to parse timestamp dates (default = False)
    @return chunks
    """
    chuncks = pd.read_csv(filePath, usecols=usecols, dtype=schema, names=columns, parse_dates=parseDate, chunksize= chunkSize)           
    return chuncks
   
def readInputs():
    """Read inputs csv by chunk 
    (calculating the chunk size using the number of rows in csv file)
    @no params
    @return inputs dataframe
    """
    startT = time.time()
    print('\nStarted reading inputs csv')
    schema = {        
        "txId": "int32",                 
    }
    usecols = [0]
    rowsInFile = 21378771
    _ , chunk_size = calculate_chunk_size(rowsInFile, 200000)
    columns = ["txId"]
    chunks = read_csv_chunk(schema,columns,usecols,INPUTS_CSV_PATH,chunk_size)
    df = pd.concat(chunks)
    df = df.drop_duplicates(subset=['txId'])
    endT = time.time()
    diff = endT - startT
    if LOG_LEVELS['time']:
        print(f"\nInputs csv read in {diff} seconds")
    return df

def readOutputs():
    """Read outputs csv by chunk 
    (calculating the chunk size using the number of rows in csv file)
    @no params
    @return outputs dataframe
    """
    startT = time.time()
    print('\nStarted reading outputs csv ')
    schema = {        
        "txId": "int32",                 
        "scriptType": "int8",                 
        "amount": "int64",                 
        "addressId": "int32",                 
    }
    usecols = [0,2,3,4]
    rowsInFile = 24613799
    _ , chunk_size = calculate_chunk_size(rowsInFile, 210000)
    columns = ["txId", "addressId","amount", "scriptType"]
    chunks = read_csv_chunk(schema,columns,usecols,OUTPUTS_CSV_PATH,chunk_size)
    df = pd.concat(chunks)
    df = df.drop_duplicates(subset=['txId'])
    endT = time.time()
    diff = endT - startT
    if LOG_LEVELS['time']:
        print(f"\nOutputs csv read in {diff} seconds")
    return df

def readTransaction():
    """Read transactions csv by chunk 
    (calculating the chunk size using the number of rows in csv file)
    @no params
    @return outputs dataframe
    """   
    startT = time.time()
    print('\nStarted reading transactions csv')
    schema = {
        "timestamp": "int32",
        "blockId": "int32",
        "txId": "int32",
        "isCoinbase": "int8",
        "fee": "int32",                
    }
    usecols = [0, 1, 2, 3, 4]
    
    rowsInFile = 10572829
    _ , chunk_size = calculate_chunk_size(rowsInFile, 200000)
    columns = ["timestamp", 'blockId' ,"txId", "isCoinbase", "fee"]    
    chunks = read_csv_chunk(schema,columns,usecols,TRANSACTIONS_CSV_PATH,chunk_size)
    df = pd.concat(chunks)    
    df = df.drop_duplicates(subset=['txId'])    
   
    df['timestamp'] = df['timestamp'].apply(unix_to_date)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    endT = time.time()
    diff = endT - startT
    if LOG_LEVELS['time']:
        print(f"\nTransaction csv read in {diff} seconds")
    return df 

def takeCSV_data():
    """Take CSV data from inputs, outputs, transactions csv and convert them to dataframes.
    Use threads to improve performances.
    
    @no params 
    @return inputs, outputs, transactions dataframes
    """
    with ThreadPoolExecutor(max_workers=MAX_THREAD_QUANTITY) as executor:
        inputs_future = executor.submit(readInputs)
        inputs = inputs_future.result()
        
    with ThreadPoolExecutor(max_workers=MAX_THREAD_QUANTITY) as executor:
        outputs_future = executor.submit(readOutputs)
        outputs = outputs_future.result()        
        
    with ThreadPoolExecutor(max_workers=MAX_THREAD_QUANTITY*3) as executor:
        tx_future = executor.submit(readTransaction)        
        tx = tx_future.result()       
            
    return inputs, outputs, tx

# --- read datas by csv files (for scraping) :

def readMap():
    """Read map csv by chunk 
    (calculating the chunk size using the number of rows in csv file)
    @no params
    @return map dataframe
    """ 
    startT = time.time()
    print('\nStarted reading map csv')
    schema = {        
        "txHash":'category',
        "addressId": "int32"              
    }
    usecols = [0,1]
    rowsInFile = 8708821
    _ , chunk_size = calculate_chunk_size(rowsInFile, 200000)
    columns = ["txHash","addressId"]
    chunks = read_csv_chunk(schema,columns,usecols,MAP_CSV_PATH,chunk_size)
    df = pd.concat(chunks)
    endT = time.time()
    diff = endT - startT
    if LOG_LEVELS['time']:
        print(f"\nMap csv read in {diff} seconds")
    return df
        
def takeMapCSV_data():  
    """Take map CSV data from map csv and convert them to dataframes.
    Use threads to improve performances.
    
    @no params 
    @return map dataframes
    """ 
    with ThreadPoolExecutor(max_workers=MAX_THREAD_QUANTITY*3) as executor:        
        map_future = executor.submit(readMap)                        
        mapDF = map_future.result()       
                    
    return mapDF

# --- main : 

def main():
    main_execution_start_time = time.time()
    
    # --- read input, output, transactions csv and create DF
    input_dataframe, outputs_dataframe, transaction_dataframe = takeCSV_data() 
    line = "------------------------"
        
    if LOG_LEVELS['time']:
        print(f"\nTime for read csv = {time.time()-main_execution_start_time} seconds")
    
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f"\n{line}Dataframes:\n\ninputs for congestion:\n{input_dataframe}\n{line}\noutputs for congestion:\n{outputs_dataframe}\n{line}\ntransaction:\n{transaction_dataframe}\n{line}")
    
    # --- obtain datas of network congestion & script type per month 
    notCoinbaseTX = transaction_dataframe.loc[transaction_dataframe['isCoinbase'] != 1] 
    notCoinbaseTX = notCoinbaseTX.drop('blockId', axis=1)
    notCoinbaseTX.set_index('timestamp',inplace=True)
    outputsToCalculateCongestion = outputs_dataframe.drop('addressId', axis=1)
    month_data_DF = analizer.processTransactions(input_dataframe, outputsToCalculateCongestion, notCoinbaseTX)
    
    # --- create plots for stats (network congestion & fees | script type)
    plot_creator.plot_fees_vs_network_congestion(month_data_DF)
    plot_creator.plot_script_type_usage(month_data_DF)
    plot_creator.plot_annual_script_type_usage(month_data_DF)
    
    # --- scraping & mining pool analysis :
    coinbaseTX = transaction_dataframe.loc[transaction_dataframe['isCoinbase'] == 1] #filter coinbase tx
    coinbaseTX = coinbaseTX.drop('fee', axis=1) # delete fee column
    coinbaseTX = coinbaseTX.drop_duplicates(subset=['txId']) #delete any duplicates
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f"coinbaseTX:{coinbaseTX}")
    
    outputsToMiningPoolsAnalisis = outputs_dataframe.drop('scriptType', axis=1) #delete script type 
    outputsToMiningPoolsAnalisis = outputsToMiningPoolsAnalisis.drop_duplicates(subset=['txId']) #delete any duplicates
    
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f"outputs To Mining Pools Analisis:\n{outputsToMiningPoolsAnalisis}")
    
    mapDF = takeMapCSV_data() #take map dataframe
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f"mapDF:\n\n{mapDF}")
    
    
    parsedTxDF = pd.merge(coinbaseTX, outputsToMiningPoolsAnalisis, on='txId') #merge coinbase tx with (parsed) outputs 
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f"parsedTxDF (1):\n\n{parsedTxDF}")
    
    parsedTxDF = pd.merge(parsedTxDF, mapDF, on='addressId') #merge previous resoult with map dataframe (to insert hash column)
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f"parsedTxDF (2):\n\n{parsedTxDF}")
    
    
    miningPoolAddressesDF = scraper.getPools() #get dataframe with txHash<-->mining pool association
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\nminingPoolAddressesDF:\n{miningPoolAddressesDF}')
    
    #merge previous resoult with parsedTxDF to obtain all coinbase tx associated to a mining pool :
    # -> included coinbase tx that have addresses that do not belong to any of the 4 mining pools
    allCoinbaseTxWithPools = pd.merge(parsedTxDF,miningPoolAddressesDF, on='txHash', how='left') 
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\nallCoinbaseTxWithPools:\n{allCoinbaseTxWithPools}')
  

    #filter transactions that not have the address associated to any mining pool
    coinbaseNotAssociated = allCoinbaseTxWithPools[allCoinbaseTxWithPools['pool'].isna()]
    coinbaseNotAssociated.drop('pool', axis=1)
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\ncoinbaseNotAssociated:\n{coinbaseNotAssociated}')
    
    #filter transactions that have address associated to a mining pool
    coinbase_associated = allCoinbaseTxWithPools[~allCoinbaseTxWithPools['pool'].isna()]
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\ncoinbase_associated:\n{coinbase_associated}')

        
    #group by 'txHash' and count transactions for each miner
    miner_counts = coinbaseNotAssociated.groupby('addressId')['txHash'].count().reset_index(name='blocks_mined')
    
    #sort miners     
    sorted_miners = miner_counts.sort_values(by='blocks_mined', ascending=False)

    #select top 4 miners 
    top_4_miners = sorted_miners.head(4)

    if LOG_LEVELS.get('debug', False):
        print(f"Top 4 miners:\n{top_4_miners}")
        
    
    others_miners = sorted_miners.iloc[4:]
    if LOG_LEVELS.get('debug', False):
        print(f"others miners:\n{others_miners}")

        
  
    # Calculate global statistics
    global_blocks_mined, global_total_rewards = analizer.calculate_pool_statistics(coinbase_associated)
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\nglobal_blocks_mined:\n{global_blocks_mined}')
        print(f'\nglobal_total_rewards:\n{global_total_rewards}')

    # Calculate bi-monthly statistics
    bi_monthly_blocks_mined, bi_monthly_total_rewards = analizer.calculate_bi_monthly_statistics(coinbase_associated)
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\npbi_monthly_blocks_mined:\n{bi_monthly_blocks_mined}')
        print(f'\nbi_monthly_total_rewards:\n{bi_monthly_total_rewards}')
    
    # Plot statistics
    plot_creator.plot_blocks_mined_by_top_4_miners(top_4_miners)
    plot_creator.plot_total_blocks_mined(global_blocks_mined)
    plot_creator.plot_bi_monthly_blocks_mined(bi_monthly_blocks_mined)
    plot_creator.plot_total_rewards(global_total_rewards)
    plot_creator.plot_bi_monthly_rewards(bi_monthly_total_rewards)

    #Eligius taint analysis
    nodes = scraper.getEligius_taint_analysis()    
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
        print(f'\nnodes:\n{nodes}')
    
    graph_DF = pd.DataFrame(nodes)
    print(graph_DF)
    
    plot_creator.plot_Eligius_path(graph_DF)
    
        
def test_eligius_graph():
    #Eligius taint analysis
    nodes = scraper.getEligius_taint_analysis()    
    
    graph_DF = pd.DataFrame(nodes)
    print(graph_DF)
    
    plot_creator.plot_Eligius_path(graph_DF)
    


if __name__ == "__main__":
    os.system("cls" if os.name == "nt" else "clear")  # clear console
    execution_start_time = time.time()
    main()
    #test_eligius_graph()
    end_time = time.time()
    elapsed_time = end_time - execution_start_time
    if LOG_LEVELS['time']:
        print(f"\nTotal execution time: {elapsed_time} seconds")
