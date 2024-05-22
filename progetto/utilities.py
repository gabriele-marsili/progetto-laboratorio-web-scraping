import pandas as pd
import datetime

SETTINGS = {
    'MAX_THREAD_QUANTITY' : 13,
    'ELIGIUS_ANALYSIS_STEPS':7,
    'SELENIUM_HEADLESS_MODE' : True
}

LOG_LEVELS = {
    'debug' : True,
    'results' : True,
    'processing' : True,
    'all infos' : True,
    'time' : True,
    'reduce spam':True
}


def preprocess_data(df):
    # Convert timestamp to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df

def unix_to_date(timestamp, date_type = 'datetime'):
    """
    Converte un timestamp Unix in un oggetto datetime / 
    lo converte in formato gg/mm/aaaa - hh:mm:ss se date_type != datetime.
    
    @param timestamp (int): Timestamp Unix da convertire.
    @param date_type (str) (optional) : tipo di conversione ritornata
    
    @return :
     - datetime.datetime: Oggetto datetime corrispondente al timestamp Unix.
        oppure 
     - str: Data formattata come 'gg/mm/aaaa - hh:mm:ss'.
    """
    if(date_type != 'datetime'):
        dt = datetime.datetime.fromtimestamp(timestamp)
        return format_datetime(dt)
    
    return datetime.datetime.fromtimestamp(timestamp)

def format_datetime(dt):
    """
    Formatta un oggetto datetime in formato 'gg/mm/aaaa - hh:mm:ss'.
    
    @param dt (datetime.datetime): Oggetto datetime da formattare.
    
    @return : str: Data formattata come 'gg/mm/aaaa - hh:mm:ss'.
    """
    return dt.strftime('%d/%m/%Y - %H:%M:%S')

def calculate_chunk_size(total_rows, desired_chunk_size):
    """Calcola il numero dei chunk e la relativa dimenione in base 
    al totale delle righe del file ed alla dimensione desiderata dei chunk.

    @param total_rows : totale delle righe del file
    @param desired_chunk_size : dimensione di un chunk 
    
    @return num_chunks, actual_chunk_size : il nuumero dei chunk e la relativa size
    """
    
    num_chunks = total_rows // desired_chunk_size
    if total_rows % desired_chunk_size != 0:
        num_chunks += 1
    actual_chunk_size = total_rows // num_chunks
    return num_chunks, actual_chunk_size