import requests
import time
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import re
import os
import pandas as pd
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent
from urllib.request import Request, urlopen
import random 

# Aggiunge la directory contenente utilities.py al percorso di ricerca dei moduli
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utilities import LOG_LEVELS, SETTINGS

HEADLESS_MODE = SETTINGS['SELENIUM_HEADLESS_MODE']
BASE_LINK = "https://www.walletexplorer.com"
MAX_THREAD_QUANTITY = SETTINGS['MAX_THREAD_QUANTITY']

ELIGIUS_COINBASE_TX = 'c82c10925cc3890f1299'

sslproxies_infos = {
    'last search time': None,
    'sslproxies' : []
}

referrer_list = [
    "https://www.google.com",
    "https://www.bing.com",
    "https://search.yahoo.com",
    "https://www.duckduckgo.com",
    "https://www.baidu.com",
    "https://www.yandex.ru",
    "https://www.aol.com",
    "https://www.wikipedia.org",
    "https://www.facebook.com",
    "https://www.twitter.com",
    "https://www.linkedin.com",
    "https://www.instagram.com",
    "https://www.reddit.com",
    "https://www.pinterest.com",
    "https://www.quora.com",
    "https://www.medium.com",
    "https://news.ycombinator.com",
    "https://www.bbc.com",
    "https://www.cnn.com",
    "https://www.nytimes.com",
    "https://www.theguardian.com",
    "https://www.forbes.com",
    "https://www.bloomberg.com"
]

specific_referrer_list = [ # referrer form walletexplorer pages 
    'https://www.walletexplorer.com/wallet/7644ed877fa28a88?from_address=1G43MvhzCqRz1ctsQUmgU4LgLuSVdfU557',
    'https://www.walletexplorer.com',
    'https://www.walletexplorer.com/?q=',
    'https://www.walletexplorer.com/wallet/DeepBit.net',
    'https://www.walletexplorer.com/wallet/DeepBit.net/addresses',
    'https://www.walletexplorer.com/info',
    'https://www.walletexplorer.com/privacy',
    'https://www.walletexplorer.com/wallet/BTCCPool',
]

class RequestError(Exception):
    def __init__(self, message, erroCode):
        super().__init__(message)
        self.erroCode = erroCode

    def __str__(self):
        return f"{self.erroCode}: {self.args[0]}"

def generate_proxies(forceNew = False): 
    """generate and return (free) proxies by sslproxies.org if 
    the last request made to sslproxies is at least 3 seconds ago, 
    otherwise it returns the last generated proxies
    
    @param forceNew : if True the function will request new proxies to sslproxies.org
    @return : proxies list with ip & port for every proxy
    """
    proxies = []
    lastReqTime = sslproxies_infos['last search time']
    
    if forceNew or (lastReqTime is None or time.time() - lastReqTime > 3):
        user_agent = getRandomUserAgent()
        proxies_req = Request('https://www.sslproxies.org/')
        proxies_req.add_header('User-Agent', user_agent)
        proxies_doc = urlopen(proxies_req).read().decode('utf8')
        soup = bs(proxies_doc, 'html.parser')
        proxies_table = soup.find('table', class_='table table-striped table-bordered')        
        
        for row in proxies_table.tbody.find_all('tr'):
            td = row.find_all('td')
            proxies.append({
            'ip':   td[0].string,
            'port': td[1].string})

        sslproxies_infos['sslproxies'] = proxies
        sslproxies_infos['last search time'] = time.time()
    else:
        proxies = sslproxies_infos['sslproxies']
  
    return proxies

def getRandomUserAgent():
    """generate and return a random user agent
    @no params
    @return : random user agent string
    """
    ua = UserAgent()     
    return ua.random

def getRequestUtils(specific = False):
    """Returns utils for a request (headers, proxies)
    @param specific : if True the function will use specific proxies and referrer 
    @return : headers, proxies to use for a request / request session
    """
    
    # Liste di opzioni dinamiche per gli headers
    accept_language_options = [
        "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7",
        "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    ]

    sec_ch_ua_options = [
        "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
        "\"Opera\";v=\"80\", \"Chromium\";v=\"94\", \"Not(A:Brand\";v=\"24\"",
        "\"Microsoft Edge\";v=\"90\", \"Chromium\";v=\"90\", \"Not(A:Brand\";v=\"24\""
    ]

    sec_ch_ua_platform_options = [
        "\"macOS\"",
        "\"Windows\"",
        "\"Linux\""
    ]

    user_A = getRandomUserAgent()
    if specific : 
        referrer = random.choice(specific_referrer_list)        
    else:
        referrer = random.choice(referrer_list)
    
    proxies = generate_proxies(specific)
    

    headers = {
        'User-Agent': user_A,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "cache-control": "max-age=0",
        "accept-language": random.choice(accept_language_options),
        "sec-ch-ua": random.choice(sec_ch_ua_options),
        "sec-ch-ua-platform": random.choice(sec_ch_ua_platform_options),
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        'referer': referrer,
        "referrerPolicy": "strict-origin-when-cross-origin",
        "credentials": "omit"

    }
    
    return headers, proxies
              
def getWalletAddresses(url):  
    """Get wallet addresses associated to a pool by the WalletExplorer's url. 
    
    @params : url : url of the WalletExplorer address page of a pool.

    @return : list of addresses associated to the pool
    """
    
    startT = time.time()
    
    if LOG_LEVELS['time']:
        print("get wallet addresses started")
        
    if LOG_LEVELS['all infos'] or LOG_LEVELS['debug']:
        print(f"going to get wallet addresses with url {url}") 
         
    headers, proxies = getRequestUtils()
    
    addresses = [] 
    
  
    time.sleep(0.75)
    timeBeforeRequest = time.time()
    response = requests.get(url,headers=headers, proxies=random.choice(proxies)) 
    if LOG_LEVELS['time']:
        print(f"request time = {time.time()-timeBeforeRequest} seconds")
    attempt = 0
    while attempt < 20 and (response.text.startswith("Too") or response.status_code < 200 or response.status_code > 299):
        attempt+= 1
        if LOG_LEVELS['debug']:
            print(f"Unsuccessful response in attempt {attempt}/15 - response status code = {response.status_code}")
        
        new_headers, new_proxies = getRequestUtils(specific=True)
        chosenProxy = random.choice(new_proxies)
        time.sleep(5)
        response = requests.get(url, proxies=chosenProxy, headers=new_headers)        
        
        
        if attempt == 20:
            if LOG_LEVELS['debug']:
                print(f'unsuccessful response:\n{response}\nTxt:\n{response.text}')
            raise RequestError(f'Error in get wallet address request\nresponse status = {response.status_code}\nsession headers = {new_headers}\nchosen proxy = {chosenProxy}', response.status_code)
                


    html_content = response.text
    soup = bs(html_content,'html.parser')
    
    table = soup.find('table') 
    if table:
        trs = table.findAll('tr')
        
        if not LOG_LEVELS['reduce spam'] and ( LOG_LEVELS['all infos'] or LOG_LEVELS['debug']):
            print(f"going to process {len(trs)} table rows")  
    
        i = 0
        for tr in trs:
            if not LOG_LEVELS['reduce spam'] and (LOG_LEVELS['all infos'] or LOG_LEVELS['debug'] or LOG_LEVELS['processing']):
                print(f"processing row {i+1}/{len(trs)}") 
            i+=1
            td = tr.find('td')  # Find the first td element
            
            
            if td and td.a:  # Check if td and td.a exist
                href = td.a['href']
                address = href.split("/")[-1]
                addresses.append(address)  # Use append instead of push
                if not LOG_LEVELS['reduce spam'] and (LOG_LEVELS['all infos'] or LOG_LEVELS['debug'] or LOG_LEVELS['processing']):
                    print(f"-> found  wallet : {address}") 
      
    if LOG_LEVELS['all infos'] or LOG_LEVELS['debug'] or LOG_LEVELS['results']:
        print(f"Found {len(addresses)} wallets") 
    
    if LOG_LEVELS['time']:
        print(f"get wallet addresses ended in {time.time()-startT} seconds")
        
    return addresses

def getWalletAddress_multiplePages(url):
    """Finds and returns wallet addresses for a mininng pool also if it has addresses in more than one page
    @param url : url of the first page of addresses of a pool
    @return : all wallet addresses found for the pool    
    """
    
      
    startT = time.time()
    if LOG_LEVELS['time']:
        print("get wallet addresses (with Selenium) started")
    driver = setup_selenium_driver()
    num_pages = get_number_of_pages(driver, url)
    driver.quit()
    
    addresses = []
    
    for page_number in range(num_pages+1):
        if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']:
            print(f"processing page {page_number}")
            

        if page_number != 1:
            page_url = f"{url}?page={page_number}"
        else:
            page_url = url
                    
        curent_addresses = getWalletAddresses(page_url)
        addresses += curent_addresses
                
   
            
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['results']:
        print(f"Processed {num_pages} pages and found {len(addresses)} addresses")
    
    if LOG_LEVELS['time']:
        print(f"get wallet addresses (with multiple pages) ended in {time.time()-startT} seconds")
    return addresses
  
def setup_selenium_driver():
    """Setup the Selenium driver and return it.
    
    @params : no params

    @return : Selenium web driver (webdriver.Chrome)
    """
    options = Options()
    if HEADLESS_MODE:
        options.add_argument('--headless=new')
        if LOG_LEVELS['debug'] or LOG_LEVELS['all infos']:
            print("Using Selenium in headless mode...")
    driver = webdriver.Chrome(options=options)
    return driver

def get_number_of_pages(driver, url):
    """Finds and returns the number of pages associated to a pool.
    
    @params : driver : Selenium web driver
    @params : url : url of the first addresses page of a pool

    @return : quantity of addresses pages of the pool
    """
    
    driver.get(url)
    time.sleep(2)
    
    paging_info = driver.find_elements(By.XPATH, '//*[@id="main"]/div[1]')[0]
    page_text = paging_info.text
    
    match = re.search(r'Page 1 / (\d+)', page_text)
    if match:
        num_pages = int(match.group(1))
        if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']:
            print(f"Found {num_pages} pages to process")
    else:
        num_pages = 1  # Default to 1 if the number of pages cannot be found
    
    return num_pages

def get_addresses_from_page(driver, url, page_number):
    """Finds and returns the addresses associated to a pool.
    
    @params : driver : Selenium web driver
    @params : url : url of the page of the page from which to obtain the addresses

    @return : addresses found in the page
    """
    
    addresses = []
    
    try:
        if page_number != 1:
            new_url = f"{url}?page={page_number}"
            driver.get(new_url)
            time.sleep(0.2)
        else:
            driver.get(url)
        
        if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']:
            print(f"processing page {page_number}")
            
        table = driver.find_element(By.TAG_NAME, 'table')
        trs = table.find_elements(By.TAG_NAME, 'tr')
        
        for i, tr in enumerate(trs, start=1):
            td_list = tr.find_elements(By.TAG_NAME, 'td')
            if len(td_list) > 0:
                td = td_list[0]
                try:
                    a_tag = td.find_element(By.TAG_NAME, 'a')
                    if a_tag:
                        href = a_tag.get_attribute('href')
                        address = href.split("/")[-1]
                        addresses.append(address)
                        if not LOG_LEVELS['reduce spam'] and (LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']):
                            print(f"-> found address {address}")

                except Exception as e:
                    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']:
                        print(f"address {i} is not a standard address -> skipped")
                        
                    if LOG_LEVELS['debug']:
                        print(f"error:\n{e}")

                    continue
    except Exception as e:
        if LOG_LEVELS['debug']:
            print(f"error in get address from page:\n{e}")
    
    finally:
        return addresses

def get_W_addresses_Selenium(url):
    """Finds and returns all the wallet addresses associated to a pool.
    
    @params : url : url of the first page of addresses of a pool

    @return : all wallet addresses found for the pool
    """
    
    startT = time.time()
    if LOG_LEVELS['time']:
        print("get wallet addresses (with Selenium) started")
    driver = setup_selenium_driver()
    num_pages = get_number_of_pages(driver, url)
    driver.quit()
    
    # Imposta MAX_THREAD_QUANTITY a num_pages se è maggiore
    max_t_quantity = min(MAX_THREAD_QUANTITY, num_pages)
    addresses = []
    
    def process_pages(start_page, end_page): #processa le pagine da start_page a end_page
        local_driver = setup_selenium_driver()
        local_addresses = []
        for page in range(start_page, end_page + 1):
            curent_addresses = get_addresses_from_page(local_driver, url, page)
            local_addresses += curent_addresses
        
        local_driver.quit()
        return local_addresses
    
    with ThreadPoolExecutor(max_workers=max_t_quantity) as executor:
        future_to_page = {executor.submit(process_pages, i, min(i + num_pages // max_t_quantity, num_pages)): i for i in range(1, num_pages + 1, num_pages // max_t_quantity)}
        for future in as_completed(future_to_page):
            results = future.result()
            addresses += results
            
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['results']:
        print(f"Processed {num_pages} pages and found {len(addresses)} addresses")
    
    if LOG_LEVELS['time']:
        print(f"get wallet addresses (with Selenium) ended in {time.time()-startT} seconds")
    return addresses
       
def getPools(): 
    """Finds and returns all the addresses associated with each of the 4 mining pools considered
    
    @params : no params

    @return : dataframe of transactions associated to a pool
    """

    startT = time.time()
    if LOG_LEVELS['time']:
        print("Get pools started")
            
    eligiusAddressesLink = f"{BASE_LINK}/wallet/Eligius.st/addresses"
    deepBitAddressesLink = BASE_LINK+'/wallet/DeepBit.net'+'/addresses'    
    bitMinterAddressesLink = BASE_LINK+'/wallet/BitMinter.com'+'/addresses'    
    BTCGuildAddressesLink = BASE_LINK + '/wallet/BTCGuild.com'+ '/addresses'
    
    
    BTCGuild_WalletAddresses = getWalletAddress_multiplePages(BTCGuildAddressesLink)
    
    eligius_WalletAddresses = get_W_addresses_Selenium(eligiusAddressesLink)
    
    deepBit_WalletAddresses = getWalletAddresses(deepBitAddressesLink)
    
    bitMinter_WalletAddresses = get_W_addresses_Selenium(bitMinterAddressesLink)
    
    
    
    pools = {
        'Eligius' : eligius_WalletAddresses,
        'DeepBit' : deepBit_WalletAddresses,
        'BitMinter' : bitMinter_WalletAddresses,
        'BTCGuild' : BTCGuild_WalletAddresses,
    }
    
    # Creo una lista di tuple (address, pool) per poi poterla convertire in un dataframe 
    pool_data = [(address, pool) for pool, addresses in pools.items() for address in addresses]

    # Converto la lista di tuple in un DataFrame con colonne txHash e pool
    df_pools = pd.DataFrame(pool_data, columns=['txHash', 'pool'])
    
    if LOG_LEVELS['time']:
        print(f"get pools ended in {time.time()-startT} seconds ")
    
    return df_pools
    

def getTxAsNode(txId):
    """Searchs the single transaction by it's hash on WalletExplorer, 
    finds transaction's inputs and outputs and return the transaction as a node
    
    @params : txId : transaction hash 

    @return : the node related to the transaction as a dictionary with txId, inputs and outputs keys
    """

    tx_node = {
        'txId': txId,
        'inputs': [],
        'outputs': [],        
    }
    
    try:
        driver = setup_selenium_driver()
        if(txId == ELIGIUS_COINBASE_TX):
            driver.get(BASE_LINK)
            time.sleep(0.5)        
            
            inputSpace = driver.find_element(By.XPATH, '/html/body/div[2]/form/p/label/input')
            submitButton = driver.find_element(By.XPATH, '/html/body/div[2]/form/p/input')
            
            inputSpace.send_keys(txId)    
            submitButton.click()
        else:
            link = f'{BASE_LINK}/txid/{txId}'
            
            driver.get(link)
            time.sleep(0.5)
            
        
        infoTable = driver.find_element(By.CLASS_NAME, 'info')        
        infoTableBody = infoTable.find_element(By.TAG_NAME,'tbody')
        infoTableFirstTr = infoTableBody.find_elements(By.TAG_NAME,'tr')[0]
        tx_node['txId'] = infoTableFirstTr.find_element(By.TAG_NAME,'td').text
        
        txTable = driver.find_element(By.XPATH, '/html/body/div[2]/table[2]')
        txTableBody = txTable.find_element(By.TAG_NAME,'tbody')
        txTableSecondTr = txTableBody.find_element(By.XPATH,'/html/body/div[2]/table[2]/tbody/tr[2]')
        
        raw_inputs = txTableSecondTr.find_element(By.XPATH, '/html/body/div[2]/table[2]/tbody/tr[2]/td[1]')
        raw_outputs = txTableSecondTr.find_element(By.XPATH, '/html/body/div[2]/table[2]/tbody/tr[2]/td[2]')    
        raw_inputs_trs = raw_inputs.find_element(By.TAG_NAME,'tbody').find_elements(By.TAG_NAME,'tr')
        raw_outputs_trs = raw_outputs.find_element(By.TAG_NAME,'tbody').find_elements(By.TAG_NAME,'tr')
        
        inputs = []
        for tr in raw_inputs_trs:
            if txId == ELIGIUS_COINBASE_TX:
                firstTd = tr.find_elements(By.TAG_NAME,'td')[0]
                inputTxt = firstTd.text
                inputs.append(inputTxt)
                
            else:
                firstTd = tr.find_element(By.CLASS_NAME,'small')
                a_tag = firstTd.find_element(By.TAG_NAME, 'a')
                href = a_tag.get_attribute('href')        
                
                if 'txid' in href:
                    txid = href.split('/txid/')[1]
                    inputs.append(txid)
                else:
                    inputTxt = a_tag.text
                    inputs.append(inputTxt)
            
        tx_node['inputs'] = inputs
        
        outputs = []
        for tr in raw_outputs_trs:
            firstTd = tr.find_element(By.CLASS_NAME,'small')
            try:
                a_tag = firstTd.find_element(By.TAG_NAME, 'a')
                href = a_tag.get_attribute('href')        
                
                if 'txid' in href:
                    txid = href.split('/txid/')[1]
                    outputs.append(txid)
                else:
                    outputTxt = a_tag.text
                    outputs.append(outputTxt)

            except Exception as e :
                if 'unspent' in firstTd.text and (LOG_LEVELS['debug'] or LOG_LEVELS['all infos']):
                    print("unspent tx -> skip it")                
                elif LOG_LEVELS['debug']:
                    print(f"error = {e}")
        
        tx_node['outputs'] = outputs
        
        if not LOG_LEVELS['reduce spam'] and (LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']):
            print(f"-> found node:\n {tx_node}")
    except Exception as e:
        if LOG_LEVELS['debug']:
            print(f"error in get tx as node:\n{e}")
            
    finally:            
        return tx_node

def getEligius_taint_analysis():
    """carries out the taint analysis on the Eligius pool for k steps, obtaining the graph of the path of the Bitcoins
    
    @params : no params

    @return : list of nodes of the graph related of Eligius pool taint analysis
    """

    startT = time.time()
    if LOG_LEVELS['time']:
        print("Eligius taint analysis miners started")
    
    steps = SETTINGS['ELIGIUS_ANALYSIS_STEPS']
    if steps < 0:
        raise ValueError('ELIGIUS_ANALYSIS_STEPS must be greater than 0')

    nodes = []
    last_outputs = []
    for step in range(steps):
        startLen = len(nodes)
        currentOutputs = []
        if step == 0:
            eligiusCoinbaseNode = getTxAsNode(ELIGIUS_COINBASE_TX)
            nodes.append(eligiusCoinbaseNode)        
            currentOutputs += eligiusCoinbaseNode['outputs']
        else:  
            if (not LOG_LEVELS['reduce spam'] or LOG_LEVELS['results']) and (LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']):
                print(f"going to proceed {len(last_outputs)} transactions")
                  
            max_t_quantity = min(MAX_THREAD_QUANTITY, len(last_outputs))
            if max_t_quantity > 7: #reduce the max threads quantity to 7 to do not overload the use of resources
                max_t_quantity = 7
            with ThreadPoolExecutor(max_workers=max_t_quantity) as executor:
                future_to_node = {executor.submit(getTxAsNode, id): id for id in last_outputs}
                for future in as_completed(future_to_node):
                    node = future.result()
                    nodes.append(node)
                    currentOutputs += node['outputs']
            
            
                
        last_outputs = currentOutputs
        
        if (not LOG_LEVELS['reduce spam'] or LOG_LEVELS['results']) and (LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['processing']):
            print(f"-> found {len(nodes)-startLen} nodes at step {step}/{steps}")
            
    if LOG_LEVELS['debug'] or LOG_LEVELS['all infos'] or LOG_LEVELS['results']:
        print(f"Found {len(nodes)} nodes in {steps} steps")            
    
    if LOG_LEVELS['time']:
        print(f"Eligius taint analysis miners ended in {time.time()-startT} seconds")
     
    return nodes

               
            
if __name__ == "__main__": #for module tests 
    os.system("cls" if os.name == "nt" else "clear")  # clear console
    
    pools = getPools()
    print(f"\npools = {pools}")
    
    nodesEligius_T_analisys = getEligius_taint_analysis()
    print(f"nodesEligius_T_analisys:\n{nodesEligius_T_analisys}")
    