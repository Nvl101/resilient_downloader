'''
the release version of Resilient Downloader
migrating codes from poc2_resume_download.py
'''
import os
import datetime
import time
import random
import re
import requests
import shutil
import tempfile
import urllib
from pkg_resources import ExtractionError


# download to this folder
TARGET_DIR = r'C:\Users\Your_Own\Downloads'

# connection parameters
# proxies support http_proxy + https_proxy, may support socks5
PROXIES = {} # config this dictionary if you use a proxy
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 5
# sometimes taking a nap makes it easier
RANDOM_NAP_CHANCE = 1/4 # possibility to call random nap, between 0 and 1
RANDOM_NAP_TIME = 15
RECONNECT_NAP_TIME = 15
# sometimes take a nap before it slows down or breaks
PASSIVE_NAP_CHECKPOINT = 3 * 1024 ** 3 # 3 MB
PASSIVE_NAP_CHANCE = 1/4
PASSIVE_NAP_TIME = 20

class ReadEmptyError(Exception):
    ''' reading content returns empty while not finished'''
    pass

class PassiveNapException(Exception):
    '''
    '''
    pass

def extract_filename(url: str) -> str:
    '''
    INPUT:
        url: string, the complete url of input
    OUTPUT:
        decoded_filename, string, decoded string of 
    '''
    url_basename = os.path.basename(url)
    url_filename = re.findall(r'filename=(.+)', url_basename)
    if len(url_filename) == 0:
        return_name = urllib.parse.unquote(url_basename)
    else:
        encoded_filename = url_filename[0]
        decoded_filename = urllib.parse.unquote(encoded_filename)
        return_name = decoded_filename
    return_name = return_name.split('&')[0]
    if return_name == '' or not is_legal_filename(return_name):
        return datetime.datetime.now().strftime('untitled_download %m-%d_%H-%M')
    return return_name

def is_legal_filename(filename: str) -> bool:
    '''intake a string, decide whether it's legal filename'''
    if set(filename) & set('/?<>\:*|"') is not set(): # if containing illegal characters
        return False
    if len(filename) > 128:
        return False
    return True

def size_convert(size_byte:int) -> str:
    '''
    convert bytes to gb/mb/kb strings
    '''
    if size_byte < 1024:
        return f'{size_byte} B'
    if size_byte < 1024 ** 2:
        return f'{round(size_byte/1024, 2)} KB'
    if size_byte < 1024 ** 3:
        return f'{round(size_byte/1024**2, 2)} MB'
    return f'{round(size_byte/1024**3, 3)} GB'

def retry_cooldown(attempts:int)->float:
    '''
    INPUT:
    attempts: integer, attenpts taken
    OUTPUT:
    cooldown_time, float, time to sleep before next retry
    '''
    if attempts < 1:
        return 0.1
    if attempts == 1:
        return 1
    if attempts == 2:
        return 1
    final_cooldown = 3 + 3 * random.random()
    return final_cooldown

def print_status(total_size: int, fetch_size: int, attempts: int):
    ''' print progress bar'''
    display_status = f'\t{size_convert(fetch_size)}/ {size_convert(total_size)} [{attempts}]     '
    print(display_status, end = '\r')

def make_request(source_url, headers = None):
    '''return a requests.get obj
    '''
    while True:
        attempts = 0
        try:
            req = requests.get(source_url, stream = True, proxies = PROXIES, headers = headers,
                verify =True, allow_redirects = True, timeout = (CONNECT_TIMEOUT, READ_TIMEOUT)
            )
            break
        except requests.exceptions.Timeout:
            attempts += 1
            if attempts % 3 == 0:
                time.sleep(RECONNECT_NAP_TIME)
            else:
                time.sleep(1)
        except requests.exceptions.ConnectionError as ConnectionError:
            attempts += 1
            #warnings.warn(f'retrying on conn error: {str(ConnectionError)}')
            if attempts % 3 == 0:
                time.sleep(RECONNECT_NAP_TIME)
            else:
                time.sleep(5)
    req.raise_for_status()
    return req

def resilient_download(source_url: str, target_file: str, verbose: bool = True):
    '''
    Executing the resilient download
    '''
    temp_file = tempfile.mktemp()
    with make_request(source_url) as req1, \
        open(temp_file, 'wb') as file1:
        # initiating the connection
        file_size = int(req1.headers['Content-Length'])
        print(f'file size: {file_size}')
        request_raw = req1.raw
        # fetch with retry mechanism
        fetched_length = 0
        fetched_length_checkpoint = 0
        reconnects = 0
        # handling a reconnecting exception
        nap_time = 0
        need_reconnect = False
        while True:
            try:
                content = request_raw.read(10*1024)
                if len(content) > 0:
                    # write into file if fetched something
                    file1.write(content)
                    fetched_length += len(content)
                    fetched_length_checkpoint += len(content)
                else:
                    raise ExtractionError('read content returns empty')
                if fetched_length_checkpoint > PASSIVE_NAP_CHECKPOINT:
                    if random.random() < PASSIVE_NAP_CHANCE:
                        raise PassiveNapException('checkpoint reached, taking a nap')
            # passively take a nap before it gets too slow
            except PassiveNapException:
                nap_time = PASSIVE_NAP_TIME
                need_reconnect = True
            # catch exception for read timeout
            # keyboard interrupt to pause
            except KeyboardInterrupt:
                req1.close()
                input('Paused: press Enter to resume...')
                need_reconnect = True
            # catch exception for read timeout
            except:
                # randomly take a break if being exhausted
                if RANDOM_NAP_CHANCE > 0 and reconnects > 3:
                    if random.random() > RANDOM_NAP_CHANCE:
                        nap_time = RANDOM_NAP_TIME
                else:
                    nap_time = nap_time = 3 + random.random()*3
                need_reconnect = True
            finally:
                # if all done, stop fetching
                if fetched_length >= file_size:
                    req1.close()
                    break
                # if need reconnect, reconnect here
                if need_reconnect:
                    req1.close()
                    time.sleep(nap_time)
                    resume_position = str(int(fetched_length))
                    resume_header = {'Range': f'bytes={resume_position}-'}
                    req1 = make_request(source_url, resume_header)
                    request_raw = req1.raw
                    need_reconnect = False
                # update status
                if verbose:
                    print_status(file_size, fetched_length, reconnects)
    shutil.move(temp_file, target_file)
    if verbose:
        print(f'download successful after {reconnects} retries')


def main():
    '''
    main: downloads file from user input URL to target directory
    '''
    print('Enter your download URL')
    source_url = input(':: ')
    target_filename = extract_filename(source_url)
    target_path = os.path.join(TARGET_DIR, target_filename)
    resilient_download(source_url, target_path)
    print(f'downloaded to path:\n{target_path}')

# file size: 93743601
if __name__ == '__main__':
    main()
