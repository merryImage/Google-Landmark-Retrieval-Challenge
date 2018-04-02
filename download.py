import numpy as np
import pandas as pd
from urllib import request
from multiprocessing import Pool, cpu_count, Manager
import os
import tqdm
import pdb
import time
import socket

socket.setdefaulttimeout(300)

log_file = 'bad_index_img.txt'
path = './index_image/'
df = pd.read_csv('index.csv')
if os.path.exists(log_file):
    df_log = pd.read_table(log_file, header=None)
    df_log['id'] = df_log[0].map(lambda x:x[0:16])
    df = df[df['id'].isin(list(df_log['id']))]
    os.remove(log_file)
    
df['url'] = df['url'].map(lambda x:x[0:-6] + 's512/' if x[-6:-1] == 's1600' else x)
files = os.listdir(path)

def download_image(url, name):
    try:
        request.urlretrieve(url, path + name)
    except:
        error = name + ' cannot be downloaded'
        print (error)
        with open('bad_index_img.txt', 'a') as fw:
            fw.write(name[:-4] + '\n')

start = time.time()
pool = Pool(processes = cpu_count())
for i in range(len(df)):
    name = df.iloc[i]['id'] + '.jpg'
    #if name not in files:
    url = df.iloc[i]['url']
    pool.apply_async(download_image, (url, name,))
pool.close()
pool.join()
end = time.time()
print(end - start)

