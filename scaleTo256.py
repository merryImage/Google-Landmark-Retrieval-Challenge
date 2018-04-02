from multiprocessing import Pool, cpu_count
from PIL import Image
import os
import pandas as pd

def scale(img, size):
    w, h = img.size
    if (w <= h and w == size) or (h <= w and h == size):
        return img
    if w < h:
        ow = size
        oh = int(size * h / w)
        return img.resize((ow, oh), Image.BILINEAR)
    else:
        oh = size
        ow = int(size * w / h)
        return img.resize((ow, oh), Image.BILINEAR)

def save_scaled_img(name):
    try:
        im = Image.open('./index_image/' + name)
        if im.mode != 'RGB':
            im = im.convert('RGB')
        resize_im = scale(im, 256)
        resize_im.save('./resize_index_image/' + name)
    except:
        with open('bad_index.txt', 'a') as f:
            f.write(name[:-4] + '\n')
        print ('failed to save ' + name)

index_files = os.listdir('./index_image/')
resize_files = os.listdir('./resize_index_image/')
df_index = pd.DataFrame(index_files)
df_resize = pd.DataFrame(resize_files)
df = df_index[~df_index[0].isin(df_resize[0])]
files = list(df[0])

pool = Pool(processes = cpu_count())
for name in files:
    pool.apply_async(save_scaled_img, (name,))
pool.close()
pool.join()
