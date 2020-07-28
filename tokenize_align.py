import os
import pandas as pd
import requests
import json
import glob
import requests
import json
import uuid
import time
from file_handling import create_directory

def get_sen_token (js,lang):
    url_en= 'https://auth.anuvaad.org/tokenize-sentence'
    url_hi='https://auth.anuvaad.org/tokenize-hindi-sentence'
    if lang =='en':
        req=requests.post(url_en,json=js)
    elif lang=='hi':
        req=requests.post(url_hi,json=js)
    else:
        print('Not valid langauge code')
        return None
    obj = json.loads(req.text)
    #print(obj)
    sentences= [i.strip().replace('\n',' ').replace('\r',' ') for l in obj['data'] for i in l['text']]
    return sentences


def write_tok_file_and_get_len(filename,sen_l,lang):
    with open(filename,mode='w',encoding='utf-16') as fl:
        count=0
        for l in sen_l:
            if (l.count('\"')%2) !=0:
                #print(l)
                l=l.replace('\"','')
                #print(l)
            if len(l.split())<2:
                continue
            fl.write(l+'\n')
            count+=1
    return count

def upload_document(token, filepath, fileType='file/txt'):
    data  = open(filepath, 'rb')
    url   = 'https://auth.anuvaad.org/upload'
    try:
        r           = requests.post(url = url, data = data,
                                 headers = {'Content-Type': 'text/plain'})
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print (e.response.text)
        return None
    print("upload:\n",r.text)
    data.close()
    obj         = json.loads(r.text)
    return obj['data']
def download_file(token, save_dir, file_id, prefix, extension='.txt'):
    url          = 'https://auth.anuvaad.org/download/{}'.format(file_id)
    header       = {'Authorization': 'Bearer {}'.format(token)}
    try:
        r        = requests.get(url, headers=header, timeout=10)
        print(r)
        with open(os.path.join(save_dir, file_id+'_'+prefix + extension), 'wb') as f:
            f.write(r.content)
            print('file %s, downloaded at %s' % (file_id, save_dir))
    except requests.exceptions.Timeout:
        print(f'Timeout for URL: {url}')
        return
    except requests.exceptions.TooManyRedirects:
        print(f'TooManyRedirects for URL: {url}')
        return
    except requests.exceptions.RequestException as e:
        print(f'RequestException for URL: {url}')
        return
    
def submit_alignment_files(token, file_id1, lang_code1, file_id2, lang_code2):
    url = 'https://auth.anuvaad.org/anuvaad-etl/extractor/aligner/v1/sentences/align'
    body = {
        "source": {
            "filepath": file_id1,
            "locale": lang_code1
        },
        "target": {
            "filepath": file_id2,
            "locale": lang_code2
      }
    }
    header          = {
                            'Authorization': 'Bearer {}'.format(token),
                            'Content-Type': 'application/json'
                      }
    try:
        r           = requests.post(url = url, headers=header, data = json.dumps(body))
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print (e.response.text)
        return None
    print("Submit allignment:\n",r.text)
    return json.loads(r.text)


def get_alignment_result(token, job_id):
    url          = 'https://auth.anuvaad.org/anuvaad-etl/extractor/aligner/v1/alignment/jobs/get/{}'.format(job_id)
    #print(url)
    header       = {'Authorization': 'Bearer {}'.format(token)}
    
    try:
        r        = requests.get(url, headers=header)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print (e.response.text)
        return None, None
    rsp          = json.loads(r.text)
    if rsp is not None and len(rsp) > 0:
        if rsp[0]['status'] == 'COMPLETED':
            return True, rsp
    return False, None

def extract_bitext(token, output_dir,source_filepath, target_filepath):
    create_directory(output_dir)
    src_resp = upload_document(token, source_filepath, 'file/txt')
    tgt_resp = upload_document(token, target_filepath, 'file/txt')
    if src_resp['filepath'] is not None and tgt_resp['filepath'] is not None:
            align_rsp = submit_alignment_files(token, src_resp['filepath'], 'en', tgt_resp['filepath'], 'hi')
            if align_rsp is not None and align_rsp['jobID'] is not None:
                print('alignment jobId %s' % (align_rsp['jobID']))
            
                while(1):
                    status, rsp = get_alignment_result(token, align_rsp['jobID'])
                    #print(rsp)
                    if status:
                        print('jobId %s, completed successfully' % (align_rsp['jobID']))
                        download_file(token, output_dir, rsp[0]['output']['almostMatch']['source'],\
                                      os.path.basename(source_filepath).split('.')[0]+'_aligned_am_src')
                        download_file(token, output_dir, rsp[0]['output']['almostMatch']['target'],\
                                      os.path.basename(target_filepath).split('.')[0]+'_aligned_am_tgt')

                        download_file(token, output_dir, rsp[0]['output']['match']['source'],\
                                      os.path.basename(source_filepath).split('.')[0]+'_aligned_m_src')
                        download_file(token, output_dir, rsp[0]['output']['match']['target'],\
                                      os.path.basename(target_filepath).split('.')[0]+'_aligned_m_tgt')
                        break
                    else:
                        print('jobId %s, still running, waiting for 20 seconds' % (align_rsp['jobID']))
                        time.sleep(10)
                