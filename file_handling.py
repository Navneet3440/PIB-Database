# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 16:45:12 2020

@author: navneet
"""
import requests
import urllib
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import csv
import os
import re
from retry import retry
from langdetect import detect
from langdetect import DetectorFactory

def create_directory(path):
    try:
        os.mkdir(path)
        return True
    except FileExistsError as fe_error:
        return True
    except OSError as error:
        print(error)
    return False

def write_text_file(file_name,w_text,ministry_name,lang):
    with open(file_name,mode='w',encoding='utf-16') as file_w:
            file_w.write(ministry_name.strip()+'\n')
            #file_w.write(n_title.strip().replace('\n',' ').replace('\r',' ')+'\n')
            for line in w_text:
                for ln in line.split('\n'):
                    if len(ln.strip())>0:
                        '''
                        if lang=='hi':
                            try:
                                if (detect(ln.strip().replace('\r','')))=='en':
                                    continue
                            except:
                                pass
                                #print('ok')
                        elif lang=='en':
                            try:
                                if ((detect(ln.strip().repalce('\r','')))== 'hi'):
                                    continue
                            except:
                                pass
                            #print('ok')
                            '''
                        file_w.write(ln.strip().replace('\r','')+'\n')
                        
def get_file_content(filepath):
    with open(filepath,mode='r',encoding='utf-16') as fl:
        try:
            data=fl.read()
        except:
            print('error, retuning None for file:',os.path.basename(filepath), sep='\n')
            return None
    return data