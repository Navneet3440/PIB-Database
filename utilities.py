from langdetect import detect

def detect_non_eng(en_line):
    for line_split in en_line.split():
        
        try:
            if(detect(line_split.lower()) in ['hi','bn','gu','kn','ml','mr','ne','pa','ta','te','ur']):                  #in ['hi','bn','gu','kn','ml','mr','ne','pa','ta','te','ur'):
                #print(line_split,detect(line_split.lower()))
                return True
            else:
                continue
        except:
            #print('error',k,sep='\n')
            continue
    return False
