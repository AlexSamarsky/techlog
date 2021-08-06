import shutil

with open('seg_all.txt','wb') as wfd:
    for f in ['seg1.txt','seg2.txt']:
        with open(f,'rb') as fd:
            shutil.copyfileobj(fd, wfd)
