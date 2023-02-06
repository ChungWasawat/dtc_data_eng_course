from hashlib import sha1

def hashit(data):
    return sha1(data.lower().encode('utf-8')).hexdigest()
    
