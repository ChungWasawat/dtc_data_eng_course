from hashlib import sha1

def hashit(data):
    return sha1(data.lower().encode('utf-8')).hexdigest()
    
print(hashit('bun.wasawat@outlook.com'))