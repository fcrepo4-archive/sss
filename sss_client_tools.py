import hashlib

def md5_checksum(path):
    f = open(path, "r")
    m = hashlib.md5()
    m.update(f.read())
    digest = m.hexdigest()
    return digest

# generate the digest of a file
print md5_checksum("/home/richard/Development/swordserver/sss/example.zip")