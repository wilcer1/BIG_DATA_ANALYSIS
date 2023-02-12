from Crypto.Cipher import AES
import base64
import os
import struct
import hashlib


def encrypt_file(file_name, password, output_file=None):
    chunksize = 64*1024
    output_file = output_file or (file_name + '.enc')
    iv = os.urandom(16)
    kdf = hashlib.pbkdf2_hmac('sha256', password.encode(), b'salt', 100000)
    key = kdf[:32]
    encryptor = AES.new(key, AES.MODE_CBC, iv)
    filesize = os.path.getsize(file_name)

    with open(file_name, 'rb') as infile:
        with open(output_file, 'wb') as outfile:
            outfile.write(struct.pack('<Q', filesize))
            outfile.write(iv)

            while True:
                chunk = infile.read(chunksize)
                if len(chunk) == 0:
                    break
                elif len(chunk) % 16 != 0:
                    chunk += b' ' * (16 - len(chunk) % 16)

                outfile.write(encryptor.encrypt(chunk))

    os.remove(file_name)


def decrypt_file(file_name, password, output_file=None):
    chunksize = 64*1024
    output_file = output_file or (file_name[:-4])
    with open(file_name, 'rb') as infile:
        filesize = struct.unpack('<Q', infile.read(struct.calcsize('Q')))[0]
        iv = infile.read(16)
        kdf = hashlib.pbkdf2_hmac('sha256', password.encode(), b'salt', 100000)
        key = kdf[:32]
        decryptor = AES.new(key, AES.MODE_CBC, iv)

        with open(output_file, 'wb') as outfile:
            while True:
                chunk = infile.read(chunksize)
                if len(chunk) == 0:
                    break
                outfile.write(decryptor.decrypt(chunk))
            outfile.truncate(filesize)

    os.remove(file_name)


def main():
    encrypt_file("shell.py", "admin", "shell.enc")
    # decrypt_file("shell.enc", "admin", "shell.py")

if __name__ == "__main__":
    main()