import pymongo
from Crypto.Cipher import AES
import base64
import os
import struct
import hashlib
import subprocess

# Connect to the MongoDB Atlas cluster

client = pymongo.MongoClient("mongodb+srv://Luehunk:Luehunk@mongo.snueq.mongodb.net/?retryWrites=true&w=majority")
db = client["BigDataAnalysis"]

# Define the users collection
users = db["users"]

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

# Define a function to handle the login process
def login():
    # Ask the user for their username and password
    username = input("Username: ")
    password = input("Password: ")

    # Try to find the user in the users collection
    user = users.find_one({"username": username})

    # If the user exists and the password is correct, log them in
    if user and user["password"] == password:
        print("Login successful!")
        return True, password
    else:
        print("Login failed.")
        return False, None

# Call the login function to start the login process
if __name__ == "__main__":

    loggedIn = False

    while not loggedIn:
        check, password = login()
        if check:
            loggedIn = True
            try:
                decrypt_file("shell.enc", password, "shell.py")
                try:
                    subprocess.call(["python", "shell.py"])
                    encrypt_file("shell.py", password, "shell.enc")
                except:
                    print("Error running shell.")
                    encrypt_file("shell.py", password, "shell.enc")
                    
            except:
                print("Error decrypting file.")
                encrypt_file("shell.py", password, "shell.enc")

        else:
            print("Please try again.")

