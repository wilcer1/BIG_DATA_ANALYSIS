import pymongo

# Connect to the MongoDB Atlas cluster

client = pymongo.MongoClient("mongodb+srv://Luehunk:Luehunk@mongo.snueq.mongodb.net/?retryWrites=true&w=majority")
db = client["BigDataAnalysis"]

# Define the users collection
users = db["users"]

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
        return True
    else:
        print("Login failed.")
        return False

# Call the login function to start the login process
if __name__ == "__main__":
    if login():
        print("Welcome to the secret area!")
    else:
        print("Please try again.")

