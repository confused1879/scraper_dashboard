from cryptography.fernet import Fernet
import base64
import os

def generate_key():
    """Generate a compatible Fernet key"""
    return Fernet.generate_key()

def encrypt_database(input_db_path="linkedin_data.db", output_db_path="linkedin_data.encrypted.db"):
    """Encrypt the database file"""
    # Generate key
    key = generate_key()
    fernet = Fernet(key)
    
    # Read and encrypt database
    with open(input_db_path, 'rb') as file:
        file_data = file.read()
    encrypted_data = fernet.encrypt(file_data)
    
    # Save encrypted database
    with open(output_db_path, 'wb') as file:
        file.write(encrypted_data)
    
    # Save key to secrets.toml
    os.makedirs('.streamlit', exist_ok=True)
    with open('.streamlit/secrets.toml', 'w') as f:
        f.write(f'db_key = "{key.decode()}"')
        
    print(f"Encryption key saved to .streamlit/secrets.toml")
    print(f"Key value: {key.decode()}")
    return key

if __name__ == "__main__":
    encrypt_database() 