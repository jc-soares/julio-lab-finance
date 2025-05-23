from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.fernet import Fernet
import os

# Generate EC key pair for ECDH
private_key = ec.generate_private_key(ec.SECP256R1())
public_key = private_key.public_key()

# Save private and public keys to files
with open('private_key.pem', 'wb') as f:
    f.write(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ))
with open('public_key.pem', 'wb') as f:
    f.write(public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ))

# Derive shared secret using ECDH
shared_key = private_key.exchange(ec.ECDH(), public_key)
derived_key = HKDF(
    algorithm=hashes.SHA256(),
    length=32,
    salt=None,
    info=b'handshake data'
).derive(shared_key)

# Generate a symmetric key for encrypting credentials
symmetric_key = Fernet.generate_key()
fernet = Fernet(symmetric_key)

# Encrypt the symmetric key with derived AES key
aesgcm = AESGCM(derived_key)
nonce = os.urandom(12)
enc_symmetric_key = aesgcm.encrypt(nonce, symmetric_key, None)
with open('enc_symmetric_key.enc', 'wb') as f:
    f.write(nonce + enc_symmetric_key)

# Prompt for credentials
username = input('Enter your username: ')
password = input('Enter your password: ')

# Encrypt credentials
enc_username = fernet.encrypt(username.encode())
enc_password = fernet.encrypt(password.encode())

# Save encrypted credentials
with open('enc_credentials.enc', 'wb') as f:
    f.write(enc_username + b'\n' + enc_password)

print('Keys and encrypted credentials saved locally.')