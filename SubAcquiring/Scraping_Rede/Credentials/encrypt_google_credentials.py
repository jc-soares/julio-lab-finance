from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.fernet import Fernet
import json


# Load EC private key
with open('private_key.pem', 'rb') as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)

# Load peer public key
with open('public_key.pem', 'rb') as f:
    public_key = serialization.load_pem_public_key(f.read())

# Derive shared secret and AES key
shared_key = private_key.exchange(ec.ECDH(), public_key)
derived_key = HKDF(
    algorithm=hashes.SHA256(),
    length=32,
    salt=None,
    info=b'handshake data'
).derive(shared_key)

# Load and decrypt the symmetric key
with open('enc_symmetric_key.enc', 'rb') as f:
    data = f.read()
nonce, enc_symmetric_key = data[:12], data[12:]
aesgcm = AESGCM(derived_key)
symmetric_key = aesgcm.decrypt(nonce, enc_symmetric_key, None)

fernet = Fernet(symmetric_key)

credentials = json.load(open('private_google_credentials.json'))

# Encrypt credentials
enc_credentials = fernet.encrypt(json.dumps(credentials).encode())

# Save encrypted credentials
with open('enc_google_credentials.enc', 'wb') as f:
    f.write(enc_credentials)

print('Credentials and encrypted credentials saved locally.')