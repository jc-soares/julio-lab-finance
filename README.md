# Credenciais:
- se nao tiver private key correta, faz essa ordem:
    - rode o script generate_public_private_key.py na pasta Credentials
        - o programa gerará private, public, symmetric e credentials
    - depois, salve na mesma pasta a credencial do google (key) com o nome private_google_credentials.json
    - então, rode o script encrypt_google_credentials.py na mesma pasta
    - pronto, agora pode até deletar a private_google_credentials.json
- não precisa de ter private_google_credentials.json se já tiver tudo exceto ela (incluindo a private key)