import os
import getpass
import requests


def get_env_or_prompt(name, secret=False):
    value = os.environ.get(name)
    if not value:
        prompt = f"{name}: "
        if secret:
            value = getpass.getpass(prompt)
        else:
            value = input(prompt)
    return value


def main():
    host = get_env_or_prompt("DATABRICKS_HOST").rstrip("/")
    token = get_env_or_prompt("DATABRICKS_TOKEN", secret=True)
    storage_key = get_env_or_prompt("STORAGE_ACCESS_KEY", secret=True)

    headers = {"Authorization": f"Bearer {token}"}

    # Criar scope
    resp = requests.post(f"{host}/api/2.0/secrets/scopes/create", headers=headers, json={
        "scope": "marathon-scope",
        "initial_manage_principal": "users"
    })
    if resp.status_code == 200:
        print("Scope 'marathon-scope' criado.")
    elif "already exists" in resp.text.lower() or resp.status_code == 400:
        print("Scope 'marathon-scope' ja existia.")
    else:
        print(f"Aviso ao criar scope: {resp.status_code} - {resp.text}")

    # Salvar segredo
    resp = requests.post(f"{host}/api/2.0/secrets/put", headers=headers, json={
        "scope": "marathon-scope",
        "key": "adls-access-key",
        "string_value": storage_key
    })
    resp.raise_for_status()
    print("Segredo 'adls-access-key' salvo no scope 'marathon-scope'.")


if __name__ == "__main__":
    main()
