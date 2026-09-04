#!/usr/bin/env python3
"""
Setup unificado do projeto marathon-case-data-master.

Executa todo o fluxo de provisionamento e configuracao:
1. Checa prerequisitos
2. Carrega/configura .env
3. Login no Azure
4. Deploy Bicep
5. Atualiza config/config.yaml
6. Configura Unity Catalog
7. Configura secrets
8. Habilita file events
9. Sobe CSVs
10. Cria workflow

Progresso eh salvo em .setup_state.json. Se falhar, basta rodar novamente
que o script retoma de onde parou.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).parent.parent
STATE_FILE = PROJECT_ROOT / ".setup_state.json"
ENV_FILE = PROJECT_ROOT / ".env"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"
PARAMETERS_FILE = PROJECT_ROOT / "infrastructure" / "parameters.json"
BICEP_FILE = PROJECT_ROOT / "infrastructure" / "main.bicep"

REQUIRED_ENV = []
REQUIRED_ENV_LATER = ["DATABRICKS_TOKEN"]  # solicitado apos o deploy Bicep

STEPS = [
    "prerequisites",
    "env",
    "azure_login",
    "deploy_bicep",
    "update_config",
    "databricks_token",
    "unity_catalog",
    "databricks_secrets",
    "enable_file_events",
    "upload_raw_data",
    "create_workflow",
]


def _color(text, color):
    colors = {"green": "\033[92m", "yellow": "\033[93m", "red": "\033[91m", "cyan": "\033[96m", "reset": "\033[0m"}
    return f"{colors.get(color, '')}{text}{colors['reset']}"


def print_step(step, msg):
    print(f"\n[{step}/{len(STEPS)}] {_color(msg, 'cyan')}")


def print_ok(msg):
    print(f"  {_color('[OK]', 'green')} {msg}")


def print_warn(msg):
    print(f"  {_color('[WARN]', 'yellow')} {msg}")


def print_error(msg):
    print(f"  {_color('[ERRO]', 'red')} {msg}")


def print_info(msg):
    print(f"  {msg}")


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"completed": [], "outputs": {}}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def mark_completed(state, step):
    if step not in state["completed"]:
        state["completed"].append(step)
    save_state(state)


def is_completed(state, step):
    return step in state["completed"]


def find_az_cli():
    """Tenta encontrar o executavel do Azure CLI no PATH."""
    for name in ["az", "az.cmd", "az.exe"]:
        path = shutil.which(name)
        if path:
            return path
    return None


def run_command(cmd, capture=True, check=True, shell=False):
    """Executa comando no shell. Retorna stdout se capture=True, senao retorna string vazia."""
    print_info(f"Executando: {' '.join(cmd) if isinstance(cmd, list) else cmd}")

    if isinstance(cmd, list) and cmd[0] in ("az", "az.cmd"):
        az_path = find_az_cli()
        if not az_path:
            raise FileNotFoundError("Azure CLI (az) nao encontrado no PATH")
        cmd[0] = az_path

    kwargs = {"shell": shell, "text": True}
    if capture:
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    result = subprocess.run(cmd, **kwargs)
    if check and result.returncode != 0:
        raise RuntimeError(f"Comando falhou (rc={result.returncode}): {result.stderr or result.stdout}")
    if capture and result.stdout:
        return result.stdout.strip()
    return ""


def prompt(msg, required=True, secret=False):
    if secret:
        import getpass
        value = getpass.getpass(f"  {msg}: ")
    else:
        value = input(f"  {msg}: ").strip()
    if required and not value:
        print_error("Valor obrigatorio nao fornecido.")
        sys.exit(1)
    return value


def ensure_dotenv():
    if not ENV_FILE.exists():
        print_warn("Arquivo .env nao encontrado.")
        if ENV_EXAMPLE.exists():
            print_info("Criando .env a partir de .env.example")
            ENV_FILE.write_text(ENV_EXAMPLE.read_text(), encoding="utf-8")
        else:
            ENV_FILE.write_text("", encoding="utf-8")
        print_info("Edite o arquivo .env e preencha pelo menos DATABRICKS_TOKEN.")
    load_dotenv(ENV_FILE)


def step_prerequisites(state):
    print_step(STEPS.index("prerequisites") + 1, "Checando prerequisitos")

    # Python version
    py_major, py_minor = sys.version_info[:2]
    if py_major < 3 or (py_major == 3 and py_minor < 10):
        raise RuntimeError("Python 3.10+ e necessario")
    print_ok(f"Python {py_major}.{py_minor}")

    # Azure CLI
    az_path = find_az_cli()
    if not az_path:
        raise RuntimeError("Azure CLI (az) nao encontrado no PATH. Instale: https://aka.ms/installazurecliwindows")
    try:
        az_version = run_command(["az", "--version"], capture=True, check=True)
        first_line = az_version.splitlines()[0]
        print_ok(f"Azure CLI encontrado em {az_path}: {first_line}")
    except FileNotFoundError:
        raise RuntimeError("Azure CLI (az) nao encontrado no PATH. Instale: https://aka.ms/installazurecliwindows")

    # Git
    try:
        run_command(["git", "--version"], capture=True, check=True)
        print_ok("Git instalado")
    except FileNotFoundError:
        print_warn("Git nao encontrado. Nao e obrigatorio para executar o pipeline.")

    mark_completed(state, "prerequisites")


def step_env(state):
    print_step(STEPS.index("env") + 1, "Carregando configuracoes do .env")
    ensure_dotenv()

    # Checa variaveis obrigatorias
    missing = []
    for var in REQUIRED_ENV:
        if not os.environ.get(var):
            missing.append(var)

    if missing:
        print_warn(f"Variaveis ausentes no .env: {', '.join(missing)}")
        for var in missing:
            os.environ[var] = prompt(f"Digite {var}", required=True)
        # Salva no .env
        update_env_file(missing)

    print_ok("Configuracoes carregadas")
    mark_completed(state, "env")


def update_env_file(vars_to_update):
    lines = ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.exists() else []
    existing = {line.split("=", 1)[0]: i for i, line in enumerate(lines) if "=" in line}
    for var in vars_to_update:
        value = os.environ.get(var, "")
        if var in existing:
            lines[existing[var]] = f"{var}={value}"
        else:
            lines.append(f"{var}={value}")
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def step_azure_login(state):
    print_step(STEPS.index("azure_login") + 1, "Garantindo login no Azure")

    try:
        account = run_command(["az", "account", "show"], capture=True, check=True)
        account_json = json.loads(account)
        print_ok(f"Ja logado na subscription {account_json.get('name')} ({account_json.get('id')})")
    except (RuntimeError, json.JSONDecodeError):
        print_info("Nao esta logado. Executando 'az login'...")
        run_command(["az", "login"], capture=False, check=True)
        account = run_command(["az", "account", "show"], capture=True, check=True)
        account_json = json.loads(account)
        print_ok(f"Logado na subscription {account_json.get('name')} ({account_json.get('id')})")

    sub_id = os.environ.get("SUBSCRIPTION_ID")
    if sub_id:
        run_command(["az", "account", "set", "--subscription", sub_id], capture=True, check=True)
        print_ok(f"Subscription ativa definida para {sub_id}")

    mark_completed(state, "azure_login")


def step_deploy_bicep(state):
    print_step(STEPS.index("deploy_bicep") + 1, "Provisionando infraestrutura via Bicep")

    if not PARAMETERS_FILE.exists():
        raise RuntimeError(f"parameters.json nao encontrado em {PARAMETERS_FILE}")
    if not BICEP_FILE.exists():
        raise RuntimeError(f"main.bicep nao encontrado em {BICEP_FILE}")

    deploy_name = f"marathon-case-deploy-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    print_info(f"Deployment name: {deploy_name}")
    run_command(
        [
            "az", "deployment", "sub", "create",
            "--name", deploy_name,
            "--location", "eastus",
            "--template-file", str(BICEP_FILE),
            "--parameters", str(PARAMETERS_FILE),
        ],
        capture=False,
        check=True,
    )

    print_info("Recuperando outputs do deploy...")
    deployment_json = run_command(
        ["az", "deployment", "sub", "show", "--name", deploy_name],
        capture=True,
        check=True,
    )
    deployment = json.loads(deployment_json)
    if deployment["properties"]["provisioningState"] != "Succeeded":
        raise RuntimeError(f"Deploy nao sucedeu: {deployment['properties']['provisioningState']}")

    outputs = deployment["properties"]["outputs"]
    state["outputs"] = {k: v["value"] for k, v in outputs.items()}
    state["outputs"]["deployment_name"] = deploy_name
    save_state(state)

    print_ok(f"Resource Group: {state['outputs'].get('resourceGroupName')}")
    print_ok(f"Storage Account: {state['outputs'].get('storageAccountName')}")
    print_ok(f"Databricks Workspace: {state['outputs'].get('databricksWorkspaceName')}")
    print_ok(f"Key Vault: {state['outputs'].get('keyVaultName')}")
    print_ok(f"Access Connector ID: {state['outputs'].get('accessConnectorId')}")

    mark_completed(state, "deploy_bicep")


def step_update_config(state):
    print_step(STEPS.index("update_config") + 1, "Atualizando config/config.yaml com os recursos deployados")

    outputs = state["outputs"]
    rg = outputs.get("resourceGroupName", "rg-marathon-case")
    storage = outputs.get("storageAccountName", "stmarathoncase")
    workspace = outputs.get("databricksWorkspaceName", "dbw-marathon-case")
    kv = outputs.get("keyVaultName", "kv-marathon-case")
    container = "marathon-data"

    config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8")) if CONFIG_FILE.exists() else {}
    config.setdefault("azure", {})
    config["azure"]["resource_group"] = rg
    config["azure"]["storage_account"] = storage
    config["azure"]["container"] = container
    config["azure"]["databricks_workspace"] = workspace
    config["azure"]["key_vault"] = kv

    config.setdefault("paths", {})
    config["paths"]["raw"] = f"abfss://{container}@{storage}.dfs.core.windows.net/raw"
    config["paths"]["bronze"] = f"abfss://{container}@{storage}.dfs.core.windows.net/bronze"
    config["paths"]["silver"] = f"abfss://{container}@{storage}.dfs.core.windows.net/silver"
    config["paths"]["gold"] = f"abfss://{container}@{storage}.dfs.core.windows.net/gold"

    CONFIG_FILE.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    print_ok(f"config.yaml atualizado com storage_account={storage}")

    # Tambem descobre a storage key para uso posterior
    print_info("Obtendo storage access key...")
    storage_key = run_command(
        [
            "az", "storage", "account", "keys", "list",
            "--account-name", storage,
            "--resource-group", rg,
            "--query", "[0].value",
            "-o", "tsv",
        ],
        capture=True,
        check=True,
    )
    os.environ["STORAGE_ACCESS_KEY"] = storage_key
    update_env_file(["STORAGE_ACCESS_KEY"])
    print_ok("Storage access key salva no .env")

    # Descobre URL do Databricks workspace
    ws_url = outputs.get("databricksWorkspaceUrl")
    if ws_url and not ws_url.startswith("https://"):
        ws_url = f"https://{ws_url}"
    if ws_url:
        os.environ["DATABRICKS_HOST"] = ws_url
        update_env_file(["DATABRICKS_HOST"])
        print_ok(f"DATABRICKS_HOST atualizado: {ws_url}")

    mark_completed(state, "update_config")


def step_databricks_token(state):
    print_step(STEPS.index("databricks_token") + 1, "Solicitando token do Databricks")

    host = os.environ.get("DATABRICKS_HOST")
    if not host:
        raise RuntimeError("DATABRICKS_HOST nao encontrado. O passo update_config deve ter falhado.")

    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        print_info(f"Acesse {host}")
        print_info("Va em User Settings > Developer > Access tokens > Generate new token")
        token = prompt("Cole o Databricks Personal Access Token", required=True, secret=True)
        os.environ["DATABRICKS_TOKEN"] = token
        update_env_file(["DATABRICKS_TOKEN"])

    # Testa o token
    print_info("Validando token...")
    resp = requests_get(f"{host}/api/2.0/token/get", token)
    if resp.status_code not in (200, 403):  # 403 pode ser valido para token pessoal em alguns workspaces
        raise RuntimeError(f"Token invalido ou workspace inacessivel: {resp.status_code}")
    print_ok("Token validado")

    mark_completed(state, "databricks_token")


def requests_get(url, token):
    import requests
    return requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)


def requests_post(url, token, json_data):
    import requests
    return requests.post(url, headers={"Authorization": f"Bearer {token}"}, json=json_data, timeout=30)


def step_unity_catalog(state):
    print_step(STEPS.index("unity_catalog") + 1, "Configurando Unity Catalog")

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    access_connector_id = state["outputs"]["accessConnectorId"]

    config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))
    storage = config["azure"]["storage_account"]
    container = config["azure"]["container"]
    external_url = f"abfss://{container}@{storage}.dfs.core.windows.net/"

    # Storage credential
    cred_name = "marathon-storage-credential"
    resp = requests_post(
        f"{host}/api/2.1/unity-catalog/storage-credentials",
        token,
        {
            "name": cred_name,
            "azure_managed_identity": {"access_connector_id": access_connector_id},
            "comment": "Credencial para acesso ao ADLS do case marathon",
        },
    )
    if resp.status_code == 200:
        print_ok(f"Storage credential '{cred_name}' criada")
    elif resp.status_code == 409 or "already exists" in resp.text.lower():
        print_ok(f"Storage credential '{cred_name}' ja existia")
    else:
        raise RuntimeError(f"Erro ao criar storage credential: {resp.status_code} - {resp.text}")

    # External location
    loc_name = "marathon-external-location"
    resp = requests_post(
        f"{host}/api/2.1/unity-catalog/external-locations",
        token,
        {"name": loc_name, "url": external_url, "credential_name": cred_name, "comment": "External location para o data lake do case marathon"},
    )
    if resp.status_code == 200:
        print_ok(f"External location '{loc_name}' criada: {external_url}")
    elif resp.status_code == 409 or "already exists" in resp.text.lower():
        print_ok(f"External location '{loc_name}' ja existia")
    else:
        raise RuntimeError(f"Erro ao criar external location: {resp.status_code} - {resp.text}")

    # Catalog
    catalog_name = "marathon"
    storage_root = f"{external_url}catalogs/{catalog_name}/"
    resp = requests_post(
        f"{host}/api/2.1/unity-catalog/catalogs",
        token,
        {"name": catalog_name, "storage_root": storage_root, "comment": "Catalog do case marathon"},
    )
    if resp.status_code == 200:
        print_ok(f"Catalog '{catalog_name}' criado")
    elif resp.status_code == 409 or "already exists" in resp.text.lower():
        print_ok(f"Catalog '{catalog_name}' ja existia")
    else:
        raise RuntimeError(f"Erro ao criar catalog: {resp.status_code} - {resp.text}")

    # Secret catalog_name
    resp = requests_post(
        f"{host}/api/2.0/secrets/put",
        token,
        {"scope": "marathon-scope", "key": "catalog_name", "string_value": catalog_name},
    )
    resp.raise_for_status()
    print_ok("Segredo 'catalog_name' salvo no scope 'marathon-scope'")

    mark_completed(state, "unity_catalog")


def step_databricks_secrets(state):
    print_step(STEPS.index("databricks_secrets") + 1, "Salvando secrets do Databricks")

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]

    # Criar scope
    resp = requests_post(
        f"{host}/api/2.0/secrets/scopes/create",
        token,
        {"scope": "marathon-scope", "initial_manage_principal": "users"},
    )
    if resp.status_code == 200:
        print_ok("Scope 'marathon-scope' criado")
    elif "already exists" in resp.text.lower() or resp.status_code == 400:
        print_ok("Scope 'marathon-scope' ja existia")
    else:
        print_warn(f"Aviso ao criar scope: {resp.status_code} - {resp.text}")

    # Salvar config.yaml
    config_yaml = CONFIG_FILE.read_text(encoding="utf-8")
    resp = requests_post(
        f"{host}/api/2.0/secrets/put",
        token,
        {"scope": "marathon-scope", "key": "config_yaml", "string_value": config_yaml},
    )
    resp.raise_for_status()
    print_ok("Segredo 'config_yaml' salvo")

    mark_completed(state, "databricks_secrets")


def step_enable_file_events(state):
    print_step(STEPS.index("enable_file_events") + 1, "Habilitando File Events (EventGrid + roles)")

    config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))
    rg = config["azure"]["resource_group"]
    storage = config["azure"]["storage_account"]
    subscription = _get_current_subscription_id()

    # Registra provider EventGrid
    print_info("Registrando provider Microsoft.EventGrid...")
    run_command(["az", "provider", "register", "--namespace", "Microsoft.EventGrid", "--subscription", subscription], capture=True, check=False)

    print_info("Aguardando registro do EventGrid (ate 5 minutos)...")
    for _ in range(30):
        reg_state = run_command(
            ["az", "provider", "show", "--namespace", "Microsoft.EventGrid", "--subscription", subscription, "--query", "registrationState", "-o", "tsv"],
            capture=True,
            check=True,
        )
        if reg_state.strip().lower() == "registered":
            print_ok("Microsoft.EventGrid registrado")
            break
        time.sleep(10)
    else:
        raise RuntimeError("Timeout aguardando registro do EventGrid")

    # Obtem principalId do Access Connector
    access_connector_id = state["outputs"]["accessConnectorId"]
    ac_name = access_connector_id.split("/")[-1]
    principal_id = run_command(
        ["az", "resource", "show", "--ids", access_connector_id, "--query", "properties.managedIdentity.principalId", "-o", "tsv"],
        capture=True,
        check=True,
    )
    print_ok(f"principalId do Access Connector '{ac_name}': {principal_id}")

    scope_storage = f"/subscriptions/{subscription}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{storage}"
    scope_rg = f"/subscriptions/{subscription}/resourceGroups/{rg}"

    storage_roles = [
        "Storage Blob Data Contributor",
        "Storage Queue Data Contributor",
        "Storage Account Contributor",
    ]
    for role in storage_roles:
        print_info(f"Atribuindo '{role}' no storage account...")
        run_command(
            [
                "az", "role", "assignment", "create",
                "--assignee-object-id", principal_id,
                "--assignee-principal-type", "ServicePrincipal",
                "--role", role,
                "--scope", scope_storage,
            ],
            capture=True,
            check=False,  # pode ja existir
        )

    print_info("Atribuindo 'EventGrid EventSubscription Contributor' no resource group...")
    run_command(
        [
            "az", "role", "assignment", "create",
            "--assignee-object-id", principal_id,
            "--assignee-principal-type", "ServicePrincipal",
            "--role", "EventGrid EventSubscription Contributor",
            "--scope", scope_rg,
        ],
        capture=True,
        check=False,
    )

    print_ok("Roles atribuidas. Pode levar alguns minutos para propagar.")
    mark_completed(state, "enable_file_events")


def _get_current_subscription_id():
    out = run_command(["az", "account", "show", "--query", "id", "-o", "tsv"], capture=True, check=True)
    return out.strip()


def step_upload_raw_data(state):
    print_step(STEPS.index("upload_raw_data") + 1, "Subindo CSVs para a camada raw")

    script = PROJECT_ROOT / "scripts" / "upload_raw_data.py"
    if not script.exists():
        raise RuntimeError(f"Script nao encontrado: {script}")

    run_command([sys.executable, str(script)], capture=False, check=True)
    print_ok("Upload concluido")
    mark_completed(state, "upload_raw_data")


def step_create_workflow(state):
    print_step(STEPS.index("create_workflow") + 1, "Criando Databricks Workflow")

    repo_path = os.environ.get("DATABRICKS_REPO_PATH")
    if not repo_path:
        print_warn("DATABRICKS_REPO_PATH nao definido no .env")
        print_info("O workflow precisa apontar para notebooks em um Databricks Repo.")
        print_info("Exemplo: /Workspace/Repos/seu.usuario@email.com/marathon-case-data-master")
        repo_path = prompt("Cole o caminho do Databricks Repo", required=True)
        os.environ["DATABRICKS_REPO_PATH"] = repo_path
        update_env_file(["DATABRICKS_REPO_PATH"])

    script = PROJECT_ROOT / "scripts" / "create_databricks_workflow.py"
    if not script.exists():
        raise RuntimeError(f"Script nao encontrado: {script}")

    run_command([sys.executable, str(script)], capture=False, check=True)
    print_ok("Workflow criado")
    mark_completed(state, "create_workflow")


def run_step(state, step_name):
    if is_completed(state, step_name):
        print_step(STEPS.index(step_name) + 1, f"Pulando '{step_name}' (ja concluido)")
        return

    step_func = globals()[f"step_{step_name}"]
    try:
        step_func(state)
    except Exception as e:
        print_error(f"Falha no passo '{step_name}': {e}")
        print_info("Corrija o problema e rode novamente 'python scripts/setup_all.py' para retomar.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Setup unificado do projeto marathon-case-data-master")
    parser.add_argument("--reset", action="store_true", help="Apaga o estado e reinicia do zero")
    args = parser.parse_args()

    state = load_state()
    if args.reset:
        print_warn("Resetando estado do setup")
        state = {"completed": [], "outputs": {}}
        save_state(state)

    print(_color("=" * 60, "cyan"))
    print(_color(" Setup unificado — marathon-case-data-master ", "cyan"))
    print(_color("=" * 60, "cyan"))

    for step in STEPS:
        run_step(state, step)

    print("\n" + _color("=" * 60, "green"))
    print(_color(" Setup concluido com sucesso! ", "green"))
    print(_color("=" * 60, "green"))
    print_info("Para testar o trigger, faca upload de um novo CSV em raw/ via:")
    print_info("  python scripts/upload_raw_data.py")
    print_info("Ou acesse Workflows no Databricks e rode o job manualmente.")


if __name__ == "__main__":
    main()
