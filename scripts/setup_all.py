#!/usr/bin/env python3
"""
Setup unificado do projeto marathon-case-data-master (versao Terraform).

Fluxo:
1. Checa prerequisitos (Python, Azure CLI, Terraform)
2. Login no Azure
3. Cria infraestrutura Azure via Terraform (resource group, storage, workspace, access connector, key vault)
4. Atualiza config.yaml e obtem storage key
5. Gera Azure AD token para a API do Databricks
6. Pede o Databricks Account ID (instrucoes na tela)
7. Cria/escolhe metastore e atribui ao workspace (scripts/setup_unity_catalog.py)
8. Salva secrets no Databricks
9. Habilita file events
10. Sobe CSVs
11. Implanta notebooks no workspace
12. Cria workflow
13. Cria SQL Warehouse e dashboard AI/BI

Progresso salvo em .setup_state.json (nao versionado).
"""

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).parent.parent
STATE_FILE = PROJECT_ROOT / ".setup_state.json"
ENV_FILE = PROJECT_ROOT / ".env"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"
TERRAFORM_DIR = PROJECT_ROOT / "infrastructure" / "terraform"
DATABRICKS_TERRAFORM_DIR = TERRAFORM_DIR / "databricks"

DATABRICKS_AAD_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"

STEPS = [
    "prerequisites",
    "env",
    "azure_login",
    "deploy_terraform",
    "update_config",
    "account_id",
    "unity_catalog",
    "databricks_secrets",
    "enable_file_events",
    "upload_raw_data",
    "deploy_notebooks",
    "create_workflow",
    "create_dashboard",
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


def find_executable(name):
    for ext in ["", ".cmd", ".exe", ".bat"]:
        path = shutil.which(name + ext)
        if path:
            return path
    return shutil.which(name)


def find_databricks_terraform():
    candidates = [find_executable("terraform")]
    if os.name == "nt":
        winget_root = Path.home() / "AppData" / "Local" / "Microsoft" / "WinGet" / "Packages"
        candidates.extend(winget_root.glob("Hashicorp.Terraform_*/*terraform.exe"))

    for candidate in candidates:
        if not candidate:
            continue
        result = subprocess.run(
            [str(candidate), "version", "-json"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        platform = json.loads(result.stdout).get("platform", "")
        if platform != "windows_386":
            return str(candidate)

    raise RuntimeError("O provider Databricks requer Terraform 64-bit. Instale a versao Windows AMD64.")


def run_command(cmd, capture=True, check=True, shell=False, cwd=None):
    print_info(f"Executando: {' '.join(cmd) if isinstance(cmd, list) else cmd}")

    if isinstance(cmd, list) and cmd[0] in ("az", "az.cmd", "az.exe", "terraform"):
        exe = find_executable(cmd[0])
        if not exe:
            raise FileNotFoundError(f"Comando nao encontrado no PATH: {cmd[0]}")
        cmd[0] = exe

    kwargs = {"shell": shell, "text": True}
    if cwd:
        kwargs["cwd"] = cwd
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
        print_info("Edite o arquivo .env se quiser customizar ALERT_EMAIL ou DATABRICKS_WORKSPACE_ROOT.")
    load_dotenv(ENV_FILE)


def update_env_file(vars_to_update):
    lines = ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.exists() else []
    existing = {line.split("=", 1)[0]: i for i, line in enumerate(lines) if "=" in line and not line.startswith("#")}
    for var in vars_to_update:
        value = os.environ.get(var, "")
        if var in existing:
            lines[existing[var]] = f"{var}={value}"
        else:
            lines.append(f"{var}={value}")
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def step_prerequisites(state):
    print_step(STEPS.index("prerequisites") + 1, "Checando prerequisitos")

    py_major, py_minor = sys.version_info[:2]
    if py_major < 3 or (py_major == 3 and py_minor < 10):
        raise RuntimeError("Python 3.10+ e necessario")
    print_ok(f"Python {py_major}.{py_minor}")

    az_path = find_executable("az")
    if not az_path:
        raise RuntimeError("Azure CLI nao encontrado. Instale: https://aka.ms/installazurecliwindows")
    az_version = run_command(["az", "--version"], capture=True, check=True)
    print_ok(f"Azure CLI: {az_version.splitlines()[0]}")

    tf_path = find_executable("terraform")
    if not tf_path:
        raise RuntimeError("Terraform nao encontrado. Instale: https://developer.hashicorp.com/terraform/install")
    tf_version = run_command(["terraform", "-version"], capture=True, check=True)
    print_ok(f"Terraform: {tf_version.splitlines()[0]}")

    try:
        run_command(["git", "--version"], capture=True, check=True)
        print_ok("Git instalado")
    except FileNotFoundError:
        print_warn("Git nao encontrado (nao obrigatorio)")

    mark_completed(state, "prerequisites")


def step_env(state):
    print_step(STEPS.index("env") + 1, "Carregando configuracoes do .env")
    ensure_dotenv()
    print_ok("Configuracoes carregadas")
    mark_completed(state, "env")


def step_azure_login(state):
    print_step(STEPS.index("azure_login") + 1, "Garantindo login no Azure")

    try:
        account = run_command(["az", "account", "show"], capture=True, check=True)
        account_json = json.loads(account)
        print_ok(f"Ja logado: {account_json.get('name')} ({account_json.get('id')})")
    except (RuntimeError, json.JSONDecodeError):
        print_info("Nao logado. Executando az login...")
        run_command(["az", "login"], capture=False, check=True)
        account = run_command(["az", "account", "show"], capture=True, check=True)
        account_json = json.loads(account)
        print_ok(f"Logado: {account_json.get('name')} ({account_json.get('id')})")

    sub_id = os.environ.get("SUBSCRIPTION_ID")
    if sub_id:
        run_command(["az", "account", "set", "--subscription", sub_id], capture=True, check=True)
        print_ok(f"Subscription ativa: {sub_id}")

    mark_completed(state, "azure_login")


def step_deploy_terraform(state):
    print_step(STEPS.index("deploy_terraform") + 1, "Provisionando infraestrutura Azure via Terraform")

    if not TERRAFORM_DIR.exists():
        raise RuntimeError(f"Diretorio Terraform nao encontrado: {TERRAFORM_DIR}")

    print_info("terraform init")
    run_command(["terraform", "init"], capture=False, check=True, cwd=str(TERRAFORM_DIR))

    print_info("terraform apply (pode levar 5-10 minutos)")
    run_command(["terraform", "apply", "-auto-approve"], capture=False, check=True, cwd=str(TERRAFORM_DIR))

    print_info("Recuperando outputs...")
    outputs_json = run_command(["terraform", "output", "-json"], capture=True, check=True, cwd=str(TERRAFORM_DIR))
    outputs = json.loads(outputs_json)
    state["outputs"].update({k: v["value"] for k, v in outputs.items()})
    save_state(state)

    print_ok(f"Resource Group: {state['outputs'].get('resource_group_name')}")
    print_ok(f"Storage Account: {state['outputs'].get('storage_account_name')}")
    print_ok(f"Databricks Workspace: {state['outputs'].get('databricks_workspace_name')}")
    print_ok(f"Workspace URL: https://{state['outputs'].get('databricks_workspace_url')}")
    print_ok(f"Access Connector ID: {state['outputs'].get('access_connector_id')}")

    mark_completed(state, "deploy_terraform")


def step_update_config(state):
    print_step(STEPS.index("update_config") + 1, "Atualizando config/config.yaml")

    outputs = state["outputs"]
    rg = outputs.get("resource_group_name")
    storage = outputs.get("storage_account_name")
    container = outputs.get("container_name")
    workspace = outputs.get("databricks_workspace_name")
    kv = outputs.get("key_vault_name")

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

    host = f"https://{outputs.get('databricks_workspace_url')}"
    os.environ["DATABRICKS_HOST"] = host
    update_env_file(["DATABRICKS_HOST"])
    print_ok(f"DATABRICKS_HOST: {host}")

    os.environ["DATABRICKS_WORKSPACE_ID"] = str(outputs.get("workspace_id"))
    update_env_file(["DATABRICKS_WORKSPACE_ID"])
    print_ok(f"DATABRICKS_WORKSPACE_ID: {outputs.get('workspace_id')}")

    os.environ["ACCESS_CONNECTOR_ID"] = outputs.get("access_connector_id")
    update_env_file(["ACCESS_CONNECTOR_ID"])
    print_ok("ACCESS_CONNECTOR_ID salvo no .env")

    mark_completed(state, "update_config")


def get_aad_token_for_databricks():
    token_json = run_command(
        ["az", "account", "get-access-token", "--resource", DATABRICKS_AAD_RESOURCE],
        capture=True,
        check=True,
    )
    return json.loads(token_json)["accessToken"]


def step_account_id(state):
    print_step(STEPS.index("account_id") + 1, "Verificando Unity Catalog")

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    workspace_id = state["outputs"].get("workspace_id")

    # Tenta usar PAT do .env se existir
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token or not token.startswith("dapi"):
        print_info("")
        print_info("=" * 60)
        print_info("ACAO MANUAL NECESSARIA: gerar Databricks Personal Access Token")
        print_info("=" * 60)
        print_info("Passos:")
        print_info(f"  1. Abra o workspace: {host}")
        print_info("  2. Clique no icone do usuario (canto superior direito) > User Settings")
        print_info("  3. Va em Developer > Access tokens")
        print_info("  4. Clique em 'Generate new token'")
        print_info("     - Name: setup-marathon")
        print_info("     - Lifetime: sem expiracao (recomendado para a demo)")
        print_info("  5. Cole o token abaixo (comeca com 'dapi...')")
        print_info("=" * 60)
        try:
            webbrowser.open(host)
            print_info("Workspace aberto no navegador. Navegue ate User Settings > Developer > Access tokens.")
        except Exception:
            pass

        token = prompt("Cole o Databricks Personal Access Token")
        if not token.startswith("dapi"):
            raise RuntimeError("Token invalido. Deve comecar com 'dapi'.")
        os.environ["DATABRICKS_TOKEN"] = token
        update_env_file(["DATABRICKS_TOKEN"])
        print_ok("PAT salvo no .env")

    print_info("Verificando se Unity Catalog ja esta ativado...")
    print_info(f"Workspace ID usado: {workspace_id}")
    resp = requests.get(
        f"{host}/api/2.1/unity-catalog/catalogs",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    print_info(f"Resposta: {resp.status_code} - {resp.text[:200]}")
    if resp.status_code == 200:
        catalogs = resp.json().get("catalogs", [])
        catalog_names = [c.get("name") for c in catalogs]
        print_ok(f"Unity Catalog ativado. Catalogs encontrados: {catalog_names}")
        if "marathon" in catalog_names or catalog_names:
            print_ok("Unity Catalog ja ativado. Account ID nao necessario.")
        state["outputs"]["unity_catalog_ready"] = True
        save_state(state)
        mark_completed(state, "account_id")
        return

    if resp.status_code == 403:
        print_warn("Token invalido para o Unity Catalog. Gerando um novo PAT...")
        token = prompt("Cole o Databricks Personal Access Token")
        if not token.startswith("dapi"):
            raise RuntimeError("Token invalido. Deve comecar com 'dapi'.")
        os.environ["DATABRICKS_TOKEN"] = token
        update_env_file(["DATABRICKS_TOKEN"])
        print_ok("Novo PAT salvo")
        # Tenta novamente
        resp = requests.get(
            f"{host}/api/2.1/unity-catalog/catalogs",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if resp.status_code == 200:
            catalogs = resp.json().get("catalogs", [])
            catalog_names = [c.get("name") for c in catalogs]
            print_ok(f"Unity Catalog ativado. Catalogs: {catalog_names}")
            state["outputs"]["unity_catalog_ready"] = True
            save_state(state)
            mark_completed(state, "account_id")
            return

    print_warn("Unity Catalog nao ativado. Precisamos do Account ID.")
    account_id = os.environ.get("DATABRICKS_ACCOUNT_ID")
    if not account_id:
        print_info("")
        print_info("=" * 60)
        print_info("ACAO MANUAL NECESSARIA: obter o Databricks Account ID")
        print_info("=" * 60)
        print_info("Passos:")
        print_info(f"  1. Abra o workspace: {host}")
        print_info("  2. Clique no icone do usuario (canto superior direito)")
        print_info("  3. Selecione 'Manage Account' ou 'Account Console'")
        print_info("  4. A URL do navegador tera o formato:")
        print_info("     https://accounts.azuredatabricks.net/?account_id=XXXXXXXX")
        print_info("  5. Copie o numero XXXXXXXX")
        print_info("=" * 60)

        try:
            webbrowser.open(host)
            print_info("Workspace aberto no navegador.")
        except Exception:
            pass

        account_id = prompt("Cole o Databricks Account ID")
        os.environ["DATABRICKS_ACCOUNT_ID"] = account_id
        update_env_file(["DATABRICKS_ACCOUNT_ID"])

    state["outputs"]["databricks_account_id"] = account_id
    save_state(state)
    print_ok(f"Account ID: {account_id}")
    mark_completed(state, "account_id")


def _test_uc_token(host, token):
    resp = requests.get(
        f"{host}/api/2.1/unity-catalog/catalogs",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    return resp.status_code == 200


def step_unity_catalog(state):
    print_step(STEPS.index("unity_catalog") + 1, "Configurando Unity Catalog")

    # Sempre usa o workspace URL do estado atual (Terraform), nunca do .env se estiver desatualizado
    host = f"https://{state['outputs']['databricks_workspace_url']}"
    os.environ["DATABRICKS_HOST"] = host
    update_env_file(["DATABRICKS_HOST"])
    print_ok(f"Workspace URL atualizado: {host}")

    token = os.environ.get("DATABRICKS_TOKEN")

    if not token or not _test_uc_token(host, token):
        print_warn("Token do .env invalido ou sem acesso ao Unity Catalog.")
        print_info("")
        print_info("=" * 60)
        print_info("ACAO MANUAL NECESSARIA: gerar Databricks Personal Access Token")
        print_info("=" * 60)
        print_info("Passos:")
        print_info(f"  1. Abra o workspace: {host}")
        print_info("  2. Clique no icone do usuario (canto superior direito) > User Settings")
        print_info("  3. Va em Developer > Access tokens")
        print_info("  4. Clique em 'Generate new token'")
        print_info("     - Name: setup-marathon")
        print_info("     - Lifetime: sem expiracao (recomendado para a demo)")
        print_info("  5. Cole o token abaixo (comeca com 'dapi...')")
        print_info("=" * 60)
        try:
            webbrowser.open(host)
            print_info("Workspace aberto no navegador. Navegue ate User Settings > Developer > Access tokens.")
        except Exception:
            pass

        token = prompt("Cole o Databricks Personal Access Token")
        if not token.startswith("dapi"):
            raise RuntimeError("Token invalido. Deve comecar com 'dapi'.")
        os.environ["DATABRICKS_TOKEN"] = token
        update_env_file(["DATABRICKS_TOKEN"])
        print_ok("PAT salvo no .env")

    catalog_name = os.environ.get("CATALOG_NAME", "marathon")
    print_ok(f"Catalog name: {catalog_name}")

    script = PROJECT_ROOT / "scripts" / "setup_unity_catalog.py"
    if not script.exists():
        raise RuntimeError(f"Script nao encontrado: {script}")

    run_command([sys.executable, str(script)], capture=False, check=True)
    print_ok("Unity Catalog configurado")
    mark_completed(state, "unity_catalog")


def step_databricks_secrets(state):
    print_step(STEPS.index("databricks_secrets") + 1, "Salvando secrets no Databricks")

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]

    resp = requests.post(
        f"{host}/api/2.0/secrets/scopes/create",
        headers={"Authorization": f"Bearer {token}"},
        json={"scope": "marathon-scope", "initial_manage_principal": "users"},
        timeout=30,
    )
    if resp.status_code == 200:
        print_ok("Scope 'marathon-scope' criado")
    elif "already exists" in resp.text.lower() or resp.status_code == 400:
        print_ok("Scope 'marathon-scope' ja existia")
    else:
        print_warn(f"Aviso ao criar scope: {resp.status_code} - {resp.text}")

    config_yaml = CONFIG_FILE.read_text(encoding="utf-8")
    resp = requests.post(
        f"{host}/api/2.0/secrets/put",
        headers={"Authorization": f"Bearer {token}"},
        json={"scope": "marathon-scope", "key": "config_yaml", "string_value": config_yaml},
        timeout=30,
    )
    resp.raise_for_status()
    print_ok("Segredo 'config_yaml' salvo")

    resp = requests.post(
        f"{host}/api/2.0/secrets/put",
        headers={"Authorization": f"Bearer {token}"},
        json={"scope": "marathon-scope", "key": "catalog_name", "string_value": "marathon"},
        timeout=30,
    )
    resp.raise_for_status()
    print_ok("Segredo 'catalog_name' salvo (marathon)")

    mark_completed(state, "databricks_secrets")


def step_enable_file_events(state):
    print_step(STEPS.index("enable_file_events") + 1, "Habilitando File Events (EventGrid + roles)")

    outputs = state["outputs"]
    rg = outputs.get("resource_group_name")
    storage = outputs.get("storage_account_name")
    principal_id = outputs.get("access_connector_principal_id")
    subscription = run_command(["az", "account", "show", "--query", "id", "-o", "tsv"], capture=True, check=True)

    print_info("Registrando provider Microsoft.EventGrid...")
    run_command(["az", "provider", "register", "--namespace", "Microsoft.EventGrid", "--subscription", subscription], capture=True, check=False)

    print_info("Aguardando registro do EventGrid...")
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
            check=False,
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


def step_upload_raw_data(state):
    print_step(STEPS.index("upload_raw_data") + 1, "Subindo CSVs para a camada raw")

    outputs = state["outputs"]
    storage = outputs.get("storage_account_name")
    rg = outputs.get("resource_group_name")

    print_info("Obtendo storage access key atualizada...")
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
    print_ok("Storage access key atualizada no .env")

    script = PROJECT_ROOT / "scripts" / "upload_raw_data.py"
    if not script.exists():
        raise RuntimeError(f"Script nao encontrado: {script}")

    run_command([sys.executable, str(script)], capture=False, check=True)
    print_ok("Upload concluido")
    mark_completed(state, "upload_raw_data")


def _test_jobs_token(host, token):
    resp = requests.get(
        f"{host}/api/2.1/jobs/list?limit=1",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    return resp.status_code in (200, 400)  # 400 = bad request mas token valido; 403 = invalido


def _workspace_object_exists(host, token, object_path):
    """Verifica se um objeto existe no workspace (Workspace API v2)."""
    resp = requests.get(
        f"{host}/api/2.0/workspace/get-status",
        headers={"Authorization": f"Bearer {token}"},
        params={"path": object_path},
        timeout=30,
    )
    return resp.status_code == 200


def step_deploy_notebooks(state):
    print_step(STEPS.index("deploy_notebooks") + 1, "Implantando notebooks no Databricks Workspace")

    host = f"https://{state['outputs']['databricks_workspace_url']}"
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token or not _test_jobs_token(host, token):
        raise RuntimeError("DATABRICKS_TOKEN invalido ou sem acesso ao workspace.")

    api_root = (os.environ.get("DATABRICKS_WORKSPACE_ROOT") or "/Shared/marathon-case").strip().rstrip("/")
    if api_root.startswith("/Workspace"):
        api_root = api_root.removeprefix("/Workspace")
    if not api_root.startswith("/"):
        api_root = f"/{api_root}"
    notebooks_root = f"{api_root}/notebooks"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.post(
        f"{host}/api/2.0/workspace/mkdirs",
        headers=headers,
        json={"path": notebooks_root},
        timeout=30,
    )
    resp.raise_for_status()

    notebook_files = sorted((PROJECT_ROOT / "notebooks").glob("*.py"))
    required = {
        "00_bronze_orchestrator.py",
        "01_bronze_ingestion.py",
        "02_silver_etl.py",
        "03_gold_aggregations.py",
        "04_weather_enrichment.py",
    }
    missing = sorted(required - {path.name for path in notebook_files})
    if missing:
        raise RuntimeError(f"Notebooks obrigatorios ausentes: {', '.join(missing)}")

    for notebook_file in notebook_files:
        destination = f"{notebooks_root}/{notebook_file.stem}"
        content = base64.b64encode(notebook_file.read_bytes()).decode("ascii")
        resp = requests.post(
            f"{host}/api/2.0/workspace/import",
            headers=headers,
            json={
                "path": destination,
                "format": "SOURCE",
                "language": "PYTHON",
                "content": content,
                "overwrite": True,
            },
            timeout=60,
        )
        resp.raise_for_status()
        if not _workspace_object_exists(host, token, destination):
            raise RuntimeError(f"Notebook nao encontrado apos upload: {destination}")
        print_ok(f"Notebook implantado: {destination}")

    workflow_root = f"/Workspace{api_root}"
    previous_root = state["outputs"].get("databricks_workspace_root")
    os.environ["DATABRICKS_WORKSPACE_ROOT"] = workflow_root
    update_env_file(["DATABRICKS_WORKSPACE_ROOT"])
    state["outputs"]["databricks_workspace_root"] = workflow_root
    if previous_root != workflow_root and "create_workflow" in state["completed"]:
        state["completed"].remove("create_workflow")
        print_info("Workflow existente sera recriado para usar os notebooks implantados.")
    save_state(state)
    print_ok(f"Codigo implantado em: {workflow_root}")
    mark_completed(state, "deploy_notebooks")


def step_create_workflow(state):
    print_step(STEPS.index("create_workflow") + 1, "Criando Databricks Workflow")

    # Garante que o host e token do Databricks sao os atuais (evita valores desatualizados do .env)
    host = f"https://{state['outputs']['databricks_workspace_url']}"
    os.environ["DATABRICKS_HOST"] = host
    print_ok(f"Workspace URL: {host}")

    token = os.environ.get("DATABRICKS_TOKEN")
    if not token or not _test_jobs_token(host, token):
        print_warn("O token atual nao tem acesso a API de Jobs.")
        print_info("Gere um novo token NO WORKSPACE ACIMA e cole abaixo.")
        token = prompt("Cole o Databricks Personal Access Token")
        if not token.startswith("dapi"):
            raise RuntimeError("Token invalido. Deve comecar com 'dapi'.")
        os.environ["DATABRICKS_TOKEN"] = token
        update_env_file(["DATABRICKS_TOKEN"])
        print_ok("Novo PAT salvo")

    workspace_root = os.environ.get("DATABRICKS_WORKSPACE_ROOT") or state["outputs"].get("databricks_workspace_root")
    if not workspace_root:
        workspace_root = os.environ.get("DATABRICKS_REPO_PATH")
        if workspace_root:
            print_warn("Usando DATABRICKS_REPO_PATH legado. Prefira DATABRICKS_WORKSPACE_ROOT.")
    if not workspace_root:
        raise RuntimeError("Caminho dos notebooks nao definido. Execute primeiro o passo deploy_notebooks.")
    os.environ["DATABRICKS_WORKSPACE_ROOT"] = workspace_root

    script = PROJECT_ROOT / "scripts" / "create_databricks_workflow.py"
    if not script.exists():
        raise RuntimeError(f"Script nao encontrado: {script}")

    run_command([sys.executable, str(script)], capture=False, check=True)
    print_ok("Workflow criado")
    mark_completed(state, "create_workflow")


def step_create_dashboard(state):
    print_step(STEPS.index("create_dashboard") + 1, "Criando SQL Warehouse e dashboard AI/BI")

    host = f"https://{state['outputs']['databricks_workspace_url']}"
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token or not _test_jobs_token(host, token):
        raise RuntimeError("DATABRICKS_TOKEN invalido ou sem acesso ao workspace.")
    if not DATABRICKS_TERRAFORM_DIR.exists():
        raise RuntimeError(f"Terraform do Databricks nao encontrado: {DATABRICKS_TERRAFORM_DIR}")

    terraform = find_databricks_terraform()
    os.environ["DATABRICKS_HOST"] = host
    run_command([terraform, "init", "-input=false"], capture=False, check=True, cwd=str(DATABRICKS_TERRAFORM_DIR))
    run_command([terraform, "apply", "-auto-approve", "-input=false"], capture=False, check=True, cwd=str(DATABRICKS_TERRAFORM_DIR))

    outputs_json = run_command([terraform, "output", "-json"], capture=True, check=True, cwd=str(DATABRICKS_TERRAFORM_DIR))
    outputs = json.loads(outputs_json)
    warehouse_id = outputs["sql_warehouse_id"]["value"]
    http_path = outputs["sql_warehouse_http_path"]["value"]
    dashboard_id = outputs["dashboard_id"]["value"]
    dashboard_url = f"{host}/dashboardsv3/{dashboard_id}/published"

    os.environ["DATABRICKS_HTTP_PATH"] = http_path
    update_env_file(["DATABRICKS_HTTP_PATH"])
    state["outputs"].update({
        "sql_warehouse_id": warehouse_id,
        "sql_warehouse_http_path": http_path,
        "dashboard_id": dashboard_id,
        "dashboard_url": dashboard_url,
    })
    save_state(state)
    print_ok(f"SQL Warehouse: {warehouse_id}")
    print_ok(f"Dashboard publicado: {dashboard_url}")
    mark_completed(state, "create_dashboard")


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

    if ENV_FILE.exists():
        load_dotenv(ENV_FILE, override=True)
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
    if state["outputs"].get("dashboard_url"):
        print_info(f"Dashboard AI/BI: {state['outputs']['dashboard_url']}")


if __name__ == "__main__":
    main()
