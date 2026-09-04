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
11. Cria workflow

Progresso salvo em .setup_state.json (nao versionado).
"""

import argparse
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


def find_executable(name):
    for ext in ["", ".cmd", ".exe", ".bat"]:
        path = shutil.which(name + ext)
        if path:
            return path
    return shutil.which(name)


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
        print_info("Edite o arquivo .env se quiser customizar ALERT_EMAIL ou DATABRICKS_REPO_PATH.")
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
    token = get_aad_token_for_databricks()
    workspace_id = state["outputs"].get("workspace_id")

    os.environ["DATABRICKS_TOKEN"] = token
    update_env_file(["DATABRICKS_TOKEN"])

    print_info("Verificando se Unity Catalog ja esta ativado...")
    print_info(f"Workspace ID usado: {workspace_id}")
    resp = requests.get(
        f"{host}/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    print_info(f"Resposta: {resp.status_code} - {resp.text[:200]}")
    if resp.status_code == 200:
        print_ok("Unity Catalog ja ativado. Account ID nao necessario.")
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


def step_unity_catalog(state):
    print_step(STEPS.index("unity_catalog") + 1, "Configurando Unity Catalog")

    if not os.environ.get("DATABRICKS_TOKEN"):
        print_info("Gerando Azure AD token para API do Databricks...")
        token = get_aad_token_for_databricks()
        os.environ["DATABRICKS_TOKEN"] = token
        update_env_file(["DATABRICKS_TOKEN"])
        print_ok("Azure AD token obtido")

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
    print_ok("Segredo 'catalog_name' salvo")

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
