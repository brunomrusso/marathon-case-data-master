import json
import shutil
import subprocess
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from setup_all import find_databricks_terraform
from upload_raw_data import wait_for_storage_access


PROJECT_ROOT = Path(__file__).parent.parent
BOOTSTRAP_DIR = PROJECT_ROOT / "infrastructure" / "terraform" / "bootstrap"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def run(command):
    subprocess.run(command, cwd=BOOTSTRAP_DIR, check=True)


def main():
    az = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
    if not az:
        raise RuntimeError("Azure CLI nao encontrado")
    subprocess.run(
        [az, "provider", "register", "--namespace", "Microsoft.EventGrid", "--wait"],
        check=True,
    )

    terraform = find_databricks_terraform()
    run([terraform, "init", "-input=false"])
    group_exists = subprocess.run(
        [az, "group", "exists", "--name", "rg-marathon-case"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().lower() == "true"
    state = subprocess.run(
        [terraform, "state", "list"],
        cwd=BOOTSTRAP_DIR,
        capture_output=True,
        text=True,
    ).stdout
    if group_exists and "azurerm_resource_group.target" not in state:
        raise RuntimeError("Remova o ambiente descartavel rg-marathon-case antes de executar o bootstrap.")
    run([terraform, "apply", "-auto-approve", "-input=false"])
    result = subprocess.run(
        [terraform, "output", "-json"],
        cwd=BOOTSTRAP_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    outputs = {key: value["value"] for key, value in json.loads(result.stdout).items()}

    service = BlobServiceClient(
        account_url=f"https://{outputs['seed_storage_account']}.blob.core.windows.net",
        credential=DefaultAzureCredential(),
    )
    container = service.get_container_client(outputs["seed_container"])
    wait_for_storage_access(container)
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        raise RuntimeError(f"Nenhum CSV encontrado em {RAW_DIR}")
    for path in csv_files:
        with path.open("rb") as data:
            container.get_blob_client(path.name).upload_blob(data, overwrite=True)
        print(f"Seed enviado: {path.name}")

    variables = {
        "AZURE_CLIENT_ID": outputs["azure_client_id"],
        "AZURE_TENANT_ID": outputs["azure_tenant_id"],
        "AZURE_SUBSCRIPTION_ID": outputs["azure_subscription_id"],
        "TF_BACKEND_RESOURCE_GROUP": outputs["state_resource_group"],
        "TF_BACKEND_STORAGE_ACCOUNT": outputs["state_storage_account"],
        "TF_BACKEND_CONTAINER": outputs["state_container"],
        "SEED_STORAGE_ACCOUNT": outputs["seed_storage_account"],
        "SEED_CONTAINER": outputs["seed_container"],
    }
    print("\nConfigure estas GitHub Actions Variables no environment production:")
    for key, value in variables.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    main()
