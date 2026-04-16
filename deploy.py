#!/usr/bin/env python3
"""Deploy Airflow DAGs to S3 with per-user suffix and owner customization."""

import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

SCRIPT_DIR = Path(__file__).resolve().parent

PROJECTS = {
    "migrator": {
        "dir": "data-iceberg-migrator",
        "dags": {
            "mapr": {
                "file": "migration_dag_mapr_to_s3.py",
                "dag_id": "mapr_to_s3_migration",
                "owner_marker": "'owner': 'data-migration'",
            },
            "iceberg": {
                "file": "migration_dag_iceberg.py",
                "dag_id": "iceberg_migration",
                "owner_marker": "'owner': 'data-migration'",
            },
            "folder": {
                "file": "migration_dag_folder_copy.py",
                "dag_id": "folder_only_data_copy",
                "owner_marker": "'owner': 'data-migration'",
            },
            "metadata": {
                "file": "migration_dag_metadata.py",
                "dag_id": "s3_to_s3_metadata_migration",
                "owner_marker": "'owner': 'data-migration'",
            },
        },
        "shared_utils": [
            ("utils/__init__.py", "utils/__init__.py"),
            ("utils/migrations/__init__.py", "utils/migrations/__init__.py"),
            ("utils/migrations/shared.py", "utils/migrations/shared.py"),
            ("utils/migrations/partition_utils.py", "utils/migrations/partition_utils.py"),
            (
                "utils/migrations/metadata_strategies/__init__.py",
                "utils/migrations/metadata_strategies/__init__.py",
            ),
            (
                "utils/migrations/metadata_strategies/iceberg_to_iceberg.py",
                "utils/migrations/metadata_strategies/iceberg_to_iceberg.py",
            ),
        ],
    },
    "ranger": {
        "dir": "ranger-policies-generator",
        "dags": {
            "ranger": {
                "file": "ranger_policies_generator_airflow3.py",
                "dag_id": "ranger_policy_automation",
                "owner_marker": "'owner': 'trino-admin'",
            },
        },
        "shared_utils": [
            ("utils/__init__.py", "utils/__init__.py"),
            ("utils/migrations/__init__.py", "utils/migrations/__init__.py"),
            ("utils/migrations/ranger_utils.py", "utils/migrations/ranger_utils.py"),
        ],
    },
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Deploy Airflow DAGs to S3 with per-user suffix and owner customization."
    )
    parser.add_argument(
        "--project",
        choices=list(PROJECTS.keys()),
        help="Project to deploy from",
    )
    parser.add_argument(
        "--dag",
        nargs="+",
        help="DAG shortcut(s) to deploy",
    )
    parser.add_argument("--owner", help="Owner name for DAG default_args")
    parser.add_argument("--suffix", help="Suffix to append to DAG IDs")
    parser.add_argument("--env-file", help="Env file to upload for selected DAGs")
    parser.add_argument(
        "--skip-shared-utils",
        action="store_true",
        help="Skip uploading shared utility files",
    )
    parser.add_argument(
        "--skip-env-shared",
        action="store_true",
        help="Skip uploading env.shared",
    )
    parser.add_argument("--s3-bucket", help="S3 bucket name (without s3:// prefix)")
    parser.add_argument("--dags-prefix", help="S3 key prefix for DAG files (e.g. 'airflow/tenant/dags/')")
    parser.add_argument("--s3-endpoint", help="S3 endpoint URL")
    parser.add_argument("--s3-access-key", help="S3 access key")
    parser.add_argument("--s3-secret-key", help="S3 secret key")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print upload plan without uploading",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )
    return parser.parse_args()


def prompt_choice(prompt_text: str, options: list[str]) -> str:
    print(f"\n{prompt_text}")
    for i, option in enumerate(options, 1):
        print(f"  {i}. {option}")
    while True:
        choice = input("Enter number: ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx]
        except ValueError:
            pass
        print(f"Invalid choice. Enter a number between 1 and {len(options)}.")


def prompt_multi_choice(prompt_text: str, options: list[str]) -> list[str]:
    print(f"\n{prompt_text}")
    for i, option in enumerate(options, 1):
        print(f"  {i}. {option}")
    while True:
        raw = input("Enter numbers (comma-separated): ").strip()
        try:
            indices = [int(x.strip()) - 1 for x in raw.split(",")]
            if all(0 <= idx < len(options) for idx in indices):
                return [options[idx] for idx in indices]
        except ValueError:
            pass
        print(f"Invalid input. Enter comma-separated numbers between 1 and {len(options)}.")


def discover_env_files() -> list[str]:
    env_files = []
    for f in sorted(SCRIPT_DIR.glob("env.*")):
        if f.name == "env.shared" or f.name.endswith(".example"):
            continue
        env_files.append(f.name)
    return env_files


def resolve_interactive(args):
    interactive = False

    if not args.project:
        interactive = True
        args.project = prompt_choice(
            "Select project:", list(PROJECTS.keys())
        )

    project = PROJECTS[args.project]
    dag_shortcuts = list(project["dags"].keys())

    if not args.dag:
        if len(dag_shortcuts) == 1:
            args.dag = dag_shortcuts
        else:
            interactive = True
            args.dag = prompt_multi_choice("Select DAG(s):", dag_shortcuts)

    for shortcut in args.dag:
        if shortcut not in project["dags"]:
            print(f"Error: Unknown DAG shortcut '{shortcut}' for project '{args.project}'.")
            print(f"Available: {', '.join(dag_shortcuts)}")
            sys.exit(1)

    if not args.owner:
        interactive = True
        args.owner = input("\nEnter owner: ").strip()
        if not args.owner:
            print("Error: Owner cannot be empty.")
            sys.exit(1)

    if not args.suffix:
        interactive = True
        args.suffix = input("Enter suffix: ").strip()
        if not args.suffix:
            print("Error: Suffix cannot be empty.")
            sys.exit(1)

    if interactive and not args.skip_env_shared:
        env_shared_path = SCRIPT_DIR / "env.shared"
        if env_shared_path.exists():
            answer = input("\nUpload env.shared? [Y/n] ").strip().lower()
            if answer == "n":
                args.skip_env_shared = True

    # Only prompt for env file in interactive mode; when all required args
    # are provided via CLI, treat missing --env-file as "no env file".
    if interactive and args.env_file is None:
        env_files = discover_env_files()
        if env_files:
            options = ["None"] + env_files
            choice = prompt_choice("Select per-DAG env file:", options)
            if choice != "None":
                args.env_file = choice


def resolve_s3_config(args):
    env_shared_path = SCRIPT_DIR / "env.shared"
    env_values = {}
    if env_shared_path.exists():
        env_values = dotenv_values(env_shared_path)

    required = [
        ("s3_bucket", "DEPLOY_S3_BUCKET", "Enter S3 bucket name: "),
        ("dags_prefix", "DEPLOY_DAGS_PREFIX", "Enter DAGs S3 prefix (e.g. airflow/tenant/dags/): "),
    ]

    optional = [
        ("s3_endpoint", "S3_ENDPOINT"),
        ("s3_access_key", "S3_ACCESS_KEY"),
        ("s3_secret_key", "S3_SECRET_KEY"),
    ]

    for attr, env_key, prompt in required:
        if getattr(args, attr):
            continue
        env_val = env_values.get(env_key, "")
        if env_val:
            setattr(args, attr, env_val)
        else:
            val = input(prompt).strip()
            if not val:
                print(f"Error: {env_key} is required.")
                sys.exit(1)
            setattr(args, attr, val)

    for attr, env_key in optional:
        if getattr(args, attr):
            continue
        env_val = env_values.get(env_key, "")
        if env_val:
            setattr(args, attr, env_val)

    if not args.dags_prefix.endswith("/"):
        args.dags_prefix += "/"


def build_upload_plan(args) -> list[tuple[str, str, str | None]]:
    """Build list of (local_path, s3_key, modified_content_or_None) tuples."""
    project = PROJECTS[args.project]
    project_dir = SCRIPT_DIR / project["dir"]
    uploads = []

    for shortcut in args.dag:
        dag_info = project["dags"][shortcut]
        local_path = project_dir / dag_info["file"]
        if not local_path.exists():
            print(f"Error: Source file not found: {local_path}")
            sys.exit(1)

        content = local_path.read_text()
        content = content.replace(
            f"dag_id='{dag_info['dag_id']}'",
            f"dag_id='{dag_info['dag_id']}_{args.suffix}'",
        )
        content = content.replace(
            dag_info["owner_marker"],
            f"'owner': '{args.owner}'",
        )

        dag_stem = local_path.stem
        s3_key = f"{args.dags_prefix}{dag_stem}_{args.suffix}.py"
        uploads.append((str(local_path), s3_key, content))

        if args.env_file:
            env_path = SCRIPT_DIR / args.env_file
            if not env_path.exists():
                print(f"Error: Env file not found: {env_path}")
                sys.exit(1)
            env_s3_key = f"{args.dags_prefix}utils/migration_configs/env.{dag_stem}_{args.suffix}"
            uploads.append((str(env_path), env_s3_key, None))

    if not args.skip_shared_utils:
        for src_rel, dst_rel in project["shared_utils"]:
            local_path = project_dir / src_rel
            if not local_path.exists():
                print(f"Error: Shared util not found: {local_path}")
                sys.exit(1)
            s3_key = f"{args.dags_prefix}{dst_rel}"
            uploads.append((str(local_path), s3_key, None))

    if not args.skip_env_shared:
        env_shared_path = SCRIPT_DIR / "env.shared"
        if not env_shared_path.exists():
            print(f"Error: env.shared not found: {env_shared_path}\n"
                  f"Use --skip-env-shared to skip uploading it.")
            sys.exit(1)
        s3_key = f"{args.dags_prefix}utils/migration_configs/env.shared"
        uploads.append((str(env_shared_path), s3_key, None))

    return uploads


def print_upload_plan(uploads: list[tuple[str, str, str | None]], bucket: str):
    print("\nUpload plan:")
    print(f"  Bucket: {bucket}")
    print()
    for local_path, s3_key, content in uploads:
        modified = " (modified)" if content is not None else ""
        print(f"  {local_path}")
        print(f"    -> s3://{bucket}/{s3_key}{modified}")
        print()


def upload_to_s3(
    uploads: list[tuple[str, str, str | None]],
    bucket: str,
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
):
    import boto3

    client_kwargs = {}
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    if access_key:
        client_kwargs["aws_access_key_id"] = access_key
    if secret_key:
        client_kwargs["aws_secret_access_key"] = secret_key

    s3 = boto3.client("s3", **client_kwargs)

    for local_path, s3_key, content in uploads:
        if content is not None:
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=content.encode("utf-8"),
            )
        else:
            s3.upload_file(local_path, bucket, s3_key)
        print(f"  Uploaded: s3://{bucket}/{s3_key}")


def main():
    args = parse_args()
    resolve_interactive(args)
    resolve_s3_config(args)

    uploads = build_upload_plan(args)
    print_upload_plan(uploads, args.s3_bucket)

    if args.dry_run:
        print("Dry run — no files uploaded.")
        return

    if not args.yes:
        confirm = input("Proceed? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            sys.exit(0)

    print("\nUploading...")
    try:
        upload_to_s3(
            uploads, args.s3_bucket,
            getattr(args, "s3_endpoint", None),
            getattr(args, "s3_access_key", None),
            getattr(args, "s3_secret_key", None),
        )
    except Exception as exc:
        from botocore.exceptions import NoCredentialsError
        if isinstance(exc, NoCredentialsError):
            print(
                "\nNo S3 credentials found. Provide them via:\n"
                "  --s3-access-key / --s3-secret-key CLI args\n"
                "  S3_ACCESS_KEY / S3_SECRET_KEY in env.shared\n"
                "  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars\n"
                "  ~/.aws/credentials file or IAM instance role"
            )
        else:
            print(f"\nS3 upload failed: {exc}")
        sys.exit(1)
    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        sys.exit(1)
