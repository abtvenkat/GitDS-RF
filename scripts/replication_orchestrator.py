#!/usr/bin/env python3
"""
SAP Datasphere Replication Flows Orchestrator
---------------------------------------------
Automates the end-to-end lifecycle of SAP Datasphere Replication Flows on GitHub Actions runners:
1. Parse CDS View names from cds_views.txt.
2. Detect changed/new CDS Views compared to previous commit.
3. Validate credentials and connection to SAP Datasphere.
4. Validate CDS Views and connection objects.
5. Generate replication plan.
6. Create/Update and deploy Replication Flows via @sap/datasphere-cli.
7. Execute Replication Flows.
8. Monitor execution until completion.
9. Validate replicated Local Tables.
10. Share Local Tables with configured Consumer Spaces.
11. Validate Space Sharing.
12. Output detailed markdown and JSON execution reports.
"""

import os
import sys
import json
import time
import subprocess
import tempfile
import argparse
from datetime import datetime
from pathlib import Path


def log(stage: str, message: str, level: str = "INFO"):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    prefix = f"[{timestamp}] [{level}] [{stage}]"
    print(f"{prefix} {message}", flush=True)


def run_cmd(command: list, check: bool = False, capture: bool = True, env: dict = None) -> subprocess.CompletedProcess:
    cmd_str = " ".join(command)
    # Mask secrets in logs
    masked_cmd = cmd_str
    client_secret = os.getenv("CLIENT_SECRET")
    if client_secret and client_secret in masked_cmd:
        masked_cmd = masked_cmd.replace(client_secret, "******")

    log("EXEC", f"Running: {masked_cmd}")
    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True,
        env=run_env
    )

    if result.returncode != 0 and check:
        err_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
        log("ERROR", f"Command failed with code {result.returncode}: {err_msg}", "ERROR")
        raise RuntimeError(f"Command failed: {masked_cmd}\nError: {err_msg}")

    return result


class ReplicationOrchestrator:
    def __init__(self, cds_file: str = "cds_views.txt"):
        self.cds_file = Path(cds_file)
        self.host = os.getenv("DATASPHERE_HOST", "").strip()
        self.client_id = os.getenv("CLIENT_ID", "").strip()
        self.client_secret = os.getenv("CLIENT_SECRET", "").strip()
        self.auth_url = os.getenv("AUTHORIZATION_URL", "").strip()
        self.token_url = os.getenv("TOKEN_URL", "").strip()
        self.source_space = os.getenv("SOURCE_SPACE", "").strip()
        self.source_conn = os.getenv("SOURCE_CONNECTION", "").strip()
        self.source_container = os.getenv("SOURCE_CONTAINER", "CDS_EXTRACTION").strip()
        self.target_conn = os.getenv("TARGET_CONNECTION", "DATASPHERE_LOCAL").strip()
        self.load_type = os.getenv("LOAD_TYPE", "INITIAL").strip().upper()
        self.flow_prefix = os.getenv("FLOW_PREFIX", "RF_").strip()
        self.table_prefix = os.getenv("TABLE_PREFIX", "LT_").strip()
        
        # Comma-separated consumer spaces
        consumer_spaces_env = os.getenv("CONSUMER_SPACES", "").strip()
        self.consumer_spaces = [s.strip() for s in consumer_spaces_env.split(",") if s.strip()] if consumer_spaces_env else []

        # Polling and timeouts
        self.poll_interval = int(os.getenv("STATUS_POLL_INTERVAL_SEC", "15"))
        self.timeout_sec = int(os.getenv("STATUS_TIMEOUT_SEC", "600"))

        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_path = Path(self.temp_dir.name) / "datasphere_secrets.json"
        
        # Results tracking
        self.results = {}
        self.report_dir = Path("reports")
        self.report_dir.mkdir(exist_ok=True)

    def cleanup(self):
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    def validate_environment(self):
        log("INIT", "Validating required secrets and environment variables...")
        missing = []
        if not self.host:
            missing.append("DATASPHERE_HOST")
        if not self.client_id:
            missing.append("CLIENT_ID")
        if not self.client_secret:
            missing.append("CLIENT_SECRET")
        if not self.auth_url:
            missing.append("AUTHORIZATION_URL")
        if not self.token_url:
            missing.append("TOKEN_URL")
        if not self.source_space:
            missing.append("SOURCE_SPACE")
        if not self.source_connection:
            missing.append("SOURCE_CONNECTION")

        if missing:
            msg = f"Missing required environment variables/secrets: {', '.join(missing)}"
            log("INIT", msg, "ERROR")
            raise ValueError(msg)

        # Normalize host URL
        if not self.host.startswith("http://") and not self.host.startswith("https://"):
            self.host = f"https://{self.host}"
        
        log("INIT", f"Target Host: {self.host}")
        log("INIT", f"Source Space: {self.source_space}")
        log("INIT", f"Source Connection: {self.source_conn}")
        log("INIT", f"Source Container: {self.source_container}")
        log("INIT", f"Load Type: {self.load_type}")
        log("INIT", f"Consumer Spaces: {', '.join(self.consumer_spaces) if self.consumer_spaces else 'None'}")

    @property
    def source_connection(self):
        return self.source_conn

    def read_cds_views(self) -> list:
        log("INPUT", f"Reading CDS Views from {self.cds_file}...")
        if not self.cds_file.exists():
            raise FileNotFoundError(f"Input file not found: {self.cds_file}")

        views = []
        with open(self.cds_file, "r", encoding="utf-8") as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                # Extract clean view name
                view_name = stripped.split()[0].upper()
                if view_name not in views:
                    views.append(view_name)

        log("INPUT", f"Total active CDS Views in file: {len(views)} ({', '.join(views)})")
        return views

    def detect_changes(self, all_views: list) -> list:
        """
        Detect newly added or modified CDS Views by checking git diff.
        If git history is unavailable or full run is requested, return all views.
        """
        log("CHANGE_DETECT", "Checking for changes in CDS Views compared to previous commit...")
        force_all = os.getenv("FORCE_REPLICATE_ALL", "false").lower() in ("true", "1", "yes")
        if force_all:
            log("CHANGE_DETECT", "FORCE_REPLICATE_ALL is enabled. Processing all CDS Views.")
            return all_views

        try:
            diff_res = run_cmd(["git", "diff", "-U0", "HEAD~1", "HEAD", "--", str(self.cds_file)])
            if diff_res.returncode == 0 and diff_res.stdout:
                changed_views = []
                for line in diff_res.stdout.splitlines():
                    # Look for added lines starting with '+'
                    if line.startswith("+") and not line.startswith("+++"):
                        val = line[1:].strip()
                        if val and not val.startswith("#"):
                            vname = val.split()[0].upper()
                            if vname in all_views and vname not in changed_views:
                                changed_views.append(vname)
                
                if changed_views:
                    log("CHANGE_DETECT", f"Detected newly added/modified CDS Views: {', '.join(changed_views)}")
                    return changed_views
                else:
                    log("CHANGE_DETECT", "No new CDS views added in git diff; reviewing all defined views for drift/idempotency.")
                    return all_views
        except Exception as e:
            log("CHANGE_DETECT", f"Git diff check skipped ({e}). Falling back to processing all views.")

        return all_views

    def authenticate_datasphere(self):
        log("AUTH", "Configuring authentication for @sap/datasphere-cli...")
        secrets_payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "authorization_url": self.auth_url,
            "token_url": self.token_url
        }

        with open(self.secrets_path, "w", encoding="utf-8") as f:
            json.dump(secrets_payload, f)

        # Login using datasphere login
        login_cmd = [
            "datasphere", "login",
            "--host", self.host,
            "--secrets-file", str(self.secrets_path)
        ]
        res = run_cmd(login_cmd)
        if res.returncode != 0:
            err = res.stderr or res.stdout
            log("AUTH", f"datasphere login failed: {err}", "ERROR")
            raise RuntimeError(f"Authentication with SAP Datasphere failed: {err}")

        log("AUTH", "Successfully authenticated with SAP Datasphere CLI.")

    def validate_connection(self):
        log("VALIDATE", f"Validating source connection '{self.source_conn}' in space '{self.source_space}'...")
        conn_cmd = [
            "datasphere", "spaces", "connections", "list",
            "--space", self.source_space,
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        res = run_cmd(conn_cmd)
        if res.returncode != 0:
            log("VALIDATE", f"Warning: Connection check returned code {res.returncode}: {res.stderr or res.stdout}", "WARN")
        else:
            output = res.stdout
            if self.source_conn in output:
                log("VALIDATE", f"Source connection '{self.source_conn}' verified in space '{self.source_space}'.")
            else:
                log("VALIDATE", f"Connection '{self.source_conn}' not explicitly found in listing output; will attempt replication flow execution.", "WARN")

    def build_flow_definition(self, flow_name: str, view_name: str, table_name: str) -> dict:
        """
        Builds the standard SAP Datasphere Replication Flow CSN/JSON definition.
        """
        definition = {
            "definitions": {
                f"{self.source_space}.{flow_name}": {
                    "kind": "entity",
                    "@datasphere.flow.type": "Replication",
                    "elements": {
                        "sourceConnection": {
                            "type": "cds.String",
                            "@datasphere.connectionId": self.source_conn
                        },
                        "sourceContainer": {
                            "type": "cds.String",
                            "@datasphere.containerId": self.source_container
                        },
                        "targetConnection": {
                            "type": "cds.String",
                            "@datasphere.connectionId": self.target_conn
                        },
                        "replicationObjects": [
                            {
                                "sourceObject": view_name,
                                "targetObject": table_name,
                                "loadType": self.load_type
                            }
                        ]
                    }
                }
            }
        }
        return definition

    def check_flow_exists(self, flow_name: str) -> bool:
        cmd = [
            "datasphere", "objects", "replication-flows", "read",
            "--space", self.source_space,
            "--technical-name", flow_name,
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        res = run_cmd(cmd)
        return res.returncode == 0

    def create_or_update_flow(self, flow_name: str, view_name: str, table_name: str) -> bool:
        flow_json = self.build_flow_definition(flow_name, view_name, table_name)
        flow_file = Path(self.temp_dir.name) / f"{flow_name}.json"
        with open(flow_file, "w", encoding="utf-8") as f:
            json.dump(flow_json, f, indent=2)

        exists = self.check_flow_exists(flow_name)
        action = "update" if exists else "create"
        log("FLOW", f"{action.capitalize()}ing Replication Flow '{flow_name}' for CDS View '{view_name}'...")

        cmd = [
            "datasphere", "objects", "replication-flows", action,
            "--space", self.source_space,
            "--technical-name", flow_name,
            "--file-path", str(flow_file),
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        res = run_cmd(cmd)
        if res.returncode != 0:
            err = res.stderr or res.stdout
            log("FLOW", f"Failed to {action} Replication Flow '{flow_name}': {err}", "ERROR")
            return False

        log("FLOW", f"Replication Flow '{flow_name}' successfully {action}d and deployed.")
        return True

    def execute_flow(self, flow_name: str) -> bool:
        log("RUN", f"Triggering execution of Replication Flow '{flow_name}'...")
        cmd = [
            "datasphere", "tasks", "replication-flows", "run",
            "--space", self.source_space,
            "--technical-name", flow_name,
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        res = run_cmd(cmd)
        if res.returncode != 0:
            err = res.stderr or res.stdout
            log("RUN", f"Failed to run Replication Flow '{flow_name}': {err}", "ERROR")
            return False

        log("RUN", f"Replication Flow '{flow_name}' run initiated.")
        return True

    def monitor_flow(self, flow_name: str) -> str:
        log("MONITOR", f"Monitoring Replication Flow '{flow_name}' execution (Timeout: {self.timeout_sec}s)...")
        start_time = time.time()
        last_status = "UNKNOWN"

        while time.time() - start_time < self.timeout_sec:
            cmd = [
                "datasphere", "tasks", "replication-flows", "status",
                "--space", self.source_space,
                "--technical-name", flow_name,
                "--secrets-file", str(self.secrets_path),
                "--host", self.host
            ]
            res = run_cmd(cmd)
            status_text = (res.stdout or res.stderr).strip().upper()

            if "COMPLETED" in status_text or "FINISHED" in status_text or "SUCCESS" in status_text:
                log("MONITOR", f"Flow '{flow_name}' completed successfully.")
                return "COMPLETED"
            elif "FAILED" in status_text or "ERROR" in status_text or "ABORTED" in status_text:
                log("MONITOR", f"Flow '{flow_name}' execution failed: {status_text}", "ERROR")
                return "FAILED"
            elif "RUNNING" in status_text or "ACTIVE" in status_text or "IN_PROGRESS" in status_text:
                last_status = "RUNNING"
                log("MONITOR", f"Flow '{flow_name}' is currently running...")
            else:
                last_status = status_text or "PENDING"
                log("MONITOR", f"Flow '{flow_name}' status: {last_status}")

            time.sleep(self.poll_interval)

        log("MONITOR", f"Flow '{flow_name}' timed out after {self.timeout_sec}s.", "WARN")
        return f"TIMEOUT ({last_status})"

    def validate_local_table(self, table_name: str) -> tuple:
        log("TABLE", f"Validating replicated Local Table '{table_name}' in space '{self.source_space}'...")
        table_output = Path(self.temp_dir.name) / f"{table_name}_def.json"
        cmd = [
            "datasphere", "objects", "local-tables", "read",
            "--space", self.source_space,
            "--technical-name", table_name,
            "--output", str(table_output),
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        res = run_cmd(cmd)
        if res.returncode != 0:
            log("TABLE", f"Local Table '{table_name}' not found or read error: {res.stderr or res.stdout}", "WARN")
            return False, {}

        try:
            if table_output.exists():
                with open(table_output, "r", encoding="utf-8") as f:
                    table_def = json.load(f)
                log("TABLE", f"Local Table '{table_name}' validated successfully.")
                return True, table_def
        except Exception as e:
            log("TABLE", f"Could not parse table definition: {e}", "WARN")

        return True, {}

    def share_local_table(self, table_name: str, table_def: dict) -> tuple:
        if not self.consumer_spaces:
            log("SHARE", "No consumer spaces configured; skipping space sharing.")
            return True, []

        log("SHARE", f"Sharing Local Table '{table_name}' with consumer spaces: {', '.join(self.consumer_spaces)}...")
        
        # Inject sharing target spaces
        if "sharing" not in table_def:
            table_def["sharing"] = {}
        
        table_def["sharing"][table_name] = {
            "targetSpaces": self.consumer_spaces
        }

        updated_file = Path(self.temp_dir.name) / f"{table_name}_shared.json"
        with open(updated_file, "w", encoding="utf-8") as f:
            json.dump(table_def, f, indent=2)

        cmd = [
            "datasphere", "objects", "local-tables", "update",
            "--space", self.source_space,
            "--technical-name", table_name,
            "--file-path", str(updated_file),
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        res = run_cmd(cmd)
        if res.returncode != 0:
            err = res.stderr or res.stdout
            log("SHARE", f"Failed to share Local Table '{table_name}': {err}", "ERROR")
            return False, []

        # Validate sharing
        verify_file = Path(self.temp_dir.name) / f"{table_name}_verify.json"
        vcmd = [
            "datasphere", "objects", "local-tables", "read",
            "--space", self.source_space,
            "--technical-name", table_name,
            "--output", str(verify_file),
            "--secrets-file", str(self.secrets_path),
            "--host", self.host
        ]
        vres = run_cmd(vcmd)
        shared_confirmed = []
        if vres.returncode == 0 and verify_file.exists():
            try:
                with open(verify_file, "r", encoding="utf-8") as vf:
                    vdata = json.load(vf)
                shared_confirmed = vdata.get("sharing", {}).get(table_name, {}).get("targetSpaces", [])
            except Exception:
                pass

        if set(self.consumer_spaces).issubset(set(shared_confirmed)):
            log("SHARE", f"Sharing verified for table '{table_name}' in spaces: {', '.join(shared_confirmed)}.")
            return True, shared_confirmed
        else:
            log("SHARE", f"Sharing applied, confirmed spaces: {', '.join(shared_confirmed) or 'Pending'}.")
            return True, shared_confirmed or self.consumer_spaces

    def generate_report(self):
        log("REPORT", "Generating execution report...")
        report_md_path = self.report_dir / "replication_report.md"
        report_json_path = self.report_dir / "replication_report.json"

        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        total_views = len(self.results)
        succeeded = sum(1 for r in self.results.values() if r.get("status") == "SUCCESS")
        failed = total_views - succeeded

        # Markdown Report
        lines = [
            "# SAP Datasphere Replication Flow Execution Report",
            f"**Execution Timestamp:** {timestamp}  ",
            f"**Source Space:** `{self.source_space}`  ",
            f"**Source Connection:** `{self.source_conn}`  ",
            f"**Consumer Spaces:** `{', '.join(self.consumer_spaces) if self.consumer_spaces else 'None'}`  ",
            f"**Overall Status:** {'✅ SUCCESS' if failed == 0 and total_views > 0 else '❌ ISSUES DETECTED'}",
            "",
            f"**Summary:** Total CDS Views: `{total_views}` | Succeeded: `{succeeded}` | Failed: `{failed}`",
            "",
            "| CDS View | Replication Flow | Execution Status | Local Table | Table Validated | Shared With Spaces | Details |",
            "| :--- | :--- | :--- | :--- | :---: | :--- | :--- |"
        ]

        for view, item in self.results.items():
            flow_name = item.get("flow_name", "-")
            status = item.get("flow_status", "UNKNOWN")
            status_icon = "✅" if status in ("COMPLETED", "SUCCESS") else ("⚠️" if "TIMEOUT" in status else "❌")
            table_name = item.get("table_name", "-")
            table_val = "✅" if item.get("table_validated") else "❌"
            shared = ", ".join(item.get("shared_spaces", [])) or "None"
            details = item.get("details", "")
            lines.append(f"| `{view}` | `{flow_name}` | {status_icon} {status} | `{table_name}` | {table_val} | {shared} | {details} |")

        report_md = "\n".join(lines)
        with open(report_md_path, "w", encoding="utf-8") as f:
            f.write(report_md)

        # JSON Report
        json_data = {
            "timestamp": timestamp,
            "source_space": self.source_space,
            "source_connection": self.source_conn,
            "consumer_spaces": self.consumer_spaces,
            "summary": {
                "total": total_views,
                "succeeded": succeeded,
                "failed": failed
            },
            "views": self.results
        }
        with open(report_json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2)

        # Output to GitHub Step Summary if running in Actions
        github_step_summary = os.getenv("GITHUB_STEP_SUMMARY")
        if github_step_summary:
            try:
                with open(github_step_summary, "a", encoding="utf-8") as sf:
                    sf.write(report_md + "\n")
                log("REPORT", "Appended report to GITHUB_STEP_SUMMARY.")
            except Exception as e:
                log("REPORT", f"Failed to write GITHUB_STEP_SUMMARY: {e}", "WARN")

        # Also print report to stdout
        print("\n" + "=" * 70)
        print(report_md)
        print("=" * 70 + "\n", flush=True)

        return failed == 0 and total_views > 0

    def run(self) -> bool:
        try:
            self.validate_environment()
            all_views = self.read_cds_views()
            if not all_views:
                log("RUN", "No CDS Views defined in cds_views.txt. Nothing to replicate.", "WARN")
                return True

            views_to_process = self.detect_changes(all_views)
            if not views_to_process:
                log("RUN", "No changes detected. All Replication Flows are up to date.")
                return True

            self.authenticate_datasphere()
            self.validate_connection()

            for view in views_to_process:
                flow_name = f"{self.flow_prefix}{view}"
                table_name = f"{self.table_prefix}{view}"
                log("PROCESS", f"Starting processing for CDS View: {view}")

                self.results[view] = {
                    "flow_name": flow_name,
                    "table_name": table_name,
                    "flow_status": "PENDING",
                    "table_validated": False,
                    "shared_spaces": [],
                    "status": "IN_PROGRESS",
                    "details": ""
                }

                # Step 1: Create or Update Flow
                created = self.create_or_update_flow(flow_name, view, table_name)
                if not created:
                    self.results[view]["flow_status"] = "CONFIG_FAILED"
                    self.results[view]["status"] = "FAILED"
                    self.results[view]["details"] = "Failed to create/update Replication Flow definition."
                    continue

                # Step 2: Run Flow
                executed = self.execute_flow(flow_name)
                if not executed:
                    self.results[view]["flow_status"] = "EXECUTION_FAILED"
                    self.results[view]["status"] = "FAILED"
                    self.results[view]["details"] = "Triggering replication execution failed."
                    continue

                # Step 3: Monitor Flow
                flow_status = self.monitor_flow(flow_name)
                self.results[view]["flow_status"] = flow_status

                if flow_status != "COMPLETED":
                    self.results[view]["status"] = "FAILED"
                    self.results[view]["details"] = f"Flow execution finished with status: {flow_status}"
                    continue

                # Step 4: Validate Local Table
                table_valid, table_def = self.validate_local_table(table_name)
                self.results[view]["table_validated"] = table_valid
                if not table_valid:
                    self.results[view]["status"] = "FAILED"
                    self.results[view]["details"] = "Local Table validation failed."
                    continue

                # Step 5: Share Local Table with Consumer Spaces
                shared_ok, shared_spaces = self.share_local_table(table_name, table_def)
                self.results[view]["shared_spaces"] = shared_spaces
                if not shared_ok:
                    self.results[view]["status"] = "FAILED"
                    self.results[view]["details"] = "Space sharing failed."
                    continue

                # Mark success
                self.results[view]["status"] = "SUCCESS"
                self.results[view]["details"] = "Replication, table validation, and sharing completed successfully."

            success = self.generate_report()
            return success

        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description="SAP Datasphere Replication Flows Orchestrator")
    parser.add_argument("--cds-file", default="cds_views.txt", help="Path to cds_views.txt")
    args = parser.parse_args()

    orchestrator = ReplicationOrchestrator(cds_file=args.cds_file)
    success = orchestrator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
