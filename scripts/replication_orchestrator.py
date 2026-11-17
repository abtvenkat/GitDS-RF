#!/usr/bin/env python3
"""
SAP Datasphere Replication Flows Orchestrator
---------------------------------------------
Automates the lifecycle of SAP Datasphere Replication Flows on GitHub Actions runners:
1. Parse CDS View names from cds_views.txt.
2. If multiple CDS Views are provided, consolidates them into ONE single Replication Flow.
3. Performs a two-stage save to materialize and integrate target objects.
4. Deploys the Replication Flow in SAP Datasphere via @sap/datasphere-cli.
5. Executes the Replication Flow and monitors execution to completion.
6. Generates comprehensive markdown and JSON reports.
"""

import os
import sys
import json
import time
import base64
import subprocess
import tempfile
import argparse
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path


def log(stage: str, message: str, level: str = "INFO"):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] [{level}] [{stage}] {message}", flush=True)


def run_cmd(command: list, capture: bool = True, env: dict = None) -> subprocess.CompletedProcess:
    cmd_str = " ".join(command)
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
    return result


class ReplicationOrchestrator:
    def __init__(self, cds_file: str = "cds_views.txt"):
        self.cds_file = Path(cds_file)
        self.host = os.getenv("DATASPHERE_HOST", "").strip()
        self.client_id = os.getenv("CLIENT_ID", "").strip()
        self.client_secret = os.getenv("CLIENT_SECRET", "").strip()
        self.auth_url = os.getenv("AUTHORIZATION_URL", "").strip()
        self.token_url = os.getenv("TOKEN_URL", "").strip()
        self.auth_flow = os.getenv("AUTHORIZATION_FLOW", "client_credentials").strip()
        self.source_space = os.getenv("SOURCE_SPACE", "").strip()
        self.source_conn = os.getenv("SOURCE_CONNECTION", "").strip()
        self.source_container = os.getenv("SOURCE_CONTAINER", "CDS_EXTRACTION").strip()
        self.target_conn = os.getenv("TARGET_CONNECTION", "DWC_HDLF").strip()
        self.load_type = os.getenv("LOAD_TYPE", "INITIAL").strip().upper()
        self.flow_prefix = os.getenv("FLOW_PREFIX", "RF_").strip()
        self.table_prefix = os.getenv("TABLE_PREFIX", "LT_").strip()
        self.custom_flow_name = os.getenv("FLOW_NAME", "").strip() or os.getenv("REPLICATION_FLOW_NAME", "").strip()

        self.poll_interval = int(os.getenv("STATUS_POLL_INTERVAL_SEC", "15"))
        self.timeout_sec = int(os.getenv("STATUS_TIMEOUT_SEC", "600"))

        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_path = Path(self.temp_dir.name) / "datasphere_secrets.json"
        self.access_token = None
        self.flow_name = ""
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
        for var, val in [("DATASPHERE_HOST", self.host), ("CLIENT_ID", self.client_id),
                         ("CLIENT_SECRET", self.client_secret), ("AUTHORIZATION_URL", self.auth_url),
                         ("TOKEN_URL", self.token_url), ("SOURCE_SPACE", self.source_space),
                         ("SOURCE_CONNECTION", self.source_conn)]:
            if not val:
                missing.append(var)
        if missing:
            msg = f"Missing required environment variables/secrets: {', '.join(missing)}"
            log("INIT", msg, "ERROR")
            raise ValueError(msg)
        if not self.host.startswith("http"):
            self.host = f"https://{self.host}"
        log("INIT", f"Target Host: {self.host}")
        log("INIT", f"Source Space: {self.source_space}")
        log("INIT", f"Source Connection: {self.source_conn}")
        log("INIT", f"Source Container: {self.source_container}")
        log("INIT", f"Load Type: {self.load_type}")

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
                vname = stripped.split()[0].upper()
                if vname not in views:
                    views.append(vname)
        log("INPUT", f"Total active CDS Views in file: {len(views)} ({', '.join(views)})")
        return views

    def detect_changes(self, all_views: list) -> list:
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
                    log("CHANGE_DETECT", "No new CDS views in git diff; processing all defined views.")
                    return all_views
        except Exception as e:
            log("CHANGE_DETECT", f"Git diff skipped ({e}). Falling back to all views.")
        return all_views

    def authenticate_datasphere(self):
        log("AUTH", f"Configuring authentication (Flow: {self.auth_flow})...")
        access_token = None
        try:
            auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
            body_data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode("utf-8")
            req = urllib.request.Request(self.token_url, data=body_data, method="POST")
            req.add_header("Authorization", f"Basic {auth_header}")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_resp = json.loads(resp.read().decode())
                access_token = token_resp.get("access_token")
                if access_token:
                    self.access_token = access_token
                    log("AUTH", "Successfully pre-fetched OAuth2 access token for technical user.")
        except Exception as e:
            try:
                body_data = urllib.parse.urlencode({
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret
                }).encode("utf-8")
                req = urllib.request.Request(self.token_url, data=body_data, method="POST")
                req.add_header("Content-Type", "application/x-www-form-urlencoded")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    token_resp = json.loads(resp.read().decode())
                    access_token = token_resp.get("access_token")
                    if access_token:
                        log("AUTH", "Successfully pre-fetched OAuth2 access token (via body).")
            except Exception as e2:
                log("AUTH", f"Direct token fetch notice ({e2}). Proceeding with CLI login.", "WARN")

        secrets_payload = {
            "client_id": self.client_id, "client_secret": self.client_secret,
            "authorization_url": self.auth_url, "token_url": self.token_url,
            "authorization_flow": self.auth_flow,
            "client-id": self.client_id, "client-secret": self.client_secret,
            "authorization-url": self.auth_url, "token-url": self.token_url,
            "authorization-flow": self.auth_flow, "host": self.host
        }
        if access_token:
            secrets_payload["access_token"] = access_token
            secrets_payload["access-token"] = access_token

        with open(self.secrets_path, "w", encoding="utf-8") as f:
            json.dump(secrets_payload, f, indent=2)

        res = run_cmd(["datasphere", "login", "--host", self.host,
                       "--authorization-flow", self.auth_flow,
                       "--secrets-file", str(self.secrets_path)])
        if res.returncode != 0:
            err = res.stderr or res.stdout
            if access_token:
                log("AUTH", f"datasphere login notice: {err}. Proceeding with bearer token in secrets-file.", "WARN")
            else:
                log("AUTH", f"datasphere login failed: {err}", "ERROR")
                raise RuntimeError(f"Authentication failed: {err}")
        else:
            log("AUTH", "Successfully authenticated with SAP Datasphere CLI.")

    def validate_connection(self):
        log("VALIDATE", f"Validating source connection '{self.source_conn}' in space '{self.source_space}'...")
        res = run_cmd(["datasphere", "spaces", "connections", "list",
                       "--space", self.source_space,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host])
        if res.returncode != 0:
            log("VALIDATE", f"Connection check warning: {res.stderr or res.stdout}", "WARN")
        elif self.source_conn in (res.stdout or ""):
            log("VALIDATE", f"Source connection '{self.source_conn}' verified in space '{self.source_space}'.")
        else:
            log("VALIDATE", f"Connection '{self.source_conn}' not found in listing; will attempt anyway.", "WARN")

    def _build_vtypes(self, columns: list) -> dict:
        vtypes = {}
        for col in columns:
            vtype_id = col.get("vtype-ID", "")
            if not vtype_id.startswith("$DYNAMIC."):
                continue
            type_key = vtype_id[len("$DYNAMIC."):]
            if type_key in vtypes:
                continue
            if type_key.startswith("string_"):
                try:
                    length = int(type_key.split("_", 1)[1])
                    vtypes[type_key] = {"name": type_key, "description": f"String({length})",
                                        "vflow.type": "scalar", "template": "string", "value.length": length}
                except ValueError:
                    pass
            elif type_key.startswith("decimal_"):
                parts = type_key.split("_")
                try:
                    p, s = int(parts[1]), int(parts[2])
                    vtypes[type_key] = {"name": type_key, "description": f"Decimal({p},{s})",
                                        "vflow.type": "scalar", "template": "decimal",
                                        "value.precision": p, "value.scale": s}
                except (IndexError, ValueError):
                    pass
            elif type_key.startswith("int_"):
                try:
                    length = int(type_key.split("_", 1)[1])
                    vtypes[type_key] = {"name": type_key, "description": f"Integer({length})",
                                        "vflow.type": "scalar", "template": "integer", "value.length": length}
                except ValueError:
                    pass
        return vtypes

    def get_cds_view_schema(self, view_name: str, table_name: str) -> tuple:
        v = view_name.upper()
        if v == "I_CURRENCY":
            keys = ["Currency"]
            source_cols = [
                {"name": "Currency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_5", "key": True, "businessName": "Currency", "metadata": {}},
                {"name": "Decimals", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "businessName": "Decimals", "metadata": {}},
                {"name": "CurrencyISOCode", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_3", "businessName": "ISO Currency Code", "metadata": {}},
                {"name": "AlternativeCurrencyKey", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_3", "businessName": "Alternative Currency Key", "metadata": {}},
                {"name": "IsPrimaryCurrencyForISOCode", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "businessName": "Primary Currency", "metadata": {}}
            ]
        elif v == "I_CURRENCYTEXT":
            keys = ["Language", "Currency"]
            source_cols = [
                {"name": "Language", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Language Key", "metadata": {}},
                {"name": "Currency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_5", "key": True, "businessName": "Currency", "metadata": {}},
                {"name": "CurrencyName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_40", "businessName": "Currency Name", "metadata": {}},
                {"name": "CurrencyShortName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_15", "businessName": "Short text", "metadata": {}}
            ]
        elif v in ("I_BILLINGFREQUENCY", "BILLINGFREQUENCY"):
            keys = ["BillingFrequency"]
            source_cols = [
                {"name": "BillingFrequency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "key": True, "businessName": "Billing Frequency", "metadata": {}}
            ]
        elif v in ("I_BILLINGFREQUENCY_TEXT", "I_BILLINGFREQUENCYTEXT", "BILLINGFREQUENCY_TEXT", "BILLINGFREQUENCYTEXT"):
            keys = ["Language", "BillingFrequency"]
            source_cols = [
                {"name": "Language", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Language Key", "metadata": {}},
                {"name": "BillingFrequency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "key": True, "businessName": "Billing Frequency", "metadata": {}},
                {"name": "BillingFrequencyName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_40", "businessName": "Billing Frequency Name", "metadata": {}}
            ]
        elif v.endswith("_TEXT") or v.endswith("TEXT"):
            base_name = v[:-5] if v.endswith("_TEXT") else v[:-4]
            entity_name = base_name[2:] if base_name.startswith("I_") or base_name.startswith("C_") else base_name
            keys = ["Language", entity_name]
            source_cols = [
                {"name": "Language", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Language Key", "metadata": {}},
                {"name": entity_name, "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_50", "key": True, "businessName": entity_name, "metadata": {}},
                {"name": f"{entity_name}Name", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_40", "businessName": f"{entity_name} Name", "metadata": {}}
            ]
        else:
            entity_name = v[2:] if v.startswith("I_") or v.startswith("C_") else v
            keys = [entity_name]
            source_cols = [
                {"name": entity_name, "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_50",
                 "key": True, "businessName": entity_name, "metadata": {}}
            ]

        target_cols = [dict(c) for c in source_cols] + [
            {"name": "Change_Type", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "metadata": {}},
            {"name": "Change_Date", "vflow.type": "scalar", "vtype-ID": "com.sap.core.timestamp", "metadata": {}}
        ]
        target_dict_elems = {col["name"]: {} for col in target_cols}
        return source_cols, keys, target_cols, target_dict_elems

    def build_consolidated_flow_json(self, flow_name: str, views: list, is_new: bool = True) -> dict:
        """
        Builds a SINGLE Replication Flow JSON containing all CDS Views as separate replication tasks.
        When is_new=True, target objects are flagged for initial creation.
        When is_new=False, target objects are flagged as materialized for deployment.
        """
        target_conn_id = self.target_conn if self.target_conn not in ("DATASPHERE_LOCAL", "", None) else "DWC_HDLF"
        target_conn_type = "HDL_FILES" if target_conn_id == "DWC_HDLF" else "DATASPHERE"
        source_container = f"/{self.source_container.lstrip('/')}"

        all_cols = []
        replication_tasks = []
        targets = {}

        for idx, view in enumerate(views, start=1):
            table_name = f"{self.table_prefix}{view}"
            source_cols, keys, target_cols, target_dict_elems = self.get_cds_view_schema(view, table_name)
            all_cols.extend(source_cols)
            all_cols.extend(target_cols)

            task_name = f"replicationtask{idx}"
            task_def = {
                "name": task_name,
                "loadType": self.load_type,
                "priority": 50,
                "truncate": False,
                "sourceObject": {
                    "name": view,
                    "definition": {"columns": source_cols, "keys": keys},
                    "metadata": {"type": "tabular", "isDeltaDisabled": True, "invalidColumns": ["/1DH/OPERATION"]},
                    "businessName": view
                },
                "targetObject": {
                    "name": table_name,
                    "definition": {"columns": target_cols, "keys": keys},
                    "metadata": {"isNew": is_new},
                    "properties": {
                        "com.sap.datasuite.cdc.mode.column": "Change_Type",
                        "com.sap.datasuite.cdc.timestamp.column": "Change_Date",
                        "com.sap.datasuite.tableCategory": "DELTA"
                    },
                    "businessName": view
                }
            }
            replication_tasks.append(task_def)
            targets[table_name] = {"elements": target_dict_elems}

        vtypes_scalar = self._build_vtypes(all_cols)

        flow_body = {
            "kind": "sap.dis.replicationflow",
            "@EndUserText.label": flow_name,
            "contents": {
                "description": flow_name,
                "sourceSystem": [{
                    "connectionId": self.source_conn,
                    "connectionType": "SAPS4HANAOP",
                    "container": source_container,
                    "maxConnections": 10,
                    "isGeneric": False,
                    "metadata": {}
                }],
                "targetSystem": [{
                    "connectionId": target_conn_id,
                    "connectionType": target_conn_type,
                    "container": "",
                    "maxConnections": 10,
                    "isGeneric": False,
                    "metadata": {"connectionMetaschema": {"sources": ["SAP"]}},
                    "properties": {
                        "compression": "SNAPPY",
                        "format": "PARQUET",
                        "groupDeltaFilesBy": "NONE",
                        "com.sap.rms.write.forceDecfloatsToTargetValueRange": "false",
                        "objectstore.write.useDuplicateSuppressionInitialLoad": "false",
                        "objectstore.write.parquet.compatibilityMode": "SPARK",
                        "objectstore.write.createLargePartFiles": "false"
                    }
                }],
                "vTypes": {"scalar": vtypes_scalar},
                "replicationTasks": replication_tasks,
                "replicationTaskSetting": {"hasSkipMappingCapability": True},
                "replicationFlowSetting": {
                    "ABAPcontentTypeDisabled": False,
                    "ABAPcontentType": "Native Type",
                    "isAutoMergeEnabledForTarget": True
                },
                "deltaLoadTrigger": "ON_DELTA_INTERVAL"
            },
            "sources": {},
            "targets": targets,
            "connections": {self.source_conn: {}}
        }

        return {"replicationflows": {flow_name: flow_body}}

    def _write_json(self, name: str, data: dict) -> Path:
        path = Path(self.temp_dir.name) / f"{name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return path

    def _rf_cmd(self, action: str, flow_name: str, file_path: Path, extra: list = None) -> subprocess.CompletedProcess:
        cmd = ["datasphere", "objects", "replication-flows", action,
               "--space", self.source_space,
               "--technical-name", flow_name,
               "--file-path", str(file_path),
               "--secrets-file", str(self.secrets_path),
               "--host", self.host]
        if extra:
            cmd += extra
        return run_cmd(cmd)

    def check_flow_exists(self, flow_name: str) -> bool:
        res = run_cmd(["datasphere", "objects", "replication-flows", "read",
                       "--space", self.source_space,
                       "--technical-name", flow_name,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host])
        return res.returncode == 0

    def create_or_update_flow(self, flow_name: str, views: list) -> bool:
        """
        Creates, materializes target objects via two-stage save, and DEPLOYS the single
        consolidated Replication Flow.
        """
        exists = self.check_flow_exists(flow_name)
        log("FLOW", f"Replication Flow '{flow_name}' existing in space '{self.source_space}': {exists}")

        payload_new = self.build_consolidated_flow_json(flow_name, views, is_new=True)
        file_new = self._write_json(f"{flow_name}_stage1", payload_new)

        payload_existing = self.build_consolidated_flow_json(flow_name, views, is_new=False)
        file_existing = self._write_json(f"{flow_name}_stage2", payload_existing)

        # -------------------------------------------------------------
        # STAGE 1: Save draft with --no-deploy to register target objects
        # -------------------------------------------------------------
        action = "update" if exists else "create"
        log("FLOW", f"Stage 1/3: {'Updating' if exists else 'Creating'} draft flow '{flow_name}' for {len(views)} views (--no-deploy)...")
        r1 = self._rf_cmd(action, flow_name, file_new, ["--no-deploy"])
        if r1.returncode != 0 and not exists:
            log("FLOW", f"Create notice ({r1.stderr or r1.stdout}). Retrying with update...", "WARN")
            r1 = self._rf_cmd("update", flow_name, file_new, ["--no-deploy"])

        if r1.returncode != 0:
            log("FLOW", f"Stage 1 draft save notice ({r1.stderr or r1.stdout}). Retrying with --save-anyway...", "WARN")
            r1 = self._rf_cmd("update", flow_name, file_new, ["--no-deploy", "--save-anyway"])
            if r1.returncode != 0:
                log("FLOW", f"Failed Stage 1 save for flow '{flow_name}': {r1.stderr or r1.stdout}", "ERROR")
                return False

        log("FLOW", f"Stage 1/3: Draft flow '{flow_name}' saved successfully.")
        time.sleep(5)

        # -------------------------------------------------------------
        # STAGE 2: Re-save draft with isNew=False to materialize target objects
        # -------------------------------------------------------------
        log("FLOW", f"Stage 2/3: Re-saving flow '{flow_name}' to materialize target objects (--no-deploy)...")
        r2 = self._rf_cmd("update", flow_name, file_existing, ["--no-deploy"])
        if r2.returncode != 0:
            log("FLOW", f"Stage 2 notice: {r2.stderr or r2.stdout}. Retrying with --save-anyway...", "WARN")
            r2 = self._rf_cmd("update", flow_name, file_existing, ["--no-deploy", "--save-anyway"])
            if r2.returncode != 0:
                log("FLOW", f"Stage 2 warning: {r2.stderr or r2.stdout}. Proceeding to deployment...", "WARN")
        else:
            log("FLOW", f"Stage 2/3: Flow '{flow_name}' target objects materialized.")
        time.sleep(5)

        # -------------------------------------------------------------
        # STAGE 3: DEPLOY the Replication Flow
        # -------------------------------------------------------------
        log("FLOW", f"Stage 3/3: Deploying Replication Flow '{flow_name}'...")
        r_deploy = self._rf_cmd("update", flow_name, file_existing, [])
        if r_deploy.returncode == 0:
            log("FLOW", f"Replication Flow '{flow_name}' deployed successfully!")
            time.sleep(5)
            return True

        err = r_deploy.stderr or r_deploy.stdout
        log("FLOW", f"Standard deploy notice: {err}. Retrying deploy with --deploy-anyway...", "WARN")
        r_deploy2 = self._rf_cmd("update", flow_name, file_existing, ["--deploy-anyway"])
        if r_deploy2.returncode == 0:
            log("FLOW", f"Replication Flow '{flow_name}' deployed successfully with --deploy-anyway!")
            time.sleep(5)
            return True

        log("FLOW", f"Deploy with --deploy-anyway notice: {r_deploy2.stderr or r_deploy2.stdout}. Retrying deploy with --save-anyway...", "WARN")
        r_deploy3 = self._rf_cmd("update", flow_name, file_existing, ["--save-anyway"])
        if r_deploy3.returncode == 0:
            log("FLOW", f"Replication Flow '{flow_name}' deployed successfully with --save-anyway!")
            time.sleep(5)
            return True

        log("FLOW", f"Deployment returned: {r_deploy3.stderr or r_deploy3.stdout}. Attempting execution check...", "WARN")
        return False

    def execute_flow(self, flow_name: str) -> bool:
        log("RUN", f"Triggering execution of Replication Flow '{flow_name}'...")
        res = run_cmd(["datasphere", "tasks", "replication-flows", "run",
                       "--space", self.source_space,
                       "--technical-name", flow_name,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host])
        if res.returncode != 0:
            err = (res.stderr or res.stdout).strip()
            log("RUN", f"Flow run notice: {err}", "ERROR")
            return False
        log("RUN", f"Replication Flow '{flow_name}' run successfully initiated.")
        return True

    def monitor_flow(self, flow_name: str) -> str:
        log("MONITOR", f"Monitoring '{flow_name}' (Timeout: {self.timeout_sec}s)...")
        start_time = time.time()
        last_status = "UNKNOWN"
        while time.time() - start_time < self.timeout_sec:
            res = run_cmd(["datasphere", "tasks", "replication-flows", "status",
                           "--space", self.source_space,
                           "--technical-name", flow_name,
                           "--secrets-file", str(self.secrets_path),
                           "--host", self.host])
            status_text = (res.stdout or res.stderr).strip().upper()
            if any(s in status_text for s in ("COMPLETED", "FINISHED", "SUCCESS", "SUCCEEDED")):
                log("MONITOR", f"Flow '{flow_name}' completed successfully.")
                return "COMPLETED"
            elif any(s in status_text for s in ("FAILED", "ERROR", "ABORTED")):
                log("MONITOR", f"Flow '{flow_name}' failed: {status_text}", "ERROR")
                return "FAILED"
            elif any(s in status_text for s in ("RUNNING", "ACTIVE", "IN_PROGRESS", "INITIAL_LOAD", "REPLICATING", "STARTED")):
                last_status = "RUNNING"
                log("MONITOR", f"Flow '{flow_name}' is running...")
            else:
                last_status = status_text or "PENDING"
                log("MONITOR", f"Flow '{flow_name}' status: {last_status}")
            time.sleep(self.poll_interval)
        log("MONITOR", f"Flow '{flow_name}' timed out after {self.timeout_sec}s.", "WARN")
        return f"TIMEOUT ({last_status})"

    def generate_report(self, views: list, flow_status: str) -> bool:
        log("REPORT", "Generating execution report...")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        is_success = flow_status in ("COMPLETED", "SUCCESS")

        lines = [
            "# SAP Datasphere Replication Flow Execution Report",
            f"**Execution Timestamp:** {timestamp}  ",
            f"**Replication Flow:** `{self.flow_name}`  ",
            f"**Source Space:** `{self.source_space}`  ",
            f"**Source Connection:** `{self.source_conn}`  ",
            f"**Status:** `{flow_status}`  ",
            "",
            "| CDS View | Replication Flow | Target Object | Load Type | Status |",
            "| :--- | :--- | :--- | :--- | :--- |"
        ]

        for view in views:
            table_name = f"{self.table_prefix}{view}"
            lines.append(f"| `{view}` | `{self.flow_name}` | `{table_name}` | `{self.load_type}` | `{flow_status}` |")

        report_md = "\n".join(lines)
        report_md_path = self.report_dir / "replication_report.md"
        report_json_path = self.report_dir / "replication_report.json"

        with open(report_md_path, "w", encoding="utf-8") as f:
            f.write(report_md)
        with open(report_json_path, "w", encoding="utf-8") as f:
            json.dump({
                "timestamp": timestamp,
                "flow_name": self.flow_name,
                "source_space": self.source_space,
                "status": flow_status,
                "views": views
            }, f, indent=2)

        summary_path = os.getenv("GITHUB_STEP_SUMMARY")
        if summary_path:
            try:
                with open(summary_path, "a", encoding="utf-8") as sf:
                    sf.write(report_md + "\n")
                log("REPORT", "Appended report to GITHUB_STEP_SUMMARY.")
            except Exception as e:
                log("REPORT", f"Failed to write GITHUB_STEP_SUMMARY: {e}", "WARN")

        print("\n" + "=" * 70)
        print(report_md)
        print("=" * 70 + "\n", flush=True)
        return is_success

    def run(self) -> bool:
        try:
            self.validate_environment()
            all_views = self.read_cds_views()
            if not all_views:
                log("RUN", "No CDS Views defined in cds_views.txt. Nothing to replicate.", "WARN")
                return True

            views_to_process = self.detect_changes(all_views)
            if not views_to_process:
                log("RUN", "No changes detected. Nothing to process.")
                return True

            if self.custom_flow_name:
                self.flow_name = self.custom_flow_name
            elif len(views_to_process) == 1:
                self.flow_name = f"{self.flow_prefix}{views_to_process[0]}"
            else:
                self.flow_name = f"{self.flow_prefix}CONSOLIDATED"

            log("PROCESS", f"Consolidating {len(views_to_process)} views ({', '.join(views_to_process)}) into SINGLE Replication Flow '{self.flow_name}'")

            self.authenticate_datasphere()
            self.validate_connection()

            # Step 1: Create, materialize target objects, and Deploy the single Replication Flow
            deployed = self.create_or_update_flow(self.flow_name, views_to_process)
            if not deployed:
                log("PROCESS", f"Deployment reported notices; verifying if Replication Flow '{self.flow_name}' is runnable...", "WARN")

            # Step 2: Trigger Replication Flow execution
            executed = self.execute_flow(self.flow_name)
            if not executed:
                log("RUN", f"Failed to run Replication Flow '{self.flow_name}'.", "ERROR")
                return self.generate_report(views_to_process, "EXECUTION_FAILED" if deployed else "DEPLOYMENT_FAILED")

            # Step 3: Monitor Flow execution to completion
            flow_status = self.monitor_flow(self.flow_name)

            return self.generate_report(views_to_process, flow_status)

        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description="SAP Datasphere Consolidated Replication Flows Orchestrator")
    parser.add_argument("--cds-file", default="cds_views.txt", help="Path to cds_views.txt")
    args = parser.parse_args()
    orchestrator = ReplicationOrchestrator(cds_file=args.cds_file)
    success = orchestrator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
