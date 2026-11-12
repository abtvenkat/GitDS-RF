#!/usr/bin/env python3
"""
SAP Datasphere Replication Flows Orchestrator
---------------------------------------------
Automates the end-to-end lifecycle of SAP Datasphere Replication Flows on GitHub Actions runners.
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

        consumer_spaces_env = os.getenv("CONSUMER_SPACES", "").strip()
        self.consumer_spaces = [s.strip() for s in consumer_spaces_env.split(",") if s.strip()] if consumer_spaces_env else []

        self.poll_interval = int(os.getenv("STATUS_POLL_INTERVAL_SEC", "15"))
        self.timeout_sec = int(os.getenv("STATUS_TIMEOUT_SEC", "600"))

        self.temp_dir = tempfile.TemporaryDirectory()
        self.secrets_path = Path(self.temp_dir.name) / "datasphere_secrets.json"
        self.access_token = None
        self.results = {}
        self.report_dir = Path("reports")
        self.report_dir.mkdir(exist_ok=True)

    def cleanup(self):
        try:
            self.temp_dir.cleanup()
        except Exception:
            pass

    @property
    def source_connection(self):
        return self.source_conn

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
        log("INIT", f"Consumer Spaces: {', '.join(self.consumer_spaces) if self.consumer_spaces else 'None'}")

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
                log("AUTH", f"datasphere login notice: {err}. Proceeding with bearer token.", "WARN")
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
            log("VALIDATE", f"Source connection '{self.source_conn}' verified.")
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
        else:
            keys = [f"{view_name}_KEY"]
            source_cols = [
                {"name": f"{view_name}_KEY", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_50",
                 "key": True, "businessName": f"{view_name} Key", "metadata": {}}
            ]

        target_cols = [dict(c) for c in source_cols] + [
            {"name": "Change_Type", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "metadata": {}},
            {"name": "Change_Date", "vflow.type": "scalar", "vtype-ID": "com.sap.core.timestamp", "metadata": {}}
        ]
        target_dict_elems = {col["name"]: {} for col in target_cols}
        return source_cols, keys, target_cols, target_dict_elems

    def build_flow_json(self, flow_name: str, view_name: str, table_name: str, is_new: bool) -> dict:
        """
        Build the wrapped replication flow JSON payload.
        is_new=True  -> Phase 1 create: tells Datasphere this target object is NEW and must be provisioned.
        is_new=False -> Phase 2+ update/deploy: target object already registered.
        """
        target_conn_id = self.target_conn if self.target_conn not in ("DATASPHERE_LOCAL", "", None) else "DWC_HDLF"
        target_conn_type = "HDL_FILES" if target_conn_id == "DWC_HDLF" else "DATASPHERE"
        source_container = f"/{self.source_container.lstrip('/')}"

        source_cols, keys, target_cols, target_dict_elems = self.get_cds_view_schema(view_name, table_name)
        all_cols = source_cols + target_cols
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
                "replicationTasks": [{
                    "name": "replicationtask1",
                    "loadType": "INITIAL",
                    "priority": 50,
                    "truncate": False,
                    "sourceObject": {
                        "name": view_name,
                        "definition": {"columns": source_cols, "keys": keys},
                        "metadata": {"type": "tabular", "isDeltaDisabled": True, "invalidColumns": ["/1DH/OPERATION"]},
                        "businessName": view_name
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
                        "businessName": view_name
                    }
                }],
                "replicationTaskSetting": {"hasSkipMappingCapability": True},
                "replicationFlowSetting": {
                    "ABAPcontentTypeDisabled": False,
                    "ABAPcontentType": "Native Type",
                    "isAutoMergeEnabledForTarget": True
                },
                "deltaLoadTrigger": "ON_DELTA_INTERVAL"
            },
            "sources": {},
            "targets": {table_name: {"elements": target_dict_elems}},
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

    def check_table_exists(self, table_name: str) -> bool:
        res = run_cmd(["datasphere", "objects", "local-tables", "list",
                       "--space", self.source_space,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host])
        if res.returncode == 0 and res.stdout:
            return table_name.upper() in res.stdout.upper()
        return False

    def delete_flow_if_exists(self, flow_name: str):
        log("FLOW", f"Attempting to stop and delete existing flow '{flow_name}' for clean setup...")
        # Stop first if running
        run_cmd(["datasphere", "tasks", "replication-flows", "stop",
                 "--space", self.source_space,
                 "--technical-name", flow_name,
                 "--secrets-file", str(self.secrets_path),
                 "--host", self.host])
        time.sleep(2)
        res = run_cmd(["datasphere", "objects", "replication-flows", "delete",
                       "--space", self.source_space,
                       "--technical-name", flow_name,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host,
                       "--no-interaction"])
        if res.returncode == 0:
            log("FLOW", f"Deleted flow '{flow_name}' successfully.")
        else:
            log("FLOW", f"Could not delete flow '{flow_name}': {res.stderr or res.stdout}", "WARN")

    def create_or_update_flow(self, flow_name: str, view_name: str, table_name: str) -> bool:
        """
        SAP Datasphere robust lifecycle for target objects in HDL_FILES:
        1. Clean slate: stop and delete existing flow so no stale/unsaved target object state remains.
        2. Check if target Local Table already exists in the space.
        3. If table already exists: save and deploy with isNew=False.
        4. If table is new:
           - Phase 1: create with --no-deploy --save-anyway (isNew=True) -> Registers target object.
           - Phase 2: update with --no-deploy --save-anyway (isNew=False) -> Mandatory 2nd save.
           - Phase 3: update with --save-anyway (WITHOUT --no-deploy) -> Deploys flow & activates target table.
        """
        # Stop and delete any existing flow for a 100% clean deployment
        if self.check_flow_exists(flow_name):
            self.delete_flow_if_exists(flow_name)
            time.sleep(3)

        exists = self.check_flow_exists(flow_name)
        log("FLOW", f"Flow '{flow_name}' exists after cleanup: {exists}")

        # Check if target local table already exists in the space
        table_exists = self.check_table_exists(table_name)
        log("FLOW", f"Target Local Table '{table_name}' already exists in space: {table_exists}")

        # Prepare payloads
        file_new = self._write_json(f"{flow_name}_phase1_isNew_true",
                                    self.build_flow_json(flow_name, view_name, table_name, is_new=True))
        file_existing = self._write_json(f"{flow_name}_phase2_isNew_false",
                                         self.build_flow_json(flow_name, view_name, table_name, is_new=False))

        # --- Case A: Table already exists in space (e.g. from prior manual/auto deploy) ---
        if table_exists:
            log("FLOW", f"Target table '{table_name}' exists. Configuring flow with isNew=False...")
            if not exists:
                r_create = self._rf_cmd("create", flow_name, file_existing, ["--save-anyway"])
                if r_create.returncode == 0:
                    log("FLOW", f"Replication Flow '{flow_name}' created & deployed for existing table.")
                    time.sleep(5)
                    return True
            r_upd = self._rf_cmd("update", flow_name, file_existing, ["--save-anyway"])
            if r_upd.returncode == 0:
                log("FLOW", f"Replication Flow '{flow_name}' updated & deployed for existing table.")
                time.sleep(5)
                return True

        # --- Case B: Table is new - Multi-phase save to register target objects ---
        # Phase 1: Save draft with isNew=True (signals Datasphere to provision new target object)
        if not exists:
            log("FLOW", f"[Phase 1] Creating '{flow_name}' (isNew=True, --no-deploy --save-anyway)...")
            r1 = self._rf_cmd("create", flow_name, file_new, ["--no-deploy", "--save-anyway"])
            if r1.returncode != 0:
                err = (r1.stderr or r1.stdout).strip()
                log("FLOW", f"[Phase 1] Notice: {err}. Retrying with update...", "WARN")
                r1 = self._rf_cmd("update", flow_name, file_new, ["--no-deploy", "--save-anyway"])
        else:
            log("FLOW", f"[Phase 1] Updating '{flow_name}' (isNew=True, --no-deploy --save-anyway)...")
            r1 = self._rf_cmd("update", flow_name, file_new, ["--no-deploy", "--save-anyway"])

        time.sleep(5)  # Allow Datasphere backend to provision the target object

        # Phase 2: Save again with isNew=False (SAP requirement: "save once again before deploying")
        log("FLOW", f"[Phase 2] Saving '{flow_name}' again (isNew=False, --no-deploy --save-anyway)...")
        r2 = self._rf_cmd("update", flow_name, file_existing, ["--no-deploy", "--save-anyway"])
        if r2.returncode != 0:
            log("FLOW", f"[Phase 2] Notice: {r2.stderr or r2.stdout}. Retrying plain update with --no-deploy...", "WARN")
            self._rf_cmd("update", flow_name, file_existing, ["--no-deploy"])

        time.sleep(5)  # Allow second save to settle

        # Phase 3: Deploy (update WITHOUT --no-deploy, with --save-anyway)
        log("FLOW", f"[Phase 3] Deploying '{flow_name}' (update with --save-anyway)...")
        r3 = self._rf_cmd("update", flow_name, file_existing, ["--save-anyway"])
        if r3.returncode == 0:
            log("FLOW", f"[Phase 3] '{flow_name}' deployed successfully! Target objects are now active/dark.")
            time.sleep(5)
            return True

        log("FLOW", f"[Phase 3] Update with --save-anyway failed: {(r3.stderr or r3.stdout).strip()}. Retrying plain update...", "WARN")
        r3b = self._rf_cmd("update", flow_name, file_existing)
        if r3b.returncode == 0:
            log("FLOW", f"[Phase 3] '{flow_name}' deployed via plain update.")
            time.sleep(5)
            return True

        log("FLOW", f"Failed to deploy Replication Flow '{flow_name}': {(r3b.stderr or r3b.stdout).strip()}", "ERROR")
        return False

    def execute_flow(self, flow_name: str) -> bool:
        log("RUN", f"Triggering execution of Replication Flow '{flow_name}'...")
        res = run_cmd(["datasphere", "tasks", "replication-flows", "run",
                       "--space", self.source_space,
                       "--technical-name", flow_name,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host])
        if res.returncode != 0:
            log("RUN", f"Failed to run '{flow_name}': {res.stderr or res.stdout}", "ERROR")
            return False
        log("RUN", f"Replication Flow '{flow_name}' run initiated.")
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
            if any(s in status_text for s in ("COMPLETED", "FINISHED", "SUCCESS")):
                log("MONITOR", f"Flow '{flow_name}' completed successfully.")
                return "COMPLETED"
            elif any(s in status_text for s in ("FAILED", "ERROR", "ABORTED")):
                log("MONITOR", f"Flow '{flow_name}' failed: {status_text}", "ERROR")
                return "FAILED"
            elif any(s in status_text for s in ("RUNNING", "ACTIVE", "IN_PROGRESS")):
                last_status = "RUNNING"
                log("MONITOR", f"Flow '{flow_name}' is running...")
            else:
                last_status = status_text or "PENDING"
                log("MONITOR", f"Flow '{flow_name}' status: {last_status}")
            time.sleep(self.poll_interval)
        log("MONITOR", f"Flow '{flow_name}' timed out after {self.timeout_sec}s.", "WARN")
        return f"TIMEOUT ({last_status})"

    def validate_local_table(self, table_name: str, view_name: str = "") -> tuple:
        log("TABLE", f"Validating Local Table '{table_name}' in space '{self.source_space}'...")
        tables_found = []
        actual_table_name = table_name

        for attempt in range(1, 5):
            lres = run_cmd(["datasphere", "objects", "local-tables", "list",
                            "--space", self.source_space,
                            "--secrets-file", str(self.secrets_path),
                            "--host", self.host])
            tables_found = []
            if lres.returncode == 0 and lres.stdout:
                try:
                    data = json.loads(lres.stdout.strip())
                    items = data if isinstance(data, list) else data.get("value", data.get("objects", []))
                    tables_found = [item.get("technicalName", str(item)) if isinstance(item, dict) else str(item) for item in items]
                except Exception:
                    for line in lres.stdout.splitlines():
                        parts = line.strip().split()
                        if parts and not parts[0].startswith("-") and parts[0].lower() not in ("technical", "name", "id"):
                            tables_found.append(parts[0])

            matched = None
            for t in tables_found:
                if t == table_name or t.upper() == table_name.upper():
                    matched = t
                    break
                elif view_name and (view_name.upper() in t.upper()):
                    matched = t
                    break
            if matched:
                actual_table_name = matched
                log("TABLE", f"Matched local table '{actual_table_name}'.")
                break
            else:
                log("TABLE", f"Attempt {attempt}/4: Tables found: {tables_found}")
                if attempt < 4:
                    time.sleep(10)

        # Try to read the table definition
        table_def = {}
        read_success = False
        table_output = Path(self.temp_dir.name) / f"{actual_table_name}_def.json"

        for extra_args in [
            ["--accept", "application/vnd.sap.datasphere.object.content.design-time+json", "--output", str(table_output)],
            ["--output", str(table_output)],
            []
        ]:
            cmd = ["datasphere", "objects", "local-tables", "read",
                   "--space", self.source_space,
                   "--technical-name", actual_table_name,
                   "--secrets-file", str(self.secrets_path),
                   "--host", self.host] + extra_args
            rres = run_cmd(cmd)
            if rres.returncode == 0:
                if "--output" in extra_args and table_output.exists():
                    try:
                        with open(table_output, "r", encoding="utf-8") as f:
                            table_def = json.load(f)
                        read_success = True
                        break
                    except Exception:
                        pass
                elif rres.stdout:
                    try:
                        raw = rres.stdout.strip()
                        idx = raw.find("{")
                        if idx != -1:
                            table_def = json.loads(raw[idx:])
                            read_success = True
                            break
                    except Exception:
                        pass

        table_in_space = any(t.upper() == actual_table_name.upper() for t in tables_found)
        if read_success or table_in_space:
            log("TABLE", f"Local Table '{actual_table_name}' validated successfully.")
            return True, table_def, actual_table_name

        log("TABLE", f"Local Table '{actual_table_name}' could not be validated.", "WARN")
        return False, {}, actual_table_name

    def share_local_table(self, table_name: str, table_def: dict, view_name: str = "") -> tuple:
        if not self.consumer_spaces:
            log("SHARE", "No consumer spaces configured; skipping sharing.")
            return True, []

        log("SHARE", f"Sharing '{table_name}' with: {', '.join(self.consumer_spaces)}...")

        # Try sharing_only format first (most reliable), then full CSN if available
        payload_options = []

        sharing_only = {"sharing": {table_name: {"targetSpaces": self.consumer_spaces}}}
        payload_options.append(("sharing_only", sharing_only))

        if table_def and isinstance(table_def, dict) and "definitions" in table_def:
            full_def = dict(table_def)
            full_def.setdefault("sharing", {})[table_name] = {"targetSpaces": self.consumer_spaces}
            payload_options.insert(0, ("full_csn", full_def))

        last_err = ""
        for opt_name, payload in payload_options:
            share_file = self._write_json(f"{table_name}_share_{opt_name}", payload)
            log("SHARE", f"Attempting {opt_name} format...")
            res = run_cmd(["datasphere", "objects", "local-tables", "update",
                           "--space", self.source_space,
                           "--technical-name", table_name,
                           "--file-path", str(share_file),
                           "--secrets-file", str(self.secrets_path),
                           "--host", self.host])
            if res.returncode == 0:
                log("SHARE", f"Sharing applied via {opt_name}.")
                return True, self.consumer_spaces
            else:
                last_err = res.stderr or res.stdout
                log("SHARE", f"{opt_name} notice: {last_err}", "WARN")

        log("SHARE", f"Failed to share '{table_name}': {last_err}", "ERROR")
        return False, []

    def generate_report(self):
        log("REPORT", "Generating execution report...")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        total_views = len(self.results)
        succeeded = sum(1 for r in self.results.values() if r.get("status") == "SUCCESS")
        failed = total_views - succeeded

        lines = [
            "# SAP Datasphere Replication Flow Execution Report",
            f"**Execution Timestamp:** {timestamp}  ",
            f"**Source Space:** `{self.source_space}`  ",
            f"**Source Connection:** `{self.source_conn}`  ",
            f"**Consumer Spaces:** `{', '.join(self.consumer_spaces) if self.consumer_spaces else 'None'}`  ",
            f"**Overall Status:** {'✅ SUCCESS' if failed == 0 and total_views > 0 else '❌ ISSUES DETECTED'}",
            "",
            f"**Summary:** Total: `{total_views}` | Succeeded: `{succeeded}` | Failed: `{failed}`",
            "",
            "| CDS View | Replication Flow | Execution Status | Local Table | Table Validated | Shared With | Details |",
            "| :--- | :--- | :--- | :--- | :---: | :--- | :--- |"
        ]

        for view, item in self.results.items():
            flow_name = item.get("flow_name", "-")
            status = item.get("flow_status", "UNKNOWN")
            icon = "✅" if status in ("COMPLETED", "SUCCESS") else ("⚠️" if "TIMEOUT" in status else "❌")
            tname = item.get("table_name", "-")
            tval = "✅" if item.get("table_validated") else "❌"
            shared = ", ".join(item.get("shared_spaces", [])) or "None"
            details = item.get("details", "")
            lines.append(f"| `{view}` | `{flow_name}` | {icon} {status} | `{tname}` | {tval} | {shared} | {details} |")

        report_md = "\n".join(lines)
        report_md_path = self.report_dir / "replication_report.md"
        report_json_path = self.report_dir / "replication_report.json"

        with open(report_md_path, "w", encoding="utf-8") as f:
            f.write(report_md)
        with open(report_json_path, "w", encoding="utf-8") as f:
            json.dump({"timestamp": timestamp, "source_space": self.source_space,
                       "summary": {"total": total_views, "succeeded": succeeded, "failed": failed},
                       "views": self.results}, f, indent=2)

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
        return failed == 0 and total_views > 0

    def run(self) -> bool:
        try:
            self.validate_environment()
            all_views = self.read_cds_views()
            if not all_views:
                log("RUN", "No CDS Views defined. Nothing to replicate.", "WARN")
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

                # Step 1: Create/Update & Deploy Replication Flow
                created = self.create_or_update_flow(flow_name, view, table_name)
                if not created:
                    self.results[view].update({"flow_status": "CONFIG_FAILED", "status": "FAILED",
                                               "details": "Failed to create/deploy Replication Flow."})
                    continue

                # Step 2: Run Flow
                executed = self.execute_flow(flow_name)
                if not executed:
                    self.results[view].update({"flow_status": "EXECUTION_FAILED", "status": "FAILED",
                                               "details": "Triggering replication execution failed."})
                    continue

                # Step 3: Monitor Flow
                flow_status = self.monitor_flow(flow_name)
                self.results[view]["flow_status"] = flow_status
                if flow_status != "COMPLETED":
                    self.results[view].update({"status": "FAILED",
                                               "details": f"Flow execution finished with status: {flow_status}"})
                    continue

                # Step 4: Validate Local Table
                table_valid, table_def, actual_table_name = self.validate_local_table(table_name, view_name=view)
                self.results[view]["table_name"] = actual_table_name
                self.results[view]["table_validated"] = table_valid
                if not table_valid:
                    self.results[view].update({"status": "FAILED", "details": "Local Table validation failed."})
                    continue

                # Step 5: Share Local Table
                shared_ok, shared_spaces = self.share_local_table(actual_table_name, table_def, view_name=view)
                self.results[view]["shared_spaces"] = shared_spaces
                if not shared_ok:
                    self.results[view].update({"status": "FAILED", "details": "Space sharing failed."})
                    continue

                self.results[view].update({"status": "SUCCESS",
                                           "details": "Replication, validation, and sharing completed successfully."})

            return self.generate_report()
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
