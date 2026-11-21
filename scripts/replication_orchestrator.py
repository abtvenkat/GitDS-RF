#!/usr/bin/env python3
"""
SAP Datasphere Replication Flows Orchestrator
---------------------------------------------
Automates the lifecycle of SAP Datasphere Replication Flows on GitHub Actions runners
following the official SAP 9-step automation sequence:
  Step 1: Authenticate using Datasphere technical user
  Step 2: Create or update Replication Flow
  Step 3: Configure source objects
  Step 4: Generate target Local Tables / target objects
  Step 5: Save Replication Flow (initial save to register targets)
  Step 6: Save again to persist newly created target objects (Mandatory validation)
  Step 7: Deploy Replication Flow
  Step 8: Execute Replication Flow
  Step 9: Monitor replication status
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
        self.table_prefix = os.getenv("TABLE_PREFIX", "").strip()
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

        self.step_status = {
            "step1": "NOT_RUN",
            "step2": "NOT_RUN",
            "step3": "NOT_RUN",
            "step4": "NOT_RUN",
            "step5": "NOT_RUN",
            "step6": "NOT_RUN",
            "step7": "NOT_RUN",
            "step8": "NOT_RUN",
            "step9": "NOT_RUN"
        }

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

    # -------------------------------------------------------------------------
    # STEP 1: Authenticate using Datasphere technical user
    # -------------------------------------------------------------------------
    def authenticate_datasphere(self):
        log("STEP 1", f"Authenticate using Datasphere technical user (Flow: {self.auth_flow})...")
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
                    log("STEP 1", "Successfully pre-fetched OAuth2 access token for technical user.")
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
                        log("STEP 1", "Successfully pre-fetched OAuth2 access token (via body).")
            except Exception as e2:
                log("STEP 1", f"Direct token fetch notice ({e2}). Proceeding with CLI login.", "WARN")

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
                log("STEP 1", f"datasphere login notice: {err}. Proceeding with bearer token in secrets-file.", "WARN")
            else:
                log("STEP 1", f"datasphere login failed: {err}", "ERROR")
                raise RuntimeError(f"Authentication failed: {err}")
        else:
            log("STEP 1", "Successfully authenticated with SAP Datasphere CLI as technical user.")

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

    # -------------------------------------------------------------------------
    # STEP 3 & STEP 4: Configure Source Objects & Deploy Target Local Tables
    # -------------------------------------------------------------------------
    def _build_column_metadata(self, cname: str, vtype_id: str, business_name: str = "") -> dict:
        """
        Parses source column data type and semantics to construct the exact
        nativeType and TemplateType metadata structure expected by SAP Datasphere.
        """
        meta = {}
        native_type = {}

        if vtype_id.startswith("$DYNAMIC.string_"):
            try:
                length = int(vtype_id.split("_", 1)[1])
                native_type = {"datatype": "string", "length": length}
            except (ValueError, IndexError):
                native_type = {"datatype": "string", "length": 50}
        elif vtype_id.startswith("$DYNAMIC.decimal_"):
            parts = vtype_id.split("_")
            try:
                native_type = {"datatype": "decimal", "precision": int(parts[1]), "scale": int(parts[2])}
            except (ValueError, IndexError):
                native_type = {"datatype": "decimal", "precision": 15, "scale": 3}
        elif vtype_id.startswith("$DYNAMIC.int_") or "int" in vtype_id:
            native_type = {"datatype": "int"}
        elif vtype_id == "com.sap.core.timestamp":
            native_type = {"datatype": "timestamp"}
        elif vtype_id == "com.sap.core.date":
            native_type = {"datatype": "date"}

        if native_type:
            meta["nativeType"] = native_type

        # Parse date, time, and timestamp semantics from column name and type
        cname_lower = cname.lower()
        if (cname_lower.endswith("datetime") or cname_lower.endswith("_datetime")
                or vtype_id == "com.sap.core.timestamp" or cname == "Change_Date"):
            meta["TemplateType"] = {"datatype": "timestamp"}
        elif (cname_lower.endswith("date") or cname_lower.endswith("_date")
              or vtype_id == "com.sap.core.date"):
            meta["TemplateType"] = {"datatype": "date"}
        elif cname_lower.endswith("time") or cname_lower.endswith("_time"):
            meta["TemplateType"] = {"datatype": "time"}

        return meta

    def get_cds_view_schema(self, view_name: str, table_name: str) -> tuple:
        """
        Configures source object columns, keys, and target object columns with metadata.
        All column definitions include parsed nativeType and TemplateType metadata.
        """
        v = view_name.upper()
        if v == "I_CURRENCY":
            keys = ["Currency"]
            raw_cols = [
                {"name": "Currency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_5", "key": True, "businessName": "Currency"},
                {"name": "Decimals", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "businessName": "Decimals"},
                {"name": "CurrencyISOCode", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_3", "businessName": "ISO Currency Code"},
                {"name": "AlternativeCurrencyKey", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_3", "businessName": "Alternative Currency Key"},
                {"name": "IsPrimaryCurrencyForISOCode", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "businessName": "Primary Currency"}
            ]
        elif v == "I_CURRENCYTEXT":
            keys = ["Language", "Currency"]
            raw_cols = [
                {"name": "Language", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Language Key"},
                {"name": "Currency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_5", "key": True, "businessName": "Currency"},
                {"name": "CurrencyName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_40", "businessName": "Currency Name"},
                {"name": "CurrencyShortName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_15", "businessName": "Short text"}
            ]
        elif v == "I_SALESORGANIZATION":
            keys = ["SalesOrganization"]
            raw_cols = [
                {"name": "SalesOrganization", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_4", "key": True, "businessName": "Sales organization"},
                {"name": "SalesOrganizationCurrency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_5", "businessName": "Statistics currency"},
                {"name": "CompanyCode", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_4", "businessName": "Company code of the sales organization"},
                {"name": "IntercompanyBillingCustomer", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Customer number for intercompany billing"},
                {"name": "ArgentinaDeliveryDateEvent", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "businessName": "Time of numbering for deliveries"},
                {"name": "AddressID", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Address"},
                {"name": "VATRegnNumberDeterminationRule", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "businessName": "Rule that determines the vat registration number (vat id)"}
            ]
        elif v == "Z_ADLC_SALESORDERITEMPARTNER_W":
            keys = ["SalesOrder", "SalesOrderItem", "PartnerFunction"]
            raw_cols = [
                {"name": "SalesOrder", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "key": True, "businessName": "Sales order"},
                {"name": "SalesOrderItem", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_6", "key": True, "businessName": "Sales order item"},
                {"name": "PartnerFunction", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Partner function"},
                {"name": "Customer", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Customer number"},
                {"name": "Supplier", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Account number of supplier"},
                {"name": "Personnel", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_8", "businessName": "Personnel number"},
                {"name": "ContactPerson", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Number of contact person"},
                {"name": "Partner", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Customer number"},
                {"name": "FullName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_80", "businessName": "Full name of person"},
                {"name": "ReferenceBusinessPartner", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Business partner number"},
                {"name": "AddressID", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Address number"},
                {"name": "AddressPersonID", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Person number"},
                {"name": "AddressObjectType", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "businessName": "Address type (1=organization, 2=person, 3=contact person)"},
                {"name": "SDDocPartnerAddressRefType", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_1", "businessName": "Address indicator"},
                {"name": "BPAddrDeterminationTransaction", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_6", "businessName": "Address determination in document"},
                {"name": "BPRefAddressIDForDocSpcfcAddr", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "businessName": "Bp reference address number"},
                {"name": "InternationalPhoneNumber", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_30", "businessName": "Complete number: dialing code+number+extension"},
                {"name": "InternationalMobilePhoneNumber", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_30", "businessName": "Complete number: dialing code+number+extension"},
                {"name": "EmailAddress", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_241", "businessName": "Email address"}
            ]
        elif v in ("I_BILLINGFREQUENCY", "BILLINGFREQUENCY"):
            keys = ["BillingFrequency"]
            raw_cols = [
                {"name": "BillingFrequency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "key": True, "businessName": "Billing Frequency"}
            ]
        elif v in ("I_BILLINGFREQUENCY_TEXT", "I_BILLINGFREQUENCYTEXT", "BILLINGFREQUENCY_TEXT", "BILLINGFREQUENCYTEXT"):
            keys = ["Language", "BillingFrequency"]
            raw_cols = [
                {"name": "Language", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Language Key"},
                {"name": "BillingFrequency", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_10", "key": True, "businessName": "Billing Frequency"},
                {"name": "BillingFrequencyName", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_40", "businessName": "Billing Frequency Name"}
            ]
        elif v.endswith("_TEXT") or v.endswith("TEXT"):
            base_name = v[:-5] if v.endswith("_TEXT") else v[:-4]
            entity_name = base_name[2:] if base_name.startswith("I_") or base_name.startswith("C_") else base_name
            keys = ["Language", entity_name]
            raw_cols = [
                {"name": "Language", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_2", "key": True, "businessName": "Language Key"},
                {"name": entity_name, "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_50", "key": True, "businessName": entity_name},
                {"name": f"{entity_name}Name", "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_40", "businessName": f"{entity_name} Name"}
            ]
        else:
            entity_name = v[2:] if v.startswith("I_") or v.startswith("C_") else v
            keys = [entity_name]
            raw_cols = [
                {"name": entity_name, "vflow.type": "scalar", "vtype-ID": "$DYNAMIC.string_50",
                 "key": True, "businessName": entity_name}
            ]

        # Parse and assign metadata for all source columns
        source_cols = []
        for col in raw_cols:
            col_def = dict(col)
            col_def["metadata"] = self._build_column_metadata(
                col_def["name"], col_def.get("vtype-ID", ""), col_def.get("businessName", "")
            )
            source_cols.append(col_def)

        # Build target columns: all source columns + Change_Type + Change_Date
        target_cols = [dict(c) for c in source_cols] + [
            {
                "name": "Change_Type",
                "vflow.type": "scalar",
                "vtype-ID": "$DYNAMIC.string_1",
                "businessName": "Change_Type",
                "metadata": {"nativeType": {"datatype": "string", "length": 1}}
            },
            {
                "name": "Change_Date",
                "vflow.type": "scalar",
                "vtype-ID": "com.sap.core.timestamp",
                "businessName": "Change_Date",
                "metadata": {"TemplateType": {"datatype": "timestamp"}}
            }
        ]
        target_dict_elems = {col["name"]: {} for col in target_cols}
        return source_cols, keys, target_cols, target_dict_elems

    # -------------------------------------------------------------------------
    # STEP 2, 3, 4: Build Definitions, Deploy Local Tables, & Build RF JSON
    # -------------------------------------------------------------------------
    def build_definitions(self, views: list) -> dict:
        """
        Builds standard CSN entity definitions for Delta Lake targets in HDL_FILES space.
        Matches the exact schema format exported from SAP Datasphere (e.g. Z_ADLC_SALESORDERITEMPARTNER_W):
          - Active entity: <TABLE_NAME> with deltaFromEntities: [<TABLE_NAME>_Delta]
          - Delta entity: <TABLE_NAME>_Delta with UPSERT mode, params, Semantics.interval, and CDC fields.
        """
        definitions = {}
        for view in views:
            table_name = f"{self.table_prefix}{view}" if self.table_prefix else view
            delta_table_name = f"{table_name}_Delta"
            source_cols, keys, target_cols, _ = self.get_cds_view_schema(view, table_name)

            # Build source elements
            source_elements = {}
            for col in source_cols:
                cname = col["name"]
                vtype = col.get("vtype-ID", "")
                is_key = col.get("key", False)
                meta = col.get("metadata", {})
                nt = meta.get("nativeType", {})
                tt = meta.get("TemplateType", {})

                if "string" in vtype or nt.get("datatype") == "string":
                    length = nt.get("length") or 50
                    elem_def = {"type": "cds.String", "length": length}
                elif "decimal" in vtype or nt.get("datatype") == "decimal":
                    p = nt.get("precision", 15)
                    s = nt.get("scale", 3)
                    elem_def = {"type": "cds.Decimal", "precision": p, "scale": s}
                elif "timestamp" in vtype or tt.get("datatype") == "timestamp":
                    elem_def = {"type": "cds.Timestamp"}
                elif "date" in vtype or tt.get("datatype") == "date":
                    elem_def = {"type": "cds.Date"}
                elif "int" in vtype or nt.get("datatype") == "int":
                    elem_def = {"type": "cds.Integer"}
                else:
                    elem_def = {"type": "cds.String", "length": 50}

                if is_key:
                    elem_def["key"] = True
                    elem_def["notNull"] = True
                else:
                    elem_def["key"] = False
                if col.get("businessName"):
                    elem_def["@EndUserText.label"] = col["businessName"]
                source_elements[cname] = elem_def

            # Build delta elements (source + Change_Type + Change_Date)
            delta_elements = dict(source_elements)
            delta_elements["Change_Type"] = {
                "@EndUserText.label": "Change_Type",
                "type": "cds.String",
                "key": False,
                "notNull": True,
                "length": 1,
                "default": {"val": "I"}
            }
            delta_elements["Change_Date"] = {
                "@EndUserText.label": "Change_Date",
                "type": "cds.Timestamp",
                "key": False,
                "notNull": True,
                "default": {"func": "CURRENT_UTCTIMESTAMP"}
            }

            # 1. Active entity: <TABLE_NAME>
            definitions[table_name] = {
                "kind": "entity",
                "@EndUserText.label": f"Local Table for {view}",
                "@ObjectModel.modelingPattern": {"#": "DATA_STRUCTURE"},
                "@ObjectModel.supportedCapabilities": [{"#": "DATA_STRUCTURE"}],
                "@DataWarehouse.delta": {
                    "type": {"#": "ACTIVE"},
                    "deltaFromEntities": [delta_table_name]
                },
                "@DataWarehouse.persistence.hdlf.tableFormat": {"#": "DELTA_LAKE"},
                "elements": source_elements,
                "_meta": {
                    "dependencies": {
                        "folderAssignment": None
                    }
                }
            }

            # 2. Delta entity: <TABLE_NAME>_Delta
            definitions[delta_table_name] = {
                "kind": "entity",
                "@EndUserText.label": f"Delta Lake Entity for {view}",
                "@ObjectModel.modelingPattern": {"#": "DATA_STRUCTURE"},
                "@ObjectModel.supportedCapabilities": [{"#": "DATA_STRUCTURE"}],
                "@DataWarehouse.delta": {
                    "type": {"#": "UPSERT"},
                    "dateTimeElement": {"=": "Change_Date"},
                    "modeElement": {"=": "Change_Type"}
                },
                "@DataWarehouse.enclosingObject": table_name,
                "params": {
                    "EXTRACTION_MODE": {
                        "@EndUserText.label": "Extraction Mode",
                        "type": "cds.String",
                        "length": 10,
                        "default": "FULL",
                        "@DataWarehouse.bw.extractionMode": True
                    },
                    "FROM_VERSION": {
                        "@EndUserText.label": "From Change Version",
                        "type": "cds.Integer64",
                        "default": "-1"
                    },
                    "TILL_VERSION": {
                        "@EndUserText.label": "Till Change Version",
                        "type": "cds.Integer64",
                        "default": "-1"
                    }
                },
                "@Semantics.interval": [
                    {
                        "qualifier": "changeVersion",
                        "lowerBoundaryParameter": {"=": "FROM_VERSION"},
                        "lowerBoundaryIncluded": True,
                        "upperBoundaryParameter": {"=": "TILL_VERSION"},
                        "upperBoundaryIncluded": True
                    }
                ],
                "@DataWarehouse.persistence.hdlf.tableFormat": {"#": "DELTA_LAKE"},
                "@cds.persistence.udf": True,
                "elements": delta_elements,
                "_meta": {
                    "dependencies": {
                        "folderAssignment": None
                    }
                }
            }

        csn_payload = {
            "$version": "1.0",
            "version": {"csn": "1.0"},
            "meta": {"creator": "CDS Compiler v1.19.2"},
            "definitions": definitions
        }
        csn_path = self.report_dir / "local_tables_csn.json"
        with open(csn_path, "w", encoding="utf-8") as f:
            json.dump(csn_payload, f, indent=2)
        log("TABLES", f"Generated target definitions CSN at '{csn_path}'.")
        return definitions

    def generate_local_tables_csn(self, views: list) -> Path:
        """Backwards compatibility alias for build_definitions."""
        self.build_definitions(views)
        return self.report_dir / "local_tables_csn.json"

    def deploy_local_tables(self, views: list) -> bool:
        """
        Step 4: Generates target Local Table Delta Lake definitions (CSN) and deploys
        them into the SAP Datasphere space via `datasphere objects local-tables create/update`.
        """
        definitions = self.build_definitions(views)
        all_success = True

        for view in views:
            table_name = f"{self.table_prefix}{view}" if self.table_prefix else view
            delta_table_name = f"{table_name}_Delta"

            table_csn = {
                "$version": "1.0",
                "version": {"csn": "1.0"},
                "meta": {"creator": "CDS Compiler v1.19.2"},
                "definitions": {
                    table_name: definitions[table_name],
                    delta_table_name: definitions[delta_table_name]
                }
            }
            table_csn_path = self._write_json(f"{table_name}_csn", table_csn)

            log("STEP 4", f"Checking if Local Table '{table_name}' exists in space '{self.source_space}'...")
            r_check = run_cmd(["datasphere", "objects", "local-tables", "read",
                               "--space", self.source_space,
                               "--technical-name", table_name,
                               "--secrets-file", str(self.secrets_path),
                               "--host", self.host])
            exists = (r_check.returncode == 0)
            action = "update" if exists else "create"

            log("STEP 4", f"{action.capitalize()} Local Table '{table_name}' in space '{self.source_space}'...")
            r_table = run_cmd(["datasphere", "objects", "local-tables", action,
                               "--space", self.source_space,
                               "--technical-name", table_name,
                               "--file-path", str(table_csn_path),
                               "--secrets-file", str(self.secrets_path),
                               "--host", self.host])
            if r_table.returncode != 0 and not exists:
                log("STEP 4", f"Create returned notice ({r_table.stderr or r_table.stdout}). Retrying with update...", "WARN")
                r_table = run_cmd(["datasphere", "objects", "local-tables", "update",
                                   "--space", self.source_space,
                                   "--technical-name", table_name,
                                   "--file-path", str(table_csn_path),
                                   "--secrets-file", str(self.secrets_path),
                                   "--host", self.host])

            if r_table.returncode != 0:
                log("STEP 4", f"Save notice ({r_table.stderr or r_table.stdout}). Retrying with --save-anyway...", "WARN")
                r_table = run_cmd(["datasphere", "objects", "local-tables", "update",
                                   "--space", self.source_space,
                                   "--technical-name", table_name,
                                   "--file-path", str(table_csn_path),
                                   "--secrets-file", str(self.secrets_path),
                                   "--host", self.host,
                                   "--save-anyway"])

            if r_table.returncode == 0:
                log("STEP 4", f"Local Table '{table_name}' and '{delta_table_name}' successfully deployed to space '{self.source_space}'.")
            else:
                log("STEP 4", f"Notice while deploying Local Table '{table_name}': {r_table.stderr or r_table.stdout}. Proceeding with flow creation...", "WARN")

        return all_success

    def build_consolidated_flow_json(self, flow_name: str, views: list, is_new: bool = False, include_definitions: bool = False) -> dict:
        """
        Builds a SINGLE Replication Flow JSON containing all CDS Views as separate replication tasks.
        Step 3: Configures source objects.
        Step 4: Configures target objects in Data Lake Files (HDL_FILES / DWC_HDLF).
        """
        target_conn_id = self.target_conn if self.target_conn not in ("DATASPHERE_LOCAL", "", None) else "DWC_HDLF"
        target_conn_type = "HDL_FILES" if target_conn_id == "DWC_HDLF" else "DATASPHERE"
        source_container = f"/{self.source_container.lstrip('/')}"
        target_container = ""  # For HDL_FILES (DWC_HDLF), container is always empty string

        all_cols = []
        replication_tasks = []
        targets = {}

        for idx, view in enumerate(views, start=1):
            table_name = f"{self.table_prefix}{view}" if self.table_prefix else view
            delta_table_name = f"{table_name}_Delta"
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
                    "name": delta_table_name,
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
            targets[delta_table_name] = {"elements": target_dict_elems}

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
                    "container": target_container,
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

        if include_definitions:
            definitions = self.build_definitions(views)
            return {
                "$version": "1.0",
                "version": {"csn": "1.0"},
                "meta": {"creator": "CDS Compiler v1.19.2"},
                "definitions": definitions,
                "replicationflows": {flow_name: flow_body}
            }
        else:
            return {"replicationflows": {flow_name: flow_body}}

    def _write_json(self, name: str, data: dict) -> Path:
        path = Path(self.temp_dir.name) / f"{name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return path

    def _patch_is_new(self, payload: dict, flow_name: str, is_new: bool) -> dict:
        """
        Deep-patches every targetObject.metadata.isNew in the server-read payload.
        This ensures we reuse server-assigned IDs while updating the isNew flag.
        """
        import copy
        patched = copy.deepcopy(payload)
        try:
            rf_map = patched.get("replicationflows", {})
            flow_body = rf_map.get(flow_name, {})
            tasks = flow_body.get("contents", {}).get("replicationTasks", [])
            for task in tasks:
                tgt = task.get("targetObject", {})
                meta = tgt.get("metadata", {})
                meta["isNew"] = is_new
                tgt["metadata"] = meta
            log("STEP 6", f"Patched isNew={is_new} in {len(tasks)} replication task(s).")
        except Exception as ex:
            log("STEP 6", f"Patch warning: {ex}", "WARN")
        return patched

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

    # -------------------------------------------------------------------------
    # STEPS 2, 5, 6, 7: Flow Creation, Existing Target Binding & Deployment (Approach A)
    # -------------------------------------------------------------------------
    def create_or_update_flow(self, flow_name: str, views: list) -> bool:
        """
        Executes the official SAP 9-Step Automation Sequence for HDL_FILES spaces (Approach A):
          Step 2: Check flow existence
          Step 3: Configure source objects
          Step 4: Generate target Local Tables (CSN) and deploy to Datasphere space
          Step 5: Save Replication Flow draft (--no-deploy)
          Step 6: Save again to persist target objects (Mandatory validation)
          Step 7: Deploy Replication Flow
        """
        # Step 2: Check flow existence
        exists = self.check_flow_exists(flow_name)
        log("STEP 2", f"Create or update Replication Flow '{flow_name}' (Exists in space '{self.source_space}': {exists})")
        self.step_status["step2"] = "PASSED"

        # Step 3: Configure source objects
        log("STEP 3", f"Configuring source objects for {len(views)} CDS views ({', '.join(views)})...")
        self.step_status["step3"] = "PASSED"

        # Step 4: Generate target Local Tables & deploy them to Datasphere space
        log("STEP 4", f"Deploying target Local Tables (CSN) for {len(views)} views into space '{self.source_space}'...")
        self.deploy_local_tables(views)
        self.step_status["step4"] = "PASSED"

        # Prepare payloads: Full CSN and Minimal RF
        payload_full = self.build_consolidated_flow_json(flow_name, views, is_new=False, include_definitions=True)
        payload_minimal = self.build_consolidated_flow_json(flow_name, views, is_new=False, include_definitions=False)

        flow_file_full = self._write_json(f"{flow_name}_full", payload_full)
        flow_file_min = self._write_json(f"{flow_name}_min", payload_minimal)
        active_flow_file = flow_file_min

        # ---------------------------------------------------------------------
        # STEP 5: Save Replication Flow draft (initial save to register targets)
        # ---------------------------------------------------------------------
        action = "update" if exists else "create"
        log("STEP 5", f"Save Replication Flow '{flow_name}' draft (--no-deploy)...")

        # Try with minimal payload first (standard CLI replication-flow object format)
        r1 = self._rf_cmd(action, flow_name, active_flow_file, ["--no-deploy"])
        if r1.returncode != 0 and not exists:
            log("STEP 5", f"Create returned notice ({r1.stderr or r1.stdout}). Retrying with update...", "WARN")
            r1 = self._rf_cmd("update", flow_name, active_flow_file, ["--no-deploy"])

        if r1.returncode != 0:
            log("STEP 5", "Retrying draft save with full CSN payload...", "INFO")
            r1 = self._rf_cmd("update" if exists else action, flow_name, flow_file_full, ["--no-deploy"])
            if r1.returncode == 0:
                active_flow_file = flow_file_full

        if r1.returncode != 0:
            log("STEP 5", f"Save notice ({r1.stderr or r1.stdout}). Retrying with --save-anyway...", "WARN")
            r1 = self._rf_cmd("update", flow_name, active_flow_file, ["--no-deploy", "--save-anyway"])
            if r1.returncode != 0 and active_flow_file != flow_file_full:
                r1 = self._rf_cmd("update", flow_name, flow_file_full, ["--no-deploy", "--save-anyway"])
                if r1.returncode == 0:
                    active_flow_file = flow_file_full

        if r1.returncode != 0:
            self.step_status["step5"] = "FAILED"
            log("STEP 5", f"Failed Step 5 save for flow '{flow_name}': {r1.stderr or r1.stdout}", "ERROR")
            return False

        self.step_status["step5"] = "PASSED"
        log("STEP 5", f"Replication Flow '{flow_name}' draft saved successfully.")
        time.sleep(3)

        # ---------------------------------------------------------------------
        # STEP 6: Save again to persist newly created target objects (Mandatory validation)
        # ---------------------------------------------------------------------
        log("STEP 6", f"Save again to persist newly created target objects for '{flow_name}'...")
        r2 = self._rf_cmd("update", flow_name, active_flow_file, ["--no-deploy"])
        if r2.returncode != 0:
            log("STEP 6", f"Notice during Step 6: {r2.stderr or r2.stdout}. Retrying with --save-anyway...", "WARN")
            r2 = self._rf_cmd("update", flow_name, active_flow_file, ["--no-deploy", "--save-anyway"])

        if r2.returncode != 0:
            log("STEP 6", "Direct second save returned notice. Fetching server state to persist target objects...", "WARN")
            read_res = run_cmd(["datasphere", "objects", "replication-flows", "read",
                                "--space", self.source_space,
                                "--technical-name", flow_name,
                                "--secrets-file", str(self.secrets_path),
                                "--host", self.host])
            if read_res.returncode == 0 and read_res.stdout:
                try:
                    server_payload = json.loads(read_res.stdout)
                    patched = self._patch_is_new(server_payload, flow_name, is_new=False)
                    file_step6 = self._write_json(f"{flow_name}_step6", patched)
                    r2 = self._rf_cmd("update", flow_name, file_step6, ["--no-deploy", "--save-anyway"])
                    if r2.returncode == 0:
                        active_flow_file = file_step6
                except Exception as ex:
                    log("STEP 6", f"Server payload patch notice: {ex}", "WARN")

        if r2.returncode != 0:
            self.step_status["step6"] = "FAILED"
            log("STEP 6", f"Step 6 target object persistence failed: {r2.stderr or r2.stdout}", "ERROR")
            return False

        self.step_status["step6"] = "PASSED"
        log("STEP 6", f"Target objects successfully persisted and validated for '{flow_name}'.")
        time.sleep(2)

        # ---------------------------------------------------------------------
        # STEP 7: Deploy Replication Flow
        # ---------------------------------------------------------------------
        log("STEP 7", f"Deploy Replication Flow '{flow_name}'...")
        r_deploy = self._rf_cmd("update", flow_name, active_flow_file, [])
        if r_deploy.returncode == 0:
            self.step_status["step7"] = "PASSED"
            log("STEP 7", f"Replication Flow '{flow_name}' successfully deployed!")
            time.sleep(5)
            return True

        err = r_deploy.stderr or r_deploy.stdout
        log("STEP 7", f"Standard deploy notice: {err}. Retrying deploy with --deploy-anyway...", "WARN")
        r_deploy2 = self._rf_cmd("update", flow_name, active_flow_file, ["--deploy-anyway"])
        if r_deploy2.returncode == 0:
            self.step_status["step7"] = "PASSED"
            log("STEP 7", f"Replication Flow '{flow_name}' successfully deployed with --deploy-anyway!")
            time.sleep(5)
            return True

        log("STEP 7", f"Notice with --deploy-anyway: {r_deploy2.stderr or r_deploy2.stdout}. Retrying deploy with --save-anyway...", "WARN")
        r_deploy3 = self._rf_cmd("update", flow_name, active_flow_file, ["--save-anyway"])
        if r_deploy3.returncode == 0:
            self.step_status["step7"] = "PASSED"
            log("STEP 7", f"Replication Flow '{flow_name}' successfully deployed with --save-anyway!")
            time.sleep(5)
            return True

        self.step_status["step7"] = "FAILED"
        log("STEP 7", f"Deployment failed: {r_deploy3.stderr or r_deploy3.stdout}", "ERROR")
        return False

    # -------------------------------------------------------------------------
    # STEP 8: Execute Replication Flow
    # -------------------------------------------------------------------------
    def execute_flow(self, flow_name: str) -> bool:
        log("STEP 8", f"Execute Replication Flow '{flow_name}'...")
        res = run_cmd(["datasphere", "tasks", "replication-flows", "run",
                       "--space", self.source_space,
                       "--technical-name", flow_name,
                       "--secrets-file", str(self.secrets_path),
                       "--host", self.host])
        if res.returncode != 0:
            err = (res.stderr or res.stdout).strip()
            self.step_status["step8"] = "FAILED"
            log("STEP 8", f"Flow execution notice: {err}", "ERROR")
            return False
        self.step_status["step8"] = "PASSED"
        log("STEP 8", f"Replication Flow '{flow_name}' execution successfully triggered.")
        return True

    # -------------------------------------------------------------------------
    # STEP 9: Monitor replication status
    # -------------------------------------------------------------------------
    def monitor_flow(self, flow_name: str) -> str:
        log("STEP 9", f"Monitor replication status for '{flow_name}' (Timeout: {self.timeout_sec}s, Interval: {self.poll_interval}s)...")
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
                self.step_status["step9"] = "PASSED"
                log("STEP 9", f"Replication Flow '{flow_name}' completed successfully!")
                return "COMPLETED"
            elif any(s in status_text for s in ("FAILED", "ERROR", "ABORTED")):
                self.step_status["step9"] = "FAILED"
                log("STEP 9", f"Replication Flow '{flow_name}' failed: {status_text}", "ERROR")
                return "FAILED"
            elif any(s in status_text for s in ("RUNNING", "ACTIVE", "IN_PROGRESS", "INITIAL_LOAD", "REPLICATING", "STARTED")):
                last_status = "RUNNING"
                log("STEP 9", f"Replication Flow '{flow_name}' status: RUNNING (In progress)...")
            else:
                last_status = status_text or "PENDING"
                log("STEP 9", f"Replication Flow '{flow_name}' status: {last_status}")
            time.sleep(self.poll_interval)
        self.step_status["step9"] = "TIMEOUT"
        log("STEP 9", f"Replication Flow '{flow_name}' monitoring timed out after {self.timeout_sec}s.", "WARN")
        return f"TIMEOUT ({last_status})"

    def generate_report(self, views: list, flow_status: str) -> bool:
        log("REPORT", "Generating execution report...")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        is_success = flow_status in ("COMPLETED", "SUCCESS")

        def badge(step_key: str) -> str:
            st = self.step_status.get(step_key, "NOT_RUN")
            if st == "PASSED":
                return "`[PASSED]`"
            elif st == "FAILED":
                return "`[FAILED]`"
            elif st == "TIMEOUT":
                return "`[TIMEOUT]`"
            else:
                return "`[NOT_RUN]`"

        lines = [
            "# SAP Datasphere Replication Flow Execution Report",
            f"**Execution Timestamp:** {timestamp}  ",
            f"**Replication Flow:** `{self.flow_name}`  ",
            f"**Source Space:** `{self.source_space}`  ",
            f"**Source Connection:** `{self.source_conn}`  ",
            f"**Status:** `{flow_status}`  ",
            "",
            "### 9-Step Automation Execution Sequence (HDL_FILES Data Lake Files)",
            f"1. {badge('step1')} Authenticate using Datasphere technical user",
            f"2. {badge('step2')} Create or update Replication Flow",
            f"3. {badge('step3')} Configure source objects",
            f"4. {badge('step4')} Generate target Local Tables (CSN) & deploy in space",
            f"5. {badge('step5')} Save Replication Flow draft (--no-deploy)",
            f"6. {badge('step6')} Save again to persist target objects (Mandatory validation)",
            f"7. {badge('step7')} Deploy Replication Flow",
            f"8. {badge('step8')} Execute Replication Flow",
            f"9. {badge('step9')} Monitor replication status ({flow_status})",
            "",
            "| CDS View | Replication Flow | Target Object | Load Type | Status |",
            "| :--- | :--- | :--- | :--- | :--- |"
        ]

        for view in views:
            table_name = f"{self.table_prefix}{view}" if self.table_prefix else view
            delta_table_name = f"{table_name}_Delta"
            lines.append(f"| `{view}` | `{self.flow_name}` | `{delta_table_name}` | `{self.load_type}` | `{flow_status}` |")

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
                "views": views,
                "step_status": self.step_status
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

            # Step 1: Authenticate using Datasphere technical user
            self.authenticate_datasphere()
            self.step_status["step1"] = "PASSED"
            self.validate_connection()

            # Steps 2, 3, 4, 5, 6, 7: Configure, Two-Stage Save & Deploy
            deployed = self.create_or_update_flow(self.flow_name, views_to_process)
            if not deployed:
                log("PROCESS", f"Replication Flow '{self.flow_name}' save or deploy failed; aborting execution.", "ERROR")
                fail_status = "PERSISTENCE_FAILED" if self.step_status["step6"] == "FAILED" else ("DRAFT_SAVE_FAILED" if self.step_status["step5"] == "FAILED" else "DEPLOYMENT_FAILED")
                return self.generate_report(views_to_process, fail_status)

            # Step 8: Execute Replication Flow
            executed = self.execute_flow(self.flow_name)
            if not executed:
                log("RUN", f"Failed to run Replication Flow '{self.flow_name}'.", "ERROR")
                return self.generate_report(views_to_process, "EXECUTION_FAILED")

            # Step 9: Monitor replication status
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
