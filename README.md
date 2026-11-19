# Fully Automated SAP Datasphere Replication Flows

Automate end-to-end SAP Datasphere Replication Flows directly from Git commits using GitHub Actions. All operations execute remotely on GitHub-hosted runners with zero local computer execution.

---

## Architecture & Workflow

```mermaid
flowchart TD
    A["User updates cds_views.txt"] --> B["git push origin main"]
    B --> C["GitHub Actions Triggered (replication.yml)"]
    C --> D["Checkout & Change Detection (git diff)"]
    D --> E["Authenticate with SAP Datasphere (@sap/datasphere-cli)"]
    E --> F["Validate Source Connection & Space"]
    F --> G["Stage 1: Save Draft Flow (--no-deploy, isNew: true)"]
    G --> H["Stage 2: Materialize Target Objects (--no-deploy, isNew: false)"]
    H --> I["Stage 3: Deploy Replication Flow & Targets"]
    I --> J["Stage 4: Execute Replication Tasks (tasks run)"]
    J --> K["Monitor Execution until COMPLETED"]
    K --> L["Generate Execution Report & GitHub Summary"]
```

---

## 1. CDS View Input

Input CDS Views via `cds_views.txt` in the root of the repository:

```text
# Enter one CDS View technical name per line.
# Blank lines and lines starting with '#' are ignored.

I_BILLINGFREQUENCY
I_BILLINGFREQUENCY_TEXT
I_CURRENCYTEXT
```

- **Format:** One CDS View name per line.
- **Consolidation:** All active CDS views are consolidated into a **single Replication Flow** (`RF_CONSOLIDATED` or custom `FLOW_NAME`).
- **Comments & Blank Lines:** Ignored automatically.
- **Change Detection:** Only newly added or changed CDS Views are processed in subsequent runs. Existing flows and targets are preserved.

---

## 2. GitHub Secrets & Variables Configuration

Configure your GitHub repository: **Settings** → **Secrets and variables** → **Actions**.

### Required Secrets (Repository Secrets)

| Secret Name | Description | Example |
| :--- | :--- | :--- |
| `DATASPHERE_HOST` | SAP Datasphere tenant host URL | `https://my-tenant.hcs.cloud.sap` |
| `CLIENT_ID` | OAuth 2.0 Client ID | `sb-clone-123!b456...` |
| `CLIENT_SECRET` | OAuth 2.0 Client Secret | `wXyZ123456...` |
| `AUTHORIZATION_URL` | OAuth 2.0 Authorization URL | `https://my-tenant.authentication.sap.hana.ondemand.com/oauth/authorize` |
| `TOKEN_URL` | OAuth 2.0 Token URL | `https://my-tenant.authentication.sap.hana.ondemand.com/oauth/token` |

### Required Variables or Secrets (Repository Variables)

| Variable Name | Description | Example | Default |
| :--- | :--- | :--- | :--- |
| `SOURCE_SPACE` | Technical ID of the source Datasphere space | `RAD_S3X` | *(Required)* |
| `SOURCE_CONNECTION` | Technical ID of the source connection | `S3XReplicationflows` | *(Required)* |
| `SOURCE_CONTAINER` | Source container/namespace | `CDS_EXTRACTION` | `CDS_EXTRACTION` |
| `TARGET_CONNECTION` | Target connection technical ID | `DWC_HDLF` | `DWC_HDLF` |
| `FLOW_NAME` | Custom name for the consolidated Replication Flow | `RF_CONSOLIDATED` | `RF_CONSOLIDATED` |
| `LOAD_TYPE` | Replication load mode (`INITIAL` or `INITIAL_AND_DELTA`) | `INITIAL` | `INITIAL` |
| `STATUS_TIMEOUT_SEC` | Maximum seconds to wait for flow run completion | `600` | `600` |
| `STATUS_POLL_INTERVAL_SEC`| Seconds between status checks | `15` | `15` |

---

## 3. Official 9-Step Technical User Automation Sequence

When automating Replication Flow creation with a Datasphere technical user, the automation follows this exact 9-step sequence:

| Step | Operation | Implementation Detail |
| :--- | :--- | :--- |
| **1** | **Authenticate using Datasphere technical user** | Headless OAuth2 client credentials pre-fetch & `@sap/datasphere-cli login` |
| **2** | **Create or update Replication Flow** | Checks space for existing technical name & builds consolidated payload |
| **3** | **Configure source objects** | Maps source CDS view columns, keys, and extraction container |
| **4** | **Generate target Local Tables** | Defines Delta Lake target structures with CDC `Change_Type` and `Change_Date` |
| **5** | **Save Replication Flow** | Initial save with `--no-deploy` and `isNew: true` (registers target objects) |
| **6** | **Save again to persist newly created target objects** | **Critical Step**: Re-saves with `--no-deploy` and `isNew: false` to persist targets |
| **7** | **Deploy Replication Flow** | Deploys flow and targets via `update` (with `--deploy-anyway` / `--save-anyway`) |
| **8** | **Execute Replication Flow** | Triggers execution task via `datasphere tasks replication-flows run` |
| **9** | **Monitor replication status** | Polls `datasphere tasks replication-flows status` until `COMPLETED` |

> [!IMPORTANT]
> **Why Step 6 is essential:**  
> Successful creation of the Replication Flow (Step 5) registers target objects in draft mode. Datasphere requires the flow to be saved once again (Step 6) to persist the newly created target objects in the repository before they are ready for deployment (Step 7). Skipping Step 6 leaves target objects uncommitted and causes deployment to fail with *"There are unsaved target objects. Please save again"*.

---

## 4. Manual Execution & Overrides

You can trigger the workflow manually from the **Actions** tab in GitHub:
- Click **SAP Datasphere Replication Flows** → **Run workflow**.
- Check **Force replication of all CDS Views** to bypass git diff and re-replicate all views declared in `cds_views.txt`.
