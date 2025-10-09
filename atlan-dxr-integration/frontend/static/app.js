const FIELD_GROUPS = [
  {
    id: "dxr",
    title: "Data X-Ray configuration",
    description:
      "Provide DXR API access along with sampling limits for downstream summaries.",
    fields: [
      {
        env: "DXR_BASE_URL",
        target: "dxr_base_url",
        label: "DXR base URL",
        type: "url",
        required: true,
        default: "https://atlan.dataxray.io/api",
        placeholder: "https://acme.dataxray.io/api",
        help: "Use the API base URL (include the /api suffix).",
      },
      {
        env: "DXR_PAT",
        target: "dxr_pat",
        label: "DXR personal access token",
        type: "password",
        required: true,
        default: "",
        placeholder: "dxr_pat_...",
        help: "Generated from DXR → Settings → Personal access tokens.",
      },
      {
        env: "DXR_CLASSIFICATION_TYPES",
        target: "dxr_classification_types",
        label: "Classification types",
        type: "text",
        required: false,
        default: "LABEL,ANNOTATOR",
        placeholder: "LABEL,ANNOTATOR",
        transform: "csv-list",
        help: "Comma separated whitelist. Leave empty to ingest every classification.",
      },
      {
        env: "DXR_SAMPLE_FILE_LIMIT",
        target: "dxr_sample_file_limit",
        label: "Sample file limit",
        type: "number",
        required: true,
        default: 5,
        min: 0,
        help: "Number of example files to attach to each dataset summary.",
      },
      {
        env: "DXR_FILE_FETCH_LIMIT",
        target: "dxr_file_fetch_limit",
        label: "Per-label fetch cap",
        type: "number",
        required: true,
        default: 200,
        min: 0,
        help: "Maximum files fetched from DXR for a single classification (0 = unlimited).",
      },
    ],
  },
  {
    id: "atlan",
    title: "Atlan configuration",
    description: "Define the Atlan workspace and naming conventions for generated assets.",
    fields: [
      {
        env: "ATLAN_BASE_URL",
        target: "atlan_base_url",
        label: "Atlan base URL",
        type: "url",
        required: true,
        default: "https://workspace.atlan.com",
        placeholder: "https://ohalo-partner.atlan.com",
      },
      {
        env: "ATLAN_API_TOKEN",
        target: "atlan_api_token",
        label: "Atlan API token",
        type: "password",
        required: true,
        default: "",
        placeholder: "atlan_api_...",
        help: "Token scoped with connection-admin permissions.",
      },
      {
        env: "ATLAN_GLOBAL_CONNECTION_QUALIFIED_NAME",
        target: "atlan_global_connection_qualified_name",
        label: "Global connection qualified name",
        type: "text",
        required: true,
        default: "default/custom/dxr-unstructured-attributes",
      },
      {
        env: "ATLAN_GLOBAL_CONNECTION_NAME",
        target: "atlan_global_connection_name",
        label: "Global connection name",
        type: "text",
        required: true,
        default: "dxr-unstructured-attributes",
      },
      {
        env: "ATLAN_GLOBAL_CONNECTOR_NAME",
        target: "atlan_global_connector_name",
        label: "Global connector name",
        type: "text",
        required: true,
        default: "custom",
      },
      {
        env: "ATLAN_GLOBAL_DOMAIN",
        target: "atlan_global_domain_name",
        label: "Global domain (optional)",
        type: "text",
        required: false,
        default: "DXR Unstructured",
      },
      {
        env: "ATLAN_DATABASE_NAME",
        target: "atlan_database_name",
        label: "Database name",
        type: "text",
        required: true,
        default: "dxr",
      },
      {
        env: "ATLAN_SCHEMA_NAME",
        target: "atlan_schema_name",
        label: "Schema name",
        type: "text",
        required: true,
        default: "labels",
      },
      {
        env: "ATLAN_DATASET_PATH_PREFIX",
        target: "atlan_dataset_path_prefix",
        label: "Dataset path prefix",
        type: "text",
        required: false,
        default: "dxr",
        help: "Appended after the schema in dataset qualified names.",
      },
      {
        env: "ATLAN_DATASOURCE_CONNECTION_PREFIX",
        target: "atlan_datasource_connection_prefix",
        label: "Datasource connection prefix",
        type: "text",
        required: true,
        default: "dxr-datasource",
      },
      {
        env: "ATLAN_DATASOURCE_DOMAIN_PREFIX",
        target: "atlan_datasource_domain_prefix",
        label: "Datasource domain prefix (optional)",
        type: "text",
        required: false,
        default: "DXR",
      },
      {
        env: "ATLAN_BATCH_SIZE",
        target: "atlan_batch_size",
        label: "Atlan batch size",
        type: "number",
        required: true,
        default: 20,
        min: 1,
        help: "Maximum asset records pushed per upsert request.",
      },
      {
        env: "ATLAN_TAG_NAMESPACE",
        target: "atlan_tag_namespace",
        label: "Atlan tag namespace",
        type: "text",
        required: true,
        default: "DXR",
      },
    ],
  },
  {
    id: "logging",
    title: "Runtime options",
    description: "Control verbosity for troubleshooting.",
    fields: [
      {
        env: "LOG_LEVEL",
        target: "log_level",
        label: "Log level",
        type: "text",
        required: true,
        default: "INFO",
        transform: "uppercase",
      },
    ],
  },
];

const STORAGE_KEY = "dxr_app_config_v1";
const CONFIG_ID = "dxr-workflow-config";
const CONFIG_ENDPOINT = `/workflows/v1/config/${CONFIG_ID}`;

const DEFAULT_ENV_VALUES = FIELD_GROUPS.flatMap((group) => group.fields).reduce(
  (acc, field) => {
    acc[field.env] = field.default ?? "";
    return acc;
  },
  {},
);

const STATUS_ELEMENT = document.getElementById("status-banner");
const VERSION_ELEMENT = document.getElementById("app-version");
const FORM_ELEMENT = document.getElementById("config-form");
const SECTIONS_CONTAINER = document.getElementById("form-sections");
const RUN_BUTTON = document.getElementById("run-workflow");
const RESET_BUTTON = document.getElementById("reset-defaults");
const CLEAR_STORAGE_BUTTON = document.getElementById("clear-storage");

function renderForm() {
  SECTIONS_CONTAINER.innerHTML = "";

  FIELD_GROUPS.forEach((group) => {
    const section = document.createElement("section");
    section.className = "form-section";

    const title = document.createElement("h2");
    title.textContent = group.title;
    section.appendChild(title);

    if (group.description) {
      const description = document.createElement("p");
      description.textContent = group.description;
      section.appendChild(description);
    }

    const grid = document.createElement("div");
    grid.className = "fieldset-grid";

    group.fields.forEach((field) => {
      const wrapper = document.createElement("div");
      wrapper.className = "form-field";
      wrapper.dataset.env = field.env;

      const label = document.createElement("label");
      label.setAttribute("for", field.env);
      label.textContent = field.required ? `${field.label} *` : field.label;
      wrapper.appendChild(label);

      const control = field.multiline
        ? document.createElement("textarea")
        : document.createElement("input");

      control.id = field.env;
      control.name = field.env;
      control.dataset.target = field.target;
      control.placeholder = field.placeholder ?? "";
      control.autocomplete = "off";

      if (field.type === "number") {
        control.type = "number";
        if (typeof field.min === "number") {
          control.min = String(field.min);
        }
      } else if (field.type === "password") {
        control.type = "password";
        control.autocomplete = "new-password";
      } else if (field.type === "url") {
        control.type = "url";
      } else {
        control.type = "text";
      }

      if (field.required) {
        control.required = true;
      }

      control.addEventListener("input", () => {
        control.classList.remove("invalid");
        saveBrowserConfig(readFormEnvValues());
      });

      wrapper.appendChild(control);

      if (field.help) {
        const help = document.createElement("span");
        help.className = "help-text";
        help.textContent = field.help;
        wrapper.appendChild(help);
      }

      grid.appendChild(wrapper);
    });

    section.appendChild(grid);
    SECTIONS_CONTAINER.appendChild(section);
  });
}

function loadBrowserConfig() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return {};
    }
    return JSON.parse(raw);
  } catch (error) {
    console.warn("Failed to parse stored configuration:", error);
    return {};
  }
}

function saveBrowserConfig(envValues) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(envValues));
  } catch (error) {
    console.warn("Unable to persist configuration locally:", error);
  }
}

function applyConfig(envValues) {
  FIELD_GROUPS.forEach((group) => {
    group.fields.forEach((field) => {
      const input = document.getElementById(field.env);
      if (!input) {
        return;
      }
      const value = envValues[field.env];
      input.value = value !== undefined && value !== null ? String(value) : "";
      input.classList.remove("invalid");
    });
  });
}

function convertConfigToEnv(config) {
  const envValues = {};
  FIELD_GROUPS.forEach((group) => {
    group.fields.forEach((field) => {
      if (!(field.target in config)) {
        return;
      }

      const rawValue = config[field.target];
      if (rawValue === undefined || rawValue === null) {
        return;
      }

      let value = rawValue;
      if (field.transform === "csv-list" && Array.isArray(value)) {
        value = value.join(", ");
      }

      envValues[field.env] = String(value);
    });
  });
  return envValues;
}

async function fetchServerConfig() {
  try {
    const response = await fetch(CONFIG_ENDPOINT);
    if (!response.ok) {
      return {};
    }

    const result = await response.json();
    if (!result?.success || !result?.data) {
      return {};
    }

    return convertConfigToEnv(result.data);
  } catch (error) {
    console.warn("Failed to fetch stored configuration:", error);
    return {};
  }
}

async function persistServerConfig(payload) {
  try {
    const response = await fetch(CONFIG_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorMessage = await response.text();
      throw new Error(`Save failed: ${response.status} ${errorMessage}`);
    }

    return true;
  } catch (error) {
    console.error("Failed to persist configuration:", error);
    return false;
  }
}

function readFormEnvValues() {
  const values = {};
  FIELD_GROUPS.forEach((group) => {
    group.fields.forEach((field) => {
      const input = document.getElementById(field.env);
      if (!input) {
        return;
      }
      values[field.env] = input.value ?? "";
    });
  });
  return values;
}

function setStatus(type, message) {
  if (!STATUS_ELEMENT) {
    return;
  }
  STATUS_ELEMENT.className = `status ${type ?? ""}`.trim();
  STATUS_ELEMENT.textContent = message ?? "";
}

function clearStatus() {
  if (!STATUS_ELEMENT) {
    return;
  }
  STATUS_ELEMENT.className = "status hidden";
  STATUS_ELEMENT.textContent = "";
}

function parseFieldValue(field, rawValue) {
  if (rawValue === undefined || rawValue === null) {
    return { empty: true };
  }

  if (Array.isArray(rawValue)) {
    if (field.transform === "csv-list") {
      const options = rawValue
        .map((item) => String(item).trim())
        .filter(Boolean)
        .map((item) => item.toUpperCase());
      return { ok: true, value: options.length ? options : null };
    }
    return { ok: true, value: rawValue };
  }

  let value = rawValue;
  if (typeof value === "string") {
    value = value.trim();
    if (!value) {
      return { empty: true };
    }
  }

  if (field.type === "number") {
    const parsed = Number(value);
    if (Number.isNaN(parsed)) {
      return { error: "Enter a valid number" };
    }
    return { ok: true, value: parsed };
  }

  if (field.transform === "csv-list") {
    const options = String(value)
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => item.toUpperCase());
    return { ok: true, value: options.length ? options : null };
  }

  if (field.transform === "uppercase") {
    return { ok: true, value: String(value).toUpperCase() };
  }

  return { ok: true, value: String(value) };
}

function validateAndBuildPayload() {
  const payload = {};
  let firstInvalidInput = null;

  FIELD_GROUPS.forEach((group) => {
    group.fields.forEach((field) => {
      const input = document.getElementById(field.env);
      if (!input) {
        return;
      }

      const rawValue = input.value;
      input.classList.remove("invalid");

      const result = parseFieldValue(field, rawValue);

      if (result.error) {
        input.classList.add("invalid");
        if (!firstInvalidInput) {
          firstInvalidInput = input;
        }
        return;
      }

      if (result.empty) {
        if (field.required && !firstInvalidInput) {
          input.classList.add("invalid");
          firstInvalidInput = input;
        }
        return;
      }

      payload[field.target] = result.value;
    });
  });

  if (firstInvalidInput) {
    firstInvalidInput.focus({ preventScroll: false });
    const label = firstInvalidInput.labels?.[0]?.textContent ?? "A required field";
    throw new Error(`${label.replace("*", "").trim()} is required.`);
  }

  return payload;
}

function convertEnvToPayload(envValues) {
  const payload = {};

  FIELD_GROUPS.forEach((group) => {
    group.fields.forEach((field) => {
      const rawValue = envValues[field.env];
      const result = parseFieldValue(field, rawValue);

      if (result.error || result.empty) {
        return;
      }

      payload[field.target] = result.value;
    });
  });

  return payload;
}

async function runWorkflow(event) {
  event.preventDefault();
  clearStatus();

  try {
    const payload = validateAndBuildPayload();
    saveBrowserConfig(readFormEnvValues());

    const persisted = await persistServerConfig(payload);
    if (!persisted) {
      setStatus(
        "error",
        "Unable to store configuration in the Temporal state store. The workflow run will continue with local values.",
      );
    } else {
      setStatus("info", "Starting workflow…");
    }

    RUN_BUTTON.disabled = true;

    const response = await fetch("/workflows/v1/start", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    const result = await response.json().catch(() => ({}));

    if (!response.ok || !result.success) {
      const errorMessage = result?.message ?? `Failed to start workflow (HTTP ${response.status}).`;
      throw new Error(errorMessage);
    }

    const workflowId = result?.data?.workflow_id ?? "unknown";
    setStatus("success", `Workflow started successfully. Temporal workflow ID: ${workflowId}`);
  } catch (error) {
    console.error("Failed to start workflow", error);
    setStatus("error", error?.message ?? "Failed to start workflow.");
  } finally {
    RUN_BUTTON.disabled = false;
  }
}

async function resetToDefaults() {
  applyConfig(DEFAULT_ENV_VALUES);
  saveBrowserConfig(DEFAULT_ENV_VALUES);
  const payload = convertEnvToPayload(DEFAULT_ENV_VALUES);
  const persisted = await persistServerConfig(payload);
  setStatus(
    persisted ? "info" : "error",
    persisted
      ? "Configuration reset to template defaults."
      : "Unable to persist defaults to the Temporal state store.",
  );
}

async function clearStoredValues() {
  localStorage.removeItem(STORAGE_KEY);
  applyConfig(DEFAULT_ENV_VALUES);
  saveBrowserConfig(DEFAULT_ENV_VALUES);
  const payload = convertEnvToPayload(DEFAULT_ENV_VALUES);
  const persisted = await persistServerConfig(payload);
  setStatus(
    persisted ? "info" : "error",
    persisted
      ? "Saved values cleared. Defaults restored for the next session."
      : "Local values cleared, but server persistence failed.",
  );
}

function updateAppMeta() {
  if (!VERSION_ELEMENT) {
    return;
  }
  const host = window.location.origin;
  VERSION_ELEMENT.textContent = `Temporal gateway: ${host}`;
}

async function initialise() {
  renderForm();

  const [serverEnv, browserEnv] = await Promise.all([
    fetchServerConfig(),
    Promise.resolve(loadBrowserConfig()),
  ]);

  const initialEnv = { ...DEFAULT_ENV_VALUES, ...serverEnv, ...browserEnv };
  applyConfig(initialEnv);
  saveBrowserConfig(initialEnv);
  updateAppMeta();
  clearStatus();

  FORM_ELEMENT?.addEventListener("submit", runWorkflow);
  RESET_BUTTON?.addEventListener("click", (event) => {
    event.preventDefault();
    resetToDefaults();
  });
  CLEAR_STORAGE_BUTTON?.addEventListener("click", (event) => {
    event.preventDefault();
    clearStoredValues();
  });
}

initialise().catch((error) => {
  console.error("Failed to initialise configuration UI", error);
  setStatus("error", "Failed to initialise the configuration UI. Check console for details.");
});
