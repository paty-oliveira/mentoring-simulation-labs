# Mentoring Simulation Labs — dbt E-commerce Project

## Project overview

Simulates a simple e-commerce data warehouse with four raw entities loaded as dbt **seeds** (no external data source required):

| Seed              | Description         |
| ----------------- | ------------------- |
| `raw_customers`   | 10 customer records |
| `raw_products`    | 10 product records  |
| `raw_orders`      | 20 order headers    |
| `raw_order_items` | 40 order line items |

### Model layers

```
seeds/                        ← raw CSV data (no Snowflake source needed)
models/
  staging/                    ← clean & rename (views)
    stg_customers
    stg_products
    stg_orders
    stg_order_items
  intermediate/               ← join & enrich (views)
    int_order_items_enriched
  marts/                      ← business-ready tables
    fct_orders
    fct_order_items
    dim_customers
    dim_products
macros/                       ← utility macros
  generate_schema_name        ← overrides dbt Cloud schema
```

---

## Setup

### Option A — dbt Cloud (recommended)

1. **Create a dbt Cloud account** at [cloud.getdbt.com](https://cloud.getdbt.com) and start a new project.

2. **Connect to Snowflake** in the dbt Cloud UI (_Account settings → Connections_). You will need:
   - Account identifier (e.g. `xy12345.us-east-1`)
   - Database, warehouse, role, and schema (use `dbt_<your_name>` as your personal dev schema)

3. **Link this repository** as the dbt Cloud project repository (_Project settings → Repository_).

4. **Open the dbt Cloud IDE**, then run the following commands in the command bar:

   ```
   dbt deps
   dbt seed
   dbt run
   dbt test
   ```

---

### Option B — local dbt Core

#### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows
```

#### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

#### 3. Configure your Snowflake profile

Copy `profiles.yml` to `~/.dbt/profiles.yml` and fill in your credentials:

```yaml
ecommerce:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account> # e.g. xy12345.us-east-1
      user: <your_user>
      password: <your_password>
      role: <your_role> # e.g. TRANSFORMER
      database: <your_database>
      warehouse: <your_warehouse>
      schema: dbt_dev
      threads: 4
```

#### 3. Install packages

```bash
dbt deps
```

#### 4. Load seed data

```bash
dbt seed
```

#### 5. Run all models

```bash
dbt run
```

#### 6. Run tests

```bash
dbt test
```
