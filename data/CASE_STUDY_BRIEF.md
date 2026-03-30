## **Background**

You are joining a SaaS company that provides a contract management platform. The platform generates product usage events and stores user/account data.

The business goal for this case study is to produce a small set of analytics-ready tables that answer a concrete question:

- *“How is document signing activity trending per account (and per plan/country)?”*

In practice, this means turning raw event logs + account data into:

- Clean, deduplicated base tables (raw/staging)
- A well-defined analytics layer that a BI tool or analyst can use directly

Constraints to consider:

- The raw data contains personal/sensitive data
- Data quality issues occur in production
- Analysts need clean, trustworthy models

Your task is to design and implement a small but production-quality pipeline that ingests the CSVs, applies basic governance/quality, and outputs an analytics layer that supports the business question above. If part of the task costs too much effort try to elaborate what you would do, why and how as a comment so we can understand your thinking.

## **Provided Data**

You receive two CSV files in the `data/` directory:

### **events.csv**

Product usage events exported from the application. Each event row includes user information as it was at the time of the event.

| **Column**   | **Description**                          |
| ------------ | ---------------------------------------- |
| event_id     | Unique event identifier                  |
| user_id      | The user who triggered the event         |
| user_name    | Full name of the user                    |
| user_email   | Email of the user                        |
| user_phone   | Phone number of the user (may be empty)  |
| user_country | Two-letter country code of the user      |
| account_id   | The account the user belongs to          |
| ip_address   | IP address of the request                |
| event_type   | Type of event (login, document_created, document_signed, etc.) |
| timestamp    | When the event occurred                  |
| user_agent   | Browser/client information               |

### **accounts.csv**

Account/company data from the billing system.

| **Column**   | **Description**                          |
| ------------ | ---------------------------------------- |
| account_id   | Unique account identifier                |
| company_name | Company name                             |
| plan         | Subscription plan (free, starter, business, enterprise) |
| country      | Two-letter country code                  |
| created_date | When the account was created             |
| is_paying    | Whether the account is on a paid plan    |

**Note:** The data contains quality issues. Discovering and handling them is part of the task.

## **Requirements**

### **1. Data Ingestion**

### **2. Data Modeling**

Transform the raw data into an analytics layer that answers the business question from the Background.

You can use any approach (SQL, Python, dbt, etc.). What matters is the structure and clarity of the models.

### **3. Data Quality**

- Identify current data quality issues in the provided CSVs and how you could handle them
- Suggest and if possible add *preventative* checks so these issues are detected in future runs

We’re not expecting a perfect “enterprise” setup—just evidence that you think about preventing regressions, not only fixing today’s dataset.

### **Bonus (optional)**

To avoid time pressure, pick **at most one** bonus item. A strong core submission is better than rushed extras.

Choose one:

- **Orchestration**: Orchestrate the pipeline steps (the specific tool is up to you)
- **Containerization**: Provide a Dockerfile or docker-compose setup so someone can run the pipeline with a single command
- **Data lineage**: Document how data flows from source to analytics

## **Deliverables**

Submit a git repository with:

```
repo/
  data/              # provided CSV files
  ingestion/         # Ingestion code
  main/              # Everything handling after the ingestion
  orchestration/     # (bonus) pipeline orchestration
  containerization/  # (bonus) containerization
  README.md          # documentation

```

### **README should cover:**

- How to set up and run the pipeline
- Architecture decisions and trade-offs
- Data quality issues you found and how you handled them
- What you would do differently in a production environment

## **Time Expectation**

We expect this to take approximately **4-6 hours** for the required parts. The bonus tasks are truly optional -- a well-executed core submission is better than a rushed complete one.

## **Questions?**

If anything is unclear, please reach out. We would rather you ask than make incorrect assumptions.