# HR Data Cleaning & Validation Pipeline

A complete, end-to-end **data cleaning and validation pipeline** for HR datasets, implemented in Python using `pandas`.

This project takes a raw HR dataset and transforms it into a fully cleaned, validated, and structured dataset ready for analytics or modeling.

---

## 📁 Project Structure

```
hr-data-cleaning-pipeline/
│
├── data/
│   └── raw/
│       └── hr_raw.csv           # only raw data is versioned in Git
│
├── src/
│   └── pipeline.py              # main cleaning & validation script
│
└── README.md
```

You can optionally ignore generated files:

```
data/interim/
data/processed/
```

---

## 🔄 Pipeline Overview (7 Steps)

The project follows a clear, modular 7-step pipeline.

---

### **1️⃣ Load Raw Dataset**

Load the raw CSV file from:

```
data/raw/hr_raw.csv
```

---

### **2️⃣ Handle Missing Values**

Missing values are cleaned as follows:

#### 🔹 Numeric columns  
Filled using **column mean**

#### 🔹 Text / categorical columns  
Filled using **"Unknown"**

---

### **3️⃣ Remove Duplicates & Enforce Unique IDs**

- Remove fully duplicated rows  
- Ensure uniqueness of:
  - `EmpID`
  - `EmployeeNumber`

---

### **4️⃣ Fix Data Types**

Assign proper data types:

- Numeric → `int` / `float`  
- Categorical → `category`  
- EmpID → `string`

---

### **5️⃣ Detect & Flag Outliers (IQR Method)**

Outliers are detected using the **Interquartile Range (IQR)** method.

Example generated columns:

```
MonthlyIncome_outlier
DistanceFromHome_outlier
```

---

### **6️⃣ Data Quality & Business Rule Validation**

Rules applied:

| Rule | Description |
|------|-------------|
| empid_not_null | EmpID must not be empty |
| empid_unique | EmpID must be unique |
| age_positive | Age > 0 |
| age_reasonable | Age must be between 16–70 |
| monthly_income_positive | No negative incomes |
| email_format | Must match basic email regex |
| hire_before_termination | HireDate must be ≤ TerminationDate |
| junior_not_manager | JobLevel 1 cannot have "manager" role |

Validation report is saved to:

```
data/processed/validation_report.csv
```

---

### **7️⃣ Save Final Outputs**

The pipeline outputs:

```
data/interim/hr_with_outliers_flags.csv
data/processed/hr_final.csv
data/processed/validation_report.csv
```

Only the **raw data** should remain in the repository.

---

## ▶️ Running the Pipeline

```
python src/pipeline.py
```

Install dependencies:

```
pip install pandas
```

---

## 📦 Recommended Files

### ✔ `.gitignore`

```
data/interim/
data/processed/
__pycache__/
*.ipynb_checkpoints
```

### ✔ `requirements.txt`

```
pandas
```
