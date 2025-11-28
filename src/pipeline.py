HR Data Cleaning & Validation Pipeline

# =====================================================
# 0) IMPORTS
# =====================================================
import pandas as pd
import re
from pathlib import Path

# =====================================================
# 1) CONFIG – Paths
# =====================================================

# Use __file__ if running as a script, else fallback
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()  # Fallback for Jupyter or interactive env

DATA_DIR = BASE_DIR / "data"
RAW_PATH = DATA_DIR / "raw" / "hr_raw.csv"
INTERIM_PATH = DATA_DIR / "interim" / "hr_with_outliers_flags.csv"
FINAL_PATH = DATA_DIR / "processed" / "hr_final.csv"
VALID_REPORT_PATH = DATA_DIR / "processed" / "validation_report.csv"

# Ensure directories exist
(DATA_DIR / "interim").mkdir(parents=True, exist_ok=True)
(DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)

# =====================================================
# 2) LOAD RAW DATA
# =====================================================
df = pd.read_csv(RAW_PATH)

# =====================================================
# 3) HANDLE MISSING VALUES
# =====================================================
numeric_cols_initial = [
    "DistanceFromHome", "HourlyRate", "NumCompaniesWorked",
    "PercentSalaryHike", "TotalWorkingYears", "YearsAtCompany",
    "YearsSinceLastPromotion", "YearsWithCurrManager"
]
for col in numeric_cols_initial:
    if col in df.columns:
        df[col] = df[col].fillna(df[col].mean())

text_cols = [
    "AgeGroup", "Attrition", "BusinessTravel", "Department",
    "Education", "EducationField", "JobRole", "MaritalStatus",
    "Over18", "OverTime"
]
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].fillna("Unknown")

numeric_cols_remaining = [
    "DailyRate", "EmployeeCount", "EmployeeNumber",
    "EnvironmentSatisfaction", "JobInvolvement", "JobLevel",
    "JobSatisfaction", "MonthlyIncome", "MonthlyRate",
    "PerformanceRating", "StandardHours", "StockOptionLevel",
    "WorkLifeBalance", "RelationshipSatisfaction"
]
for col in numeric_cols_remaining:
    if col in df.columns:
        df[col] = df[col].fillna(df[col].mean())

# =====================================================
# 4) REMOVE DUPLICATES & ENFORCE UNIQUE IDs
# =====================================================
df = df.drop_duplicates()
id_cols = ["EmpID", "EmployeeNumber"]
for col in id_cols:
    if col in df.columns:
        df = df.drop_duplicates(subset=[col])

# =====================================================
# 5) FIX DATA TYPES
# =====================================================
numeric_columns = [
    "Age", "DailyRate", "DistanceFromHome", "Education", "EmployeeCount",
    "EmployeeNumber", "EnvironmentSatisfaction", "HourlyRate",
    "JobInvolvement", "JobLevel", "JobSatisfaction", "MonthlyIncome",
    "MonthlyRate", "NumCompaniesWorked", "PercentSalaryHike",
    "PerformanceRating", "RelationshipSatisfaction", "StandardHours",
    "StockOptionLevel", "TotalWorkingYears", "TrainingTimesLastYear",
    "WorkLifeBalance", "YearsAtCompany", "YearsInCurrentRole",
    "YearsSinceLastPromotion", "YearsWithCurrManager"
]
for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

categorical_columns = [
    "BusinessTravel", "Department", "EducationField", "Gender",
    "JobRole", "MaritalStatus", "Over18", "OverTime",
    "AgeGroup", "SalarySlab"
]
for col in categorical_columns:
    if col in df.columns:
        df[col] = df[col].astype("category")

if "EmpID" in df.columns:
    df["EmpID"] = df["EmpID"].astype(str)

# =====================================================
# 6) OUTLIER DETECTION (IQR)
# =====================================================
def detect_outliers_iqr(data, column):
    """Detect outliers using IQR method and create a flag column."""
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
    flag_col = f"{column}_outlier"
    data[flag_col] = ((data[column] < lower) | (data[column] > upper)).astype(int)
    return data

numeric_cols_outliers = [
    "DailyRate", "DistanceFromHome", "MonthlyIncome", "MonthlyRate",
    "PercentSalaryHike", "TotalWorkingYears", "YearsWithCurrManager"
]
for col in numeric_cols_outliers:
    if col in df.columns:
        df = detect_outliers_iqr(df, col)

df.to_csv(INTERIM_PATH, index=False)

# =====================================================
# 7) DATA VALIDATION RULES
# =====================================================
def is_positive(series): return series > 0
def in_range(series, lo, hi): return series.between(lo, hi)
def match_regex(series, pattern): return series.astype(str).str.match(pattern, na=False)
def not_null(series): return ~series.isnull()

rules = [
    ("empid_not_null", lambda s: not_null(s), ["EmpID"], "EmpID must not be null"),
    ("empid_unique", lambda s: ~s.duplicated(), ["EmpID"], "EmpID must be unique"),
    ("age_positive", lambda s: is_positive(s), ["Age"], "Age must be positive"),
    ("age_range", lambda s: in_range(s, 16, 70), ["Age"], "Age must be 16–70"),
    ("income_positive", lambda s: s >= 0, ["MonthlyIncome"], "Income cannot be negative"),
    ("email_format", lambda s: match_regex(s, r"^[^@]+@[^@]+\.[^@]+$"), ["WorkEmail"], "Invalid email format"),
    ("hire_before_termination",
     lambda df_: pd.to_datetime(df_["HireDate"], errors="coerce") <= 
                 pd.to_datetime(df_["TerminationDate"], errors="coerce"),
     ["HireDate", "TerminationDate"], "HireDate must be <= TerminationDate"),
    ("junior_not_manager",
     lambda df_: ~((df_["JobLevel"] == 1) & (df_["JobRole"].str.lower().str.contains("manager"))),
     ["JobLevel", "JobRole"], "JobLevel 1 cannot have a manager role")
]

violations = []
for rule_name, rule_func, cols, message in rules:
    if not all(c in df.columns for c in cols):
        continue
    try:
        mask = rule_func(df[cols[0]] if len(cols)==1 else df)
        failed = df.index[~mask.fillna(False)]
    except Exception:
        continue
    for idx in failed:
        violations.append({
            "index": idx,
            "EmpID": df.at[idx, "EmpID"] if "EmpID" in df.columns else None,
            "rule": rule_name,
            "columns": ",".join(cols),
            "message": message,
            "values": df.loc[idx, cols].to_dict()
        })

validation_report = pd.DataFrame(violations)
validation_report.to_csv(VALID_REPORT_PATH, index=False)

# =====================================================
# 8) SAVE FINAL OUTPUT
# =====================================================
df.to_csv(FINAL_PATH, index=False)

print("✅ Pipeline completed successfully.")
print(f"Final dataset shape: {df.shape}")
print(f"Validation errors found: {len(validation_report)}")
