# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC **Import Libraries**

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Ingestions**

# COMMAND ----------

# DBTITLE 1,Customers table
customers=spark.table("advcasestudy.`shop-performance`.customers")
customers=customers.toPandas()
display(customers)
                      

# COMMAND ----------

#### Check Duplicates
print("\nDuplicate rows:")
print(customers.duplicated().sum())

# COMMAND ----------

print("\nDuplicate CustomerIDs:")
print(customers["CustomerID"].duplicated().sum())

# Make sure CustomerID is numeric
customers["CustomerID"] = pd.to_numeric(
    customers["CustomerID"],
    errors="coerce"
).astype("Int64")


# COMMAND ----------

customers["Age"] = pd.to_numeric(
    customers["Age"],
    errors="coerce"
)

print("\nAge summary:")
print(customers["Age"].describe())

print("\nInvalid ages:")
print(
    customers[
        (customers["Age"] < 0) |
        (customers["Age"] > 100)
    ]
)

# Keep missing ages as missing.
# We should NOT invent customers' ages.



# COMMAND ----------

## CLEAN UP CITY

customers["City"] = (
    customers["City"]
    .str.strip()
    .str.title()
)

print("\nCities:")
print(customers["City"].value_counts(dropna=False))

# COMMAND ----------

##  CLEAN SIGNUP DATE

customers["SignupDate"] = pd.to_datetime(
    customers["SignupDate"],
    errors="coerce"
)

print("\nInvalid/missing signup dates:")
print(customers["SignupDate"].isnull().sum())

# COMMAND ----------

customers["CustomerSegment"] = (
    customers["CustomerSegment"]
    .str.strip()
    .str.title()
)

print("\nCustomer segments:")
print(
    customers["CustomerSegment"].value_counts(
        dropna=False
    )
)

# COMMAND ----------

##FINAL MISSING VALUE CHECK
print("\nMissing values after cleaning:")
print(customers.isnull().sum())

# COMMAND ----------

### FINAL CHECK


print("\nCleaned customers shape:")
print(customers.shape)

print("\nCleaned customer table:")
print(customers.head())



# COMMAND ----------

##SAVE CLEANED CUSTOMERS

customers.to_csv(
    "cleaned_customers.csv",
    index=False
)

print("\nCleaned customers saved successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC What should we do with the missing values?
# MAGIC For your assignment, I recommend:
# MAGIC - Missing Age (180): keep as missing. Don't make up an age.
# MAGIC - Missing City (119): keep as missing. Don't guess the customer's city.
# MAGIC - Missing SignupDate: none, so nothing to fix.
# MAGIC - Missing CustomerSegment: none, so nothing to fix.
# MAGIC - Duplicates: none, so nothing needs removing.

# COMMAND ----------

# DBTITLE 1,Orders table
orders=spark.table("advcasestudy.`shop-performance`.orders")
Orders=orders.toPandas()
display(orders)

# COMMAND ----------

##CHECK MISSING VALUES

print("\nMissing values:")
print(Orders.isnull().sum())


# COMMAND ----------

##CHECK DUPLICATES

print("\nDuplicate rows:")
print(Orders.duplicated().sum())


# COMMAND ----------

# Remove exact duplicate rows
orders = orders.drop_duplicates()

# COMMAND ----------

##LEAN DATE COLUMN

Orders["OrderDate"] = pd.to_datetime(
    Orders["OrderDate"],
    errors="coerce"
)

print("\nMissing/invalid OrderDate:")
print(Orders["OrderDate"].isnull().sum())




# COMMAND ----------

 ##CHECK QUANTITY


print("\nQuantity summary:")
print(Orders["Quantity"].describe())

print("\nNegative quantities:")
print(Orders[Orders["Quantity"] < 0])

print("\nZero quantities:")
print(Orders[Orders["Quantity"] == 0])


# Convert Quantity to numeric
Orders["Quantity"] = pd.to_numeric(
    Orders["Quantity"],
    errors="coerce"
)

# Remove rows where quantity is missing
Orders = Orders.dropna(subset=["Quantity"])

# Remove zero or negative quantities
Orders = Orders[Orders["Quantity"] > 0]



# COMMAND ----------

# Convert Quantity to numeric
Orders["Quantity"] = pd.to_numeric(
    Orders["Quantity"],
    errors="coerce"
)

# Remove rows where quantity is missing
Orders = Orders.dropna(subset=["Quantity"])

# Remove zero or negative quantities
Orders = Orders[Orders["Quantity"] > 0]


# COMMAND ----------

# 8. CLEAN DISCOUNT

Orders["Discount"] = pd.to_numeric(
    Orders["Discount"],
    errors="coerce"
)

print("\nDiscount summary:")
print(Orders["Discount"].describe())

print("\nInvalid discounts:")
print(
    Orders[
        (Orders["Discount"] < 0) |
        (Orders["Discount"] > 1)
    ]
)

# Treat missing discount as 0%
Orders["Discount"] = Orders["Discount"].fillna(0)

# COMMAND ----------


###CREATE YEAR AND MONTH

Orders["Year"] = Orders["OrderDate"].dt.year

Orders["Month"] = Orders["OrderDate"].dt.month

Orders["YearMonth"] = (
    Orders["OrderDate"]
    .dt.to_period("M")
    .astype(str)
)

# COMMAND ----------

##CREATE REVENUE COLUMN

# IMPORTANT:
# UnitPrice is not in orders.
# We will get UnitPrice after joining with products.

# Revenue formula from the case study:
#
# Quantity × UnitPrice × (1 - Discount)





# COMMAND ----------

###FINAL CHECK


print("\nCleaned Orders shape:")
print(Orders.shape)

print("\nMissing values after cleaning:")
print(Orders.isnull().sum())

print("\nCleaned Orders:")
print(Orders.head())

# COMMAND ----------

# DBTITLE 1,Payments table
payments=spark.table("advcasestudy.`shop-performance`.payments")
payments=payments.toPandas()
display(payments)

# COMMAND ----------

##CHECK DUPLICATES

print("\nDuplicate rows:")
print(payments.duplicated().sum())


# COMMAND ----------

# Remove exact duplicates
payments = payments.drop_duplicates()

# COMMAND ----------

 ##CLEAN PAYMENT DATE


payments["PaymentDate"] = pd.to_datetime(
    payments["PaymentDate"],
    errors="coerce"
)

print("\nMissing/invalid PaymentDate:")
print(payments["PaymentDate"].isnull().sum())



# COMMAND ----------

 ##CLEAN PAYMENT STATUS

payments["PaymentStatus"] = (
    payments["PaymentStatus"]
    .str.strip()
    .str.title()
)

print("\nPayment statuses:")
print(payments["PaymentStatus"].value_counts())


# COMMAND ----------

##CHECK PAYMENT IDs

print("\nDuplicate PaymentIDs:")
print(payments["PaymentID"].duplicated().sum())


# COMMAND ----------

##CHECK ORDER IDs

print("\nDuplicate OrderIDs in payments:")
print(payments["OrderID"].duplicated().sum())


# COMMAND ----------

##CHECK PAYMENT ORDER LINKS

missing_orders = payments[
    ~payments["OrderID"].isin(Orders["OrderID"])
]

print("\nPayments with no matching order:")
print(len(missing_orders))


# COMMAND ----------


## FINAL CHECK

print("\nCleaned payments shape:")
print(payments.shape)

print("\nMissing values after cleaning:")
print(payments.isnull().sum())

print("\nCleaned payments:")
print(payments.shape)
print(payments.head())

# COMMAND ----------

# DBTITLE 1,Products table
products=spark.table("advcasestudy.`shop-performance`.products")
products=products.toPandas()
display(products)

# COMMAND ----------

products=spark.table("advcasestudy.`shop-performance`.products")
products=products.toPandas()
                     

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Exploratory Analysis**

# COMMAND ----------

# MAGIC %md
# MAGIC **1.Products EDA**

# COMMAND ----------

## Show how the table looks like
display(products)

# COMMAND ----------

## .uninque() shows the distinct values in a column
products["ProductName"]

# COMMAND ----------

products["ProductName"].value_counts()

# COMMAND ----------

products["Category"].value_counts()

# COMMAND ----------

## Summary of the table
products.info()

# COMMAND ----------

### Checking for duplicates 
products.duplicated().sum()

# COMMAND ----------

## Gives your stastical summary of the numeric columns
products["UnitPrice"].describe()

# COMMAND ----------

print(products["UnitPrice"].min())
print(products["UnitPrice"].max())
print(products["UnitPrice"].mean())
print(products["UnitPrice"].count())

     

# COMMAND ----------

products["UnitPrice"].describe()

# COMMAND ----------

###CHECK FOR INVALID PRICES
print("\nProducts with zero or negative prices:")

print(
    products[products["UnitPrice"] <= 0]
)

# COMMAND ----------

 ##REMOVE DUPLICATE ROWS

products = products.drop_duplicates()



# COMMAND ----------

###CLEAN TEXT COLUMNS

# Remove unnecessary spaces
products["ProductName"] = products["ProductName"].str.strip()
products["Category"] = products["Category"].str.strip()

# COMMAND ----------

# Make category names consistent
products["Category"] = products["Category"].str.title()

# COMMAND ----------

##CHECK PRODUCT IDs

print("\nDuplicate ProductIDs:")
print(products["ProductID"].duplicated().sum())

# ProductID should be an integer
products["ProductID"] = pd.to_numeric(
    products["ProductID"],
    errors="coerce"
).astype("Int64")




# COMMAND ----------

###ORT PRODUCTS BY PRODUCT ID

products = products.sort_values(
    by="ProductID"
).reset_index(drop=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Important for my assignment**
# MAGIC The case study says i should write down what i found and what i decided to do, rather than blindly deleting data.   
# MAGIC For my actual product file, the initial checks show:
# MAGIC - 20 rows
# MAGIC - 4 columns
# MAGIC - 0 missing values
# MAGIC - 0 duplicate rows
# MAGIC - ProductID is numeric
# MAGIC - UnitPrice is numeric
# MAGIC - No cleaning is needed for missing values or duplicate rows
# MAGIC - Text fields can still be standardised with .str.strip()
# MAGIC - Prices should be checked for values ≤ 0 before analysis

# COMMAND ----------

# DBTITLE 1,Customer tablet
customers.info()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC COMBINE ALL 4 TABLES

# COMMAND ----------

# COMBINE THE 4 CLEANED TABLES


# 1. Orders + Products
master = Orders.merge(
    products,
    on="ProductID",
    how="left",
    validate="many_to_one"
)

print("After joining Products:")
print(master.shape)


# 2. Add Customers
master = master.merge(
    customers,
    on="CustomerID",
    how="left",
    validate="many_to_one"
)

print("After joining Customers:")
print(master.shape)


# 3. Add Payments
master = master.merge(
    payments,
    on="OrderID",
    how="left",
    validate="many_to_one"
)

print("After joining Payments:")
print(master.shape)


# CHECK FOR BROKEN LINKS


print("\nMissing Product information:")
print(master["ProductName"].isna().sum())

print("\nMissing Customer information:")
print(master["CustomerSegment"].isna().sum())

print("\nMissing Payment information:")
print(master["PaymentStatus"].isna().sum())



# CREATE REVENUE

master["Revenue"] = (
    master["Quantity"]
    * master["UnitPrice"]
    * (1 - master["Discount"])
)


# CREATE DATE VARIABLES

master["OrderDate"] = pd.to_datetime(master["OrderDate"])

master["Year"] = master["OrderDate"].dt.year

master["Month"] = master["OrderDate"].dt.month

master["YearMonth"] = (
    master["OrderDate"]
    .dt.to_period("M")
    .astype(str)
)



# DISPLAY MASTER TABLE

print("\nMaster table:")
display(master.head())

print("\nMaster table shape:")
print(master.shape)

# COMMAND ----------

master = Orders.merge(
    products,
    on="ProductID",
    how="left"
)

print("After Products:", master.shape)


master = master.merge(
    customers,
    on="CustomerID",
    how="left"
)

print("After Customers:", master.shape)


master = master.merge(
    payments,
    on="OrderID",
    how="left"
)

print("After Payments:", master.shape)


display(master.head())

# COMMAND ----------

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

display(master)

# COMMAND ----------

# Check for duplicate rows in the master table
duplicate_count = master.duplicated().sum()

print("Number of duplicate rows:", duplicate_count)

# COMMAND ----------

# Before removing duplicates
print("Before:", master.shape)

# Count duplicates
print("Duplicate rows:", master.duplicated().sum())

# Remove duplicates
master = master.drop_duplicates()

# After removing duplicates
print("After:", master.shape)