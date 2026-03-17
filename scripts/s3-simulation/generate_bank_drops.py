
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import io
import random
from datetime import date, timedelta

# -------------------------------------------------------
# CLIENT BANK DATA
# Each client maps to its S3 folder and its debtor/loan pairs
# -------------------------------------------------------

CLIENT_DATA = {
    "CLT-001": {
        "s3_folder": "client-drops/CLT-001/",
        "loans": [
            ("DBT-0003", "LN-00003"), ("DBT-0004", "LN-00004"),
            ("DBT-0005", "LN-00005"), ("DBT-0005", "LN-00235"),
            ("DBT-0011", "LN-00011"), ("DBT-0012", "LN-00012"),
            ("DBT-0012", "LN-00217"), ("DBT-0013", "LN-00250"),
            ("DBT-0013", "LN-00013"), ("DBT-0014", "LN-00014"),
            ("DBT-0016", "LN-00016"), ("DBT-0021", "LN-00021"),
            ("DBT-0022", "LN-00022"), ("DBT-0023", "LN-00023"),
            ("DBT-0024", "LN-00024"), ("DBT-0041", "LN-00041"),
            ("DBT-0043", "LN-00043"), ("DBT-0043", "LN-00215"),
            ("DBT-0048", "LN-00048"), ("DBT-0054", "LN-00054"),
            ("DBT-0058", "LN-00058"), ("DBT-0058", "LN-00205"),
            ("DBT-0066", "LN-00066"), ("DBT-0066", "LN-00248"),
            ("DBT-0067", "LN-00067"), ("DBT-0079", "LN-00079"),
            ("DBT-0079", "LN-00214"), ("DBT-0072", "LN-00072"),
            ("DBT-0087", "LN-00087"), ("DBT-0087", "LN-00225"),
            ("DBT-0089", "LN-00089"), ("DBT-0089", "LN-00207"),
            ("DBT-0090", "LN-00090"), ("DBT-0096", "LN-00096"),
            ("DBT-0097", "LN-00097"), ("DBT-0097", "LN-00247"),
            ("DBT-0084", "LN-00084"), ("DBT-0085", "LN-00085"),
            ("DBT-0094", "LN-00094"), ("DBT-0094", "LN-00245"),
            ("DBT-0103", "LN-00103"), ("DBT-0117", "LN-00117"),
            ("DBT-0117", "LN-00220"), ("DBT-0118", "LN-00118"),
            ("DBT-0119", "LN-00119"), ("DBT-0121", "LN-00121"),
            ("DBT-0114", "LN-00114"), ("DBT-0115", "LN-00115"),
            ("DBT-0125", "LN-00125"), ("DBT-0137", "LN-00137"),
            ("DBT-0137", "LN-00249"), ("DBT-0138", "LN-00138"),
            ("DBT-0146", "LN-00146"), ("DBT-0147", "LN-00147"),
            ("DBT-0149", "LN-00149"), ("DBT-0158", "LN-00158"),
            ("DBT-0161", "LN-00161"), ("DBT-0170", "LN-00170"),
            ("DBT-0183", "LN-00183"), ("DBT-0185", "LN-00185"),
            ("DBT-0194", "LN-00194"), ("DBT-0194", "LN-00233"),
            ("DBT-0198", "LN-00198"), ("DBT-0191", "LN-00191"),
            ("DBT-0201", "LN-00201"),
        ]
    },
    "CLT-002": {
        "s3_folder": "client-drops/CLT-002/",
        "loans": [
            ("DBT-0204", "LN-00204"), ("DBT-0204", "LN-00237"),
            ("DBT-0199", "LN-00199"), ("DBT-0195", "LN-00195"),
            ("DBT-0189", "LN-00189"), ("DBT-0171", "LN-00171"),
            ("DBT-0172", "LN-00172"), ("DBT-0173", "LN-00173"),
            ("DBT-0145", "LN-00145"), ("DBT-0180", "LN-00180"),
            ("DBT-0169", "LN-00169"), ("DBT-0162", "LN-00162"),
            ("DBT-0163", "LN-00163"), ("DBT-0159", "LN-00159"),
            ("DBT-0151", "LN-00151"), ("DBT-0151", "LN-00213"),
            ("DBT-0154", "LN-00154"), ("DBT-0155", "LN-00155"),
            ("DBT-0156", "LN-00156"), ("DBT-0135", "LN-00224"),
            ("DBT-0135", "LN-00135"), ("DBT-0136", "LN-00136"),
            ("DBT-0122", "LN-00122"), ("DBT-0123", "LN-00123"),
            ("DBT-0129", "LN-00129"), ("DBT-0129", "LN-00212"),
            ("DBT-0127", "LN-00127"), ("DBT-0132", "LN-00132"),
            ("DBT-0098", "LN-00098"), ("DBT-0099", "LN-00099"),
            ("DBT-0099", "LN-00211"), ("DBT-0082", "LN-00082"),
            ("DBT-0068", "LN-00068"), ("DBT-0075", "LN-00075"),
            ("DBT-0075", "LN-00219"), ("DBT-0076", "LN-00076"),
            ("DBT-0077", "LN-00077"), ("DBT-0077", "LN-00218"),
            ("DBT-0064", "LN-00064"), ("DBT-0065", "LN-00065"),
            ("DBT-0055", "LN-00055"), ("DBT-0055", "LN-00239"),
            ("DBT-0056", "LN-00056"), ("DBT-0057", "LN-00057"),
            ("DBT-0050", "LN-00050"), ("DBT-0053", "LN-00053"),
            ("DBT-0046", "LN-00046"), ("DBT-0038", "LN-00038"),
            ("DBT-0039", "LN-00039"), ("DBT-0040", "LN-00040"),
            ("DBT-0031", "LN-00031"),
        ]
    },
    "CLT-003": {
        "s3_folder": "client-drops/CLT-003/",
        "loans": [
            ("DBT-0002", "LN-00002"), ("DBT-0029", "LN-00029"),
            ("DBT-0033", "LN-00033"), ("DBT-0009", "LN-00009"),
            ("DBT-0037", "LN-00037"), ("DBT-0042", "LN-00042"),
            ("DBT-0047", "LN-00047"), ("DBT-0049", "LN-00049"),
            ("DBT-0049", "LN-00232"), ("DBT-0052", "LN-00052"),
            ("DBT-0061", "LN-00061"), ("DBT-0062", "LN-00062"),
            ("DBT-0078", "LN-00078"), ("DBT-0083", "LN-00083"),
            ("DBT-0073", "LN-00073"), ("DBT-0080", "LN-00208"),
            ("DBT-0080", "LN-00080"), ("DBT-0100", "LN-00100"),
            ("DBT-0101", "LN-00101"), ("DBT-0128", "LN-00128"),
            ("DBT-0130", "LN-00242"), ("DBT-0130", "LN-00130"),
            ("DBT-0107", "LN-00107"), ("DBT-0108", "LN-00108"),
            ("DBT-0109", "LN-00109"), ("DBT-0104", "LN-00104"),
            ("DBT-0104", "LN-00234"), ("DBT-0105", "LN-00105"),
            ("DBT-0095", "LN-00095"), ("DBT-0086", "LN-00086"),
            ("DBT-0134", "LN-00134"), ("DBT-0134", "LN-00222"),
            ("DBT-0148", "LN-00148"), ("DBT-0113", "LN-00113"),
            ("DBT-0144", "LN-00144"), ("DBT-0139", "LN-00139"),
            ("DBT-0140", "LN-00140"), ("DBT-0141", "LN-00141"),
            ("DBT-0142", "LN-00142"), ("DBT-0157", "LN-00157"),
            ("DBT-0160", "LN-00160"), ("DBT-0150", "LN-00150"),
            ("DBT-0182", "LN-00182"), ("DBT-0176", "LN-00176"),
            ("DBT-0176", "LN-00227"), ("DBT-0177", "LN-00177"),
            ("DBT-0178", "LN-00178"), ("DBT-0178", "LN-00206"),
            ("DBT-0168", "LN-00168"), ("DBT-0165", "LN-00165"),
            ("DBT-0166", "LN-00166"), ("DBT-0190", "LN-00190"),
            ("DBT-0187", "LN-00187"), ("DBT-0187", "LN-00221"),
            ("DBT-0184", "LN-00184"), ("DBT-0197", "LN-00197"),
            ("DBT-0192", "LN-00192"), ("DBT-0193", "LN-00193"),
        ]
    },
    "CLT-004": {
        "s3_folder": "client-drops/CLT-004/",
        "loans": [
            ("DBT-0202", "LN-00202"), ("DBT-0203", "LN-00203"),
            ("DBT-0186", "LN-00186"), ("DBT-0188", "LN-00188"),
            ("DBT-0196", "LN-00196"), ("DBT-0196", "LN-00241"),
            ("DBT-0200", "LN-00200"), ("DBT-0200", "LN-00238"),
            ("DBT-0167", "LN-00167"), ("DBT-0174", "LN-00174"),
            ("DBT-0175", "LN-00175"), ("DBT-0179", "LN-00179"),
            ("DBT-0181", "LN-00181"), ("DBT-0164", "LN-00164"),
            ("DBT-0152", "LN-00152"), ("DBT-0153", "LN-00153"),
            ("DBT-0153", "LN-00223"), ("DBT-0143", "LN-00143"),
            ("DBT-0106", "LN-00106"), ("DBT-0110", "LN-00110"),
            ("DBT-0111", "LN-00111"), ("DBT-0112", "LN-00112"),
            ("DBT-0112", "LN-00226"), ("DBT-0120", "LN-00120"),
            ("DBT-0131", "LN-00131"), ("DBT-0133", "LN-00133"),
            ("DBT-0124", "LN-00124"), ("DBT-0126", "LN-00126"),
            ("DBT-0126", "LN-00230"), ("DBT-0116", "LN-00116"),
            ("DBT-0102", "LN-00102"), ("DBT-0091", "LN-00091"),
            ("DBT-0092", "LN-00092"), ("DBT-0092", "LN-00231"),
            ("DBT-0093", "LN-00093"), ("DBT-0088", "LN-00229"),
            ("DBT-0088", "LN-00088"), ("DBT-0081", "LN-00081"),
            ("DBT-0074", "LN-00074"), ("DBT-0069", "LN-00069"),
            ("DBT-0070", "LN-00070"), ("DBT-0071", "LN-00071"),
            ("DBT-0063", "LN-00063"), ("DBT-0059", "LN-00236"),
            ("DBT-0059", "LN-00059"), ("DBT-0060", "LN-00060"),
            ("DBT-0051", "LN-00051"), ("DBT-0044", "LN-00243"),
            ("DBT-0044", "LN-00044"), ("DBT-0045", "LN-00045"),
            ("DBT-0032", "LN-00032"), ("DBT-0032", "LN-00244"),
            ("DBT-0034", "LN-00034"), ("DBT-0034", "LN-00209"),
            ("DBT-0035", "LN-00228"), ("DBT-0035", "LN-00035"),
            ("DBT-0036", "LN-00036"), ("DBT-0030", "LN-00030"),
            ("DBT-0025", "LN-00025"), ("DBT-0026", "LN-00026"),
            ("DBT-0001", "LN-00001"), ("DBT-0006", "LN-00240"),
            ("DBT-0006", "LN-00006"), ("DBT-0007", "LN-00007"),
            ("DBT-0008", "LN-00008"),
        ]
    }
}

BUCKET_NAME = "csl-collections-landing"



# -------------------------------------------------------
# RECORD GENERATION
# -------------------------------------------------------

def generate_balance_records(client_id, loans, reporting_date):
    """
    Generates a list of balance update records for a single client bank.
    Injects a small percentage of dirty data to simulate real world drops.
    """
    records = []
    today = reporting_date

    for debtor_id, loan_id in loans:
        # Generate a realistic outstanding balance
        outstanding_balance = round(random.uniform(50000, 5000000), 2)

        # Generate a realistic last payment date within the last 90 days
        days_ago = random.randint(1, 90)
        last_payment_date = today - timedelta(days=days_ago)

        # Generate a realistic last payment amount
        last_payment_amount = round(random.uniform(5000, 200000), 2)

        # Generate days past due
        days_past_due = random.randint(0, 365)

        # Assign account status based on days past due
        if days_past_due == 0:
            account_status = "Performing"
        elif days_past_due <= 90:
            account_status = "Watch"
        else:
            account_status = "NPL"

        records.append({
            "ClientID": client_id,
            "DebtorID": debtor_id,
            "LoanID": loan_id,
            "ReportingDate": today,
            "CurrentOutstandingBalance": outstanding_balance,
            "LastPaymentDate": last_payment_date,
            "LastPaymentAmount": last_payment_amount,
            "DaysPastDue": days_past_due,
            "AccountStatus": account_status
        })

    return records


# -------------------------------------------------------
# DIRTY DATA INJECTION
# Simulates real world data quality issues in bank drops
# -------------------------------------------------------

def inject_dirty_data(records):
    """
    Injects a small percentage of dirty records into the dataset.
    Three types of dirty data are injected:
    1. NULL CurrentOutstandingBalance
    2. Duplicate LoanID entries for the same ReportingDate
    3. Invalid AccountStatus values outside the controlled vocabulary
    """
    total = len(records)

    # Type 1: NULL CurrentOutstandingBalance
    # Affects approximately 2% of records
    null_balance_indices = random.sample(
        range(total),
        k=max(1, int(total * 0.02))
    )
    for i in null_balance_indices:
        records[i]["CurrentOutstandingBalance"] = None

    # Type 2: Duplicate LoanID entries for the same ReportingDate
    # Pick 2 records and append exact duplicates
    duplicate_indices = random.sample(range(total), k=2)
    for i in duplicate_indices:
        records.append(records[i].copy())

    # Type 3: Invalid AccountStatus values
    # Affects approximately 2% of records
    invalid_statuses = ["WRITTEN_OFF", "Suspended", "UNKNOWN", "null"]
    invalid_indices = random.sample(
        range(total),
        k=max(1, int(total * 0.02))
    )
    for i in invalid_indices:
        records[i]["AccountStatus"] = random.choice(invalid_statuses)

    return records


# -------------------------------------------------------
# PARQUET CONVERSION AND S3 UPLOAD
# -------------------------------------------------------

def upload_parquet_to_s3(records, client_id, s3_folder, reporting_date, s3_client):
    """
    Converts records to a Parquet file in memory and uploads directly to S3.
    No file is written to disk at any point.
    """
    df = pd.DataFrame(records)

    # Enforce correct data types before writing to Parquet
    df["ReportingDate"] = pd.to_datetime(df["ReportingDate"])
    df["LastPaymentDate"] = pd.to_datetime(df["LastPaymentDate"])
    df["CurrentOutstandingBalance"] = df["CurrentOutstandingBalance"].astype(float)
    df["LastPaymentAmount"] = df["LastPaymentAmount"].astype(float)
    df["DaysPastDue"] = df["DaysPastDue"].astype(int)
    df["ClientID"] = df["ClientID"].astype(str)
    df["DebtorID"] = df["DebtorID"].astype(str)
    df["LoanID"] = df["LoanID"].astype(str)
    df["AccountStatus"] = df["AccountStatus"].astype(str)

    # Convert dataframe to Parquet bytes in memory
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)

    # Construct the S3 object key using the naming convention
    file_name = f"balance_update_{reporting_date.strftime('%Y_%m_%d')}.parquet"
    s3_key = f"{s3_folder}{file_name}"

    # Upload to S3
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key} ({len(records)} records)")
    
    
    # -------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------

def main():
    """
    Orchestrates the generation and upload of balance update files
    for all four client banks.
    """
    # Load AWS credentials from environment variables
    # Credentials are never hardcoded in this script
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"]
    )

    reporting_date = date.today()
    print(f"Generating balance update files for reporting date: {reporting_date}")
    print("-" * 60)

    for client_id, client_info in CLIENT_DATA.items():
        s3_folder = client_info["s3_folder"]
        loans = client_info["loans"]

        # Generate clean records
        records = generate_balance_records(client_id, loans, reporting_date)

        # Inject dirty data
        records = inject_dirty_data(records)

        # Upload to S3
        upload_parquet_to_s3(
            records,
            client_id,
            s3_folder,
            reporting_date,
            s3_client
        )

    print("-" * 60)
    print("All four client bank files successfully uploaded to S3.")


if __name__ == "__main__":
    main()