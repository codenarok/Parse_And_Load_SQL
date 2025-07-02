import azure.functions as func
import datetime
import json
import logging
import os
import pyodbc
from azure.storage.blob import BlobServiceClient
import re

app = func.FunctionApp()

@app.function_name(name="Parse_And_Load_SQL")
@app.route(route="parse_and_load_sql", auth_level=func.AuthLevel.FUNCTION)
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Parse_And_Load_SQL function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Please pass a JSON body with 'json_blob_url' and 'source_file_name' fields.",
            status_code=400
        )

    json_blob_url = req_body.get('json_blob_url')
    source_file_name = req_body.get('source_file_name')

    if not json_blob_url or not source_file_name:
        return func.HttpResponse(
            "Please provide 'json_blob_url' and 'source_file_name' in the request body.",
            status_code=400
        )

    try:
        logging.info(f"Attempting to read JSON from: {json_blob_url}")
        match = re.match(r"https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)", json_blob_url)
        if not match:
            raise ValueError("Invalid blob URL format provided.")
        account_name = match.group(1)
        container_name = match.group(2)
        blob_name = match.group(3)
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=func.get_managed_identity_client()
        )
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        download_stream = blob_client.download_blob()
        json_data_raw = download_stream.readall().decode('utf-8')
        extracted_data = json.loads(json_data_raw)
        logging.info("JSON file downloaded and parsed successfully.")
        financial_metrics = []
        bank_name = "Unknown Bank"
        report_year = None
        if "_" in source_file_name:
            bank_name_match = re.match(r"([A-Za-z]+)_.*\.pdf\.json", source_file_name)
            if bank_name_match:
                bank_name = bank_name_match.group(1).replace("-", " ").title()
        year_match = re.search(r"(\d{4})_Annual_report\.pdf\.json", source_file_name)
        if year_match:
            report_year = int(year_match.group(1))
        if report_year is None and "tables" in extracted_data:
            for table in extracted_data["tables"]:
                for cell in table["cells"]:
                    if "columnHeader" in cell.get("kind", "") and "£ million" in cell["content"]:
                        year_in_header = re.search(r"(\d{4})", cell["content"])
                        if year_in_header:
                            report_year = int(year_in_header.group(1))
                            break
                if report_year is not None:
                    break
        if report_year is None:
            logging.warning(f"Could not reliably infer ReportYear for {source_file_name}. Skipping structured data load.")
            return func.HttpResponse(
                f"Successfully processed JSON for {source_file_name}, but could not infer year for SQL load.",
                status_code=200
            )
        for table in extracted_data.get("tables", []):
            column_headers = {}
            for cell in table.get("cells", []):
                if cell.get("kind") == "columnHeader" and "£ million" in cell["content"]:
                    year_match = re.search(r"(\d{4})", cell["content"])
                    if year_match:
                        column_headers[cell["columnIndex"]] = int(year_match.group(1))
            for row_index in range(table["rowCount"]):
                metric_name = None
                values = {}
                for cell in table.get("cells", []):
                    if cell["rowIndex"] == row_index:
                        if cell["columnIndex"] == 0:
                            metric_name = cell["content"].strip()
                        elif cell["columnIndex"] in column_headers:
                            year = column_headers[cell["columnIndex"]]
                            raw_value = cell["content"].strip().replace(" ", "").replace(",", "")
                            if raw_value.startswith("(") and raw_value.endswith(")"):
                                try:
                                    values[year] = -abs(float(raw_value[1:-1]))
                                except ValueError:
                                    values[year] = None
                            else:
                                try:
                                    values[year] = float(raw_value)
                                except ValueError:
                                    values[year] = None
                if metric_name and any(v is not None for v in values.values()):
                    for year, value in values.items():
                        if value is not None:
                            financial_metrics.append({
                                "BankName": bank_name,
                                "ReportYear": year,
                                "MetricName": metric_name,
                                "MetricValue": value,
                                "Unit": "£ million",
                                "SourceFileName": source_file_name
                            })
        logging.info(f"Extracted {len(financial_metrics)} metrics for SQL load.")
        conn_str = os.environ["SQL_CONNECTION_STRING"]
        cnxn = pyodbc.connect(conn_str)
        cursor = cnxn.cursor()
        insert_sql = """
        INSERT INTO FinancialMetrics (BankName, ReportYear, MetricName, MetricValue, Unit, SourceFileName)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        for metric in financial_metrics:
            try:
                cursor.execute(
                    insert_sql,
                    metric["BankName"],
                    metric["ReportYear"],
                    metric["MetricName"],
                    metric["MetricValue"],
                    metric["Unit"],
                    metric["SourceFileName"]
                )
            except pyodbc.Error as e:
                sql_state = e.args[0]
                logging.error(f"SQL Error inserting metric {metric.get('MetricName')}: {sql_state} - {e}")
        cnxn.commit()
        cursor.close()
        cnxn.close()
        logging.info("Data loaded into Azure SQL Database successfully.")
        return func.HttpResponse(
            f"Successfully processed and loaded structured data for {source_file_name}",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error in Parse_And_Load_SQL: {str(e)}")
        return func.HttpResponse(
            f"Error processing or loading data: {str(e)}",
            status_code=500
        )