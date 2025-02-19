import os
import pandas as pd
import numpy as np
import random
import json
from datetime import datetime
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import logging

# Set up logging for error tracking
logger = logging.getLogger(__name__)

def index(request):
    return render(request, 'index.html')

@csrf_exempt
def upload_files(request):
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "Invalid request method"}, status=405)

    file1 = request.FILES.get('file1')  # Raw campaign data
    file2 = request.FILES.get('file2')  # Adjustment values
    try:
        date_range = json.loads(request.POST.get("date_range", '["2025-01-01", "2025-01-31"]'))
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        
     # Add validation to ensure start_date is before or equal to end_date
        if start_date > end_date:
            logger.error(f"Start date {start_date} is later than end date {end_date}.")
            return JsonResponse({"success": False, "error": "Start date cannot be later than end date."}, status=400)

    except (ValueError, TypeError, IndexError, json.JSONDecodeError) as e:
        logger.error(f"Invalid date range format: {str(e)}")
        return JsonResponse({"success": False, "error": "Invalid date range format"}, status=400)


    if not file1 or not file2:
        logger.error("Both files are required.")
        return JsonResponse({"success": False, "error": "Both files are required"}, status=400)

    logger.info(f"Received files: file1={file1.name}, file2={file2.name}, date_range={date_range}")

    try:
        os.makedirs(settings.MEDIA_ROOT, exist_ok=True)

        raw_file_path = os.path.join(settings.MEDIA_ROOT, "temp_raw_data.xlsx")
        adjustments_file_path = os.path.join(settings.MEDIA_ROOT, "temp_adjustments.xlsx")

        with open(raw_file_path, "wb") as raw_file:
            for chunk in file1.chunks():
                raw_file.write(chunk)

        with open(adjustments_file_path, "wb") as adj_file:
            for chunk in file2.chunks():
                adj_file.write(chunk)

        social_data = read_file(raw_file_path, file1.name)
        desired_numbers = read_file(adjustments_file_path, file2.name)

        if social_data is None or desired_numbers is None:
            return JsonResponse({"success": False, "error": "Error reading files"}, status=400)

        if 'Date' in social_data.columns:
            social_data['Date'] = pd.to_datetime(social_data['Date'], errors='coerce')
            social_data.dropna(subset=['Date'], inplace=True)
            adjusted_data = adjust_social_data_V5(social_data, desired_numbers, start_date, end_date)
        else:
            logger.warning("Missing 'Date' column. Proceeding without filtering by date.")
            adjusted_data = social_data.copy()

        processed_dir = os.path.join(settings.MEDIA_ROOT, "processed")
        os.makedirs(processed_dir, exist_ok=True)

        adjusted_file_path = os.path.join(processed_dir, "Adjusted_Data.csv")
        adjusted_data.to_csv(adjusted_file_path, index=False)

        return JsonResponse({
            "success": True,
            "message": "Files processed successfully",
            "download_url": f"/media/processed/Adjusted_Data.csv"
        })

    except Exception as e:
        logger.error(f"Processing error: {str(e)}")
        return JsonResponse({"success": False, "error": f"Processing error: {str(e)}"}, status=500)

def adjust_social_data_V5(social_data, desired_numbers, start_date, end_date):
    try:
        key_columns = list(set(social_data.columns).intersection(set(desired_numbers.columns)) - 
                           set(social_data.select_dtypes(include=[np.number]).columns))
        metric_columns = list(set(desired_numbers.columns) - set(key_columns))

        social_data_filtered = social_data[(social_data['Date'] >= start_date) & (social_data['Date'] <= end_date)]

        for _, row in desired_numbers.iterrows():
            filters = [
                (social_data_filtered[key] == row[key])
                for key in key_columns if key in social_data_filtered.columns and key in desired_numbers.columns
            ]
            if filters:
                matching_rows = social_data_filtered[np.logical_and.reduce(filters)]
                for metric in metric_columns:
                    if metric in matching_rows.columns and row[metric] != 0:
                        total_metric = matching_rows[metric].sum()
                        if total_metric > 0:
                            weights = matching_rows[metric] / total_metric
                            adjusted_values = np.floor(weights * row[metric])
                            adjusted_values.replace([np.inf, -np.inf], np.nan, inplace=True)
                            adjusted_values.fillna(0, inplace=True)
                            adjusted_values = adjusted_values.astype(int)

                            difference = int(row[metric]) - adjusted_values.sum()
                            if difference != 0:
                                indices = matching_rows.index.tolist()
                                for _ in range(abs(difference)):
                                    random_index = random.choice(indices)
                                    adjusted_values.iloc[indices.index(random_index)] += 1 if difference > 0 else -1

                            social_data.loc[matching_rows.index, metric] = adjusted_values
        
        return social_data

    except Exception as e:
        logger.error(f"Data adjustment error: {str(e)}")
        raise ValueError("Error adjusting data")
        
def read_file(file_path, file_name):
    try:
        # Check the file extension to determine if it's a CSV or Excel file
        if file_name.endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_name.endswith('.xlsx'):
            return pd.read_excel(file_path)
        else:
            logger.error(f"Unsupported file format for file {file_name}")
            return None
    except Exception as e:
        logger.error(f"Error reading file {file_name}: {str(e)}")
        return None

