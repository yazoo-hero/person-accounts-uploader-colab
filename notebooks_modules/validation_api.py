"""API upload functionality."""

from dash import html, Input, Output, State, callback_context, no_update
import os
import json
from datetime import datetime
from pathlib import Path
from validation_config import UPLOAD_LOG_DIR, ensure_directories
from calabrio_py.calabrio_api import ApiClient

def register_api_callbacks(app):
    """Register API upload callbacks."""
    
    @app.callback(
        [Output("upload-result", "children"),
         Output("upload-progress-container", "is_open"),
         Output("upload-progress", "value"),
         Output("upload-status", "children")],
        [Input("upload-button", "n_clicks")],
        [State("upload-grid", "rowData")],
        prevent_initial_call=True
    )
    def upload_to_calabrio_simple(n_clicks, row_data):
        if not n_clicks or not row_data:
            return "", False, 0, ""
        
        # Filtering valid rows (rows with PersonId and AbsenceId)
        valid_rows = [row for row in row_data if row.get('PersonId') and row.get('AbsenceId')]
        total_rows = len(valid_rows)
        
        if total_rows == 0:
            return html.Div([
                html.H4("Error", className="text-danger"),
                html.P("No data to upload. PersonId and AbsenceId are required.")
            ]), False, 0, ""
        
        # Initializing API client
        base_url = os.environ.get('CALABRIO_API_URL', 'https://wise.teleopticloud.com/api')
        api_key = os.environ.get('CALABRIO_API_KEY', '')
        if not api_key:
            return html.Div([
                html.H4("Error", className="text-danger"),
                html.P("API Key is not set. Please set the CALABRIO_API_KEY environment variable.")
            ]), False, 0, ""
            
        client = ApiClient(base_url, api_key)
        
        # Upload process
        success_count = 0
        error_count = 0
        
        # Preparing log file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        ensure_directories()  # Creating necessary directories
        log_filename = UPLOAD_LOG_DIR / f'upload_log_{timestamp}.txt'
        
        with open(log_filename, 'w') as log_file:
            # Processing each row
            for i, row in enumerate(valid_rows):
                person_id = row.get('PersonId', '')
                absence_id = row.get('AbsenceId', '')
                date_from = row.get('StartDate', '')
                
                try:
                    # Properly handle numbers
                    balance_in = row.get('BalanceIn', '')
                    if isinstance(balance_in, str) and balance_in.strip() == '':
                        balance_in = 0
                    balance_in = int(float(balance_in))
                    
                    accrued = row.get('Accrued', '')
                    if isinstance(accrued, str) and accrued.strip() == '':
                        accrued = 0
                    accrued = int(float(accrued))
                    
                    extra = row.get('Extra', '')
                    if isinstance(extra, str) and extra.strip() == '':
                        extra = 0
                    extra = int(float(extra))
                    
                    # Uploading using production API
                    result = client.add_or_update_person_account_for_person(
                        person_id=person_id,
                        absence_id=absence_id,
                        date_from=date_from,
                        balance_in=balance_in,
                        accrued=accrued,
                        extra=extra
                    )
                    
                    # Recording results
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'person_id': person_id,
                        'absence_id': absence_id,
                        'date_from': date_from,
                        'balance_in': balance_in,
                        'accrued': accrued,
                        'extra': extra,
                        'success': True if not result.get('error') else False,
                        'message': 'Successfully uploaded' if not result.get('error') else '',
                        'error': result.get('error', ''),
                        'api_response': result
                    }
                    
                    log_file.write(json.dumps(log_entry) + '\n')
                    
                    if not result.get('error'):
                        success_count += 1
                    else:
                        error_count += 1
                        
                except Exception as e:
                    # Recording error
                    error_message = str(e)
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'person_id': person_id,
                        'absence_id': absence_id,
                        'date_from': date_from,
                        'balance_in': balance_in if 'balance_in' in locals() else None,
                        'accrued': accrued if 'accrued' in locals() else None,
                        'extra': extra if 'extra' in locals() else None,
                        'success': False,
                        'message': '',
                        'error': error_message
                    }
                    
                    log_file.write(json.dumps(log_entry) + '\n')
                    error_count += 1
        
        # Displaying final results
        if error_count == 0:
            result_color = "success"
            icon = "✓"
            message = f"All records ({success_count}) successfully uploaded."
        elif error_count > 0 and success_count > 0:
            result_color = "warning"
            icon = "⚠️"
            message = f"{success_count} uploads succeeded, and {error_count} failed."
        else:
            result_color = "danger"
            icon = "❌"
            message = f"All uploads failed. {error_count} errors occurred."
        
        # Processing time
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_result = html.Div([
            html.H4(f"{icon} Upload Result", className=f"text-{result_color}"),
            html.P(message),
            html.P(f"Processing time: {end_time}"),
            html.P(f"Log file: {log_filename}"),
            html.P("Please check the log file for details.", className="text-muted")
        ])
        
        return final_result, True, 100, f"Completed - Success: {success_count}, Failed: {error_count}"
