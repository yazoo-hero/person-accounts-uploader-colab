"""Validation callbacks."""

from dash import Input, Output, State, callback_context, no_update
import pandas as pd
from validation_config import (
    WORKDAY_DIR, CALABRIO_DIR, CONFIG_DATA_PATH,
    get_workday_file, get_calabrio_file, get_config_file
)

from validation_utils import (
    convert_to_upload_format, create_filter_options, filter_validation_data, safe_get_column
)

from validation_calculator import BalanceCalculator

def register_callbacks(app, validation_df):
    """Register all callbacks for the validation app."""
    
    # Filtering Callback
    @app.callback(
        Output('validation-grid', 'rowData'),
        [Input('apply-filters-button', 'n_clicks'),
         Input('clear-filters-button', 'n_clicks')],
        [State('absence-type-filter', 'value'),
         State('contract-filter', 'value'),
         State('balance-match-filter', 'value'),
         State('accrual-match-filter', 'value')]
    )
    def filter_validation_data(apply_clicks, clear_clicks, absence_types, contracts, balance_matches, accrual_matches):
        ctx = callback_context
        if not ctx.triggered:
            return validation_df.to_dict('records')
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == 'clear-filters-button':
            return validation_df.to_dict('records')
        
        df = validation_df.copy()
        
        if absence_types:
            df = df[df['Workday Absence Type'].isin(absence_types)]
        
        if contracts:
            df = df[df['ContractName'].isin(contracts)]
        
        if balance_matches:
            df = df[df['Balance Match'].isin(balance_matches)]
        
        if accrual_matches:
            df = df[df['Accrual Match'].isin(accrual_matches)]
        
        return df.to_dict('records')
    
    # Data Transfer Callback
    @app.callback(
        [Output('upload-grid', 'rowData'),
         Output('tabs', 'value'),
         Output('alert', 'children'),
         Output('alert', 'is_open'),
         Output('alert', 'color')],
        [Input('prepare-button', 'n_clicks')],
        [State('validation-grid', 'selectedRows')]
    )
    def transfer_data(n_clicks, selected_rows):
        if not callback_context.triggered or not selected_rows:
            return [], 'validation', "", False, ""
        
        # Convert selected rows to upload format
        upload_rows = []
        for row in selected_rows:
            upload_row = {
                'EmploymentNumber': row.get('Workday Person Number', ''),
                'StartDate': row.get('StartDate', ''),
                'PersonId': row.get('Calabrio PersonId', ''),
                'AbsenceId': row.get('Absence ID', ''),
                'BalanceIn': row.get('Correct Balance In', 0),
                'Accrued': row.get('Correct_Accrued', 0),
                'Extra': row.get('Calabrio Extra', 0)
            }
            upload_rows.append(upload_row)
        
        return upload_rows, 'upload', f"{len(upload_rows)} records prepared for upload", True, "success"
    
    # Add Row Callback
    @app.callback(
        Output('upload-grid', 'rowData', allow_duplicate=True),
        [Input('add-row-button', 'n_clicks')],
        [State('upload-grid', 'rowData')],
        prevent_initial_call=True
    )
    def add_empty_row(n_clicks, current_data):
        if not n_clicks:
            return no_update
        
        empty_row = {
            'EmploymentNumber': '',
            'StartDate': '',
            'PersonId': '',
            'AbsenceId': '',
            'BalanceIn': 0,
            'Accrued': 0,
            'Extra': 0
        }
        
        if current_data:
            return current_data + [empty_row]
        else:
            return [empty_row]
    
    # Delete Row Callback
    @app.callback(
        Output('upload-grid', 'rowData', allow_duplicate=True),
        [Input('delete-rows-button', 'n_clicks')],
        [State('upload-grid', 'selectedRows'),
         State('upload-grid', 'rowData')],
        prevent_initial_call=True
    )
    def delete_selected_rows(n_clicks, selected_rows, current_data):
        if not n_clicks or not selected_rows or not current_data:
            return no_update
        
        # Get IDs of selected rows
        selected_ids = set()
        for row in selected_rows:
            # Uniquely identify by combination of PersonId and AbsenceId
            row_id = f"{row.get('PersonId', '')}_{row.get('AbsenceId', '')}"
            selected_ids.add(row_id)
        
        # Exclude selected rows
        new_data = []
        for row in current_data:
            row_id = f"{row.get('PersonId', '')}_{row.get('AbsenceId', '')}"
            if row_id not in selected_ids:
                new_data.append(row)
        
        return new_data
    
    # Debug Output
    @app.callback(
        Output('debug-output', 'children'),
        [Input('prepare-button', 'n_clicks'),
         Input('add-row-button', 'n_clicks'),
         Input('delete-rows-button', 'n_clicks'),
         Input('apply-filters-button', 'n_clicks'),
         Input('clear-filters-button', 'n_clicks')]
    )
    def update_debug(prepare_clicks, add_clicks, delete_clicks, apply_clicks, clear_clicks):
        ctx = callback_context
        if not ctx.triggered:
            return "No button clicked"
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        button_clicks = ctx.triggered[0]['value']
        
        return f"{button_id} button clicked {button_clicks} times"
    
    # Update Output Container
    @app.callback(
        Output('output-container', 'children'),
        [Input('upload-grid', 'rowData')]
    )
    def update_output(rows):
        if not rows:
            return "No data selected"
        return f"For upload, {len(rows)} records are prepared"
