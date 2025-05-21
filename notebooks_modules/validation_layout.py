"""Validation layout components."""

import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from dash import html, dcc
from .validation_config import (
    WORKDAY_DIR, CALABRIO_DIR, CONFIG_DATA_PATH,
    get_workday_file, get_calabrio_file, get_config_file
)

def create_filter_panel(filter_options):
    """Create filter panel component."""
    return dbc.Card([
        dbc.CardHeader("フィルター"),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Absence Type:"),
                    dcc.Dropdown(id='absence-type-filter', options=filter_options['absence_types'], multi=True)
                ], width=6),
                dbc.Col([
                    html.Label("Contract:"),
                    dcc.Dropdown(id='contract-filter', options=filter_options['contracts'], multi=True)
                ], width=6)
            ]),
            dbc.Row([
                dbc.Col([
                    html.Label("Balance Match:"),
                    dcc.Dropdown(id='balance-match-filter', options=filter_options['balance_matches'], multi=True)
                ], width=6),
                dbc.Col([
                    html.Label("Accrual Match:"),
                    dcc.Dropdown(id='accrual-match-filter', options=filter_options['accrual_matches'], multi=True)
                ], width=6)
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Button("フィルター適用", id="apply-filters-button", color="primary", className="me-2"),
                    dbc.Button("フィルタークリア", id="clear-filters-button", color="secondary")
                ], className="mt-3")
            ])
        ])
    ])

def create_validation_grid(validation_df):
    """Create validation grid component."""
    return dag.AgGrid(
        id='validation-grid',
        rowData=validation_df.to_dict('records'),
        columnDefs=[
            {'field': 'Calabrio BusinessUnitName', 'headerName': 'Business Unit'},
            {'field': 'Workday Person Number', 'headerName': 'Workday Person Number'},
            {'field': 'Latest Headcount Primary Work Email', 'headerName': 'Email'},
            {'field': 'Workday Absence Type', 'headerName': 'Absence Type'},
            {'field': 'ContractName', 'headerName': 'Contract'},
            {'field': 'Calabrio Balance In', 'headerName': 'Calabrio Balance'},
            {'field': 'Correct Balance In', 'headerName': 'Correct Balance'},
            {'field': 'Calabrio_Accrued', 'headerName': 'Calabrio Accrued'},
            {'field': 'Correct_Accrued', 'headerName': 'Correct Accrued'},
            {'field': 'Balance Match', 'headerName': 'Balance Match'},
            {'field': 'Accrual Match', 'headerName': 'Accrual Match'},
            {'field': 'Calabrio PersonId', 'headerName': 'Person ID'},
            {'field': 'AbsenceId', 'headerName': 'AbsenceId'},
            {'field': 'StartDate', 'headerName': 'Start Date'}
        ],
        dashGridOptions={
            'pagination': True,
            'paginationPageSize': 100,
            'rowSelection': 'multiple'
        }
    )

def create_upload_grid():
    """Create upload grid component."""
    return dag.AgGrid(
        id='upload-grid',
        rowData=[],
        columnDefs=[
            {'field': 'EmploymentNumber', 'headerName': 'Employment Number', 'editable': True},
            {'field': 'StartDate', 'headerName': 'Start Date', 'editable': True},
            {'field': 'PersonId', 'headerName': 'Person ID', 'editable': True},
            {'field': 'AbsenceId', 'headerName': 'AbsenceId', 'editable': True},
            {'field': 'BalanceIn', 'headerName': 'Balance', 'editable': True},
            {'field': 'Accrued', 'headerName': 'Accrued', 'editable': True},
            {'field': 'Extra', 'headerName': 'Extra', 'editable': True}
        ],
        dashGridOptions={
            'pagination': True,
            'paginationPageSize': 100,
            'rowSelection': 'multiple',
            'editType': 'fullRow'
        }
    )

def create_app_layout(filter_panel, validation_grid, upload_grid):
    """Create main app layout."""
    upload_buttons = dbc.ButtonGroup([
        dbc.Button("行を追加", id="add-row-button", color="primary", className="me-2"),
        dbc.Button("選択行を削除", id="delete-rows-button", color="danger")
    ])
    
    upload_progress = dbc.Progress(id="upload-progress", value=0, striped=True, animated=True, className="mb-3")
    upload_status = html.Div(id="upload-status")
    
    return html.Div([
        html.H1("Validation Results"),
        html.Div(id='debug-output'),
        dcc.Tabs([
            dcc.Tab(label='Validation', children=[
                filter_panel,
                html.Hr(),
                validation_grid,
                html.Hr(),
                dbc.Button("Prepare for Upload", id="prepare-button", color="success")
            ]),
            dcc.Tab(label='Upload', children=[
                html.H3("アップロード用データ"),
                upload_grid,
                html.Hr(),
                upload_buttons,
                html.Hr(),
                html.Div([
                    dbc.Button("Calabrioにアップロード", id="upload-button", color="danger"),
                    html.Div(id="upload-result"),
                    dbc.Collapse(
                        [upload_progress, upload_status],
                        id="upload-progress-container",
                        is_open=False
                    )
                ])
            ])
        ], id='tabs'),
        html.Div(id='output-container'),
        dbc.Alert(id="alert", is_open=False, duration=4000),
        dcc.Store(id='upload-store', data=[])
    ])
