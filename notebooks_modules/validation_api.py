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
        
        # 有効な行（PersonIdとAbsenceIdがある行）をフィルタリング
        valid_rows = [row for row in row_data if row.get('PersonId') and row.get('AbsenceId')]
        total_rows = len(valid_rows)
        
        if total_rows == 0:
            return html.Div([
                html.H4("エラー", className="text-danger"),
                html.P("アップロードするデータがありません。PersonIdとAbsenceIdは必須です。")
            ]), False, 0, ""
        
        # APIクライアントの初期化
        base_url = os.environ.get('CALABRIO_API_URL', 'https://wise.teleopticloud.com/api')
        api_key = os.environ.get('CALABRIO_API_KEY', '')
        if not api_key:
            return html.Div([
                html.H4("エラー", className="text-danger"),
                html.P("API Keyが設定されていません。環境変数CALABRIO_API_KEYを設定してください。")
            ]), False, 0, ""
            
        client = ApiClient(base_url, api_key)
        
        # アップロード処理
        success_count = 0
        error_count = 0
        
        # ログファイルの準備
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        ensure_directories()  # 必要なディレクトリを作成
        log_filename = UPLOAD_LOG_DIR / f'upload_log_{timestamp}.txt'
        
        with open(log_filename, 'w') as log_file:
            # 各行を処理
            for i, row in enumerate(valid_rows):
                person_id = row.get('PersonId', '')
                absence_id = row.get('AbsenceId', '')
                date_from = row.get('StartDate', '')
                
                try:
                    # 数値を適切に処理
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
                    
                    # 本番APIを使用してアップロード
                    result = client.add_or_update_person_account_for_person(
                        person_id=person_id,
                        absence_id=absence_id,
                        date_from=date_from,
                        balance_in=balance_in,
                        accrued=accrued,
                        extra=extra
                    )
                    
                    # 結果を記録
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
                    # エラーを記録
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
        
        # 最終結果の表示
        if error_count == 0:
            result_color = "success"
            icon = "✓"
            message = f"すべてのレコード ({success_count}件) が正常にアップロードされました。"
        elif error_count > 0 and success_count > 0:
            result_color = "warning"
            icon = "⚠️"
            message = f"{success_count}件のアップロードが成功し、{error_count}件が失敗しました。"
        else:
            result_color = "danger"
            icon = "❌"
            message = f"すべてのアップロードが失敗しました。{error_count}件のエラーが発生しました。"
        
        # 処理時間
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        final_result = html.Div([
            html.H4(f"{icon} アップロード結果", className=f"text-{result_color}"),
            html.P(message),
            html.P(f"処理時間: {end_time}"),
            html.P(f"ログファイル: {log_filename}"),
            html.P("詳細はログファイルを確認してください。", className="text-muted")
        ])
        
        return final_result, True, 100, f"完了 - 成功: {success_count}, 失敗: {error_count}"
