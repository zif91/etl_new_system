"""
Google Sheets API Client для интеграции с Google Sheets.
"""

import os
import logging
from typing import Dict, List, Any, Optional
import json

import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound, APIError

logger = logging.getLogger(__name__)

class GoogleSheetsClient:
    """
    Клиент для работы с Google Sheets API через gspread
    """
    
    def __init__(self, credentials_path: Optional[str] = None, sheet_id: Optional[str] = None):
        """
        Инициализирует клиент Google Sheets.
        
        Args:
            credentials_path: Путь к JSON-файлу с учетными данными сервисного аккаунта.
                              Если None, используется GOOGLE_SHEETS_CREDENTIALS_JSON из .env
            sheet_id: ID Google-таблицы. Если None, используется PROMO_SHEET_ID из .env
        """
        self.credentials_path = credentials_path or os.environ.get('GOOGLE_SHEETS_CREDENTIALS_JSON')
        self.sheet_id = sheet_id or os.environ.get('PROMO_SHEET_ID')
        self.client = None
        self.spreadsheet = None
        
        if not self.credentials_path:
            raise ValueError("GOOGLE_SHEETS_CREDENTIALS_JSON not provided")
        
        if not self.sheet_id:
            raise ValueError("PROMO_SHEET_ID not provided")
        
        self._authenticate()
    
    def _authenticate(self):
        """
        Выполняет аутентификацию с Google Sheets API используя сервисный аккаунт.
        """
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            credentials = Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=scopes
            )
            
            self.client = gspread.authorize(credentials)
            logger.info("Successfully authenticated with Google Sheets API")
            
        except FileNotFoundError:
            logger.error(f"Credentials file not found: {self.credentials_path}")
            raise
        except GoogleAuthError as e:
            logger.error(f"Authentication error with Google Sheets API: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Google Sheets authentication: {e}")
            raise
    
    def open_spreadsheet(self, spreadsheet_id: Optional[str] = None) -> gspread.Spreadsheet:
        """
        Открывает таблицу по ID.
        
        Args:
            spreadsheet_id: ID Google-таблицы. Если None, используется ID из конструктора.
            
        Returns:
            Объект таблицы
        """
        sheet_id = spreadsheet_id or self.sheet_id
        
        try:
            self.spreadsheet = self.client.open_by_key(sheet_id)
            logger.info(f"Successfully opened spreadsheet: {self.spreadsheet.title}")
            return self.spreadsheet
        except SpreadsheetNotFound:
            logger.error(f"Spreadsheet not found with ID: {sheet_id}")
            raise
        except APIError as e:
            logger.error(f"API error while opening spreadsheet: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while opening spreadsheet: {e}")
            raise
    
    def get_worksheet(self, worksheet_name: str) -> gspread.Worksheet:
        """
        Получает лист по названию.
        
        Args:
            worksheet_name: Название листа
            
        Returns:
            Объект листа
        """
        if not self.spreadsheet:
            self.open_spreadsheet()
            
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            logger.info(f"Successfully accessed worksheet: {worksheet_name}")
            return worksheet
        except WorksheetNotFound:
            logger.error(f"Worksheet not found: {worksheet_name}")
            raise
        except Exception as e:
            logger.error(f"Error accessing worksheet {worksheet_name}: {e}")
            raise
    
    def get_all_records(self, worksheet_name: str) -> List[Dict[str, Any]]:
        """
        Получает все записи с листа в виде списка словарей.
        
        Args:
            worksheet_name: Название листа
            
        Returns:
            Список словарей с данными
        """
        worksheet = self.get_worksheet(worksheet_name)
        
        try:
            records = worksheet.get_all_records()
            logger.info(f"Retrieved {len(records)} records from worksheet: {worksheet_name}")
            return records
        except Exception as e:
            logger.error(f"Error getting records from worksheet {worksheet_name}: {e}")
            raise
    
    def get_range_values(self, worksheet_name: str, cell_range: str) -> List[List[Any]]:
        """
        Получает значения из указанного диапазона.
        
        Args:
            worksheet_name: Название листа
            cell_range: Диапазон ячеек в формате A1:F10
            
        Returns:
            Список списков с данными
        """
        worksheet = self.get_worksheet(worksheet_name)
        
        try:
            values = worksheet.get(cell_range)
            logger.info(f"Retrieved values from range {cell_range} in worksheet: {worksheet_name}")
            return values
        except Exception as e:
            logger.error(f"Error getting values from range {cell_range} in worksheet {worksheet_name}: {e}")
            raise
            
    def update_range_values(self, worksheet_name: str, cell_range: str, values: List[List[Any]]) -> Dict[str, Any]:
        """
        Обновляет значения в указанном диапазоне.
        
        Args:
            worksheet_name: Название листа
            cell_range: Диапазон ячеек в формате A1:F10
            values: Список списков с данными для обновления
            
        Returns:
            Словарь с результатами обновления
        """
        worksheet = self.get_worksheet(worksheet_name)
        
        try:
            result = worksheet.update(cell_range, values)
            logger.info(f"Updated values in range {cell_range} in worksheet: {worksheet_name}")
            return result
        except Exception as e:
            logger.error(f"Error updating values in range {cell_range} in worksheet {worksheet_name}: {e}")
            raise
    
    def append_rows(self, worksheet_name: str, values: List[List[Any]], value_input_option: str = 'USER_ENTERED') -> Dict[str, Any]:
        """
        Добавляет строки в конец листа.
        
        Args:
            worksheet_name: Название листа
            values: Список списков с данными для добавления
            value_input_option: Способ интерпретации значений: 'RAW' или 'USER_ENTERED'
            
        Returns:
            Словарь с результатами добавления
        """
        worksheet = self.get_worksheet(worksheet_name)
        
        try:
            result = worksheet.append_rows(values, value_input_option=value_input_option)
            logger.info(f"Appended {len(values)} rows to worksheet: {worksheet_name}")
            return result
        except Exception as e:
            logger.error(f"Error appending rows to worksheet {worksheet_name}: {e}")
            raise
    
    def create_worksheet(self, title: str, rows: int = 100, cols: int = 20) -> gspread.Worksheet:
        """
        Создает новый лист в таблице.
        
        Args:
            title: Название нового листа
            rows: Количество строк
            cols: Количество столбцов
            
        Returns:
            Объект созданного листа
        """
        if not self.spreadsheet:
            self.open_spreadsheet()
        
        try:
            # Проверяем, существует ли уже лист с таким названием
            try:
                existing_worksheet = self.spreadsheet.worksheet(title)
                logger.warning(f"Worksheet {title} already exists. Returning existing worksheet.")
                return existing_worksheet
            except WorksheetNotFound:
                # Создаем новый лист
                worksheet = self.spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
                logger.info(f"Created new worksheet: {title}")
                return worksheet
        except Exception as e:
            logger.error(f"Error creating worksheet {title}: {e}")
            raise
    
    def delete_worksheet(self, worksheet_name: str) -> bool:
        """
        Удаляет лист из таблицы.
        
        Args:
            worksheet_name: Название листа для удаления
            
        Returns:
            True, если лист был удален
        """
        if not self.spreadsheet:
            self.open_spreadsheet()
        
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            self.spreadsheet.del_worksheet(worksheet)
            logger.info(f"Deleted worksheet: {worksheet_name}")
            return True
        except WorksheetNotFound:
            logger.warning(f"Worksheet {worksheet_name} not found. Nothing to delete.")
            return False
        except Exception as e:
            logger.error(f"Error deleting worksheet {worksheet_name}: {e}")
            raise
    
    def find_cell(self, worksheet_name: str, query: str) -> List[gspread.Cell]:
        """
        Ищет ячейки, содержащие указанный текст.
        
        Args:
            worksheet_name: Название листа
            query: Текст для поиска
            
        Returns:
            Список найденных ячеек
        """
        worksheet = self.get_worksheet(worksheet_name)
        
        try:
            cells = worksheet.findall(query)
            logger.info(f"Found {len(cells)} cells containing '{query}' in worksheet: {worksheet_name}")
            return cells
        except Exception as e:
            logger.error(f"Error finding cells in worksheet {worksheet_name}: {e}")
            raise
    
    def share_spreadsheet(self, email: str, role: str = 'reader', 
                          perm_type: str = 'user', notify: bool = False,
                          email_message: Optional[str] = None) -> Dict[str, Any]:
        """
        Предоставляет доступ к таблице.
        
        Args:
            email: Email пользователя для предоставления доступа
            role: Роль пользователя ('reader', 'writer', 'commenter', 'owner')
            perm_type: Тип разрешения ('user', 'group', 'domain', 'anyone')
            notify: Отправлять ли уведомление по email
            email_message: Сообщение для отправки (если notify=True)
            
        Returns:
            Словарь с результатами предоставления доступа
        """
        if not self.spreadsheet:
            self.open_spreadsheet()
        
        try:
            result = self.spreadsheet.share(
                email_address=email,
                perm_type=perm_type,
                role=role,
                notify=notify,
                email_message=email_message
            )
            logger.info(f"Shared spreadsheet with {email} as {role}")
            return result
        except Exception as e:
            logger.error(f"Error sharing spreadsheet with {email}: {e}")
            raise
    
    def batch_update(self, worksheet_name: str, batch_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Выполняет пакетное обновление ячеек.
        
        Args:
            worksheet_name: Название листа
            batch_data: Список словарей с данными для обновления в формате:
                [{'range': 'A1:B2', 'values': [[1, 2], [3, 4]]}, ...]
                
        Returns:
            Словарь с результатами обновления
        """
        worksheet = self.get_worksheet(worksheet_name)
        
        try:
            # Преобразуем данные в формат, требуемый gspread
            batch_requests = []
            for item in batch_data:
                batch_requests.append({
                    'range': item['range'],
                    'values': item['values']
                })
            
            result = worksheet.batch_update(batch_requests)
            logger.info(f"Batch updated {len(batch_requests)} ranges in worksheet: {worksheet_name}")
            return result
        except Exception as e:
            logger.error(f"Error performing batch update in worksheet {worksheet_name}: {e}")
            raise
