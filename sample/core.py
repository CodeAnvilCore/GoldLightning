from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.webdriver import ChromiumDriver
from selenium.webdriver.safari.webdriver import WebDriver
from selenium.webdriver.firefox.webdriver import WebDriver as FirefoxDriver
from selenium.webdriver.ie.webdriver import WebDriver as InternetExplorerDriver
import pandas as pd
import numpy as np
import time
from datetime import datetime as dt
import os
import sys
import warnings

class html_table:
    def __init__(self, table_element:WebElement):
        # html header tag name for loose type check
        self.html_table_header_tag_name = 'thead'
        # html cell tag name for unpacking cells from header row
        self.html_header_cell_tag_name = 'th'
        # html tag name for row
        self.html_row_cell_tag_name = 'tr'
        # html tag name for cell
        self.html_cell_tag_name = 'td'
        # header
        self.header = None
        # Check is table TODO: weak assertion; fix.
        assert('table' in table_element.get_attribute('class'))
        self.table_element = table_element
        # Get header
        self.get_header()
        # Get rows
        self.get_rows()
    def get_header(self):
        # Get header dict
        header_html =\
            self.table_element\
                .find_elements(by      = By.TAG_NAME
                               , value = self.html_table_header_tag_name)
        # Assume one header
        header_length = len(header_html)
        if header_length == 1:
            header_row = header_html[0]
            # Get cell elements
            header_row_cells =\
                header_row.find_elements(by      = By.TAG_NAME
                                         , value = self.html_header_cell_tag_name)
        elif header_length < 1:
            raise ValueError('Zero headers found')
        elif header_length > 1:
            raise ValueError('Multiple headers found; header information ambigious')
        # Unpack column headers
        self.header = []
        for cell in header_row_cells:
            self.header.append(cell.text)
        return self
    def get_rows(self):
        # Check header available
        assert(not self.header is None)
        # Perform row orientated comprehension because inhomogenous row lengths 
        row_list = []
        row_element_list =\
            self.table_element\
                .find_elements(by      = By.TAG_NAME
                               , value =  self.html_row_cell_tag_name)
        # log the row lengths so that anomalous rows can be filtered
        log_row_length = []
        # For each row in table (body)
        for row_element in row_element_list:
            cell_elements =\
                row_element.find_elements(by      = By.TAG_NAME
                                          , value = self.html_cell_tag_name)
            col_count = 0
            row = []
            row_length = len(cell_elements)
            # For each cell, append to row
            while col_count < row_length:
                cell = cell_elements[col_count]
                row.append(cell.text)
                col_count += 1
            row_list.append(row)
            log_row_length.append(row_length)
        # Remove any rows that are less than maximum length and warn
        max_row_length = np.max(np.array(log_row_length))
        filtered_row_list = []
        warning_row_list  = []
        for r in row_list:
            if len(r) == max_row_length:
                filtered_row_list.append(r)
            elif len(r) > 0:
                warning_row_list.append(r)
        if len(warning_row_list) > 0:
            warn_str = 'The following rows were dropped due to inhomogenous row length:\n'
            for wr in warning_row_list:
                warn_str += str(wr) + '\n'
            warnings.warn(warn_str)
        self.data_dict =\
            pd.DataFrame(data      = filtered_row_list
                         , columns = self.header)
        return self
    def to_pandas(self):
        if not self.data_dict is None:
            return pd.DataFrame.from_dict(data = self.data_dict)
        else:
            err_str = 'No data found; check source that'\
                      + ' generated the below table element:\n'\
                      + str(self.table_element)
            raise LookupError(err_str)
        
class yf_symbol:
    def __init__(self
                 , symbol:str
                 , time_range:str
                 , webdriver_name = 'Firefox'):
        # Valid time-ranges; aligned with yahoo finance options
        valid_time_dict = {'one_year'    : '1_Y'
                           , 'five_year' : '5_Y'
                           , 'maximum'   : 'MAX'}
        valid_webdriver = ['Chrome'
                           , 'Firefox']
        # Selenium webdriver mapping
        self.webdriver_map = {'Chrome'     : ChromiumDriver
                              , 'Safari'   : WebDriver
                              , 'Firefox'  : FirefoxDriver
                              , 'IE'       : InternetExplorerDriver}
        # Input type check
        assert(all([isinstance(symbol, str)
                    , isinstance(time_range, str)
                    , isinstance(webdriver_name, str)]))
        # Check time_range input aligned with yahoo finance options
        assert(time_range in valid_time_dict.keys())
        # Check Selenium manager is configured in local environment
        selenium_manager_path = os.getenv('SE_MANAGER_PATH')
        if selenium_manager_path is None:
            # Attempt to configure
            python_exe = sys.executable
            python_script_path_frags = python_exe.split('\\')[:-1]\
                                       + ['Scripts'
                                          , 'selenium-manager.exe']
            selenium_manager_path = '\\'.join(python_script_path_frags)
            warn_str = "Environment variable 'SE_MANAGER_PATH' not found;"\
                        + 'Selenium may fail to initialise\n'\
                        + "Attempting to configure 'SE_MANAGER_PATH' as:\n"\
                        + f'{selenium_manager_path}'
            os.environ['SE_MANAGER_PATH'] = selenium_manager_path
        """
        # Valid selenium webdrivers (browsers): TODO: expand support
        for other browsers.
        """
        if not (webdriver_name in valid_webdriver):
            err_str = f'Support for:\n {webdriver_name}\nIs not currently supported.'
            raise NotImplementedError(err_str)
        else:
            self.webdriver_name = webdriver_name
        # Inits
        self.driver = None
        self.symbol = symbol
        self.html_tag_with_error   = 'span'
        self.html_tag_button       = 'button'
        self.html_table_class_name = 'table'
        self.symbol_not_found_text  = f"Symbols similar to '{self.symbol.lower()}'"
        self.yf_schema              = {'Date'       : np.datetime64
                                       , 'Open'     : np.float32
                                       , 'High'     : np.float32
                                       , 'Low'      : np.float32
                                       , 'Close'    : np.float32
                                       , 'Adj Close': np.float32
                                       , 'Volume'   : np.int32}
        self.symbol_data_raw_df     = None
        self.formatted_symbol_df    = None
        """
        # Inits - yahoo page meta static info
        # Date range button meta
        """
        self.button_wait_time = 1
        self.table_wait_time  = 10
        self.yahoo_attrib_ref =\
            'data-ylk'
        self.date_range_button_yahoo_attrib_ref =\
            'elmt:menu;itc:1;elm:input;sec:qsp-historical;slk:date-select;subsec:calendar'
        self.time_range_button_value_ref =\
            'value'
        self.yahoo_time_range =\
            valid_time_dict[time_range]
        self.yahoo_datetime_column_ref = 'Date'
        self.yahoo_datetime_format     = '%b %d, %Y'
        # Build yahoo finance url associated with user symbol
        self.url = f'https://finance.yahoo.com/quote/{self.symbol}/history/'
        self.create_session()\
            .navigate_page()\
            .get_symbol_table()\
            .get_coerced_data()
    def create_session(self):
        self.driver = self.webdriver_map[self.webdriver_name]()
        return self
    def navigate_page(self):
        def __find_button_and_click__(button_attrib
                                      , button_attrib_value):
            # Get list of buttons for current page state
            button_elements =\
                self.driver\
                    .find_elements(by      = By.TAG_NAME
                                   , value = self.html_tag_button)
            target_button = None
            for button in button_elements:
                button_yahoo_attrib =\
                    button.get_attribute(button_attrib)
                if button_yahoo_attrib == button_attrib_value:
                    # Button found!
                    target_button = button
            if not target_button is None:
                # If target button found; click it
                target_button.click()
            else:
                # Failed navigation; close browser
                self.driver.close()
                err_str = 'Unable to locate button with (attribute, value):\n'\
                           + f'({button_attrib}, {button_attrib_value}\n'\
                           + 'In in url:\n'\
                           + self.url  
                raise RuntimeError(err_str)
            # Wait for date-range dialogue box to open
            time.sleep(self.button_wait_time)
        # Check that driver is loaded
        assert(not self.driver is None)
        # Navigate to yahoo finance page associated with user symbol
        self.driver.get(self.url)
        # Check that yahoo has data for user symbol
        page_err_elements =\
            self.driver\
                .find_elements(by      = By.TAG_NAME
                               , value = self.html_tag_with_error)
        symbol_not_found_check =\
            [self.symbol_not_found_text in element.text
             for element in page_err_elements]
        if any(symbol_not_found_check):
            # Bad request; close browser
            self.driver.close()
            err_str = 'No Yahoo Finance data found for user supplied'\
                      + f'symbol:\n{self.symbol}\n'\
                      + 'Attempted URL:\n'\
                      + self.url\
                      + '.'
            raise ValueError(err_str)
        # If yahoo has page for symbol then assume okay to proceed
        else:
            # Find date range button and click
            __find_button_and_click__(self.yahoo_attrib_ref
                                      , self.date_range_button_yahoo_attrib_ref)
            """
            Find date-range option button associated with user supplied input
            and click
            """
            __find_button_and_click__(self.time_range_button_value_ref
                                      , self.yahoo_time_range)
            # Wait for table to load
            time.sleep(self.table_wait_time)
            return self
    def get_symbol_table(self):
        tbl_list =\
            self.driver\
                .find_elements(by      = By.CLASS_NAME
                               , value = self.html_table_class_name)
        # Assume time-series data is in first table. TODO: fix bullshit logic
        symbol_html_table_element = tbl_list[0]
        self.symbol_data_raw_df =\
            html_table(table_element = symbol_html_table_element)\
                .to_pandas()
        # Once data is read, close driver
        self.driver.close()
        return self
    def get_coerced_data(self):
        def __coerce_column__(raw_df
                              , col
                              , col_type):
            out_df = raw_df
            try:
                out_df[col] =\
                    out_df[col].str.replace(',', '')\
                               .astype(col_type)
                if col_type in [np.float32
                                , np.float64]:
                    out_df[col] =\
                        out_df[col].apply(lambda x: round(x, 2))
                return out_df
            except TypeError as e:
                err_str = f'Failed to cast column:\n'\
                            + col\
                            + '\nto type:\n'\
                            + f'{col_type}'
                raise TypeError(err_str) from e
        # Check object in correct state
        process_conditions = [not self.symbol_data_raw_df is None
                              , self.formatted_symbol_df is None]
        if all(process_conditions):
            """
            Get fact cols; date-time column will be handled later as
            a special case
            """
            fact_schema = {}
            for k, v in self.yf_schema.items():
                if not k == self.yahoo_datetime_column_ref:
                    fact_schema[k] = v
            # Build coerced data-frame                    
            self.formatted_symbol_df = self.symbol_data_raw_df
            # Process fact columns
            for col, col_type in fact_schema.items():
                self.formatted_symbol_df =\
                    __coerce_column__(self.formatted_symbol_df
                                      , col
                                      , col_type)
            # Process dim column (date-time special case)
            date_time_col =\
                self.formatted_symbol_df[self.yahoo_datetime_column_ref]
            date_time_col =\
                pd.to_datetime(arg        = date_time_col
                               , format   = self.yahoo_datetime_format
                               , dayfirst = True)\
                    .dt.date
            self.formatted_symbol_df[self.yahoo_datetime_column_ref] =\
                date_time_col
            return self
        else:
            err_str = f'Cannot coerce; No data found for:\n{self.symbol}\n'\
                      + 'Raw data:\n'\
                      + str(self.symbol_data_raw_df.head())
            raise RuntimeError(err_str)
    def symbol_to_csv(self
                      , out_csv_path:str):
        if os.path.isfile(out_csv_path):
            err_str = f'File:\n'\
                      + out_csv_path\
                      + '\nAlready exists; try clearing old data'  
            raise FileExistsError(err_str)
        elif not self.formatted_symbol_df is None:
            self.formatted_symbol_df.to_csv(out_csv_path)
            return self
    def symbol_to_pandas(self):
        if not self.formatted_symbol_df is None:
            return self.formatted_symbol_df
        else:
            err_str = f'No data available for symbol:\n'\
                      + self.symbol\
                      +'\nCheck that source url is valid and raw data has been read'  
            raise RuntimeError(err_str)