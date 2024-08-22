import datetime as dt
import pandas as pd

class IndexModel:
    def __init__(self) -> None:
        
        # Converting into Time Series Data
        self.df = pd.read_csv('data_sources/stock_prices.csv')
        self.df['Date'] = pd.to_datetime(self.df['Date'], dayfirst=True)
        self.df.set_index('Date', inplace=True)
        
        # Identifying the top 3 stocks for each month
        self.monthly_first_day = self.df.resample('M').last()
        self.monthly_first_day.index = self.monthly_first_day.index + pd.Timedelta(days=1)
        self.monthly_first_day['Top_3_Stocks'] = self.monthly_first_day.apply(self.top_3_stocks, axis=1)
        
        # Expanding the top 3 stocks information to all days of the month
        self.df['YearMonth'] = self.df.index.to_period('M')
        self.monthly_first_day['YearMonth'] = self.monthly_first_day.index.to_period('M')
        self.df = self.df.reset_index()
        self.df = self.df.merge(self.monthly_first_day[['Top_3_Stocks', 'YearMonth']], on='YearMonth', how='left')
        self.df.set_index('Date', inplace=True)
        self.df.drop(columns=['YearMonth'], inplace=True)
    
    @staticmethod
    def top_3_stocks(row):
        # Identifying the top 3 stocks for a specific row
        return row.nlargest(3).index.tolist()

    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        
        # Converting start_date and end_date to pandas Timestamp
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        
        # Initializing the index series
        self.index_series = pd.Series(index=self.df.index, dtype=float)
        
        # Setting the initial index value to 100 on the specified start date.
        self.index_series.loc[start_date] = 100
        
        # Calculating the index values
        start_loc = self.df.index.get_loc(start_date)
        end_loc = self.df.index.get_loc(end_date)
        
        for i in range(start_loc, min(end_loc + 1, len(self.df))):
            if i == start_loc:
                continue
            
            top_3_stocks = self.df.iloc[i-1]['Top_3_Stocks']
            
            weighted_sum_today = (
                self.df.iloc[i][top_3_stocks[0]] * 0.50 + 
                self.df.iloc[i][top_3_stocks[1]] * 0.25 + 
                self.df.iloc[i][top_3_stocks[2]] * 0.25
            )
            weighted_sum_yesterday = (
                self.df.iloc[i - 1][top_3_stocks[0]] * 0.50 + 
                self.df.iloc[i - 1][top_3_stocks[1]] * 0.25 + 
                self.df.iloc[i - 1][top_3_stocks[2]] * 0.25
            )
            
            self.index_series.iloc[i] = self.index_series.iloc[i - 1] * (weighted_sum_today / weighted_sum_yesterday)
        
        # Removing any NaN values before the start date, if they exist
        self.index_series = self.index_series.dropna()
        
        # Filtering the index series to only include data up to the end date
        self.index_series = self.index_series.loc[:end_date]

    def export_values(self, file_name: str) -> None:
        if self.index_series is not None:
            formatted_index_df = self.index_series.to_frame(name='Index')
            formatted_index_df.index = formatted_index_df.index.strftime('%d/%m/%Y')
            formatted_index_df.to_csv(file_name, index=True)        
        else:
            raise ValueError("Index has not been calculated. Please call calc_index_level first.")