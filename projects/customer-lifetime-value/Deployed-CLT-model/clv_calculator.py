# %%
import pandas as pd
from datetime import datetime, timedelta

def calculate_clv(bgf, ggf, transaction_df):
    
    # Calculate probability of being alive for each customer at time T
    transaction_df["p_alive"] = bgf.conditional_probability_alive(
        frequency=transaction_df['frequency'],
        recency=transaction_df['recency'],
        T=transaction_df['T']
    )
   
    # Calculate the average volume value of each company id
    transaction_df['avg_volume'] = ggf.conditional_expected_average_profit(
        transaction_df['frequency'],
        transaction_df['monetary_value']
    )

    

    # Calculate CLV for 1 month, 3 months, and 6 months
    for time in [1, 3, 6]:
        column_name = f'CLV_{time}M'
        transaction_df[column_name] = ggf.customer_lifetime_value(
            bgf,
            transaction_df['frequency'],
            transaction_df['recency'],
            transaction_df['T'],
            transaction_df['monetary_value'],
            time=time,
            discount_rate=0,
            freq='W'
        )
    return transaction_df

def update_transaction_df(transaction_df):
    """imput the value to 0, 
    if frequency is 0, recency is 0, and T is greater than 4 (new customer and never come back/1-time buyer)

    """
    condition = (transaction_df['frequency'] == 0) & (transaction_df['recency'] == 0)

    # Update 'p_alive', 'avg_volume', 'xx 1W_repurchase' when T > 4
    transaction_df.loc[condition & (transaction_df['T'] > 4), ['p_alive', 'avg_volume']] = 0

    # Update 'CLV_1M', 'CLV_3M', 'CLV_6M' when T > 2
    transaction_df.loc[condition & (transaction_df['T'] > 2), ['CLV_1M', 'CLV_3M', 'CLV_6M']] = 0
    
    return transaction_df
# Example usage:
# Assuming you have a DataFrame named 'transaction_df'
# update_transaction_df(transaction_df)
def last_sunday_of_year(year):
    last_day = datetime(year, 12, 31)
    weekday = last_day.weekday()
    days_until_sunday = (weekday - 6) % 7
    last_sunday = last_day - timedelta(days=days_until_sunday)

    # Ensure last_sunday is a Timestamp object
    last_sunday = pd.to_datetime(last_sunday)

    # Format the Timestamp object as a string
    formatted_last_sunday = last_sunday #.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_last_sunday
if __name__ == '__main__':
    # Example usage for the year 2021
    year_2021_last_sunday = last_sunday_of_year(2021)
    year_2021_last_sunday

