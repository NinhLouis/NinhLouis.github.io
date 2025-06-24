# %%
from clv_calculator import *
from sender import *
from sqlalchemy import create_engine
from variables import INITIAL_YEAR, cols_to_save, col_to_round
import pandas as pd
import logging
import time
# Customer Potential
from lifetimes.utils import calibration_and_holdout_data
from lifetimes.utils import summary_data_from_transaction_data
from lifetimes import GammaGammaFitter
from lifetimes import BetaGeoFitter
import pyodbc
from sklearn.metrics import mean_squared_error, r2_score
 

current_time = datetime.now().strftime("%Y%m%d_%H%M")
logging.basicConfig(
    # filename=log_file,  # Log file location
    level=logging.INFO,  # Log level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

def load_data():

    # connect with dwh
    dwh = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                    'Server=;'
                    'port=;'
                    'Network Library=DBMSSOCN;'
                    'Database= dwh;'
                    'UID=;'
                    'PWD=;'
                    )
    prediction_data_df = pd.read_sql(f"""
               SELECT CompanyId as 'customer_id'
                , date 
                , sales as volume
                FROM sales 
                    """, dwh)
    logging.critical("read data from dwh..sales")

    return prediction_data_df
def export_to_sql(df,table_name):
    """
    Export a Pandas DataFrame to a SQL Server table.

    Parameters:
    - df (pd.DataFrame): The DataFrame to export.
    - table_name (str): Name of the destination table in SQL Server.

    Returns:
    - None
    """

    # Create a connection string
    connection_string = 'Driver={ODBC Driver 17 for SQL Server};' \
                        'Server=;' \
                        'port=;' \
                        'Network Library=DBMSSOCN;' \
                        'Database=dwh;' \
                        'UID=;' \
                        'PWD=;'

    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')

    try:
        # Export DataFrame to SQL Server table
        df.to_sql(table_name, engine, index=False, if_exists='append', schema='dbo')

        logging.critical(f"DataFrame successfully exported to SQL Server table '{table_name}'.")
    
    except Exception as e:
        logging.error(f"Error exporting DataFrame to SQL Server: {str(e)}")
        
def delete_rows_from_sql():
    """
    Delete rows from a SQL Server table where predicting_period is larger than today.
    Returns:
    - None
    """

    # Create a connection string
    connection_string = 'Driver={ODBC Driver 17 for SQL Server};' \
                        'Server=;' \
                        'port=;' \
                        'Network Library=DBMSSOCN;' \
                        'Database=dwh;' \
                        'UID=;' \
                        'PWD=;'

    # Establish a connection
    conn = pyodbc.connect(connection_string)

    try:
        # Create a cursor
        cursor = conn.cursor()

        # Get today's date
        today_date = datetime.now().strftime('%Y-%m-%d')

        # Construct the DELETE statement
        delete_query = f"DELETE FROM CP_CustomerLifetimeValue WHERE predicting_period > '{today_date}'"

        # Execute the DELETE statement
        cursor.execute(delete_query)

        # Commit the changes
        conn.commit()

        logging.critical(f"Rows successfully deleted from SQL Server table '{table_name}'.")
    
    except Exception as e:
        logging.error(f"Error deleting rows from SQL Server: {str(e)}")

    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

def historical_p_alive(transactions_df_ini, bgf, ggf):
# 2 - Probability alive & Average volume
    # Initialize the new table to store the results                 
    p_alive_df = pd.DataFrame(columns=cols_to_save)

# Set the initial time_value to the last Sunday of 2021
    time_value = last_sunday_of_year(INITIAL_YEAR)

 # Loop until the desired end date
    now = pd.Timestamp.now()

    while time_value <= now:
        # Transform raw transaction data into a pandas dataframe
        start_time = time.time()
        new_transaction_df = summary_data_from_transaction_data(transactions_df_ini,
                                                            customer_id_col='customer_id',
                                                            datetime_col='enddate',
                                                            monetary_value_col='volume',
                                                            observation_period_end= time_value,
                                                            freq='W')
        # logging.warning(f'Running time summary_data_from_transaction_data: {time.time() - start_time} seconds')
        start_time = time.time()
        new_transaction_df = new_transaction_df.reset_index()
        
        # Add the observation date to the dataframe
        new_transaction_df["observation_date"] = time_value
        new_transaction_df["predicting_period"] = ""
        # Calculate CLV 
        new_transaction_df = calculate_clv(bgf, ggf, new_transaction_df)
        # logging.warning(f'Running time calculate_clv: {time.time() - start_time} seconds')
        start_time = time.time()
        
        # Set value to 0 if frequency is 0, recency is 0, and T is greater than 4 (new customer and never come back/1-time buyer)
        new_transaction_df = update_transaction_df(new_transaction_df)
        
        # Append the results to the new table
        new_data_df  = new_transaction_df[cols_to_save].copy()
        p_alive_df = pd.concat([new_data_df, p_alive_df], ignore_index=True)

        # Increment time_value by one week
        time_value += timedelta(weeks=1)
    
    # Remark date from Sunday to Monday
    p_alive_df['predicting_period'] = p_alive_df['observation_date'] + timedelta(days= 1)
    
    p_alive_df[col_to_round] = p_alive_df[col_to_round].round(5)
    
    return p_alive_df[cols_to_save]

def second_step_bgf(df,bgf,ggf):
    transactions_df_ini = df.copy()
    now = datetime.now()
    this_monday = pd.to_datetime(now - timedelta(days=now.weekday())).date()  # tranforming to date only data
    last_sunday = this_monday - timedelta(days= 1)

    # Transform raw transaction data into a pandas dataframe
    transaction_df = summary_data_from_transaction_data(transactions_df_ini,
                                                            customer_id_col='customer_id',
                                                            datetime_col='enddate',
                                                            monetary_value_col='volume',
                                                            observation_period_end= last_sunday,
                                                            freq='W')
    transaction_df = transaction_df.reset_index()
    transaction_df['observation_date'] = last_sunday

    p_alive_4W_df = pd.DataFrame()
    # looping and calculating p(alive) in next 12 weeks (3M)
    for i in range(1,13):
        transaction_df['predicting_period'] = this_monday + timedelta(weeks=i) 
        if i==1:
            for time in [1, 3, 6]:
                column_name = f'CLV_{time}M'
                # for ggf is to predict from cut off date, so no need to predict futre
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
             # Calculate the average volume value of each company id
            transaction_df['avg_volume'] = ggf.conditional_expected_average_profit(
                transaction_df['frequency'],
                transaction_df['monetary_value']
            ) 
        
        # for p alive is to predict the following 12 weeks
        transaction_df['p_alive'] = bgf.conditional_probability_alive( # round to 5
            frequency= transaction_df['frequency'],
            recency= transaction_df['recency'],
            T= transaction_df['T'] + i
        )
        
        # Set p_alive to 0 if frequency is 0, recency is 0, and T is greater than 4
        transaction_df.loc[(transaction_df['frequency'] == 0) & (transaction_df['recency'] == 0) & (transaction_df['T'] + i > 4), 'p_alive'] = 0
        
        # Append a copy of the p_live_df table to the results table
        new_data_df = transaction_df.copy()
        p_alive_4W_df = pd.concat([new_data_df, p_alive_4W_df], ignore_index=True)
    
    p_alive_4W_df[col_to_round] = p_alive_4W_df[col_to_round].round(5)
    
    return p_alive_4W_df[cols_to_save]

def main_bgf(df,):

    transactions_df_ini = df.copy()
    now = datetime.now() - timedelta(days=1)
    training_enddate = now - timedelta(weeks=4) # keep X weeks as testing
    
    now = now.strftime('%Y-%m-%d')
    training_enddate = training_enddate.strftime('%Y-%m-%d')
    # Time unit: Week (W)
    rfm_cal_holdout = calibration_and_holdout_data(transactions= transactions_df_ini,
                                                customer_id_col='customer_id', 
                                                datetime_col='enddate',
                                                monetary_value_col= 'volume',
                                                freq='W', # week = time unit
                                                calibration_period_end= training_enddate,   #split the date here
                                                observation_period_end= now ) # need to take 3 weeks data
    rfm_cal_holdout.columns = ['frequency', 'recency', 'T', 'monetary_value',
       'frequency_test', 'monetary_value_test', 'duration_test']
    ## BG-NBD model ##
    bgf = BetaGeoFitter(penalizer_coef= 0.0)

    ## fitting of BG-NBD model
    bgf.fit(frequency= rfm_cal_holdout['frequency'],
            recency= rfm_cal_holdout['recency'], 
            T= rfm_cal_holdout['T'])


    ## Gamma Gamma Models
    # Transform transactions data to RFM shape 
    transactions_df = rfm_cal_holdout.copy()
    transactions_df = transactions_df.reset_index()
    ## Filter only returning purchasers
    returning_customers = transactions_df[transactions_df['frequency']>0]

    ## Fitting the model
    ggf = GammaGammaFitter(penalizer_coef = 0.0001)
        
    ggf.fit(returning_customers['frequency'],
            returning_customers['monetary_value'])
    ## parameters
    # ggf_parameters = ggf.summary
    
    return transactions_df, bgf,ggf

def evaluate_models(transactions_df, bgf, ggf):
    transactions_df['n_transations_test_pred'] = bgf.predict(t= 4, # move this to variables? weeks
                                                    frequency= transactions_df['frequency'], 
                                                    recency= transactions_df['recency'], 
                                                    T= transactions_df['T'])

    # Calculate the RMSE
    RMSE_bgf = mean_squared_error(y_true=transactions_df["monetary_value_test"],
                                y_pred=transactions_df["n_transations_test_pred"],
                            squared= False)
    bgf_mean_real = transactions_df["monetary_value_test"].mean()
    bgf_mean_predict = transactions_df["n_transations_test_pred"].mean()
    diff_bgf = bgf_mean_predict/bgf_mean_real-1
    
    normalized_RMSE_bgf = RMSE_bgf/(transactions_df['monetary_value_test'].max() - transactions_df['monetary_value_test'].min())
    # delta_bgf = (transactions_df['n_transations_test_pred'].sum()/transactions_df['monetary_value_test'].sum()) -1
    
    # Calcuate the ggf RMSE
    expected_average_volum_pred = ggf.conditional_expected_average_profit(
            transactions_df['frequency'],
            transactions_df['monetary_value']
        )
    # Calculate the RMSE
    RMSE_ggf = mean_squared_error(y_true= transactions_df["monetary_value"],
                                y_pred= expected_average_volum_pred,
                                squared= False)
    mean_ggf_real = transactions_df["monetary_value"].mean()
    mean_ggf_predict= expected_average_volum_pred.mean()
    diff_ggf = mean_ggf_predict/mean_ggf_real -1

    normalized_RMSE_ggf = RMSE_ggf/(transactions_df["monetary_value"].max() - transactions_df["monetary_value"].min())
    
    Ecaluation_matrix = {'RMSE_bgf':normalized_RMSE_bgf, 'RMSE_ggf':normalized_RMSE_ggf, 'MAE_bgf':diff_bgf, 'MAE_ggf': diff_ggf}
    return Ecaluation_matrix


if __name__ == '__main__':
    df = load_data()
    
    # model fitting
    transactions_df, bgf, ggf = main_bgf(df)
    # Generate historical data
    # p_alive_history_df = historical_p_alive(df, bgf, ggf)

    # Predict future data
    p_alive_4W_df= second_step_bgf(df, bgf, ggf)

    Ecaluation_matrix = evaluate_models(transactions_df, bgf, ggf)
    # export_to_sql(p_alive_history_df, 'CP_CustomerLifetimeValue')
    
    # delete the prediction data from last week
    delete_rows_from_sql()
    export_to_sql(p_alive_4W_df, 'CP_CustomerLifetimeValue')
    
    send_deployment_mail('test', Ecaluation_matrix)
    # p_alive_history_df.to_csv('p_alive_history_df.csv')
    # p_alive_4W_df.to_csv('p_alive_4W_df.csv')
# %%
